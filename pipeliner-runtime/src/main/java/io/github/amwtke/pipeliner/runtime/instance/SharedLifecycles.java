package io.github.amwtke.pipeliner.runtime.instance;

import io.github.amwtke.pipeliner.api.Lifecycle;
import io.github.amwtke.pipeliner.api.PipelineContext;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import lombok.extern.slf4j.Slf4j;

/**
 * Pod-wide reference counts of Lifecycle components, by object identity. Sources, sinks and instance-form runners are shared objects
 * that several PipelineInstances (rounds, run attempts, pipelines) may hold at once: open() runs on the first acquire and close() on the
 * last release. User code runs outside the monitor; concurrent acquirers wait for the one open, and an acquire during a close waits
 * for it to finish and then opens again.
 */
@Slf4j
final class SharedLifecycles {
    private static final class Ref {
        int refs = 1;
        final CompletableFuture<Void> ready = new CompletableFuture<>();
        CompletableFuture<Void> closing;
    }

    private final Map<Object, Ref> map = new IdentityHashMap<>();

    /** Takes a reference; opens the component when it is the first. Throws the open failure (nothing stays referenced then). */
    void acquire(Lifecycle c, PipelineContext ctx) throws Exception {
        while (true) {
            Ref r;
            CompletableFuture<Void> closing = null;
            boolean opener = false;
            synchronized (this) {
                r = map.get(c);
                if (r == null) { r = new Ref(); map.put(c, r); opener = true; }
                else if (r.closing != null) closing = r.closing;
                else r.refs++;
            }
            if (closing != null) { awaitQuietly(closing); continue; }
            if (opener) {
                try {
                    c.open(ctx);
                } catch (Throwable t) {
                    synchronized (this) { map.remove(c); }
                    r.ready.completeExceptionally(t);
                    throw t;
                }
                r.ready.complete(null);
                return;
            }
            try {
                r.ready.get();
                return;
            } catch (ExecutionException e) {   // the opener failed and removed the entry; it is not ours to close
                Throwable cause = e.getCause();
                if (cause instanceof Exception) throw (Exception) cause;
                if (cause instanceof Error) throw (Error) cause;
                throw e;
            } catch (InterruptedException ie) {
                release(c);
                throw ie;
            }
        }
    }

    /** Drops a reference; the last one closes the component (exceptions are logged). */
    void release(Lifecycle c) {
        Ref r;
        synchronized (this) {
            r = map.get(c);
            if (r == null || --r.refs > 0) return;
            r.closing = new CompletableFuture<>();
        }
        try {
            c.close();
        } catch (Throwable t) {
            log.warn("close of {} failed: {}", c.getClass().getName(), t.toString());
        } finally {
            synchronized (this) { map.remove(c); }
            r.closing.complete(null);
        }
    }

    private static void awaitQuietly(CompletableFuture<Void> f) throws InterruptedException {
        try { f.get(); } catch (ExecutionException ignored) { /* never completed exceptionally */ }
    }
}
