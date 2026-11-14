package io.github.amwtke.pipeliner.runtime.worker;

import io.github.amwtke.pipeliner.runtime.Interrupts;
import io.github.amwtke.pipeliner.core.dataplane.DataPlane;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import lombok.extern.slf4j.Slf4j;

/** Periodically renews the stream claims of all in-flight chunks of this pod so they are not autoclaimed by others. */
@Slf4j
public final class ClaimRenewer {
    /** Registration token for one in-flight entry. */
    public static final class Claim {
        final String consumer, streamKey, entryId;
        Claim(String consumer, String streamKey, String entryId) { this.consumer = consumer; this.streamKey = streamKey; this.entryId = entryId; }
    }

    private final DataPlane plane;
    private final long intervalMillis;
    private final Set<Claim> claims = ConcurrentHashMap.newKeySet();
    private volatile boolean stopped;
    private Thread thread;

    public ClaimRenewer(DataPlane plane, long intervalMillis) { this.plane = plane; this.intervalMillis = intervalMillis; }

    public synchronized void start() {
        if (thread != null) return;
        stopped = false;
        thread = new Thread(this::loop, "pg-claim-renewer");
        thread.setDaemon(true);
        thread.start();
    }

    public synchronized void stop() {
        stopped = true;
        if (thread != null) thread.interrupt();
        thread = null;
    }

    public Claim register(String consumer, String streamKey, String entryId) {
        Claim c = new Claim(consumer, streamKey, entryId);
        claims.add(c);
        return c;
    }

    public void unregister(Claim c) { if (c != null) claims.remove(c); }

    private void loop() {
        while (!stopped) {
            try {
                Thread.sleep(intervalMillis);
                for (Claim c : claims) {
                    try {
                        if (!plane.renew(c.consumer, c.streamKey, c.entryId)) claims.remove(c);   // acked or deleted meanwhile
                    } catch (Throwable e) {   // incl. Errors: the renewer thread must never die (claims would expire and be re-delivered)
                        if (Interrupts.ownInterrupt(stopped, e)) return;
                        if (e instanceof RuntimeException && ClusterPipelineWorker.isNoGroup((RuntimeException) e)) claims.remove(c);   // the stream/group is gone (round cleaned up)
                        else log.warn("claim renew failed for {} {}: {}", c.streamKey, c.entryId, e.toString());
                    }
                }
            } catch (InterruptedException e) {
                return;
            } catch (Throwable e) {   // never let an unexpected Error end the loop
                if (Interrupts.ownInterrupt(stopped, e)) break;
                log.warn("claim renewer loop error: {}", e.toString());
            }
        }
        plane.releaseThreadResources();
    }
}
