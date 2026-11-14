package io.github.amwtke.pipeliner.runtime.local;

import io.github.amwtke.pipeliner.api.Chunk;
import io.github.amwtke.pipeliner.api.Split;
import io.github.amwtke.pipeliner.core.engine.RuntimeConfig;
import io.github.amwtke.pipeliner.core.graph.ExecMode;
import io.github.amwtke.pipeliner.core.plan.ExecutionPlan;
import io.github.amwtke.pipeliner.core.plan.PlanPipeline;
import io.github.amwtke.pipeliner.core.plan.PlanRun;
import io.github.amwtke.pipeliner.core.store.RoundRecord;
import io.github.amwtke.pipeliner.core.store.ChunkEvent;
import io.github.amwtke.pipeliner.runtime.Services;
import io.github.amwtke.pipeliner.runtime.context.SourceContextImpl;
import io.github.amwtke.pipeliner.runtime.instance.PipelineInstance;
import io.github.amwtke.pipeliner.api.Row;
import io.github.amwtke.pipeliner.runtime.process.ChunkProcessor;
import io.github.amwtke.pipeliner.runtime.process.ChunkRetryLoop;
import io.github.amwtke.pipeliner.runtime.process.SplitIds;
import io.github.amwtke.pipeliner.runtime.process.ChunkResult;
import io.github.amwtke.pipeliner.runtime.process.Events;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import lombok.extern.slf4j.Slf4j;

/**
 * Executes a whole INLINE (single thread, depth-first) or LOCAL (thread per pipeline, bounded queues) run inside this pod.
 * Chunk ids and sink chunk ids equal those of cluster mode ({@link SplitIds}); chunks are retried by the same
 * {@link ChunkRetryLoop} as in cluster mode, and every attempt works on fresh copies of its input rows. Reports via journal events carrying
 * the run attempt; the terminal event (DONE or FAILED, never both) is appended with retries until it succeeds or the run is cancelled.
 * A cancelled executor appends nothing.
 */
@Slf4j
public final class LocalRunExecutor implements Runnable {
    private static final Object EOS = new Object();

    /** Internal control flow: this run is being cancelled, or a sibling pipeline thread already failed. Never reported as a failure. */
    private static final class StopSignal extends RuntimeException {
        private static final long serialVersionUID = 1L;
        StopSignal() { super("stopped", null, false, false); }
    }

    private final Services s;
    private final RuntimeConfig cfg;
    private final ExecutionPlan plan;
    private final RoundRecord round;
    private final PlanRun run;
    private final int attempt;
    private final Map<String, ChunkProcessor> processors = new HashMap<>();
    private final List<PipelineInstance.Lease> leases = new ArrayList<>();
    private final List<Thread> threads = new ArrayList<>();
    /** The first failure of any pipeline thread; once set every sibling stops. */
    private final AtomicReference<Throwable> failure = new AtomicReference<>();
    private volatile String failedPipeline;
    private volatile boolean cancelled;
    /** True once the terminal journal event was appended (the master will see it). */
    private volatile boolean reported;
    private Thread main;

    public LocalRunExecutor(Services s, ExecutionPlan plan, RoundRecord round, String runId, int attempt) {
        this.s = s;
        this.cfg = s.cfg;
        this.plan = plan;
        this.round = round;
        this.run = plan.run(runId);
        this.attempt = attempt;
    }

    public void start() {
        main = new Thread(this, "pg-local-" + round.getRoundId() + "-" + run.getRunId());
        main.setDaemon(true);
        main.start();
    }

    /** Stops the run without reporting anything (assignment moved or round ended). */
    public void cancel() {
        cancelled = true;
        if (main != null) main.interrupt();
        synchronized (threads) { for (Thread t : threads) t.interrupt(); }
    }

    /** True while the executor thread is running. */
    public boolean alive() { return main != null && main.isAlive(); }

    /** True when the terminal event reached the journal; a finished executor that never reported must be dropped so the run is re-driven. */
    public boolean reported() { return reported; }

    /** Waits up to {@code millis} for the executor (and with it every pipeline thread) to end; true when it ended. */
    public boolean join(long millis) {
        Thread t = main;
        if (t == null) return true;
        try { t.join(Math.max(1, millis)); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
        return !t.isAlive();
    }

    @Override public void run() {
        ChunkEvent terminal = null;
        try {
            openInstances();
            if (run.getMode() == ExecMode.INLINE) runInline(); else runThreaded();
            if (!cancelled) terminal = Events.localRun(true, round.getRoundId(), run.getRunId(), null, s.podId(), attempt, null);
        } catch (Throwable t) {
            if (!cancelled) {
                log.error("local run {}/{} failed: {}", round.getRoundId(), run.getRunId(), t.toString());
                terminal = Events.localRun(false, round.getRoundId(), run.getRunId(), failedPipeline, s.podId(), attempt, Events.describe(t));
            }
        } finally {
            for (PipelineInstance.Lease l : leases) {   // also on cancel / failure
                try { l.release(); } catch (Throwable t) { log.warn("release of a local run lease failed: {}", t.toString()); }
            }
            s.plane.releaseThreadResources();
        }
        if (terminal != null) report(terminal);
        s.plane.releaseThreadResources();
    }

    /** Appends the terminal event, retrying with backoff until it succeeds or the run is cancelled. */
    private void report(ChunkEvent ev) {
        long delay = Math.max(100, cfg.getRetryBackoffMillis());
        while (!cancelled) {
            try {
                s.plane.appendJournal(round.getRoundId(), ev);
                reported = true;
                return;
            } catch (Throwable t) {
                if (cancelled) return;
                log.warn("local run {}/{}: could not append {} (retrying): {}", round.getRoundId(), run.getRunId(), ev.getType(), t.toString());
                try { Thread.sleep(delay); } catch (InterruptedException e) { return; }
                delay = Math.min(delay * 2, 10_000);
            }
        }
    }

    /**
     * Instantiates and opens every pipeline of the run (released when the run ends). An open() failure is retried with backoff, at most
     * maxAttempts times per pipeline, as the cluster worker does; then the run fails.
     */
    private void openInstances() throws Exception {
        for (String pid : run.getPipelineIds()) {
            PipelineInstance.Lease l = null;
            for (int a = 1; l == null; a++) {
                try {
                    l = s.instances.acquire(plan, round.getRoundId(), round.getParams(), pid, attempt);
                } catch (Exception e) {
                    if (cancelled || a >= Math.max(1, cfg.getMaxAttempts())) throw e;
                    log.warn("local run {}/{}: cannot open pipeline {} (try {}/{}): {}", round.getRoundId(), run.getRunId(), pid, a, cfg.getMaxAttempts(), Events.describe(e));
                    s.metrics.chunkRetried(round.getRoundId(), pid);
                    Thread.sleep(cfg.getRetryBackoffMillis() * a);
                }
            }
            leases.add(l);
            processors.put(pid, new ChunkProcessor(s, plan, round.getRoundId(), round.getParams(), pid, l, attempt, round.getSeq()));
        }
    }

    // ---- INLINE ----

    private void runInline() throws Exception {
        for (String pid : run.getPipelineIds()) {
            if (!plan.pipeline(pid).isSource()) continue;
            try {
                for (Split split : SplitIds.assign(enumerate(pid))) {
                    ChunkResult r = processChunk(pid, split, null);
                    for (ChunkResult.EdgeChunk ec : r.outputs) pushInline(ec);
                }
            } catch (Throwable t) {
                if (failedPipeline == null) failedPipeline = pid;   // processChunk names the exact pipeline for chunk failures
                throw t;
            }
        }
    }

    private void pushInline(ChunkResult.EdgeChunk ec) throws Exception {
        ChunkResult r = processChunk(ec.channel.getToPipeline(), null, ec.chunk);
        for (ChunkResult.EdgeChunk next : r.outputs) pushInline(next);
    }

    // ---- LOCAL ----

    private boolean stopping() { return cancelled || failure.get() != null; }

    private void runThreaded() throws Exception {
        Map<String, BlockingQueue<Object>> queues = new HashMap<>();
        for (String pid : run.getPipelineIds())
            for (io.github.amwtke.pipeliner.core.plan.PlanChannel c : plan.outputsOf(pid))
                queues.put(c.getEdgeId(), new ArrayBlockingQueue<>(Math.max(1, cfg.getLocalQueueCapacity())));
        List<Thread> all = new ArrayList<>();
        for (String pid : run.getPipelineIds()) {
            Thread t = new Thread(() -> pipelineLoop(pid, queues), "pg-local-" + round.getRoundId() + "-" + run.getRunId() + "-" + pid);
            t.setDaemon(true);
            all.add(t);
        }
        synchronized (threads) { threads.addAll(all); }   // every thread is registered before any starts: a failure interrupts all siblings
        if (cancelled) throw new StopSignal();
        for (Thread t : all) t.start();
        for (Thread t : all) {   // keep waiting when cancelled: instances are released only after every pipeline thread is gone
            boolean interrupted = false;
            while (t.isAlive()) {
                try { t.join(); } catch (InterruptedException e) { interrupted = true; }
            }
            if (interrupted) Thread.currentThread().interrupt();
        }
        if (cancelled) throw new StopSignal();
        Throwable f = failure.get();
        if (f != null) rethrow(f);
    }

    private static void rethrow(Throwable f) throws Exception {
        if (f instanceof Exception) throw (Exception) f;
        if (f instanceof Error) throw (Error) f;
        throw new RuntimeException(f);
    }

    private void pipelineLoop(String pid, Map<String, BlockingQueue<Object>> queues) {
        try {
            PlanPipeline pp = plan.pipeline(pid);
            if (pp.isSource()) {
                for (Split split : SplitIds.assign(enumerate(pid))) emitAll(processChunk(pid, split, null), queues);
            } else {
                List<BlockingQueue<Object>> in = new ArrayList<>();
                for (String e : pp.getInputEdgeIds()) in.add(queues.get(e));
                int open = in.size();
                boolean[] closed = new boolean[in.size()];
                while (open > 0) {
                    if (stopping()) throw new StopSignal();
                    for (int i = 0; i < in.size(); i++) {
                        if (closed[i]) continue;
                        Object o = in.get(i).poll(20, TimeUnit.MILLISECONDS);
                        if (o == null) continue;
                        if (o == EOS) { closed[i] = true; open--; continue; }
                        emitAll(processChunk(pid, null, (Chunk) o), queues);
                    }
                }
            }
            for (io.github.amwtke.pipeliner.core.plan.PlanChannel c : plan.outputsOf(pid)) put(queues.get(c.getEdgeId()), EOS);
        } catch (Throwable t) {
            if (!cancelled && !(t instanceof StopSignal) && failure.compareAndSet(null, t)) {
                failedPipeline = failedPipeline != null ? failedPipeline : pid;
                synchronized (threads) { for (Thread o : threads) if (o != Thread.currentThread()) o.interrupt(); }
            }
        } finally {
            s.plane.releaseThreadResources();
        }
    }

    private void emitAll(ChunkResult r, Map<String, BlockingQueue<Object>> queues) throws InterruptedException {
        for (ChunkResult.EdgeChunk ec : r.outputs) put(queues.get(ec.channel.getEdgeId()), ec.chunk);
    }

    private void put(BlockingQueue<Object> q, Object o) throws InterruptedException {
        while (!q.offer(o, 100, TimeUnit.MILLISECONDS)) if (stopping()) throw new StopSignal();
    }

    // ---- shared ----

    private List<Split> enumerate(String pid) throws Exception {
        io.github.amwtke.pipeliner.core.graph.PipelineSpec spec = plan.getGraph().getPipelines().get(pid);
        return spec.getSource().enumerate(new SourceContextImpl(round.getRoundId(), round.getParams(), s.dataSources));
    }

    /** Process one chunk (a source split or a stream chunk) with the shared retry policy, then journal it. */
    private ChunkResult processChunk(String pid, Split split, Chunk streamChunk) throws Exception {
        awaitNotPaused();
        ChunkProcessor p = processors.get(pid);
        String label = split != null ? split.getSplitId() : streamChunk.getChunkId();
        ChunkRetryLoop.Outcome<ChunkResult> o = ChunkRetryLoop.run(cfg, s.metrics, round.getRoundId(), pid, label,
                this::stopping, t -> false, () -> {
                    Chunk in = split != null ? p.readSource(split) : freshCopy(streamChunk);   // every attempt gets its own rows
                    ChunkResult r = p.process(in);
                    if (cancelled) throw new StopSignal();   // a cancelled executor must not journal (the round's journal may be gone)
                    String edge = split != null ? "split" : streamChunk.getHeaders().get(Chunk.H_EDGE);
                    s.plane.appendJournal(round.getRoundId(), Events.done(round.getRoundId(), run.getRunId(), pid, in.getChunkId(), edge, s.podId(), r, attempt));
                    s.metrics.chunkProcessed(round.getRoundId(), run.getRunId(), pid, r.rowsIn, r.rowsOutTotal, r.costMillis);
                    return r;
                });
        if (o.ok()) return o.value;
        failedPipeline = pid;
        rethrow(o.failure);
        return null;
    }

    private static Chunk freshCopy(Chunk c) {
        List<Row> rows = new ArrayList<>(c.size());
        for (Row r : c.getRows()) rows.add(r.copy());
        return new Chunk(c.getChunkId(), rows, c.getHeaders());
    }

    private void awaitNotPaused() throws InterruptedException {
        while (!stopping() && s.plane.isPaused(round.getRoundId())) Thread.sleep(200);
        if (stopping()) throw new StopSignal();
    }
}
