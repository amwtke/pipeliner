package io.github.amwtke.pipeliner.runtime.local;

import io.github.amwtke.pipeliner.api.Chunk;
import io.github.amwtke.pipeliner.api.Split;
import io.github.amwtke.pipeliner.core.engine.RuntimeConfig;
import io.github.amwtke.pipeliner.core.graph.ExecMode;
import io.github.amwtke.pipeliner.core.plan.ExecutionPlan;
import io.github.amwtke.pipeliner.core.plan.PlanPipeline;
import io.github.amwtke.pipeliner.core.plan.PlanRun;
import io.github.amwtke.pipeliner.core.store.RoundRecord;
import io.github.amwtke.pipeliner.core.util.ChunkIds;
import io.github.amwtke.pipeliner.runtime.Services;
import io.github.amwtke.pipeliner.runtime.context.SourceContextImpl;
import io.github.amwtke.pipeliner.runtime.process.ChunkProcessor;
import io.github.amwtke.pipeliner.runtime.process.ChunkResult;
import io.github.amwtke.pipeliner.runtime.process.Events;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CancellationException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import lombok.extern.slf4j.Slf4j;

/**
 * Executes a whole INLINE (single thread, depth-first) or LOCAL (thread per pipeline, bounded queues) run inside this pod.
 * Chunk ids and sink chunk ids equal those of cluster mode. Reports via journal events carrying the run attempt.
 */
@Slf4j
public final class LocalRunExecutor implements Runnable {
    private static final Object EOS = new Object();

    private final Services s;
    private final RuntimeConfig cfg;
    private final ExecutionPlan plan;
    private final RoundRecord round;
    private final PlanRun run;
    private final int attempt;
    private final Map<String, ChunkProcessor> processors = new HashMap<>();
    private final List<Thread> threads = new ArrayList<>();
    private final AtomicReference<Throwable> failure = new AtomicReference<>();
    private volatile String failedPipeline;
    private volatile boolean cancelled;
    private Thread main;

    public LocalRunExecutor(Services s, ExecutionPlan plan, RoundRecord round, String runId, int attempt) {
        this.s = s;
        this.cfg = s.cfg;
        this.plan = plan;
        this.round = round;
        this.run = plan.run(runId);
        this.attempt = attempt;
        for (String pid : run.getPipelineIds())
            processors.put(pid, new ChunkProcessor(s, plan, round.getRoundId(), round.getParams(), pid));
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

    @Override public void run() {
        try {
            if (run.getMode() == ExecMode.INLINE) runInline(); else runThreaded();
            if (cancelled) return;
            s.plane.appendJournal(round.getRoundId(), Events.localRun(true, round.getRoundId(), run.getRunId(), null, s.podId(), attempt, null));
        } catch (Throwable t) {
            if (cancelled || t instanceof CancellationException || t instanceof InterruptedException && cancelled) return;
            log.error("local run {}/{} failed: {}", round.getRoundId(), run.getRunId(), t.toString());
            try {
                s.plane.appendJournal(round.getRoundId(), Events.localRun(false, round.getRoundId(), run.getRunId(), failedPipeline,
                        s.podId(), attempt, Events.describe(t)));
            } catch (RuntimeException e) { log.error("could not report local run failure", e); }
        } finally {
            s.plane.releaseThreadResources();
        }
    }

    // ---- INLINE ----

    private void runInline() throws Exception {
        for (String pid : run.getPipelineIds()) {
            if (!plan.pipeline(pid).isSource()) continue;
            int i = 0;
            for (Split sp : enumerate(pid)) {
                Split split = sp.withId(ChunkIds.source(i++));
                ChunkResult r = processChunk(pid, split, null);
                for (ChunkResult.EdgeChunk ec : r.outputs) pushInline(ec);
            }
        }
    }

    private void pushInline(ChunkResult.EdgeChunk ec) throws Exception {
        ChunkResult r = processChunk(ec.channel.getToPipeline(), null, ec.chunk);
        for (ChunkResult.EdgeChunk next : r.outputs) pushInline(next);
    }

    // ---- LOCAL ----

    private void runThreaded() throws Exception {
        Map<String, BlockingQueue<Object>> queues = new HashMap<>();
        for (String pid : run.getPipelineIds())
            for (io.github.amwtke.pipeliner.core.plan.PlanChannel c : plan.outputsOf(pid))
                queues.put(c.getEdgeId(), new ArrayBlockingQueue<>(Math.max(1, cfg.getLocalQueueCapacity())));
        for (String pid : run.getPipelineIds()) {
            Thread t = new Thread(() -> pipelineLoop(pid, queues), "pg-local-" + round.getRoundId() + "-" + run.getRunId() + "-" + pid);
            t.setDaemon(true);
            synchronized (threads) { threads.add(t); }
            t.start();
        }
        List<Thread> copy;
        synchronized (threads) { copy = new ArrayList<>(threads); }
        for (Thread t : copy) t.join();
        if (cancelled) throw new CancellationException();
        Throwable f = failure.get();
        if (f != null) throw f instanceof Exception ? (Exception) f : new RuntimeException(f);
    }

    private void pipelineLoop(String pid, Map<String, BlockingQueue<Object>> queues) {
        try {
            PlanPipeline pp = plan.pipeline(pid);
            if (pp.isSource()) {
                int i = 0;
                for (Split sp : enumerate(pid)) {
                    ChunkResult r = processChunk(pid, sp.withId(ChunkIds.source(i++)), null);
                    emitAll(r, queues);
                }
            } else {
                List<BlockingQueue<Object>> in = new ArrayList<>();
                for (String e : pp.getInputEdgeIds()) in.add(queues.get(e));
                int open = in.size();
                boolean[] closed = new boolean[in.size()];
                while (open > 0) {
                    boolean idle = true;
                    for (int i = 0; i < in.size(); i++) {
                        if (closed[i]) continue;
                        Object o = in.get(i).poll(20, TimeUnit.MILLISECONDS);
                        if (o == null) continue;
                        idle = false;
                        if (o == EOS) { closed[i] = true; open--; continue; }
                        emitAll(processChunk(pid, null, (Chunk) o), queues);
                    }
                    if (idle && cancelled) throw new CancellationException();
                }
            }
            for (io.github.amwtke.pipeliner.core.plan.PlanChannel c : plan.outputsOf(pid)) put(queues.get(c.getEdgeId()), EOS);
        } catch (Throwable t) {
            if (!cancelled && failure.compareAndSet(null, t)) {
                failedPipeline = pid;
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
        while (!q.offer(o, 100, TimeUnit.MILLISECONDS)) if (cancelled) throw new CancellationException();
    }

    // ---- shared ----

    private List<Split> enumerate(String pid) throws Exception {
        io.github.amwtke.pipeliner.core.graph.PipelineSpec spec = plan.getGraph().getPipelines().get(pid);
        List<Split> splits = spec.getSource().enumerate(new SourceContextImpl(round.getRoundId(), round.getParams(), s.dataSources));
        return splits == null ? new ArrayList<Split>() : splits;
    }

    /** Process one chunk (a source split or a stream chunk) with retries, then journal it. */
    private ChunkResult processChunk(String pid, Split split, Chunk streamChunk) throws Exception {
        awaitNotPaused();
        ChunkProcessor p = processors.get(pid);
        Exception last = null;
        for (int a = 1; a <= cfg.getMaxAttempts(); a++) {
            try {
                Chunk in = split != null ? p.readSource(split) : streamChunk;
                ChunkResult r = p.process(in);
                String edge = split != null ? "split" : streamChunk.getHeaders().get(Chunk.H_EDGE);
                s.plane.appendJournal(round.getRoundId(), Events.done(round.getRoundId(), run.getRunId(), pid, in.getChunkId(), edge, s.podId(), r, attempt));
                s.metrics.chunkProcessed(round.getRoundId(), run.getRunId(), pid, r.rowsIn, 0, r.costMillis);
                return r;
            } catch (InterruptedException e) {
                throw e;
            } catch (Exception e) {
                if (cancelled) throw new CancellationException();
                last = e;
                if (a < cfg.getMaxAttempts()) {
                    s.metrics.chunkRetried(round.getRoundId(), pid);
                    Thread.sleep(cfg.getRetryBackoffMillis() * a);
                }
            }
        }
        failedPipeline = pid;
        throw last;
    }

    private void awaitNotPaused() throws InterruptedException {
        while (!cancelled && s.plane.isPaused(round.getRoundId())) Thread.sleep(200);
        if (cancelled) throw new CancellationException();
    }
}
