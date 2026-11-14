package io.github.amwtke.pipeliner.runtime.worker;

import io.github.amwtke.pipeliner.api.Chunk;
import io.github.amwtke.pipeliner.api.Split;
import io.github.amwtke.pipeliner.core.codec.ChunkCodec;
import io.github.amwtke.pipeliner.core.dataplane.CommitRequest;
import io.github.amwtke.pipeliner.core.dataplane.CommitResult;
import io.github.amwtke.pipeliner.core.dataplane.DeadLetterRequest;
import io.github.amwtke.pipeliner.core.dataplane.Keys;
import io.github.amwtke.pipeliner.core.dataplane.OutputChunk;
import io.github.amwtke.pipeliner.core.dataplane.StreamEntry;
import io.github.amwtke.pipeliner.core.engine.RuntimeConfig;
import io.github.amwtke.pipeliner.core.plan.ExecutionPlan;
import io.github.amwtke.pipeliner.core.plan.PlanChannel;
import io.github.amwtke.pipeliner.core.plan.PlanPipeline;
import io.github.amwtke.pipeliner.core.store.RoundRecord;
import io.github.amwtke.pipeliner.runtime.Services;
import io.github.amwtke.pipeliner.runtime.instance.PipelineInstance;
import io.github.amwtke.pipeliner.runtime.process.ChunkProcessor;
import io.github.amwtke.pipeliner.runtime.process.ChunkResult;
import io.github.amwtke.pipeliner.runtime.process.ChunkRetryLoop;
import io.github.amwtke.pipeliner.runtime.process.Events;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;

/** One consumer thread of a CLUSTER pipeline: read -> process -> sinks -> atomic commit (with backpressure and retries). */
@Slf4j
public final class ClusterPipelineWorker implements Runnable {
    private final Services s;
    private final RuntimeConfig cfg;
    private final String roundId;
    private final String consumer;
    private final ClaimRenewer renewer;
    private final PlanPipeline pp;
    private final ExecutionPlan plan;
    private final Map<String, String> params;
    private ChunkProcessor processor;
    private PipelineInstance.Lease lease;
    private final List<String> inputs = new ArrayList<>();
    private final Map<String, String> streamToEdge = new HashMap<>();
    private final String splitQueue;
    private final int attempt;
    private final long roundSeq;
    private volatile boolean stopRequested;
    private volatile boolean killed;
    private Thread thread;

    /** Works on attempt {@code attempt} of the run: its streams, DLQ and pipeline instance. */
    public ClusterPipelineWorker(Services s, ExecutionPlan plan, RoundRecord round, String pipelineId, int attempt, String consumer, ClaimRenewer renewer) {
        this.attempt = attempt;
        this.roundSeq = round.getSeq();
        this.s = s;
        this.cfg = s.cfg;
        this.roundId = round.getRoundId();
        this.consumer = consumer;
        this.renewer = renewer;
        this.pp = plan.pipeline(pipelineId);
        this.plan = plan;
        this.params = round.getParams();
        this.splitQueue = pp.isSource() ? Keys.splitQueue(roundId, pipelineId, attempt) : null;
        for (Map.Entry<String, String> in : io.github.amwtke.pipeliner.runtime.RunKeys.inputStreams(roundId, pp, attempt).entrySet()) {
            inputs.add(in.getKey());
            if (in.getValue() != null) streamToEdge.put(in.getKey(), in.getValue());
        }
    }

    public void start(String threadName) {
        thread = new Thread(this, threadName);
        thread.setDaemon(true);
        thread.start();
    }

    /** Finish the current chunk, then exit. */
    public void stop() { stopRequested = true; }

    /** Abandon the current chunk (entry stays pending and will be autoclaimed) and exit. */
    public void kill() {
        killed = true;
        stopRequested = true;
        if (thread != null) thread.interrupt();
    }

    public boolean stopRequested() { return stopRequested; }

    public int attempt() { return attempt; }

    public boolean alive() { return thread != null && thread.isAlive(); }

    /** Waits up to {@code millis} for the thread to end; true when it ended (or never started). */
    public boolean join(long millis) {
        Thread t = thread;
        if (t == null) return true;
        try { t.join(Math.max(1, millis)); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
        return !t.isAlive();
    }

    @Override public void run() {
        long lastClaim = 0;
        try {
            while (!stopRequested) {
                try {
                    if (!ensureInstance()) continue;
                    if (s.plane.isPaused(roundId) || (pp.isSource() && s.plane.globalThrottle())) {
                        Thread.sleep(500);
                        continue;
                    }
                    List<StreamEntry> entries = new ArrayList<>();
                    long now = System.currentTimeMillis();
                    if (now - lastClaim >= cfg.getAutoClaimIntervalMillis()) {
                        lastClaim = now;
                        for (String key : inputs) {
                            entries = s.plane.autoClaim(consumer, key, cfg.getClaimIdleMillis(), 1);
                            if (!entries.isEmpty()) break;
                        }
                    }
                    if (entries.isEmpty()) entries = s.plane.read(consumer, inputs, 1, cfg.getReadBlockMillis());
                    // register every entry immediately: it must not be autoclaimed while earlier entries of the batch are processed
                    List<ClaimRenewer.Claim> claims = new ArrayList<>();
                    for (StreamEntry e : entries) claims.add(renewer.register(consumer, e.getStreamKey(), e.getEntryId()));
                    try {
                        for (StreamEntry e : entries) handle(e);
                    } finally {
                        for (ClaimRenewer.Claim cl : claims) renewer.unregister(cl);
                    }
                } catch (InterruptedException e) {
                    return;
                } catch (Throwable e) {   // incl. Errors: a worker loop must not die silently (chunk failures are handled in handle())
                    if (killed || stopRequested) return;
                    if (e instanceof RuntimeException && isNoGroup((RuntimeException) e)) {
                        log.debug("worker {}: stream gone ({})", consumer, e.getMessage());
                        sleepQuietly(500);
                    } else {
                        log.warn("worker {} loop error: {}", consumer, e.toString());
                        sleepQuietly(200);
                    }
                }
            }
        } finally {
            if (lease != null) lease.release();   // last thread of the pipeline on this pod closes the instance
            s.plane.releaseThreadResources();
        }
    }

    /** Takes the pipeline instance (created and opened by the first thread). An open() failure is logged and retried later. */
    private boolean ensureInstance() throws InterruptedException {
        if (lease != null) return true;
        try {
            lease = s.instances.acquire(plan, roundId, params, pp.getPipelineId(), attempt);
            processor = new ChunkProcessor(s, plan, roundId, params, pp.getPipelineId(), lease, attempt, roundSeq);
            return true;
        } catch (Throwable t) {
            if (killed || stopRequested) return false;
            log.warn("worker {}: cannot open pipeline {} of round {}: {}", consumer, pp.getPipelineId(), roundId, Events.describe(t));
            Thread.sleep(cfg.getRetryBackoffMillis() * 2);
            return false;
        }
    }

    private void handle(StreamEntry entry) throws InterruptedException {
        String inputEdge = streamToEdge.containsKey(entry.getStreamKey()) ? streamToEdge.get(entry.getStreamKey()) : "split";
        if (entry.getDeliveryCount() > cfg.getMaxDeliveries()) {   // poison chunk: it keeps killing its consumer
            deadLetter(entry, inputEdge, "exceeded max deliveries (" + entry.getDeliveryCount() + " > " + cfg.getMaxDeliveries() + ")");
            return;
        }
        ChunkRetryLoop.Outcome<Void> o = ChunkRetryLoop.run(cfg, s.metrics, roundId, pp.getPipelineId(), entry.getEntryId(),
                () -> killed || stopRequested,
                e -> {
                    if (!isNoGroup(e)) return false;   // the master deleted the (finished) round's streams: nothing left to process or dead-letter
                    log.debug("worker {}: stream {} gone while handling {}", consumer, entry.getStreamKey(), entry.getEntryId());
                    return true;
                },
                () -> {
                    Chunk in = load(entry);
                    ChunkResult res = processor.process(in);
                    commit(entry, inputEdge, in, res);
                    return null;
                });
        if (o.abandoned) return;
        if (!o.ok()) deadLetter(entry, inputEdge, Events.describe(o.failure));
    }

    private Chunk load(StreamEntry entry) throws Exception {
        if (pp.isSource()) return processor.readSource(ChunkCodec.decodeSplit(entry.getFields().get("s")));
        return ChunkCodec.decodeChunk(entry.getFields().get("c"));
    }

    private void commit(StreamEntry entry, String inputEdge, Chunk in, ChunkResult res) throws InterruptedException {
        List<OutputChunk> outputs = new ArrayList<>();
        List<String> outKeys = new ArrayList<>();
        for (ChunkResult.EdgeChunk ec : res.outputs) {
            String key = Keys.channel(roundId, ec.channel.getEdgeId(), attempt);
            outputs.add(new OutputChunk(key, ChunkCodec.encodeChunk(ec.chunk)));
            if (!outKeys.contains(key)) outKeys.add(key);
        }
        CommitRequest req = CommitRequest.builder().roundId(roundId).inputStreamKey(entry.getStreamKey()).inputEntryId(entry.getEntryId())
                .highWatermark(cfg.getHighWatermark()).outputs(outputs).consumer(consumer)
                .event(Events.done(roundId, pp.getRunId(), pp.getPipelineId(), in.getChunkId(), inputEdge, consumer, res, attempt)).build();
        while (true) {
            CommitResult r = s.plane.commit(req);
            if (r == CommitResult.OK) {
                s.metrics.chunkProcessed(roundId, pp.getRunId(), pp.getPipelineId(), res.rowsIn, res.rowsOutTotal, res.costMillis);
                return;
            }
            if (r == CommitResult.CONFLICT) {
                log.debug("commit conflict for {} {} (already handled elsewhere); dropping", entry.getStreamKey(), entry.getEntryId());
                return;
            }
            waitForRoom(outKeys);
        }
    }

    /** BUSY: wait until every output stream is below the low watermark. The claim renewer keeps our claim alive. */
    private void waitForRoom(List<String> outKeys) throws InterruptedException {
        long t0 = System.currentTimeMillis();
        while (true) {
            if (killed || stopRequested) throw new InterruptedException("worker stopping");
            boolean room = true;
            for (String k : outKeys) if (s.plane.streamLength(k) >= cfg.getLowWatermark()) { room = false; break; }
            if (room) break;
            Thread.sleep(200);
        }
        s.metrics.busyWait(roundId, pp.getPipelineId(), System.currentTimeMillis() - t0);
    }

    private void deadLetter(StreamEntry entry, String inputEdge, String error) {
        String chunkId = entry.getEntryId();
        try { chunkId = pp.isSource() ? ChunkCodec.decodeSplit(entry.getFields().get("s")).getSplitId() : ChunkCodec.decodeChunk(entry.getFields().get("c")).getChunkId(); }
        catch (RuntimeException ignored) { /* keep entry id */ }
        boolean moved = s.plane.deadLetter(DeadLetterRequest.builder().roundId(roundId).pipelineId(pp.getPipelineId()).attempt(attempt)
                .inputStreamKey(entry.getStreamKey()).inputEntryId(entry.getEntryId())
                .inputFields(new HashMap<>(entry.getFields())).error(error).consumer(consumer)
                .event(Events.dead(roundId, pp.getRunId(), pp.getPipelineId(), chunkId, inputEdge, consumer, error, attempt)).build());
        if (moved) {
            s.metrics.deadLetter(roundId, pp.getPipelineId());
            log.error("chunk {} of {} dead-lettered: {}", chunkId, pp.getPipelineId(), error);
        } else {
            log.info("chunk {} of {} not dead-lettered: already committed or claimed elsewhere ({})", chunkId, pp.getPipelineId(), error);
        }
    }

    /** True when the exception (or a cause) is a Redis NOGROUP / missing-stream error. */
    static boolean isNoGroup(Throwable t) {
        for (int i = 0; t != null && i < 10; i++, t = t.getCause()) {
            String m = t.getMessage();
            if (m != null && m.contains("NOGROUP")) return true;
        }
        return false;
    }

    private static void sleepQuietly(long ms) {
        try { Thread.sleep(ms); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
    }
}
