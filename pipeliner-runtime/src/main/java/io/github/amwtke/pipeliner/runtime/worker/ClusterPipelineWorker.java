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
import io.github.amwtke.pipeliner.runtime.process.ChunkProcessor;
import io.github.amwtke.pipeliner.runtime.process.ChunkResult;
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
    private final ChunkProcessor processor;
    private final List<String> inputs = new ArrayList<>();
    private final Map<String, String> streamToEdge = new HashMap<>();
    private final String splitQueue;
    private volatile boolean stopRequested;
    private volatile boolean killed;
    private Thread thread;

    public ClusterPipelineWorker(Services s, ExecutionPlan plan, RoundRecord round, String pipelineId, String consumer, ClaimRenewer renewer) {
        this.s = s;
        this.cfg = s.cfg;
        this.roundId = round.getRoundId();
        this.consumer = consumer;
        this.renewer = renewer;
        this.pp = plan.pipeline(pipelineId);
        this.processor = new ChunkProcessor(s, plan, roundId, round.getParams(), pipelineId);
        this.splitQueue = pp.isSource() ? Keys.splitQueue(roundId, pipelineId) : null;
        if (pp.isSource()) {
            inputs.add(splitQueue);
        } else {
            for (String edgeId : pp.getInputEdgeIds()) {
                String key = Keys.channel(roundId, edgeId);
                inputs.add(key);
                streamToEdge.put(key, edgeId);
            }
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

    public boolean alive() { return thread != null && thread.isAlive(); }

    @Override public void run() {
        long lastClaim = 0;
        try {
            while (!stopRequested) {
                try {
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
                } catch (RuntimeException e) {
                    if (killed || stopRequested) return;
                    if (isNoGroup(e)) {
                        log.debug("worker {}: stream gone ({})", consumer, e.getMessage());
                        sleepQuietly(500);
                    } else {
                        log.warn("worker {} loop error: {}", consumer, e.toString());
                        sleepQuietly(200);
                    }
                }
            }
        } finally {
            s.plane.releaseThreadResources();
        }
    }

    private void handle(StreamEntry entry) throws InterruptedException {
        String inputEdge = streamToEdge.containsKey(entry.getStreamKey()) ? streamToEdge.get(entry.getStreamKey()) : "split";
        if (entry.getDeliveryCount() > cfg.getMaxDeliveries()) {   // poison chunk: it keeps killing its consumer
            deadLetter(entry, inputEdge, "exceeded max deliveries (" + entry.getDeliveryCount() + " > " + cfg.getMaxDeliveries() + ")");
            return;
        }
        String error = null;
        for (int attempt = 1; attempt <= cfg.getMaxAttempts(); attempt++) {
            try {
                Chunk in = load(entry);
                ChunkResult res = processor.process(in);
                commit(entry, inputEdge, in, res);
                return;
            } catch (InterruptedException ie) {
                throw ie;
            } catch (Throwable e) {   // Errors (e.g. StackOverflowError, NoClassDefFoundError) count as failed attempts too
                if (killed || stopRequested) throw new InterruptedException("worker stopping");
                if (isNoGroup(e)) { // the master deleted the (finished) round's streams: nothing left to process or dead-letter
                    log.debug("worker {}: stream {} gone while handling {}", consumer, entry.getStreamKey(), entry.getEntryId());
                    return;
                }
                error = Events.describe(e);
                log.warn("chunk {} attempt {}/{} failed in {}: {}", entry.getEntryId(), attempt, cfg.getMaxAttempts(), pp.getPipelineId(), error);
                if (e instanceof VirtualMachineError) {   // the JVM is unreliable: record the failure if possible, then die
                    try { deadLetter(entry, inputEdge, error); } catch (Throwable ignored) { /* best effort */ }
                    throw (VirtualMachineError) e;
                }
                if (attempt < cfg.getMaxAttempts()) {
                    s.metrics.chunkRetried(roundId, pp.getPipelineId());
                    Thread.sleep(cfg.getRetryBackoffMillis() * attempt);
                }
            }
        }
        deadLetter(entry, inputEdge, error);
    }

    private Chunk load(StreamEntry entry) throws Exception {
        if (pp.isSource()) return processor.readSource(ChunkCodec.decodeSplit(entry.getFields().get("s")));
        return ChunkCodec.decodeChunk(entry.getFields().get("c"));
    }

    private void commit(StreamEntry entry, String inputEdge, Chunk in, ChunkResult res) throws InterruptedException {
        List<OutputChunk> outputs = new ArrayList<>();
        List<String> outKeys = new ArrayList<>();
        for (ChunkResult.EdgeChunk ec : res.outputs) {
            String key = Keys.channel(roundId, ec.channel.getEdgeId());
            outputs.add(new OutputChunk(key, ChunkCodec.encodeChunk(ec.chunk)));
            if (!outKeys.contains(key)) outKeys.add(key);
        }
        CommitRequest req = CommitRequest.builder().roundId(roundId).inputStreamKey(entry.getStreamKey()).inputEntryId(entry.getEntryId())
                .highWatermark(cfg.getHighWatermark()).outputs(outputs)
                .event(Events.done(roundId, pp.getRunId(), pp.getPipelineId(), in.getChunkId(), inputEdge, consumer, res, 0)).build();
        while (true) {
            CommitResult r = s.plane.commit(req);
            if (r == CommitResult.OK) {
                s.metrics.chunkProcessed(roundId, pp.getRunId(), pp.getPipelineId(), res.rowsIn, totalOut(res), res.costMillis);
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
        s.plane.deadLetter(DeadLetterRequest.builder().roundId(roundId).pipelineId(pp.getPipelineId())
                .inputStreamKey(entry.getStreamKey()).inputEntryId(entry.getEntryId())
                .inputFields(new HashMap<>(entry.getFields())).error(error)
                .event(Events.dead(roundId, pp.getRunId(), pp.getPipelineId(), chunkId, inputEdge, consumer, error)).build());
        s.metrics.deadLetter(roundId, pp.getPipelineId());
        log.error("chunk {} of {} dead-lettered: {}", chunkId, pp.getPipelineId(), error);
    }

    private static long totalOut(ChunkResult r) {
        long n = 0;
        for (Long v : r.rowsOut.values()) n += v;
        return n;
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
