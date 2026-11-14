package io.github.amwtke.pipeliner.core.engine;

import io.github.amwtke.pipeliner.core.graph.PipelineGraph;
import io.github.amwtke.pipeliner.core.plan.ExecutionPlan;
import io.github.amwtke.pipeliner.core.store.ChunkQuery;
import io.github.amwtke.pipeliner.core.store.ChunkRecord;
import io.github.amwtke.pipeliner.core.store.RoundStatus;
import io.github.amwtke.pipeliner.core.store.RoundView;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/** Public engine API: register graphs, submit rounds, operate on them. */
public interface PipelinerEngine {
    /** Plans immediately (fail fast); keeps graph and plan. */
    void registerGraph(PipelineGraph graph);
    ExecutionPlan plan(String graphName);
    Collection<String> graphNames();
    /** Idempotent per (graph, bizKey); returns roundId; any pod may call. */
    String submit(String graphName, String bizKey, Map<String, String> params);
    void pause(String roundId);
    void resume(String roundId);
    void cancel(String roundId);
    /** Requeues dead letters, run/pipelines back to RUNNING, round back to RUNNING. */
    void retryRun(String roundId, String runId);
    /** Resets run and all downstream runs (and their gates), cleaning their channels. */
    void rerunFrom(String roundId, String runId);
    /** Drops DLQ, chunks SKIPPED, failed pipelines to RUNNING, round marked incomplete. */
    void skipDeadLetters(String roundId, String runId);
    void signalGate(String roundId, String gateId, String signalKey, String payload);
    void openGate(String roundId, String gateId, String operator);
    RoundView view(String roundId);
    List<ChunkRecord> chunks(ChunkQuery q);
    /** "e1".., "split:&lt;p&gt;", "dlq:&lt;p&gt;" to XLEN for running runs. */
    Map<String, Long> channelLengths(String roundId);
    /** Same as {@link #channelLengths(String)} for an already loaded view (saves a second loadRound). */
    default Map<String, Long> channelLengths(io.github.amwtke.pipeliner.core.store.RoundView view) {
        return view == null ? new java.util.LinkedHashMap<>() : channelLengths(view.getRound().getRoundId());
    }
    RoundStatus awaitRound(String roundId, long timeoutMillis) throws InterruptedException;
    /** Pipelines currently instantiated (runner/source/sink resolved and opened) on this pod. */
    List<LiveInstance> liveInstances();
    /**
     * One idempotent master pass: renews/acquires the leader lease (new epoch on (re)gain), executes queued admin commands and ticks every
     * active round. Driven by the pod's own thread in LOCAL mode, by the caller in EXTERNAL mode (any pod may be called; only the lease holder
     * advances, the others return skipped "not leader"). Overlapping calls on one pod return skipped "tick in progress"; with the master
     * disabled it returns skipped "master disabled". Admin commands wait for the next tick, so their latency is up to one trigger interval.
     */
    TickResult tick();
    boolean isLeader();
    String podId();
    void start();
    void stop();
}
