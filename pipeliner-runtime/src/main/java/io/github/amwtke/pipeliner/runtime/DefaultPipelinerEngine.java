package io.github.amwtke.pipeliner.runtime;

import io.github.amwtke.pipeliner.api.ComponentFactory;
import io.github.amwtke.pipeliner.api.DataSourceRegistry;
import io.github.amwtke.pipeliner.api.PipelinerException;
import io.github.amwtke.pipeliner.core.codec.ChunkCodec;
import io.github.amwtke.pipeliner.core.dataplane.DataPlane;
import io.github.amwtke.pipeliner.core.dataplane.Keys;
import io.github.amwtke.pipeliner.core.engine.LiveInstance;
import io.github.amwtke.pipeliner.core.engine.PipelinerEngine;
import io.github.amwtke.pipeliner.core.engine.PipelinerMetrics;
import io.github.amwtke.pipeliner.core.engine.RuntimeConfig;
import io.github.amwtke.pipeliner.core.graph.ExecMode;
import io.github.amwtke.pipeliner.core.graph.PipelineGraph;
import io.github.amwtke.pipeliner.core.plan.ExecutionPlan;
import io.github.amwtke.pipeliner.core.plan.PlanGate;
import io.github.amwtke.pipeliner.core.plan.PlanPipeline;
import io.github.amwtke.pipeliner.core.plan.PlanRun;
import io.github.amwtke.pipeliner.core.store.ChunkQuery;
import io.github.amwtke.pipeliner.core.store.ChunkRecord;
import io.github.amwtke.pipeliner.core.store.ControlPlaneStore;
import io.github.amwtke.pipeliner.core.store.DuplicateRoundIdException;
import io.github.amwtke.pipeliner.core.store.GateRecord;
import io.github.amwtke.pipeliner.core.store.GateStatus;
import io.github.amwtke.pipeliner.core.store.PipelineRecord;
import io.github.amwtke.pipeliner.core.store.PipelineStatus;
import io.github.amwtke.pipeliner.core.store.RoundRecord;
import io.github.amwtke.pipeliner.core.store.RoundStatus;
import io.github.amwtke.pipeliner.core.store.RoundView;
import io.github.amwtke.pipeliner.core.store.RunRecord;
import io.github.amwtke.pipeliner.core.store.RunStatus;
import io.github.amwtke.pipeliner.core.util.ChunkIds;
import io.github.amwtke.pipeliner.core.util.ReflectionComponentFactory;
import io.github.amwtke.pipeliner.runtime.master.MasterLoop;
import io.github.amwtke.pipeliner.runtime.worker.WorkerAgent;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;
import lombok.extern.slf4j.Slf4j;

/** Default engine: master loop + worker agent per pod over a shared control plane store and data plane. */
@Slf4j
public class DefaultPipelinerEngine implements PipelinerEngine {
    private final RuntimeConfig cfg;
    private final Services s;
    private final AdminClient admin;
    private volatile MasterLoop master;
    private WorkerAgent worker;
    private boolean started;

    public DefaultPipelinerEngine(RuntimeConfig cfg, ControlPlaneStore store, DataPlane plane, DataSourceRegistry dataSources, PipelinerMetrics metrics) {
        this(cfg, store, plane, dataSources, metrics, new ReflectionComponentFactory());
    }

    /** The factory creates type-based runners (see PipelineGraph.PipelineBuilder.row(Class)). */
    public DefaultPipelinerEngine(RuntimeConfig cfg, ControlPlaneStore store, DataPlane plane, DataSourceRegistry dataSources, PipelinerMetrics metrics,
                                  ComponentFactory factory) {
        if (cfg == null || cfg.getPodId() == null || cfg.getPodId().isEmpty()) throw new PipelinerException("RuntimeConfig.podId is required");
        this.cfg = cfg;
        this.s = new Services(cfg, store, plane, dataSources, metrics, new GraphRegistry(cfg.getPlanOptions()), factory);
        this.admin = new AdminClient(s);
    }

    @Override public void registerGraph(PipelineGraph graph) { announce(s.registry.register(graph)); }

    private void announce(ExecutionPlan plan) {
        List<String> gateIds = new ArrayList<>();
        for (PlanGate g : plan.getGates()) gateIds.add(g.getGateId());
        s.metrics.graphRegistered(plan.getGraphName(), new ArrayList<>(plan.getPipelines().keySet()), gateIds);
    }

    /** Registers an already built plan (its graph must be live); mainly for tests and custom planners. */
    public void registerPlan(ExecutionPlan plan) { s.registry.registerPlan(plan); announce(plan); }

    @Override public ExecutionPlan plan(String graphName) { return s.registry.require(graphName); }

    @Override public Collection<String> graphNames() { return new ArrayList<>(s.registry.names()); }

    @Override public String submit(String graphName, String bizKey, Map<String, String> params) {
        ExecutionPlan plan = s.registry.require(graphName);
        RoundRecord existing = s.store.findRoundByBizKey(graphName, bizKey);
        if (existing != null) {
            if (!existing.getStatus().terminal()) s.plane.addActiveRound(existing.getRoundId());   // active set lives in Redis only
            return existing.getRoundId();
        }
        Map<String, String> p = new LinkedHashMap<>();
        if (params != null) p.putAll(params);
        p.put("bizKey", bizKey);
        RoundRecord created = null;
        for (int attempt = 1; created == null; attempt++) {
            try {
                created = createRound(plan, graphName, bizKey, p, newRoundId(graphName));
            } catch (DuplicateRoundIdException e) {   // an id collision (not expected with 48 random bits): retry with a fresh id
                if (attempt >= 5) throw e;
                log.warn("round id collision for graph {}, retrying: {}", graphName, e.getMessage());
            }
        }
        s.plane.addActiveRound(created.getRoundId());
        log.info("round {} submitted for graph {} bizKey {}", created.getRoundId(), graphName, bizKey);
        return created.getRoundId();
    }

    /** {@code <graph>-<yyyyMMdd-HHmmss>-<12 hex>}: at most {@link ControlPlaneStore#MAX_ROUND_ID_LENGTH} characters for a graph name within its limit. */
    static String newRoundId(String graphName) {
        return graphName + "-" + new SimpleDateFormat("yyyyMMdd-HHmmss", Locale.ROOT).format(new Date()) + "-"
                + UUID.randomUUID().toString().replace("-", "").substring(0, 12);
    }

    private RoundRecord createRound(ExecutionPlan plan, String graphName, String bizKey, Map<String, String> p, String roundId) {
        long now = System.currentTimeMillis();
        RoundRecord round = RoundRecord.builder().roundId(roundId).graphName(graphName).bizKey(bizKey).planHash(plan.getHash())
                .planJson(planJson(plan)).params(p).status(RoundStatus.CREATED).createdAt(now).seq(s.plane.nextRoundSeq(s.store.maxRoundSeq())).build();
        List<RunRecord> runs = new ArrayList<>();
        for (PlanRun r : plan.getRuns())
            runs.add(RunRecord.builder().roundId(roundId).runId(r.getRunId()).stage(r.getStage()).mode(r.getMode()).status(RunStatus.PENDING).build());
        List<GateRecord> gates = new ArrayList<>();
        for (PlanGate g : plan.getGates())
            gates.add(GateRecord.builder().roundId(roundId).gateId(g.getGateId()).type(g.getType()).beforeRuns(new ArrayList<>(g.getBeforeRuns()))
                    .status(GateStatus.CLOSED).context(new HashMap<String, String>()).build());
        List<PipelineRecord> pipes = new ArrayList<>();
        for (PlanPipeline pp : plan.getPipelines().values())
            pipes.add(PipelineRecord.builder().roundId(roundId).pipelineId(pp.getPipelineId()).runId(pp.getRunId())
                    .status(PipelineStatus.PENDING).parallelism(pp.getParallelism()).build());
        return s.store.createRound(round, runs, gates, pipes);
    }

    private static String planJson(ExecutionPlan plan) {
        try { return plan.toJson(); } catch (RuntimeException e) { return "{}"; }
    }

    // Operator commands run on the leader inside its tick (never racing it); signalGate is an idempotent data write.
    @Override public void pause(String roundId) { admin.execute("pause", roundId, null, null); }
    @Override public void resume(String roundId) { admin.execute("resume", roundId, null, null); }
    @Override public void cancel(String roundId) { admin.execute("cancel", roundId, null, null); }
    @Override public void retryRun(String roundId, String runId) { admin.execute("retryRun", roundId, runId, null); }
    @Override public void rerunFrom(String roundId, String runId) { admin.execute("rerunFrom", roundId, runId, null); }
    @Override public void skipDeadLetters(String roundId, String runId) { admin.execute("skipDeadLetters", roundId, runId, null); }
    @Override public boolean signalGate(String roundId, String gateId, String signalKey, String payload) {
        if (signalKey == null || signalKey.isEmpty() || signalKey.length() > ControlPlaneStore.MAX_SIGNAL_KEY_LENGTH)
            throw new IllegalArgumentException("signal key must be 1.." + ControlPlaneStore.MAX_SIGNAL_KEY_LENGTH + " characters, got "
                    + (signalKey == null ? "null" : String.valueOf(signalKey.length())));
        if (isStaleSignal(roundId, gateId, signalKey)) return false;
        s.store.saveGateSignal(roundId, gateId, signalKey, payload);
        return true;
    }

    /** A key {@code ...@<attempt>} lower than the current run attempt of the pipeline it names (or of the gate's after-pipelines) is a late callback. */
    private boolean isStaleSignal(String roundId, String gateId, String signalKey) {
        int attempt = ChunkIds.attemptOfSignalKey(signalKey);
        if (attempt < 0) return false;
        try {
            RoundView v = s.store.loadRoundState(roundId);
            if (v == null) return false;
            ExecutionPlan plan = s.registry.find(v.getRound().getGraphName());
            if (plan == null) return false;
            java.util.List<String> pipelines = new ArrayList<>();
            String named = ChunkIds.pipelineOfSignalKey(signalKey);
            if (named != null && plan.pipeline(named) != null) pipelines.add(named);
            else for (PlanGate g : plan.getGates()) if (g.getGateId().equals(gateId)) pipelines.addAll(g.getAfterPipelines());
            for (String p : pipelines) {
                PlanPipeline pp = plan.pipeline(p);
                if (pp == null) continue;
                for (RunRecord r : v.getRuns())
                    if (r.getRunId().equals(pp.getRunId()) && attempt < r.getAttempt()) {
                        log.debug("signal {} for gate {}/{} rejected: attempt {} < current attempt {} of {}", signalKey, roundId, gateId, attempt, r.getAttempt(), p);
                        return true;
                    }
            }
        } catch (RuntimeException e) {
            log.debug("signal staleness check failed, accepting {}: {}", signalKey, e.toString());
        }
        return false;
    }
    @Override public void openGate(String roundId, String gateId, String operator) { admin.execute("openGate", roundId, gateId, operator, false); }
    @Override public void openGate(String roundId, String gateId, String operator, boolean force) { admin.execute("openGate", roundId, gateId, operator, force); }

    @Override public RoundView view(String roundId) { return s.store.loadRound(roundId); }

    @Override public List<ChunkRecord> chunks(ChunkQuery q) { return s.store.queryChunks(q); }

    @Override public Map<String, Long> channelLengths(String roundId) { return channelLengths(s.store.loadRound(roundId)); }

    @Override public Map<String, Long> channelLengths(RoundView v) {
        Map<String, Long> out = new LinkedHashMap<>();
        if (v == null) return out;
        String roundId = v.getRound().getRoundId();
        ExecutionPlan plan = s.registry.find(v.getRound().getGraphName());
        if (plan == null) return out;
        for (RunRecord r : v.getRuns()) {
            PlanRun pr = plan.run(r.getRunId());
            if (pr == null || pr.getMode() != ExecMode.CLUSTER || (r.getStatus() != RunStatus.RUNNING && r.getStatus() != RunStatus.FAILED)) continue;
            for (io.github.amwtke.pipeliner.core.plan.PlanChannel c : plan.getChannels())
                if (c.getRunId().equals(pr.getRunId())) out.put(c.getEdgeId(), s.plane.streamLength(Keys.channel(roundId, c.getEdgeId(), r.getAttempt())));
            for (String p : pr.getPipelineIds()) {
                if (plan.pipeline(p).isSource()) out.put("split:" + p, s.plane.streamLength(Keys.splitQueue(roundId, p, r.getAttempt())));
                out.put("dlq:" + p, s.plane.streamLength(Keys.dlq(roundId, p, r.getAttempt())));
            }
        }
        return out;
    }

    @Override public RoundStatus awaitRound(String roundId, long timeoutMillis) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMillis;
        while (true) {
            RoundRecord rr = s.store.loadRoundRecord(roundId);
            if (rr == null) throw new PipelinerException("unknown round: " + roundId);
            if (rr.getStatus().terminal() || System.currentTimeMillis() >= deadline) return rr.getStatus();
            Thread.sleep(Math.min(50, Math.max(1, deadline - System.currentTimeMillis())));
        }
    }

    @Override public List<LiveInstance> liveInstances() { return s.instances.snapshot(); }

    @Override public io.github.amwtke.pipeliner.core.engine.TickResult tick() {
        MasterLoop m;
        synchronized (this) { m = master; }
        if (!cfg.isMasterEnabled()) return io.github.amwtke.pipeliner.core.engine.TickResult.skipped(io.github.amwtke.pipeliner.core.engine.TickResult.MASTER_DISABLED, false, 0, 0);
        if (m == null) return io.github.amwtke.pipeliner.core.engine.TickResult.skipped("engine not started", false, 0, 0);
        return m.tick();
    }

    @Override public boolean isLeader() {
        MasterLoop m = master;   // read once: stop()/halt() null the field concurrently
        return m != null && m.isLeader();
    }

    @Override public String podId() { return cfg.getPodId(); }

    @Override public synchronized void start() {
        if (started) return;
        cfg.validate();   // fail fast on values that would hang or break the runtime
        s.store.init();   // may throw (store down): the engine is not started then and start() can be retried
        started = true;
        if (cfg.isWorkerEnabled()) { worker = new WorkerAgent(s); worker.start(); }
        if (cfg.isMasterEnabled()) { master = new MasterLoop(s); master.start(); }
        log.info("engine {} started (master={}, worker={})", cfg.getPodId(), cfg.isMasterEnabled(), cfg.isWorkerEnabled());
    }

    @Override public synchronized void stop() { shutdown(true); }

    /** Stops like a crash: threads end, leadership is NOT released (it expires by TTL), claims are left pending. */
    public synchronized void halt() { shutdown(false); }

    private void shutdown(boolean releaseLeadership) {
        if (!started) return;
        started = false;
        if (master != null) master.stop(releaseLeadership);
        if (worker != null) worker.stop();
        master = null;
        worker = null;
        log.info("engine {} stopped", cfg.getPodId());
    }
}
