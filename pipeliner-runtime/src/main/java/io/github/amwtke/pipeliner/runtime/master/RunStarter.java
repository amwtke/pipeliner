package io.github.amwtke.pipeliner.runtime.master;

import io.github.amwtke.pipeliner.api.Split;
import io.github.amwtke.pipeliner.core.dataplane.DesiredState;
import io.github.amwtke.pipeliner.core.dataplane.Keys;
import io.github.amwtke.pipeliner.runtime.RunKeys;
import io.github.amwtke.pipeliner.core.graph.ExecMode;
import io.github.amwtke.pipeliner.core.plan.ExecutionPlan;
import io.github.amwtke.pipeliner.core.plan.PlanChannel;
import io.github.amwtke.pipeliner.core.plan.PlanRun;
import io.github.amwtke.pipeliner.core.store.ChunkRecord;
import io.github.amwtke.pipeliner.core.store.ChunkStatus;
import io.github.amwtke.pipeliner.core.store.GateRecord;
import io.github.amwtke.pipeliner.core.store.GateStatus;
import io.github.amwtke.pipeliner.core.store.PipelineRecord;
import io.github.amwtke.pipeliner.core.store.PipelineStatus;
import io.github.amwtke.pipeliner.core.store.RoundRecord;
import io.github.amwtke.pipeliner.core.store.RunRecord;
import io.github.amwtke.pipeliner.core.store.RunStatus;
import io.github.amwtke.pipeliner.core.codec.ChunkCodec;
import io.github.amwtke.pipeliner.core.util.ChunkIds;
import io.github.amwtke.pipeliner.runtime.context.SourceContextImpl;
import io.github.amwtke.pipeliner.runtime.process.Events;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;

/** Starts runs whose gates are open, enumerates source splits, and reassigns local runs of dead pods. */
@Slf4j
final class RunStarter {
    private final MasterContext c;

    RunStarter(MasterContext c) { this.c = c; }

    void startRuns(RoundData d) {
        String roundId = d.roundId();
        Map<String, DesiredState> desired = null;
        Map<String, String> assign = null;
        for (PlanRun pr : d.plan.getRuns()) {
            RunRecord run = d.run(pr.getRunId());
            if (run == null) continue;
            if (run.getStatus() == RunStatus.PENDING) {
                // idempotent, re-driven from the store state: a run reset by rerunFrom must not keep workers of its old attempt
                if (desired == null) desired = c.s.plane.desiredState(roundId);
                for (String p : pr.getPipelineIds())
                    if (desired.containsKey(p)) { c.s.plane.setDesiredParallelism(roundId, p, 0, run.getAttempt(), c.epoch()); desired.remove(p); }
                if (pr.getMode() != ExecMode.CLUSTER) {
                    if (assign == null) assign = c.s.plane.localRunAssignments(roundId);
                    if (assign.remove(pr.getRunId()) != null) c.s.plane.clearLocalRun(roundId, pr.getRunId(), c.epoch());
                }
                if (!gatesOpen(d, pr)) continue;
                if (!c.s.store.casRunStatus(roundId, pr.getRunId(), RunStatus.PENDING, RunStatus.RUNNING, null, c.epoch())) continue;
                d.touch();
                log.info("round {} run {} ({}) started", roundId, pr.getRunId(), pr.getMode());
                if (pr.getMode() == ExecMode.CLUSTER) initCluster(d, pr); else assignLocal(d, pr, run);
            } else if (run.getStatus() == RunStatus.RUNNING && pr.getMode() != ExecMode.CLUSTER) {
                // crash recovery: the master may have died between "run RUNNING" and "pipelines RUNNING"
                for (String p : pr.getPipelineIds())
                    if (d.pipe(p).getStatus() == PipelineStatus.PENDING
                            && c.s.store.casPipelineStatus(roundId, p, PipelineStatus.PENDING, PipelineStatus.RUNNING, null, c.epoch())) d.touch();
            } else if (run.getStatus() == RunStatus.RUNNING) {
                // crash recovery: a previous master may have died half-way through starting this run
                boolean anyPending = false, anyDesiredMissing = false;
                if (desired == null) desired = c.s.plane.desiredState(roundId);
                for (String p : pr.getPipelineIds()) {
                    PipelineRecord rec = d.pipe(p);
                    if (rec.getStatus() == PipelineStatus.PENDING) anyPending = true;
                    if (rec.getStatus() == PipelineStatus.RUNNING && !attemptMatches(desired.get(p), run)) anyDesiredMissing = true;
                }
                if (anyPending) { initCluster(d, pr); d.touch(); }
                if (anyDesiredMissing) {
                    for (String p : pr.getPipelineIds())
                        if (d.pipe(p).getStatus() == PipelineStatus.RUNNING && !attemptMatches(desired.get(p), run) && run.getStatus() == RunStatus.RUNNING)
                            c.s.plane.setDesiredParallelism(roundId, p, d.pipe(p).getParallelism(), run.getAttempt(), c.epoch());
                }
            }
        }
    }

    private static boolean attemptMatches(DesiredState ds, RunRecord run) { return ds != null && ds.getAttempt() == run.getAttempt(); }

    private boolean gatesOpen(RoundData d, PlanRun pr) {
        for (String g : pr.getInGateIds()) {
            GateRecord gr = d.gate(g);
            if (gr == null || gr.getStatus() != GateStatus.OPEN) return false;
        }
        return true;
    }

    private void initCluster(RoundData d, PlanRun pr) {
        String roundId = d.roundId();
        int attempt = d.run(pr.getRunId()).getAttempt();
        // attempt n starts from a clean slate: drop the keys of every older attempt first (fenced, idempotent, re-driven on recovery)
        if (attempt > 0) c.s.plane.deleteKeys(roundId, RunKeys.ofAttemptsBefore(roundId, d.plan, pr, attempt), c.epoch());
        for (PlanChannel ch : d.plan.getChannels())
            if (ch.getRunId().equals(pr.getRunId())) c.s.plane.ensureGroup(roundId, Keys.channel(roundId, ch.getEdgeId(), attempt), c.epoch());
        for (String p : pr.getPipelineIds())
            if (d.plan.pipeline(p).isSource()) c.s.plane.ensureGroup(roundId, Keys.splitQueue(roundId, p, attempt), c.epoch());
        for (String p : pr.getPipelineIds()) {
            PipelineRecord rec = d.pipe(p);
            if (rec.getStatus() != PipelineStatus.PENDING) continue;
            if (c.s.store.casPipelineStatus(roundId, p, PipelineStatus.PENDING, PipelineStatus.RUNNING, null, c.epoch()))
                c.s.plane.setDesiredParallelism(roundId, p, rec.getParallelism(), attempt, c.epoch());
        }
    }

    private void assignLocal(RoundData d, PlanRun pr, RunRecord run) {
        String roundId = d.roundId();
        for (String p : pr.getPipelineIds())
            c.s.store.casPipelineStatus(roundId, p, PipelineStatus.PENDING, PipelineStatus.RUNNING, null, c.epoch());
        reassign(d, run);
    }

    private void reassign(RoundData d, RunRecord run) {
        String roundId = d.roundId();
        String pod = pickPod(d.round());
        if (pod == null) { log.warn("no live worker able to run local run {}/{}; will retry", roundId, run.getRunId()); return; }
        int attempt = run.getAttempt() + 1;
        if (!c.s.store.updateRunAssignment(roundId, run.getRunId(), pod, attempt, c.epoch())) return;
        c.s.plane.assignLocalRun(roundId, run.getRunId(), pod, attempt, c.epoch());
        d.touch();
        log.info("local run {}/{} assigned to {} (attempt {})", roundId, run.getRunId(), pod, attempt);
    }

    /** True if the pod's heartbeat says it has this round's graph registered with the same plan hash. */
    private boolean canExecute(String pod, RoundRecord round) {
        String hash = c.s.plane.workerInfo(pod).get("graph." + round.getGraphName());
        return round.getPlanHash() != null && round.getPlanHash().equals(hash);
    }

    /** Live worker able to execute the round's plan with the fewest assigned local runs; ties prefer this pod. Null if none. */
    private String pickPod(RoundRecord round) {
        Map<String, Long> live = c.s.plane.liveWorkers(c.s.cfg.getWorkerDeadMillis());
        Map<String, Integer> load = new HashMap<>();
        for (String pod : live.keySet()) if (canExecute(pod, round)) load.put(pod, 0);
        if (load.isEmpty()) return null;
        for (String r : c.s.plane.activeRounds())
            for (String a : c.s.plane.localRunAssignments(r).values()) {
                String pod = a.substring(0, a.lastIndexOf('|'));
                if (load.containsKey(pod)) load.put(pod, load.get(pod) + 1);
            }
        String best = null;
        for (String pod : new java.util.TreeSet<>(load.keySet()))
            if (best == null || load.get(pod) < load.get(best) || load.get(pod).equals(load.get(best)) && pod.equals(c.s.podId())) best = pod;
        return best;
    }

    /**
     * Local run watchdog: reassign when the assignee is unknown, stale or lacks the plan; otherwise make sure the data-plane
     * assign entry matches the store (a crash between the two writes leaves it missing).
     */
    void watchdog(RoundData d) {
        Map<String, Long> live = null;
        Map<String, String> assign = null;
        for (PlanRun pr : d.plan.getRuns()) {
            RunRecord run = d.run(pr.getRunId());
            if (run == null || run.getStatus() != RunStatus.RUNNING || pr.getMode() == ExecMode.CLUSTER || d.allPipelinesSucceeded(pr)) continue;
            if (live == null) live = c.s.plane.liveWorkers(c.s.cfg.getWorkerDeadMillis());
            String a = run.getAssignee();
            if (a == null || !live.containsKey(a) || !canExecute(a, d.round())) {
                log.warn("local run {}/{} assignee {} is not alive or cannot run the plan; reassigning", d.roundId(), run.getRunId(), a);
                reassign(d, run);
                continue;
            }
            if (assign == null) assign = c.s.plane.localRunAssignments(d.roundId());
            if (!(a + "|" + run.getAttempt()).equals(assign.get(run.getRunId()))) {
                log.warn("local run {}/{}: assign entry '{}' differs from the store; republishing", d.roundId(), run.getRunId(), assign.get(run.getRunId()));
                c.s.plane.assignLocalRun(d.roundId(), run.getRunId(), a, run.getAttempt(), c.epoch());
            }
        }
    }

    /** Enumerates splits of running CLUSTER source pipelines (idempotent: enqueueSplit, pending chunk records, enumDone). */
    void enumerate(RoundData d) {
        String roundId = d.roundId();
        for (PlanRun pr : d.plan.getRuns()) {
            RunRecord run = d.run(pr.getRunId());
            if (run == null || run.getStatus() != RunStatus.RUNNING || pr.getMode() != ExecMode.CLUSTER) continue;
            for (String p : pr.getPipelineIds()) {
                PipelineRecord rec = d.pipe(p);
                if (!d.plan.pipeline(p).isSource() || rec.getStatus() != PipelineStatus.RUNNING || rec.isEnumDone()) continue;
                try {
                    List<Split> raw = d.plan.getGraph().getPipelines().get(p).getSource()
                            .enumerate(new SourceContextImpl(roundId, d.round().getParams(), c.s.dataSources));
                    List<Split> splits = new ArrayList<>();
                    List<ChunkRecord> pending = new ArrayList<>();
                    int i = 0;
                    if (raw != null) for (Split sp : raw) {
                        Split s = sp.withId(ChunkIds.source(i++));
                        splits.add(s);
                        pending.add(ChunkRecord.builder().roundId(roundId).pipelineId(p).chunkId(s.getSplitId()).runId(pr.getRunId())
                                .status(ChunkStatus.PENDING).inputEdge("split").splitSpec(ChunkCodec.toJson(s.getSpec()))
                                .updatedAt(System.currentTimeMillis()).build());
                    }
                    for (Split s : splits) c.s.plane.enqueueSplit(roundId, p, run.getAttempt(), s, c.epoch());
                    if (!pending.isEmpty()) c.s.store.savePendingChunks(roundId, pending);
                    c.s.store.setPipelineEnumDone(roundId, p, true, c.epoch());
                    d.touch();
                    log.info("round {} pipeline {} enumerated {} splits", roundId, p, splits.size());
                } catch (Exception e) {
                    log.error("enumeration of {}/{} failed: {}", roundId, p, e.toString());
                    c.s.store.casPipelineStatus(roundId, p, PipelineStatus.RUNNING, PipelineStatus.FAILED, "enumerate failed: " + Events.describe(e), c.epoch());
                    d.touch();
                }
            }
        }
    }
}
