package io.github.amwtke.pipeliner.runtime.master;

import io.github.amwtke.pipeliner.core.dataplane.Keys;
import io.github.amwtke.pipeliner.runtime.RunKeys;
import io.github.amwtke.pipeliner.core.graph.ExecMode;
import io.github.amwtke.pipeliner.core.plan.PlanChannel;
import io.github.amwtke.pipeliner.core.plan.PlanGate;
import io.github.amwtke.pipeliner.core.plan.PlanPipeline;
import io.github.amwtke.pipeliner.core.plan.PlanRun;
import io.github.amwtke.pipeliner.core.store.ChunkStatus;
import io.github.amwtke.pipeliner.core.store.GateRecord;
import io.github.amwtke.pipeliner.core.store.GateStatus;
import io.github.amwtke.pipeliner.core.store.PipelineRecord;
import io.github.amwtke.pipeliner.core.store.PipelineStatus;
import io.github.amwtke.pipeliner.core.store.RoundStatus;
import io.github.amwtke.pipeliner.core.store.RunRecord;
import io.github.amwtke.pipeliner.core.store.RunStatus;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;

/** Decides pipeline, run and round completion. */
@Slf4j
final class CompletionChecker {
    private final MasterContext c;
    private final JournalDrainer drainer;

    CompletionChecker(MasterContext c, JournalDrainer drainer) { this.c = c; this.drainer = drainer; }

    /**
     * Per RUNNING CLUSTER pipeline. Order matters: (inputs empty AND ready) -> drain the journal completely -> only then
     * decide from the DEAD count. Commits add outputs and the journal event atomically, so once the input streams are
     * observed empty every event is already in the journal; draining afterwards cannot miss one.
     */
    void checkPipelines(RoundData d) {
        String roundId = d.roundId();
        for (PlanRun pr : d.plan.getRuns()) {
            RunRecord run = d.run(pr.getRunId());
            if (run == null || run.getStatus() != RunStatus.RUNNING || pr.getMode() != ExecMode.CLUSTER) continue;
            Map<String, PipelineStatus> status = new HashMap<>();
            for (String p : pr.getPipelineIds()) status.put(p, d.pipe(p).getStatus());
            for (String p : pr.getPipelineIds()) {
                if (status.get(p) != PipelineStatus.RUNNING) continue;
                PlanPipeline pp = d.plan.pipeline(p);
                boolean ready = pp.isSource() ? d.pipe(p).isEnumDone() : upstreamDone(pp, status);
                if (!ready || !inputsEmpty(roundId, pp, run.getAttempt())) continue;
                if (drainer.drain(roundId, d.plan).localApplied) d.touch();   // local-run events changed run/pipeline records
                // a source is done only when every split chunk record left PENDING (DONE, or DEAD/SKIPPED): never on an empty queue alone
                if (pp.isSource() && c.s.store.countChunks(roundId, p, ChunkStatus.PENDING) > 0) {
                    if (dataPlaneLost(roundId, p, run.getAttempt())) failLost(d, pr, run, p, status);
                    continue;
                }
                boolean failed = c.s.store.countChunks(roundId, p, ChunkStatus.DEAD) > 0;
                PipelineStatus target = failed ? PipelineStatus.FAILED : PipelineStatus.SUCCEEDED;
                String err = failed ? "dead-lettered chunks in " + p : null;
                if (c.s.store.casPipelineStatus(roundId, p, PipelineStatus.RUNNING, target, err, c.epoch())) {
                    status.put(p, target);
                    d.touch();
                    c.s.plane.setDesiredParallelism(roundId, p, 0, run.getAttempt(), c.epoch());
                    log.info("round {} pipeline {} {}", roundId, p, target);
                }
            }
            for (String p : pr.getPipelineIds()) {
                if (status.get(p) == PipelineStatus.FAILED) {
                    failRun(d, pr, run, "pipeline " + p + " failed");
                    break;
                }
            }
        }
    }

    private boolean upstreamDone(PlanPipeline pp, Map<String, PipelineStatus> status) {
        for (String u : pp.getUpstreamPipelines()) if (status.get(u) != PipelineStatus.SUCCEEDED) return false;
        return true;
    }

    private boolean inputsEmpty(String roundId, PlanPipeline pp, int attempt) {
        for (String stream : RunKeys.inputStreams(roundId, pp, attempt).keySet()) if (c.s.plane.streamLength(stream) > 0) return false;
        return true;
    }

    /**
     * A source whose enumeration is done, whose split queue is empty (checked by the caller after a complete journal drain) and still has
     * PENDING chunk records lost its data plane (e.g. Redis was flushed): the splits were enqueued once and the split index (kept for the whole
     * attempt) is gone too. Nothing will ever deliver those chunks.
     */
    private boolean dataPlaneLost(String roundId, String pipelineId, int attempt) {
        return c.s.plane.streamLength(Keys.splitIndex(roundId, pipelineId, attempt)) == 0;
    }

    private void failLost(RoundData d, PlanRun pr, RunRecord run, String p, Map<String, PipelineStatus> status) {
        String msg = "data plane lost (split queue and index of " + p + " are gone while its chunks are PENDING); rerunFrom required";
        log.error("round {} pipeline {}: {}", d.roundId(), p, msg);
        if (c.s.store.casPipelineStatus(d.roundId(), p, PipelineStatus.RUNNING, PipelineStatus.FAILED, msg, c.epoch())) {
            status.put(p, PipelineStatus.FAILED);
            d.touch();
            c.s.plane.setDesiredParallelism(d.roundId(), p, 0, run.getAttempt(), c.epoch());
        }
    }

    private void failRun(RoundData d, PlanRun pr, RunRecord run, String msg) {
        String roundId = d.roundId();
        if (!c.s.store.casRunStatus(roundId, pr.getRunId(), RunStatus.RUNNING, RunStatus.FAILED, msg, c.epoch())) return;
        d.touch();
        for (String p : pr.getPipelineIds()) c.s.plane.setDesiredParallelism(roundId, p, 0, run.getAttempt(), c.epoch());
        c.s.metrics.runFinished(roundId, pr.getRunId(), "FAILED", JournalDrainer.elapsed(run));
        log.warn("round {} run {} FAILED: {}", roundId, pr.getRunId(), msg);
    }

    /** RUNNING runs whose pipelines all succeeded: reconcile row counts, then SUCCEEDED and clean the run's keys. */
    void finishRuns(RoundData d) {
        String roundId = d.roundId();
        for (PlanRun pr : d.plan.getRuns()) {
            RunRecord run = d.run(pr.getRunId());
            if (run == null || run.getStatus() != RunStatus.RUNNING || !d.allPipelinesSucceeded(pr)) continue;
            String mismatch = d.round().isIncomplete() ? null : reconcile(d, pr);
            if (mismatch != null) {
                if (c.s.store.casRunStatus(roundId, pr.getRunId(), RunStatus.RUNNING, RunStatus.FAILED, mismatch, c.epoch())) {
                    d.touch();
                    c.s.metrics.runFinished(roundId, pr.getRunId(), "FAILED", JournalDrainer.elapsed(run));
                }
                log.error("round {} run {}: {}", roundId, pr.getRunId(), mismatch);
                continue;
            }
            if (c.s.store.casRunStatus(roundId, pr.getRunId(), RunStatus.RUNNING, RunStatus.SUCCEEDED, null, c.epoch())) {
                d.touch();
                c.s.plane.deleteKeys(roundId, RunKeys.of(roundId, d.plan, pr, run.getAttempt()), c.epoch());
                c.s.plane.clearLocalRun(roundId, pr.getRunId(), c.epoch());
                c.s.metrics.runFinished(roundId, pr.getRunId(), "SUCCEEDED", JournalDrainer.elapsed(run));
                log.info("round {} run {} SUCCEEDED", roundId, pr.getRunId());
            }
        }
    }

    private String reconcile(RoundData d, PlanRun pr) {
        Map<String, Long> out = c.s.store.sumRowsOutByEdge(d.roundId(), pr.getRunId());
        Map<String, Long> in = c.s.store.sumRowsInByEdge(d.roundId(), pr.getRunId());
        for (PlanChannel ch : d.plan.getChannels()) {
            if (!ch.getRunId().equals(pr.getRunId())) continue;
            long o = out.containsKey(ch.getEdgeId()) ? out.get(ch.getEdgeId()) : 0;
            long i = in.containsKey(ch.getEdgeId()) ? in.get(ch.getEdgeId()) : 0;
            if (o != i) return "row count mismatch on edge " + ch.getEdgeId() + " (" + ch.getFromPipeline() + " -> " + ch.getToPipeline()
                    + "): produced " + o + ", consumed " + i;
        }
        return null;
    }

    /** Round-level outcome. Returns true when the round reached a terminal state. */
    boolean finishRound(RoundData d) {
        String roundId = d.roundId();
        boolean allDone = true, anyRunning = false;
        List<String> failedRuns = new ArrayList<>();
        for (PlanRun pr : d.plan.getRuns()) {
            RunStatus st = d.run(pr.getRunId()).getStatus();
            if (st != RunStatus.SUCCEEDED) allDone = false;
            if (st == RunStatus.RUNNING) anyRunning = true;
            if (st == RunStatus.FAILED) failedRuns.add(pr.getRunId());
        }
        RoundStatus target = null;
        String error = null;
        if (allDone) {
            target = RoundStatus.SUCCEEDED;
        } else if (!anyRunning) {
            Map<String, Boolean> memo = new HashMap<>();
            boolean canStart = false;
            for (PlanRun pr : d.plan.getRuns())
                if (d.run(pr.getRunId()).getStatus() == RunStatus.PENDING && canStart(d, pr.getRunId(), memo)) { canStart = true; break; }
            if (!canStart) {
                target = RoundStatus.FAILED;
                error = failedRuns.isEmpty() ? "gate rejected or timed out" : "failed runs: " + failedRuns;
            }
        }
        if (target == null) return false;
        if (!c.s.store.casRoundStatus(roundId, RoundStatus.RUNNING, target, error, c.epoch())) return false;
        log.info("round {} {} {}", roundId, target, error == null ? "" : error);
        c.s.metrics.roundFinished(d.round().getGraphName(), roundId, target.name());
        cleanup(d, target == RoundStatus.SUCCEEDED);
        return true;
    }

    /** Can a PENDING run still start some day (no failed/rejected prerequisites)? */
    private boolean canStart(RoundData d, String runId, Map<String, Boolean> memo) {
        Boolean m = memo.get(runId);
        if (m != null) return m;
        memo.put(runId, false); // cycle guard
        RunStatus st = d.run(runId).getStatus();
        boolean ok;
        if (st == RunStatus.FAILED) {
            ok = false;
        } else if (st != RunStatus.PENDING) {
            ok = true;
        } else {
            ok = true;
            for (String gid : d.plan.run(runId).getInGateIds()) {
                GateRecord g = d.gate(gid);
                PlanGate pg = d.plan.gate(gid);
                if (g.getStatus() == GateStatus.REJECTED || g.getStatus() == GateStatus.TIMEOUT) { ok = false; break; }
                if (g.getStatus() == GateStatus.CLOSED)
                    for (String ar : pg.getAfterRuns()) if (!canStart(d, ar, memo)) { ok = false; break; }
                if (!ok) break;
            }
        }
        memo.put(runId, ok);
        return ok;
    }

    /** Removes round-level control keys; on success also all data keys (failed rounds keep DLQs/channels for retryRun). */
    private void cleanup(RoundData d, boolean success) {
        String r = d.roundId();
        List<String> keys = RunKeys.control(r);
        if (success) {
            keys.add(Keys.journal(r));
            for (PlanRun pr : d.plan.getRuns()) keys.addAll(RunKeys.of(r, d.plan, pr, d.run(pr.getRunId()).getAttempt()));
        }
        c.s.plane.deleteKeys(r, keys, c.epoch());
        c.s.plane.removeActiveRound(r, c.epoch());
    }
}
