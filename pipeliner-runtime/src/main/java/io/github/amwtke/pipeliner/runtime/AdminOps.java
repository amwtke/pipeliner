package io.github.amwtke.pipeliner.runtime;

import io.github.amwtke.pipeliner.api.PipelinerException;
import io.github.amwtke.pipeliner.core.dataplane.Keys;
import io.github.amwtke.pipeliner.core.graph.ExecMode;
import io.github.amwtke.pipeliner.core.plan.ExecutionPlan;
import io.github.amwtke.pipeliner.core.plan.PlanGate;
import io.github.amwtke.pipeliner.core.plan.PlanRun;
import io.github.amwtke.pipeliner.core.store.GateStatus;
import io.github.amwtke.pipeliner.core.store.PipelineRecord;
import io.github.amwtke.pipeliner.core.store.PipelineStatus;
import io.github.amwtke.pipeliner.core.store.RoundRecord;
import io.github.amwtke.pipeliner.core.store.RoundStatus;
import io.github.amwtke.pipeliner.core.store.RoundView;
import io.github.amwtke.pipeliner.core.store.RunRecord;
import io.github.amwtke.pipeliner.core.store.RunStatus;
import io.github.amwtke.pipeliner.core.store.StaleEpochException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;

/**
 * Operator actions on rounds. Executed ONLY by the leader inside its tick (see {@link #execute}), with the leader's epoch, so they
 * never race the master's own round processing. Every CAS result is checked before the side effects that depend on it.
 */
@Slf4j
public final class AdminOps {
    private final Services s;
    private final long epoch;

    public AdminOps(Services s, long epoch) { this.s = s; this.epoch = epoch; }

    private long epoch() { return epoch; }

    /** Runs one queued command; throws PipelinerException (or StaleEpochException) on failure. */
    public void execute(Map<String, String> cmd) {
        String op = cmd.get("op"), round = cmd.get("roundId"), target = cmd.get("target");
        if (!s.store.fenceRound(round, epoch)) throw new StaleEpochException("round " + round + " is held by a newer master epoch");
        switch (op) {
            case "pause": pause(round); break;
            case "resume": resume(round); break;
            case "cancel": cancel(round); break;
            case "retryRun": retryRun(round, target); break;
            case "rerunFrom": rerunFrom(round, target); break;
            case "skipDeadLetters": skipDeadLetters(round, target); break;
            case "openGate": openGate(round, target, cmd.get("operator")); break;
            default: throw new PipelinerException("unknown admin command: " + op);
        }
    }

    private RoundRecord requireRound(String roundId) {
        RoundRecord rr = s.store.loadRoundRecord(roundId);
        if (rr == null) throw new PipelinerException("unknown round: " + roundId);
        return rr;
    }

    private void requireCas(boolean ok, String what) {
        if (!ok) throw new PipelinerException(what + " failed: state changed concurrently or master fenced");
    }

    void pause(String roundId) {
        RoundRecord rr = requireRound(roundId);
        if (rr.getStatus() != RoundStatus.RUNNING && rr.getStatus() != RoundStatus.CREATED) return;
        requireCas(s.store.casRoundStatus(roundId, rr.getStatus(), RoundStatus.PAUSED, null, epoch()), "pause");
        s.plane.setPaused(roundId, true, epoch());
    }

    void resume(String roundId) {
        RoundRecord rr = requireRound(roundId);
        if (rr.getStatus() != RoundStatus.PAUSED) return;
        requireCas(s.store.casRoundStatus(roundId, RoundStatus.PAUSED, RoundStatus.RUNNING, null, epoch()), "resume");
        s.plane.setPaused(roundId, false, epoch());
    }

    /** Store first (the CAS decides), then the fenced data-plane cleanup of every attempt of every run. */
    void cancel(String roundId) {
        RoundRecord rr = requireRound(roundId);
        if (rr.getStatus().terminal()) return;
        ExecutionPlan plan = s.registry.find(rr.getGraphName());
        RoundView v = s.store.loadRoundState(roundId);
        requireCas(s.store.casRoundStatus(roundId, rr.getStatus(), RoundStatus.CANCELLED, "cancelled", epoch()), "cancel");
        List<String> keys = RunKeys.control(roundId);
        keys.add(Keys.journal(roundId));
        if (plan != null && v != null)
            for (RunRecord run : v.getRuns()) {
                PlanRun pr = plan.run(run.getRunId());
                if (pr != null) keys.addAll(RunKeys.ofAttemptsBefore(roundId, plan, pr, run.getAttempt() + 1));
            }
        s.plane.deleteKeys(roundId, keys, epoch());
        s.plane.removeActiveRound(roundId, epoch());
    }

    void openGate(String roundId, String gateId, String operator) {
        if (!s.store.casGateStatus(roundId, gateId, GateStatus.CLOSED, GateStatus.OPEN, operator, new HashMap<String, String>(), epoch()))
            log.warn("openGate {}/{}: gate is not CLOSED; nothing done", roundId, gateId);
    }

    /** Store transitions first (all fenced CAS), then the fenced requeue of the current attempt's DLQs. */
    void retryRun(String roundId, String runId) {
        Ctx x = ctx(roundId, runId);
        if (x.run.getStatus() != RunStatus.FAILED) { log.warn("retryRun {}/{}: run is {}, not FAILED", roundId, runId, x.run.getStatus()); return; }
        if (x.pr.getMode() != ExecMode.CLUSTER) {
            // local runs simply replay under a new attempt (sinks are versioned)
            s.store.resetRuns(roundId, Collections.singletonList(runId), Collections.<String>emptyList(), epoch());
            reviveRound(roundId);
            return;
        }
        requireCas(s.store.casRunStatus(roundId, runId, RunStatus.FAILED, RunStatus.RUNNING, null, epoch()), "retryRun");
        revivePipelines(x);
        reviveRound(roundId);
        for (String p : x.pr.getPipelineIds()) s.plane.requeueDeadLetters(roundId, p, x.run.getAttempt(), epoch());
    }

    /** Store first (chunks SKIPPED, round incomplete, pipelines RUNNING), then the fenced DLQ clear. */
    void skipDeadLetters(String roundId, String runId) {
        Ctx x = ctx(roundId, runId);
        if (x.pr.getMode() != ExecMode.CLUSTER) { retryRun(roundId, runId); return; }
        s.store.markChunksSkipped(roundId, runId, epoch());
        s.store.setRoundIncomplete(roundId);
        if (x.run.getStatus() == RunStatus.FAILED) {
            requireCas(s.store.casRunStatus(roundId, runId, RunStatus.FAILED, RunStatus.RUNNING, null, epoch()), "skipDeadLetters");
            revivePipelines(x);
        } else {
            for (String p : x.pr.getPipelineIds()) s.store.casPipelineStatus(roundId, p, PipelineStatus.FAILED, PipelineStatus.RUNNING, null, epoch());
        }
        reviveRound(roundId);
        for (String p : x.pr.getPipelineIds()) s.plane.clearDeadLetters(roundId, p, x.run.getAttempt(), epoch());
    }

    /** FAILED pipelines back to RUNNING; workers get their desired parallelism back. */
    private void revivePipelines(Ctx x) {
        String roundId = x.roundId;
        for (PipelineRecord p : x.view.getPipelines()) {
            if (!p.getRunId().equals(x.run.getRunId())) continue;
            if (p.getStatus() == PipelineStatus.FAILED) s.store.casPipelineStatus(roundId, p.getPipelineId(), PipelineStatus.FAILED, PipelineStatus.RUNNING, null, epoch());
            if (p.getStatus() != PipelineStatus.SUCCEEDED) s.plane.setDesiredParallelism(roundId, p.getPipelineId(), p.getParallelism(), x.run.getAttempt(), epoch());
        }
    }

    /** A terminal round becomes RUNNING (and active) again after a retry/rerun. */
    private void reviveRound(String roundId) {
        RoundRecord rr = requireRound(roundId);
        if (rr.getStatus() == RoundStatus.FAILED || rr.getStatus() == RoundStatus.SUCCEEDED)
            requireCas(s.store.casRoundStatus(roundId, rr.getStatus(), RoundStatus.RUNNING, null, epoch()), "reviving round");
        if (rr.getStatus() != RoundStatus.CANCELLED) s.plane.addActiveRound(roundId);
    }

    /**
     * ONE atomic store reset (runs, pipelines, chunk records, downstream gates incl. their signals, round status), then the
     * round is re-added. The data plane is not touched here beyond stopping the old workers: the master removes the keys of
     * all older attempts when it starts the new attempt (RunStarter), so a crash or a zombie in between is harmless. Journal
     * events of the old attempt are dropped by the journal drainer (attempt filter).
     */
    void rerunFrom(String roundId, String runId) {
        Ctx x = ctx(roundId, runId);
        if (x.round.getStatus() == RoundStatus.CANCELLED) throw new PipelinerException("round is cancelled");
        List<String> runIds = new ArrayList<>();
        runIds.add(runId);
        runIds.addAll(x.plan.downstreamRuns(runId));
        Set<String> downstream = new HashSet<>(runIds);
        downstream.remove(runId);
        List<String> gateIds = new ArrayList<>();
        for (PlanGate g : x.plan.getGates())
            if (!g.getBeforeRuns().isEmpty() && downstream.containsAll(g.getBeforeRuns())) gateIds.add(g.getGateId());
        s.store.resetRuns(roundId, runIds, gateIds, epoch());
        RoundRecord now = requireRound(roundId);
        if (now.getStatus() == RoundStatus.PAUSED || now.getStatus() == RoundStatus.CREATED)
            requireCas(s.store.casRoundStatus(roundId, now.getStatus(), RoundStatus.RUNNING, null, epoch()), "rerunFrom");
        s.plane.setPaused(roundId, false, epoch());
        s.plane.addActiveRound(roundId);
        // best effort: stop the old attempt's workers now; RunStarter re-drives this from the store state if we crash here
        for (String id : runIds) {
            PlanRun pr = x.plan.run(id);
            for (String p : pr.getPipelineIds()) s.plane.setDesiredParallelism(roundId, p, 0, 0, epoch());
            s.plane.clearLocalRun(roundId, id, epoch());
        }
        log.info("round {} rerun from {} (reset runs {})", roundId, runId, runIds);
    }

    private Ctx ctx(String roundId, String runId) {
        RoundView v = s.store.loadRoundState(roundId);
        if (v == null) throw new PipelinerException("unknown round: " + roundId);
        ExecutionPlan plan = s.registry.require(v.getRound().getGraphName());
        PlanRun pr = plan.run(runId);
        if (pr == null) throw new PipelinerException("unknown run: " + runId);
        RunRecord run = null;
        for (RunRecord r : v.getRuns()) if (r.getRunId().equals(runId)) run = r;
        return new Ctx(roundId, v, plan, pr, run);
    }

    private static final class Ctx {
        final String roundId;
        final RoundView view;
        final RoundRecord round;
        final ExecutionPlan plan;
        final PlanRun pr;
        final RunRecord run;
        Ctx(String roundId, RoundView view, ExecutionPlan plan, PlanRun pr, RunRecord run) {
            this.roundId = roundId; this.view = view; this.round = view.getRound(); this.plan = plan; this.pr = pr; this.run = run;
        }
    }
}
