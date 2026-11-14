package io.github.amwtke.pipeliner.runtime;

import io.github.amwtke.pipeliner.api.PipelinerException;
import io.github.amwtke.pipeliner.core.dataplane.Keys;
import io.github.amwtke.pipeliner.core.graph.ExecMode;
import io.github.amwtke.pipeliner.core.plan.ExecutionPlan;
import io.github.amwtke.pipeliner.core.plan.PlanGate;
import io.github.amwtke.pipeliner.core.plan.PlanRun;
import io.github.amwtke.pipeliner.core.store.ChunkStatus;
import io.github.amwtke.pipeliner.core.store.GateRecord;
import io.github.amwtke.pipeliner.core.store.GateStatus;
import io.github.amwtke.pipeliner.core.store.PipelineRecord;
import io.github.amwtke.pipeliner.core.store.PipelineStatus;
import io.github.amwtke.pipeliner.core.store.RoundRecord;
import io.github.amwtke.pipeliner.core.store.RoundStatus;
import io.github.amwtke.pipeliner.core.store.RoundView;
import io.github.amwtke.pipeliner.core.store.RunRecord;
import io.github.amwtke.pipeliner.core.store.RunStatus;
import io.github.amwtke.pipeliner.core.store.StaleEpochException;
import io.github.amwtke.pipeliner.runtime.master.JournalSync;
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
        if (round == null || s.store.loadRoundRecord(round) == null) throw new PipelinerException("unknown round: " + round);   // never a deposition
        if (!s.store.fenceRound(round, epoch)) throw new StaleEpochException("round " + round + " is held by a newer master epoch");
        if (!s.plane.fenceRound(round, epoch)) throw new StaleEpochException("round " + round + " data plane is held by a newer master epoch");
        switch (op) {
            case "pause": pause(round); break;
            case "resume": resume(round); break;
            case "cancel": cancel(round); break;
            case "retryRun": retryRun(round, target); break;
            case "rerunFrom": rerunFrom(round, target); break;
            case "skipDeadLetters": skipDeadLetters(round, target); break;
            case "openGate": openGate(round, target, cmd.get("operator"), "true".equals(cmd.get("force"))); break;
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
        if (plan != null) requireSamePlan(rr, plan);
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
        s.metrics.roundFinished(rr.getGraphName(), roundId, RoundStatus.CANCELLED.name());
    }

    /**
     * Opens a CLOSED gate. Unless {@code force}, only when every after-run of the gate SUCCEEDED. Throws (ok=false to the caller) for an unknown
     * gate, a gate that is not CLOSED (already open, TIMEOUT, REJECTED) and a lost CAS.
     */
    void openGate(String roundId, String gateId, String operator, boolean force) {
        RoundView v = s.store.loadRoundState(roundId);
        if (v == null) throw new PipelinerException("unknown round: " + roundId);
        ExecutionPlan plan = requireSamePlan(v.getRound(), s.registry.require(v.getRound().getGraphName()));
        PlanGate pg = null;
        for (PlanGate g : plan.getGates()) if (g.getGateId().equals(gateId)) pg = g;
        GateRecord rec = null;
        for (GateRecord g : v.getGates()) if (g.getGateId().equals(gateId)) rec = g;
        if (pg == null || rec == null) throw new PipelinerException("unknown gate: " + gateId);
        if (rec.getStatus() != GateStatus.CLOSED) throw new PipelinerException("gate " + gateId + " is " + rec.getStatus() + ", not CLOSED; nothing done");
        if (!force)
            for (String ar : pg.getAfterRuns()) {
                RunRecord r = runOf(v, ar);
                if (r == null || r.getStatus() != RunStatus.SUCCEEDED)
                    throw new PipelinerException("gate " + gateId + ": after-run " + ar + " is " + (r == null ? "unknown" : r.getStatus().name())
                            + ", not SUCCEEDED; pass force=true to open it anyway");
            }
        requireCas(s.store.casGateStatus(roundId, gateId, GateStatus.CLOSED, GateStatus.OPEN, operator, new HashMap<String, String>(), epoch()), "openGate");
    }

    private static RunRecord runOf(RoundView v, String runId) {
        for (RunRecord r : v.getRuns()) if (r.getRunId().equals(runId)) return r;
        return null;
    }

    private ExecutionPlan requireSamePlan(RoundRecord rr, ExecutionPlan plan) {
        if (plan == null || rr.getPlanHash() == null || !plan.getHash().equals(rr.getPlanHash()))
            throw new PipelinerException("round " + rr.getRoundId() + " was created under a different plan version (graph '" + rr.getGraphName()
                    + "') than the one registered on the leader; refusing to act on it (finish the rolling deploy first)");
        return plan;
    }

    /** Gates to reset when {@code runIds} are rerun: all their before-runs are downstream of the rerun root and at least one after-run is rerun. */
    static List<String> gatesToReset(ExecutionPlan plan, String rootRun, List<String> runIds) {
        Set<String> rerun = new HashSet<>(runIds);
        Set<String> downstream = new HashSet<>(runIds);
        downstream.remove(rootRun);
        List<String> gateIds = new ArrayList<>();
        for (PlanGate g : plan.getGates()) {
            if (g.getBeforeRuns().isEmpty() || !downstream.containsAll(g.getBeforeRuns())) continue;
            boolean afterRerun = false;
            for (String ar : g.getAfterRuns()) if (rerun.contains(ar)) afterRerun = true;
            if (afterRerun) gateIds.add(g.getGateId());
        }
        return gateIds;
    }

    /** Refuses to revive a run whose in-gate is REJECTED/TIMEOUT (it failed because of the gate), and compacted or cancelled rounds. */
    private void requireRevivable(Ctx x, String what) {
        if (x.round.getStatus() == RoundStatus.CANCELLED) throw new PipelinerException("round is cancelled");
        if (x.round.isCompacted()) throw new PipelinerException("round was compacted; use rerunFrom");
        for (PlanGate g : x.plan.getGates()) {
            if (!g.getBeforeRuns().contains(x.run.getRunId())) continue;
            for (GateRecord gr : x.view.getGates())
                if (gr.getGateId().equals(g.getGateId()) && gr.getStatus() != GateStatus.OPEN)
                    throw new PipelinerException(what + " refused: in-gate " + g.getGateId() + " of run " + x.run.getRunId() + " is " + gr.getStatus()
                            + "; use rerunFrom on an upstream run or reopen the gate");
        }
    }

    /**
     * Fenced requeue of the current attempt's DLQs FIRST, then the store transitions (all fenced CAS): a failure in the middle leaves the run
     * FAILED, so re-issuing the retry finishes the requeue (requeue of an empty DLQ is a no-op).
     */
    void retryRun(String roundId, String runId) {
        Ctx x = ctx(roundId, runId);
        if (x.round.getStatus() == RoundStatus.CANCELLED) throw new PipelinerException("round is cancelled");
        if (x.run.getStatus() != RunStatus.FAILED) { log.warn("retryRun {}/{}: run is {}, not FAILED", roundId, runId, x.run.getStatus()); return; }
        requireRevivable(x, "retryRun");
        if (x.pr.getMode() != ExecMode.CLUSTER) {
            // local runs simply replay under a new attempt (sinks are versioned); downstream gates drop their signals
            List<String> runIds = new ArrayList<>();
            runIds.add(runId);
            runIds.addAll(x.plan.downstreamRuns(runId));
            requireAttemptsLeft(x.view, Collections.singletonList(runId), "retryRun");
            s.store.resetRuns(roundId, Collections.singletonList(runId), gatesToReset(x.plan, runId, runIds), epoch());
            reviveRound(roundId);
            return;
        }
        for (String p : x.pr.getPipelineIds()) s.plane.requeueDeadLetters(roundId, p, x.run.getAttempt(), epoch());
        requireCas(s.store.casRunStatus(roundId, runId, RunStatus.FAILED, RunStatus.RUNNING, null, epoch()), "retryRun");
        revivePipelines(x);
        reviveRound(roundId);
    }

    /**
     * CLUSTER only. Drains the journal first (so late DEAD events cannot resurrect skipped chunks), then store first (chunks SKIPPED, round
     * incomplete only if something was skipped, pipelines RUNNING), then the fenced DLQ clear.
     */
    void skipDeadLetters(String roundId, String runId) {
        Ctx x0 = ctx(roundId, runId);
        if (x0.pr.getMode() != ExecMode.CLUSTER)
            throw new PipelinerException("skipDeadLetters is only supported for CLUSTER runs (run " + runId + " is " + x0.pr.getMode() + "); use retryRun or rerunFrom");
        if (x0.round.getStatus() == RoundStatus.CANCELLED) throw new PipelinerException("round is cancelled");
        if (x0.round.isCompacted()) throw new PipelinerException("round was compacted; use rerunFrom");
        JournalSync.drain(s, epoch(), roundId, x0.plan);
        Ctx x = ctx(roundId, runId);
        requireRevivable(x, "skipDeadLetters");
        long pending = 0;
        for (String p : x.pr.getPipelineIds()) {
            pending += s.store.countChunks(roundId, p, ChunkStatus.DEAD);
            pending += s.plane.streamLength(Keys.dlq(roundId, p, x.run.getAttempt()));
        }
        if (pending == 0) throw new PipelinerException("run " + runId + " has no dead letters to skip");
        s.store.markChunksSkipped(roundId, runId, epoch());
        if (!s.store.setRoundIncomplete(roundId, epoch())) throw new StaleEpochException("round " + roundId + " is held by a newer master epoch");
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
        List<String> gateIds = gatesToReset(x.plan, runId, runIds);
        requireAttemptsLeft(x.view, runIds, "rerunFrom");
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

    /** A reset bumps every listed run's attempt: refuse (before changing anything) when the new attempt would reach ChunkProcessor.VERSION_RADIX. */
    private static void requireAttemptsLeft(RoundView v, List<String> runIds, String what) {
        for (String id : runIds) {
            RunRecord r = runOf(v, id);
            if (r != null && !io.github.amwtke.pipeliner.runtime.process.ChunkProcessor.attemptInRange(r.getAttempt() + 1))
                throw new PipelinerException(what + " refused: " + io.github.amwtke.pipeliner.runtime.process.ChunkProcessor.attemptLimitMessage(
                        v.getRound().getRoundId(), id, r.getAttempt() + 1));
        }
    }

    private Ctx ctx(String roundId, String runId) {
        RoundView v = s.store.loadRoundState(roundId);
        if (v == null) throw new PipelinerException("unknown round: " + roundId);
        ExecutionPlan plan = requireSamePlan(v.getRound(), s.registry.require(v.getRound().getGraphName()));
        PlanRun pr = plan.run(runId);
        if (pr == null) throw new PipelinerException("unknown run: " + runId);
        RunRecord run = runOf(v, runId);
        if (run == null) throw new PipelinerException("unknown run: " + runId);
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
