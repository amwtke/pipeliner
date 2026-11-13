package io.github.amwtke.pipeliner.runtime.master;

import io.github.amwtke.pipeliner.api.GateDecision;
import io.github.amwtke.pipeliner.core.graph.GateSpec;
import io.github.amwtke.pipeliner.core.graph.GateType;
import io.github.amwtke.pipeliner.core.plan.ExecutionPlan;
import io.github.amwtke.pipeliner.core.plan.PlanGate;
import io.github.amwtke.pipeliner.core.store.GateRecord;
import io.github.amwtke.pipeliner.core.store.GateStatus;
import io.github.amwtke.pipeliner.core.store.RunStatus;
import io.github.amwtke.pipeliner.runtime.context.GateContextImpl;
import java.util.HashMap;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;

/** Arms and evaluates gates: barrier, condition, external, manual, timer; applies timeout policies. */
@Slf4j
final class GateEvaluator {
    private final MasterContext c;

    GateEvaluator(MasterContext c) { this.c = c; }

    void evaluate(RoundData d) {
        String roundId = d.roundId();
        long now = System.currentTimeMillis();
        for (PlanGate pg : d.plan.getGates()) {
            GateRecord g = d.gate(pg.getGateId());
            if (g == null || g.getStatus() != GateStatus.CLOSED || !d.allSucceeded(pg.getAfterPipelines()) || !d.allRunsSucceeded(pg.getAfterRuns())) continue;
            try {
                if (g.getArmedAt() == null) {
                    Long deadline = pg.getTimeoutMillis() > 0 ? now + pg.getTimeoutMillis() : null;
                    c.s.store.armGate(roundId, g.getGateId(), now, deadline, c.epoch());
                    d.touch();
                    g.setArmedAt(now);
                    g.setDeadlineAt(deadline);
                }
                GateDecision dec = decide(d, pg, g, now);
                if (dec == GateDecision.OPEN) {
                    if (c.s.store.casGateStatus(roundId, g.getGateId(), GateStatus.CLOSED, GateStatus.OPEN, pg.getType().name().toLowerCase(), new HashMap<String, String>(), c.epoch()))
                        d.touch();
                } else if (dec == GateDecision.REJECT) {
                    if (c.s.store.casGateStatus(roundId, g.getGateId(), GateStatus.CLOSED, GateStatus.REJECTED, "condition", new HashMap<String, String>(), c.epoch()))
                        { failBeforeRuns(roundId, pg, "gate " + pg.getGateId() + " rejected"); d.touch(); }
                } else {
                    onWait(d, pg, g, now);
                }
            } catch (RuntimeException e) {
                log.warn("gate {}/{} evaluation failed: {}", roundId, g.getGateId(), e.toString());
            }
        }
    }

    private GateDecision decide(RoundData d, PlanGate pg, GateRecord g, long now) {
        if (pg.getType() == GateType.BARRIER) return GateDecision.OPEN;
        if (pg.getType() == GateType.MANUAL) return GateDecision.WAIT;
        GateSpec spec = findSpec(d.plan, pg.getGateId());
        if (spec == null || spec.getCondition() == null) return GateDecision.WAIT;
        try {
            Map<String, String> signals = c.s.store.loadGateSignals(d.roundId(), pg.getGateId());
            GateContextImpl ctx = new GateContextImpl(d.roundId(), pg.getGateId(), d.round().getParams(), signals,
                    g.getArmedAt(), now, c.s.store, c.s.dataSources);
            GateDecision dec = spec.getCondition().evaluate(ctx);
            return dec == null ? GateDecision.WAIT : dec;
        } catch (Exception e) {
            log.warn("gate {}/{} condition threw, treating as WAIT: {}", d.roundId(), pg.getGateId(), e.toString());
            return GateDecision.WAIT;
        }
    }

    private void onWait(RoundData d, PlanGate pg, GateRecord g, long now) {
        String roundId = d.roundId();
        c.s.metrics.gateWaiting(roundId, g.getGateId(), now - g.getArmedAt());
        if (g.getDeadlineAt() == null || now < g.getDeadlineAt()) return;
        switch (pg.getOnTimeout() == null ? io.github.amwtke.pipeliner.core.graph.TimeoutPolicy.FAIL : pg.getOnTimeout()) {
            case OPEN_WITH_FALLBACK:
                Map<String, String> ctx = new HashMap<>();
                ctx.put("fallback", "true");
                if (c.s.store.casGateStatus(roundId, g.getGateId(), GateStatus.CLOSED, GateStatus.OPEN, "timeout", ctx, c.epoch())) {
                    d.touch();
                    log.warn("gate {}/{} timed out; opened with fallback", roundId, g.getGateId());
                }
                break;
            case ALERT_AND_WAIT:
                if (!g.isAlerted()) {
                    log.error("ALERT: gate {}/{} exceeded its timeout and keeps waiting", roundId, g.getGateId());
                    c.s.store.markGateAlerted(roundId, g.getGateId());
                    g.setAlerted(true);
                }
                break;
            default:
                if (c.s.store.casGateStatus(roundId, g.getGateId(), GateStatus.CLOSED, GateStatus.TIMEOUT, "timeout", new HashMap<String, String>(), c.epoch()))
                { failBeforeRuns(roundId, pg, "gate " + pg.getGateId() + " timed out"); d.touch(); }
        }
    }

    private void failBeforeRuns(String roundId, PlanGate pg, String msg) {
        for (String run : pg.getBeforeRuns())
            c.s.store.casRunStatus(roundId, run, RunStatus.PENDING, RunStatus.FAILED, msg, c.epoch());
    }

    private static GateSpec findSpec(ExecutionPlan plan, String id) {
        for (GateSpec s : plan.getGraph().getGates()) if (s.getId().equals(id)) return s;
        return null;
    }
}
