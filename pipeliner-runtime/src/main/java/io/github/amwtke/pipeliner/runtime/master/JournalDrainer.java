package io.github.amwtke.pipeliner.runtime.master;

import io.github.amwtke.pipeliner.core.plan.ExecutionPlan;
import io.github.amwtke.pipeliner.core.plan.PlanRun;
import io.github.amwtke.pipeliner.core.store.ChunkEvent;
import io.github.amwtke.pipeliner.core.store.PipelineStatus;
import io.github.amwtke.pipeliner.core.store.RoundRecord;
import io.github.amwtke.pipeliner.core.store.RunRecord;
import io.github.amwtke.pipeliner.core.store.RunStatus;
import java.util.ArrayList;
import java.util.List;
import lombok.extern.slf4j.Slf4j;

/** Applies journal events to the store (offset persisted with each batch) and reacts to local-run events. */
@Slf4j
final class JournalDrainer {
    private final MasterContext c;

    JournalDrainer(MasterContext c) { this.c = c; }

    /**
     * Drains the journal completely, batch by batch: local-run events first (their CAS writes are idempotent), then the chunk
     * events together with the new offset, then the trim. A failure in a later batch therefore never loses an earlier one.
     */
    void drain(String roundId, ExecutionPlan plan) {
        RoundRecord rr = c.s.store.loadRoundRecord(roundId);
        if (rr == null) return;
        String offset = rr.getJournalOffset() == null ? "0-0" : rr.getJournalOffset();
        while (true) {
            List<ChunkEvent> events = c.s.plane.readJournal(roundId, offset, c.s.cfg.getJournalBatch());
            if (events.isEmpty()) break;
            String last = events.get(events.size() - 1).getEventId();
            RoundData d = RoundData.load(c, roundId, plan);
            if (d == null) return;
            handleLocalEvents(roundId, d, events);
            c.s.store.applyChunkEvents(roundId, withoutStale(d, events), last, c.epoch());   // throws StaleEpochException when deposed
            c.s.plane.trimJournal(roundId, last, c.epoch());
            offset = last;
        }
    }

    /**
     * Drops DONE/DEAD events of runs that were reset (PENDING) after the event was written, e.g. by rerunFrom, and events of an
     * older run attempt (a zombie worker or a stale local executor): their chunk outcome belongs to discarded state.
     */
    private List<ChunkEvent> withoutStale(RoundData d, List<ChunkEvent> events) {
        List<ChunkEvent> out = new ArrayList<>(events.size());
        for (ChunkEvent e : events) {
            RunRecord run = ("DONE".equals(e.getType()) || "DEAD".equals(e.getType())) ? d.run(e.getRunId()) : null;
            if (run != null && e.getRunAttempt() != null && e.getRunAttempt() != run.getAttempt()) {
                log.debug("dropping {} event of {}/{} from attempt {} (current attempt {})", e.getType(), d.roundId(), e.getRunId(), e.getRunAttempt(), run.getAttempt());
                continue;
            }
            if (run != null && run.getStatus() == RunStatus.PENDING) {
                log.debug("dropping stale {} event of reset run {}/{}", e.getType(), d.roundId(), e.getRunId());
                continue;
            }
            out.add(e);
        }
        return out;
    }

    private void handleLocalEvents(String roundId, RoundData d, List<ChunkEvent> events) {
        ExecutionPlan plan = d.plan;
        for (ChunkEvent e : events) {
            if (!"LOCAL_RUN_DONE".equals(e.getType()) && !"LOCAL_RUN_FAILED".equals(e.getType())) continue;
            RunRecord run = d.run(e.getRunId());
            PlanRun pr = plan.run(e.getRunId());
            if (run == null || pr == null || run.getStatus() != RunStatus.RUNNING || run.getAttempt() != e.getAttempt()) {
                log.debug("ignoring stale local event {} for {}/{} attempt {}", e.getType(), roundId, e.getRunId(), e.getAttempt());
                continue;
            }
            if ("LOCAL_RUN_DONE".equals(e.getType())) {
                for (String p : pr.getPipelineIds())
                    c.s.store.casPipelineStatus(roundId, p, PipelineStatus.RUNNING, PipelineStatus.SUCCEEDED, null, c.epoch());
                // the run itself is finished by CompletionChecker (row reconciliation first)
            } else {
                String msg = e.getError() == null ? "local run failed" : e.getError();
                for (String p : pr.getPipelineIds())
                    if (e.getPipelineId() == null || e.getPipelineId().equals(p))
                        c.s.store.casPipelineStatus(roundId, p, PipelineStatus.RUNNING, PipelineStatus.FAILED, msg, c.epoch());
                if (c.s.store.casRunStatus(roundId, e.getRunId(), RunStatus.RUNNING, RunStatus.FAILED, msg, c.epoch()))
                    c.s.metrics.runFinished(roundId, e.getRunId(), "FAILED", elapsed(run));
            }
            c.s.plane.clearLocalRun(roundId, e.getRunId(), c.epoch());
        }
    }

    static long elapsed(RunRecord r) { return r.getStartedAt() == null ? 0 : System.currentTimeMillis() - r.getStartedAt(); }
}
