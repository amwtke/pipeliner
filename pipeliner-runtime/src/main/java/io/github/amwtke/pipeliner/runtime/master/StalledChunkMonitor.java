package io.github.amwtke.pipeliner.runtime.master;

import io.github.amwtke.pipeliner.core.dataplane.Keys;
import io.github.amwtke.pipeliner.core.dataplane.PendingSummary;
import io.github.amwtke.pipeliner.core.graph.ExecMode;
import io.github.amwtke.pipeliner.core.plan.PlanPipeline;
import io.github.amwtke.pipeliner.core.plan.PlanRun;
import io.github.amwtke.pipeliner.core.store.PipelineStatus;
import io.github.amwtke.pipeliner.core.store.RunStatus;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;

/**
 * Samples the oldest pending entry of the input streams of RUNNING cluster pipelines (XPENDING, cheap), feeds oldest_pending and logs ONE warning
 * per (round, pipeline, entry) whose idle time exceeds stalledChunkAlertMillis. Sampling is throttled to every few ticks per round.
 */
@Slf4j
final class StalledChunkMonitor {
    private static final int SAMPLE_EVERY_TICKS = 4;
    private static final int MAX_ALERTED = 10_000;
    private final MasterContext c;
    private final Map<String, Long> lastSample = new HashMap<>();
    private final Set<String> alerted = new HashSet<>();

    StalledChunkMonitor(MasterContext c) { this.c = c; }

    void sample(RoundData d) {
        long now = System.currentTimeMillis();
        String roundId = d.roundId();
        Long last = lastSample.get(roundId);
        if (last != null && now - last < SAMPLE_EVERY_TICKS * c.s.cfg.getTickMillis()) return;
        lastSample.put(roundId, now);
        String graph = d.round().getGraphName();
        for (PlanRun pr : d.plan.getRuns()) {
            if (pr.getMode() != ExecMode.CLUSTER || d.run(pr.getRunId()).getStatus() != RunStatus.RUNNING) continue;
            int attempt = d.run(pr.getRunId()).getAttempt();
            for (String p : pr.getPipelineIds()) {
                if (d.pipe(p).getStatus() != PipelineStatus.RUNNING) continue;
                PlanPipeline pp = d.plan.pipeline(p);
                List<String> streams = new ArrayList<>();
                if (pp.isSource()) streams.add(Keys.splitQueue(roundId, p, attempt));
                else for (String e : pp.getInputEdgeIds()) streams.add(Keys.channel(roundId, e, attempt));
                PendingSummary worst = null;
                String worstStream = null;
                for (String st : streams) {
                    PendingSummary ps = c.s.plane.pendingSummary(st);
                    if (ps != null && ps.getPending() > 0 && (worst == null || ps.getOldestIdleMillis() > worst.getOldestIdleMillis())) { worst = ps; worstStream = st; }
                }
                c.s.metrics.oldestPending(roundId, p, worst == null ? 0 : worst.getOldestIdleMillis());
                if (worst != null && worst.getOldestIdleMillis() > c.s.cfg.getStalledChunkAlertMillis()) {
                    if (alerted.size() > MAX_ALERTED) alerted.clear();
                    if (alerted.add(roundId + "|" + p + "|" + worst.getOldestEntryId())) {
                        c.s.metrics.stalledChunk(graph, p);
                        log.warn("STALLED chunk: round {} pipeline {} entry {} on {} idle {} ms (> {}), consumer {}, deliveries {}, {} pending",
                                roundId, p, worst.getOldestEntryId(), worstStream, worst.getOldestIdleMillis(), c.s.cfg.getStalledChunkAlertMillis(),
                                worst.getOldestConsumer(), worst.getOldestDeliveryCount(), worst.getPending());
                    }
                }
            }
        }
    }

    /** Forgets the round's sampling state (round finished). */
    void forget(String roundId) {
        lastSample.remove(roundId);
        alerted.removeIf(k -> k.startsWith(roundId + "|"));
    }
}
