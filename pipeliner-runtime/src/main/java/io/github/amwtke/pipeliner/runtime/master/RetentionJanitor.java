package io.github.amwtke.pipeliner.runtime.master;

import io.github.amwtke.pipeliner.core.engine.RuntimeConfig;
import io.github.amwtke.pipeliner.runtime.Services;
import io.github.amwtke.pipeliner.core.store.RoundRecord;
import java.util.List;
import java.util.function.BooleanSupplier;
import lombok.extern.slf4j.Slf4j;

/**
 * Control-plane retention, run by the leader from the master loop: purges terminal rounds older than the round retention, compacts rounds older
 * than the chunk retention, prunes stale worker heartbeats. At most once per retentionIntervalMillis, at most retentionBatch rounds per step and
 * a small time budget per pass so master ticks (and lease renewals) stay short.
 */
@Slf4j
public final class RetentionJanitor {
    private final Services s;
    private final BooleanSupplier leader;
    private long lastRun;
    /** Rounds compactRound refused with a negative code (e.g. old Redis rounds without finishedAt): not retried every pass. */
    private final java.util.Set<String> uncompactable = new java.util.HashSet<>();

    public RetentionJanitor(Services s, BooleanSupplier leader) {
        this.s = s;
        this.leader = leader;
    }

    /** Runs a pass if enabled, leader and due. Returns whether a pass ran. Never throws. */
    public boolean runIfDue(long now) {
        RuntimeConfig cfg = s.cfg;
        if (!cfg.isRetentionEnabled() || !leader.getAsBoolean()) return false;
        if (lastRun != 0 && now - lastRun < cfg.getRetentionIntervalMillis()) return false;
        lastRun = now;
        long budget = Math.max(200L, Math.min(2000L, cfg.getLeaderTtlMillis() / 4));
        long deadline = System.currentTimeMillis() + budget;
        int purged = 0, compacted = 0, chunks = 0, pruned = 0;
        try {
            int batch = Math.max(1, cfg.getRetentionBatch());
            List<String> gone = s.store.purgeRounds(now - cfg.getRoundRetentionMillis(), batch);
            purged = gone.size();
            // purgeRounds recorded every purged id in the store's durable pending set (same atomic step), so the data plane purge survives a
            // master restart or failover; the ids are cleared only after purgeRoundData succeeded (retried every pass otherwise)
            List<String> todo = s.store.pendingPurges(Math.max(batch, 100));
            s.plane.purgeRoundData(todo);   // also when empty: advances the data plane's bounded legacy (pre-registry) key scan
            if (!todo.isEmpty()) s.store.clearPendingPurges(todo);
            if (System.currentTimeMillis() < deadline) {
                long cutoff = now - cfg.getChunkRetentionMillis();
                if (uncompactable.size() > 10_000) uncompactable.clear();
                for (String id : s.store.findTerminalRounds(cutoff, batch + uncompactable.size())) {
                    if (!leader.getAsBoolean()) break;
                    if (uncompactable.contains(id)) continue;
                    int n = s.store.compactRound(id, cutoff);
                    // 0 = nothing removed: either not eligible any more (revived) or compacted with no chunk records; only the latter counts
                    if (n > 0 || n == 0 && isCompacted(id)) { compacted++; chunks += n; }
                    else if (n < 0) { uncompactable.add(id); log.warn("retention: round {} cannot be compacted (code {}); skipping it from now on", id, n); }
                    if (System.currentTimeMillis() >= deadline) break;
                }
            }
            pruned = s.plane.pruneWorkers(10 * cfg.getWorkerDeadMillis());
        } catch (RuntimeException e) {
            log.warn("retention pass failed: {}", e.toString());
        }
        s.metrics.retentionPass(compacted, purged, pruned);
        if (purged + compacted + pruned > 0)
            log.info("retention: purged {} rounds, compacted {} rounds ({} chunk records), pruned {} stale workers", purged, compacted, chunks, pruned);
        return true;
    }

    private boolean isCompacted(String roundId) {
        RoundRecord r = s.store.loadRoundRecord(roundId);
        return r != null && r.isCompacted();
    }
}
