package io.github.amwtke.pipeliner.core.engine;

import io.github.amwtke.pipeliner.api.PipelinerException;
import io.github.amwtke.pipeliner.core.plan.PlanOptions;
import java.util.ArrayList;
import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/** Runtime tuning; defaults apply to both the builder and the no-arg constructor. */
@Data
@Builder(toBuilder = true)
@NoArgsConstructor
@AllArgsConstructor
public class RuntimeConfig {
    /** Required. */
    private String podId;
    @Builder.Default private boolean masterEnabled = true;
    @Builder.Default private boolean workerEnabled = true;
    @Builder.Default private long tickMillis = 1000;
    /**
     * Who drives the master pass: LOCAL (default) = private pg-master thread every tickMillis; EXTERNAL = no thread, an external scheduler calls
     * PipelinerEngine.tick(). Worker heartbeat/agent is always local (pod liveness must not depend on a scheduler).
     */
    @Builder.Default private MasterTickMode masterTickMode = MasterTickMode.LOCAL;
    /** EXTERNAL mode: how often the trigger is expected to fire; only used for validation (leaderTtl >= 3x) and docs/monitoring. */
    @Builder.Default private long externalTickIntervalMillis = 2000;
    @Builder.Default private long leaderTtlMillis = 15000;
    @Builder.Default private long heartbeatMillis = 5000;
    @Builder.Default private long workerDeadMillis = 20000;
    @Builder.Default private long claimIdleMillis = 60000;
    @Builder.Default private long renewIntervalMillis = 20000;
    @Builder.Default private long autoClaimIntervalMillis = 10000;
    @Builder.Default private long readBlockMillis = 1000;
    @Builder.Default private int highWatermark = 64;
    @Builder.Default private int lowWatermark = 32;
    /** Total number of attempts per chunk (1 = no retry); must be at least 1. */
    @Builder.Default private int maxAttempts = 3;
    @Builder.Default private long retryBackoffMillis = 1000;
    @Builder.Default private int journalBatch = 500;
    @Builder.Default private boolean autoscale = true;
    @Builder.Default private int maxParallelism = 8;
    @Builder.Default private long autoscaleCooldownMillis = 15000;
    @Builder.Default private long redisMemoryLimitBytes = 0;
    @Builder.Default private long stalledChunkAlertMillis = 300000;
    @Builder.Default private int localQueueCapacity = 64;
    /** How long an admin call waits for the leader to execute its command. */
    @Builder.Default private long adminTimeoutMillis = 30000;
    /** An autoclaimed entry delivered more often than this is dead-lettered without processing. */
    @Builder.Default private int maxDeliveries = 5;
    /** Control-plane data retention, run by the leader (see RetentionJanitor). */
    @Builder.Default private boolean retentionEnabled = true;
    /** Terminal rounds older than this lose their chunk records (summaries stay). */
    @Builder.Default private long chunkRetentionMillis = 3L * 24 * 3600 * 1000;
    /** Terminal rounds older than this are deleted completely. */
    @Builder.Default private long roundRetentionMillis = 30L * 24 * 3600 * 1000;
    @Builder.Default private long retentionIntervalMillis = 10L * 60 * 1000;
    /** Rounds compacted / purged per pass. */
    @Builder.Default private int retentionBatch = 100;
    /** engine.stop() waits at most this long for worker and local-run threads to finish before returning. */
    @Builder.Default private long stopJoinMillis = 10000;
    /** Null means PlanOptions.defaults(). */
    private PlanOptions planOptions;

    /** Fails fast with a clear message on values that would hang or silently break the runtime. Called by the engine at start. */
    public void validate() {
        List<String> bad = new ArrayList<>();
        if (podId == null || podId.isEmpty()) bad.add("podId is required");
        if (maxAttempts < 1) bad.add("maxAttempts must be >= 1 (it is the total number of attempts per chunk), got " + maxAttempts);
        if (lowWatermark <= 0) bad.add("lowWatermark must be > 0, got " + lowWatermark);
        if (highWatermark < lowWatermark) bad.add("highWatermark (" + highWatermark + ") must be >= lowWatermark (" + lowWatermark + ")");
        positive(bad, "tickMillis", tickMillis);
        positive(bad, "externalTickIntervalMillis", externalTickIntervalMillis);
        positive(bad, "leaderTtlMillis", leaderTtlMillis);
        positive(bad, "heartbeatMillis", heartbeatMillis);
        positive(bad, "workerDeadMillis", workerDeadMillis);
        positive(bad, "claimIdleMillis", claimIdleMillis);
        positive(bad, "renewIntervalMillis", renewIntervalMillis);
        positive(bad, "autoClaimIntervalMillis", autoClaimIntervalMillis);
        positive(bad, "readBlockMillis", readBlockMillis);
        positive(bad, "adminTimeoutMillis", adminTimeoutMillis);
        positive(bad, "retentionIntervalMillis", retentionIntervalMillis);
        if (retryBackoffMillis < 0) bad.add("retryBackoffMillis must be >= 0, got " + retryBackoffMillis);
        if (stopJoinMillis < 0) bad.add("stopJoinMillis must be >= 0, got " + stopJoinMillis);
        if (journalBatch <= 0) bad.add("journalBatch must be > 0, got " + journalBatch);
        if (maxParallelism <= 0) bad.add("maxParallelism must be > 0, got " + maxParallelism);
        if (localQueueCapacity <= 0) bad.add("localQueueCapacity must be > 0, got " + localQueueCapacity);
        if (maxDeliveries < 1) bad.add("maxDeliveries must be >= 1, got " + maxDeliveries);
        if (retentionBatch <= 0) bad.add("retentionBatch must be > 0, got " + retentionBatch);
        if (!bad.isEmpty()) throw new PipelinerException("invalid RuntimeConfig: " + String.join("; ", bad));
    }

    private static void positive(List<String> bad, String name, long v) {
        if (v <= 0) bad.add(name + " must be > 0, got " + v);
    }
}
