package io.github.amwtke.pipeliner.core.engine;

import io.github.amwtke.pipeliner.core.plan.PlanOptions;
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
    /** Null means PlanOptions.defaults(). */
    private PlanOptions planOptions;
}
