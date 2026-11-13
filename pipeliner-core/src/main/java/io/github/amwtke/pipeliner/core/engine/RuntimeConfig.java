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
    /** Null means PlanOptions.defaults(). */
    private PlanOptions planOptions;
}
