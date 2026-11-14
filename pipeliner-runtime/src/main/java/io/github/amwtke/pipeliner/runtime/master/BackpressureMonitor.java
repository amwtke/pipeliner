package io.github.amwtke.pipeliner.runtime.master;

import io.github.amwtke.pipeliner.core.dataplane.Keys;
import io.github.amwtke.pipeliner.runtime.RunKeys;
import java.util.Set;
import io.github.amwtke.pipeliner.core.engine.RuntimeConfig;
import io.github.amwtke.pipeliner.core.graph.ExecMode;
import io.github.amwtke.pipeliner.core.plan.PlanChannel;
import io.github.amwtke.pipeliner.core.plan.PlanPipeline;
import io.github.amwtke.pipeliner.core.plan.PlanRun;
import io.github.amwtke.pipeliner.core.store.PipelineRecord;
import io.github.amwtke.pipeliner.core.store.PipelineStatus;
import io.github.amwtke.pipeliner.core.store.RunStatus;
import java.util.HashMap;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;

/** Samples channel lengths, autoscales congested pipelines, and applies the global Redis memory throttle. */
@Slf4j
final class BackpressureMonitor {
    private final MasterContext c;
    private final Map<String, Long> lastScale = new java.util.concurrent.ConcurrentHashMap<>();
    private volatile boolean throttled;
    private long lastMemSample;

    BackpressureMonitor(MasterContext c) { this.c = c; }

    void monitor(RoundData d) {
        RuntimeConfig cfg = c.s.cfg;
        String roundId = d.roundId();
        c.s.metrics.highWatermark(cfg.getHighWatermark());
        for (PlanRun pr : d.plan.getRuns()) {
            if (pr.getMode() != ExecMode.CLUSTER || d.run(pr.getRunId()).getStatus() != RunStatus.RUNNING) continue;
            int attempt = d.run(pr.getRunId()).getAttempt();
            Map<String, Long> len = new HashMap<>();
            for (PlanChannel ch : RunKeys.channelsOf(d.plan, pr.getRunId())) {
                long n = c.s.plane.streamLength(Keys.channel(roundId, ch.getEdgeId(), attempt));
                len.put(ch.getEdgeId(), n);
                c.s.metrics.channelLength(roundId, ch.getEdgeId(), n);
            }
            if (!cfg.isAutoscale()) continue;
            for (String p : pr.getPipelineIds()) {
                PipelineRecord rec = d.pipe(p);
                PlanPipeline pp = d.plan.pipeline(p);
                if (rec.getStatus() != PipelineStatus.RUNNING || rec.getParallelism() >= cfg.getMaxParallelism()) continue;
                long in = 0;
                for (String stream : RunKeys.inputStreams(roundId, pp, attempt).keySet()) in += c.s.plane.streamLength(stream);
                boolean outputsFree = true;
                for (PlanChannel o : d.plan.outputsOf(p)) if (len.get(o.getEdgeId()) >= cfg.getHighWatermark()) outputsFree = false;
                String key = roundId + "|" + p;
                long now = System.currentTimeMillis();
                Long last = lastScale.get(key);
                if (in >= cfg.getHighWatermark() && outputsFree && (last == null || now - last >= cfg.getAutoscaleCooldownMillis())) {
                    int n = rec.getParallelism() + 1;
                    c.s.store.setPipelineParallelism(roundId, p, n, c.epoch());
                    c.s.plane.setDesiredParallelism(roundId, p, n, attempt, c.epoch());
                    lastScale.put(key, now);
                    log.info("autoscale {}/{} -> parallelism {}", roundId, p, n);
                }
            }
        }
    }

    /** Once per tick: global throttle of source pipelines by Redis memory. */
    void memoryThrottle() {
        long limit = c.s.cfg.getRedisMemoryLimitBytes();
        long now = System.currentTimeMillis();
        long used = -1;
        if (limit > 0 || now - lastMemSample >= 10_000) {
            used = c.s.plane.usedMemoryBytes();
            lastMemSample = now;
            c.s.metrics.redisUsedMemory(used);
        }
        if (limit <= 0) return;
        // the flag carries a TTL that is refreshed every tick while over the limit: a lost/deposed master cannot leave it set forever
        long ttl = throttleTtl();
        if (!throttled && used >= limit) { throttled = true; log.warn("redis memory {} >= {}: throttling sources", used, limit); }
        else if (throttled && used < limit * 0.8) { c.s.plane.setGlobalThrottle(false, 0); throttled = false; }
        if (throttled) c.s.plane.setGlobalThrottle(true, ttl);
    }

    /** TTL of the global throttle flag: must outlive the gap between two refreshes (a tick, or the lease keeper's ttl/3 in EXTERNAL mode). */
    private long throttleTtl() {
        long ttl = Math.max(3 * c.s.cfg.getTickMillis(), 5000L);
        if (c.s.cfg.getMasterTickMode() == io.github.amwtke.pipeliner.core.engine.MasterTickMode.EXTERNAL) ttl = Math.max(ttl, c.s.cfg.getLeaderTtlMillis());
        return ttl;
    }

    /** Lease keeper (EXTERNAL mode): re-arms the flag's TTL while this pod's last decision was "throttled", without sampling memory or ticking. */
    void refreshThrottleFlag() {
        if (throttled) c.s.plane.setGlobalThrottle(true, throttleTtl());
    }

    /** Drops the per-round autoscale cooldowns of rounds that are no longer active. */
    void retain(Set<String> active) {
        lastScale.keySet().removeIf(k -> !active.contains(k.substring(0, k.indexOf('|'))));
    }
}
