package io.github.amwtke.pipeliner.spring;

import io.github.amwtke.pipeliner.core.engine.PipelinerMetrics;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/** PipelinerMetrics backed by a Micrometer registry; tags are round/run/pipeline where known. */
public class MicrometerPipelinerMetrics implements PipelinerMetrics {
    private final MeterRegistry reg;
    private final Map<String, AtomicLong> gauges = new ConcurrentHashMap<>();

    public MicrometerPipelinerMetrics(MeterRegistry reg) {
        this.reg = reg;
    }

    @Override public void chunkProcessed(String round, String run, String pipeline, long rowsIn, long rowsOut, long costMs) {
        reg.counter("pipeliner.chunk.processed", "round", n(round), "run", n(run), "pipeline", n(pipeline)).increment();
        reg.counter("pipeliner.rows.in", "round", n(round), "run", n(run), "pipeline", n(pipeline)).increment(rowsIn);
        reg.counter("pipeliner.rows.out", "round", n(round), "run", n(run), "pipeline", n(pipeline)).increment(rowsOut);
        Timer.builder("pipeliner.chunk.cost").tags("round", n(round), "run", n(run), "pipeline", n(pipeline))
                .publishPercentiles(0.5, 0.95).register(reg).record(Duration.ofMillis(Math.max(0, costMs)));
    }

    @Override public void chunkRetried(String round, String pipeline) {
        count("pipeliner.chunk.retried", "round", round, "pipeline", pipeline).increment();
    }

    @Override public void deadLetter(String round, String pipeline) {
        count("pipeliner.chunk.dead", "round", round, "pipeline", pipeline).increment();
    }

    @Override public void busyWait(String round, String pipeline, long millis) {
        count("pipeliner.busy.wait.ms", "round", round, "pipeline", pipeline).increment(millis);
    }

    @Override public void channelLength(String round, String edgeId, long length) {
        gauge("pipeliner.channel.length", length, "round", round, "edge", edgeId);
    }

    @Override public void gateWaiting(String round, String gateId, long millis) {
        gauge("pipeliner.gate.waiting.ms", millis, "round", round, "gate", gateId);
    }

    @Override public void leaderChanged(String podId, long epoch) {
        count("pipeliner.leader.changes", "pod", podId).increment();
        gauge("pipeliner.leader.epoch", epoch, "pod", podId);
    }

    @Override public void runFinished(String round, String run, String status, long millis) {
        Timer.builder("pipeliner.run.duration").tags("round", n(round), "run", n(run), "status", n(status)).register(reg)
                .record(Duration.ofMillis(Math.max(0, millis)));
    }

    private Counter count(String name, String... tags) {
        String[] t = tags.clone();
        for (int i = 1; i < t.length; i += 2) t[i] = n(t[i]);
        return reg.counter(name, t);
    }

    private void gauge(String name, long value, String... tags) {
        String[] t = tags.clone();
        StringBuilder key = new StringBuilder(name);
        for (int i = 1; i < t.length; i += 2) { t[i] = n(t[i]); key.append('|').append(t[i]); }
        AtomicLong holder = gauges.computeIfAbsent(key.toString(), k -> {
            AtomicLong a = new AtomicLong();
            Gauge.builder(name, a, AtomicLong::get).tags(t).register(reg);
            return a;
        });
        holder.set(value);
    }

    private static String n(String s) {
        return s == null ? "none" : s;
    }
}
