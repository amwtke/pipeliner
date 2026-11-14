package io.github.amwtke.pipeliner.spring;

import io.github.amwtke.pipeliner.core.engine.PipelinerMetrics;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

/**
 * PipelinerMetrics backed by a Micrometer registry (Prometheus names after conversion: pipeliner_*).
 * Cardinality rules: counters and timers are tagged by graph (+ pipeline / run / status / gate), never by round id. Gauges that carry a
 * {@code round} tag exist only while the round is active: they are removed on {@link #roundFinished}, on {@link #leaderLost} and when not
 * refreshed for {@link #STALE_MILLIS} (a gate that opened, a paused round). The round id to graph name lookup uses a bounded LRU cache.
 */
public class MicrometerPipelinerMetrics implements PipelinerMetrics {
    static final long STALE_MILLIS = 120_000;
    private static final long SWEEP_EVERY_MILLIS = 30_000;
    private static final int GRAPH_CACHE_SIZE = 1024;

    private static final class G {
        final AtomicLong value = new AtomicLong();
        volatile boolean valid = true;   // false exports NaN (no current sample)
        volatile Meter.Id id;
        final String round;   // null for process-level gauges (never swept)
        volatile long updated = System.currentTimeMillis();
        G(String round) { this.round = round; }
    }

    private final MeterRegistry reg;
    private final Function<String, String> graphOf;
    /** Guards gauge map + registry updates as one step (register / sweep / remove must not interleave). */
    private final Object lock = new Object();
    private final Map<String, G> gauges = new ConcurrentHashMap<>();
    private final Map<String, String> graphCache = java.util.Collections.synchronizedMap(new LinkedHashMap<String, String>(64, 0.75f, true) {
        @Override protected boolean removeEldestEntry(Map.Entry<String, String> e) { return size() > GRAPH_CACHE_SIZE; }
    });
    private volatile long lastSweep = System.currentTimeMillis();

    public MicrometerPipelinerMetrics(MeterRegistry reg) { this(reg, null); }

    /** @param graphOf resolves a round id to its graph name (null result = unknown); may be null. */
    public MicrometerPipelinerMetrics(MeterRegistry reg, Function<String, String> graphOf) {
        this.reg = reg;
        this.graphOf = graphOf;
        reg.counter("pipeliner.leader.changes");   // pre-registered at 0 so increase()/rate() see the first election of every process
    }

    // ------------------------------------------------------------ hooks

    @Override public void chunkProcessed(String round, String run, String pipeline, long rowsIn, long rowsOut, long costMs) {
        Tags t = Tags.of("graph", graph(round), "pipeline", n(pipeline), "run", n(run));
        reg.counter("pipeliner.chunks.processed", t).increment();
        reg.counter("pipeliner.rows.in", t).increment(rowsIn);
        reg.counter("pipeliner.rows.out", t).increment(rowsOut);
        Timer.builder("pipeliner.chunk.duration").tags(t).register(reg).record(Duration.ofMillis(Math.max(0, costMs)));
    }

    @Override public void chunkRetried(String round, String pipeline) {
        reg.counter("pipeliner.chunk.retries", "graph", graph(round), "pipeline", n(pipeline)).increment();
    }

    @Override public void deadLetter(String round, String pipeline) {
        reg.counter("pipeliner.dead.letters", "graph", graph(round), "pipeline", n(pipeline)).increment();
    }

    @Override public void busyWait(String round, String pipeline, long millis) {
        io.micrometer.core.instrument.Counter.builder("pipeliner.busy.wait").baseUnit("seconds").tags("graph", graph(round), "pipeline", n(pipeline))
                .register(reg).increment(Math.max(0, millis) / 1000.0);
    }

    @Override public void channelLength(String round, String edgeId, long length) {
        gauge("pipeliner.channel.length", null, 1, length, round, "graph", graph(round), "round", n(round), "edge", n(edgeId));
    }

    @Override public void gateWaiting(String round, String gateId, long millis) {
        gauge("pipeliner.gate.waiting", "seconds", 1000, millis, round, "graph", graph(round), "round", n(round), "gate", n(gateId));
    }

    @Override public void graphRegistered(String graph, java.util.Collection<String> pipelineIds, java.util.Collection<String> gateIds) {
        for (String p : pipelineIds) {
            reg.counter("pipeliner.chunk.retries", "graph", graph, "pipeline", p);
            reg.counter("pipeliner.dead.letters", "graph", graph, "pipeline", p);
            reg.counter("pipeliner.stalled.chunk.alerts", "graph", graph, "pipeline", p);
            io.micrometer.core.instrument.Counter.builder("pipeliner.busy.wait").baseUnit("seconds").tags("graph", graph, "pipeline", p).register(reg);
        }
        for (String status : new String[]{"SUCCEEDED", "FAILED", "CANCELLED"}) reg.counter("pipeliner.rounds.finished", "graph", graph, "status", status);
        for (String g : gateIds)
            for (String policy : new String[]{"FAIL", "OPEN_WITH_FALLBACK", "ALERT_AND_WAIT"}) reg.counter("pipeliner.gate.timeouts", "graph", graph, "gate", g, "policy", policy);
    }

    @Override public void gateTimeout(String graph, String gateId, String policy) {
        reg.counter("pipeliner.gate.timeouts", "graph", n(graph), "gate", n(gateId), "policy", n(policy)).increment();
    }

    @Override public void leaderChanged(String podId, long epoch) {
        reg.counter("pipeliner.leader.changes").increment();
        leaderGauge(podId).value.set(1);
    }

    @Override public void leaderLost(String podId) {
        leaderGauge(podId).value.set(0);
        removeRoundGauges(null);   // another pod owns the rounds now: this pod's round gauges would only go stale
        activeRounds(0);
        gauge("pipeliner.redis.used.memory", "bytes", 1, null, null).valid = false;   // only the leader samples it: a former leader must not keep exporting its last value
    }

    @Override public void masterTick(long durationMillis) {
        Timer.builder("pipeliner.master.tick.duration").register(reg).record(Duration.ofMillis(Math.max(0, durationMillis)));
        gauge("pipeliner.master.last.tick.timestamp", "seconds", 1, System.currentTimeMillis() / 1000, null);
    }

    @Override public void runFinished(String round, String run, String status, long millis) {
        Timer.builder("pipeliner.run.duration").tags("graph", graph(round), "run", n(run), "status", n(status)).register(reg)
                .record(Duration.ofMillis(Math.max(0, millis)));
    }

    @Override public void roundFinished(String graph, String round, String status) {
        reg.counter("pipeliner.rounds.finished", "graph", n(graph), "status", n(status)).increment();
        removeRoundGauges(round);
    }

    @Override public void activeRounds(int count) { gauge("pipeliner.active.rounds", null, 1, count, null); }

    @Override public void liveInstances(String podId, int count) {
        leaderGauge(podId);   // make sure every pod exports pipeliner_leader (0 until it leads)
        gauge("pipeliner.live.instances", null, 1, count, null, "pod", n(podId));
    }

    @Override public void highWatermark(int value) { gauge("pipeliner.channel.high.watermark", null, 1, value, null); }

    @Override public void oldestPending(String round, String pipeline, long idleMillis) {
        gauge("pipeliner.oldest.pending", "seconds", 1000, idleMillis, round, "graph", graph(round), "round", n(round), "pipeline", n(pipeline));
    }

    @Override public void stalledChunk(String graph, String pipeline) {
        reg.counter("pipeliner.stalled.chunk.alerts", "graph", n(graph), "pipeline", n(pipeline)).increment();
    }

    @Override public void redisUsedMemory(long bytes) { gauge("pipeliner.redis.used.memory", "bytes", 1, bytes, null); }

    @Override public void retentionPass(int compactedRounds, int purgedRounds, int prunedWorkers) {
        reg.counter("pipeliner.retention.purged.rounds").increment(purgedRounds);
        reg.counter("pipeliner.retention.compacted.rounds").increment(compactedRounds);
        gauge("pipeliner.retention.last.run.timestamp", "seconds", 1, System.currentTimeMillis() / 1000, null);
    }

    // ------------------------------------------------------------ internals

    private G leaderGauge(String pod) { return gauge("pipeliner.leader", null, 1, null, null, "pod", n(pod)); }

    /** Registers (once) and updates a gauge; value null leaves the current value. {@code scale} divides the stored value for display. */
    private G gauge(String name, String unit, double scale, Number value, String round, String... tags) {
        synchronized (lock) {
        long now = System.currentTimeMillis();
        sweep(now);
        StringBuilder key = new StringBuilder(name);
        for (int i = 1; i < tags.length; i += 2) key.append('|').append(tags[i]);
        G g = gauges.computeIfAbsent(key.toString(), k -> {
            G h = new G(round);
            Gauge.Builder<AtomicLong> b = Gauge.builder(name, h.value, v -> h.valid ? v.get() / scale : Double.NaN).tags(tags);
            if (unit != null) b.baseUnit(unit);
            h.id = b.register(reg).getId();
            return h;
        });
        g.updated = now;
        if (value != null) { g.value.set(value.longValue()); g.valid = true; }
        return g;
        }
    }

    private void removeRoundGauges(String round) {
        synchronized (lock) {
            for (Map.Entry<String, G> e : gauges.entrySet()) {
                G g = e.getValue();
                if (g.round != null && (round == null || round.equals(g.round)) && gauges.remove(e.getKey(), g)) reg.remove(g.id);
            }
        }
        if (round != null) graphCache.remove(round);
    }

    /** Called with {@link #lock} held. */
    private void sweep(long now) {
        if (now - lastSweep < SWEEP_EVERY_MILLIS) return;
        lastSweep = now;
        for (Map.Entry<String, G> e : gauges.entrySet()) {
            G g = e.getValue();
            if (g.round != null && now - g.updated > STALE_MILLIS && gauges.remove(e.getKey(), g)) reg.remove(g.id);
        }
    }

    private String graph(String round) {
        if (round == null) return "none";
        String g = graphCache.get(round);
        if (g != null) return g;
        try {
            g = graphOf == null ? null : graphOf.apply(round);
        } catch (RuntimeException e) {
            return "unknown";   // transient lookup failure: not cached, retried on the next hook
        }
        if (g == null) g = "unknown";
        graphCache.put(round, g);
        return g;
    }

    private static String n(String s) { return s == null ? "none" : s; }
}
