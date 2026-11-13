package io.github.amwtke.pipeliner.spring;

import static org.junit.jupiter.api.Assertions.*;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class MicrometerPipelinerMetricsTest {
    private final SimpleMeterRegistry reg = new SimpleMeterRegistry();
    private final AtomicInteger lookups = new AtomicInteger();
    private final MicrometerPipelinerMetrics m = new MicrometerPipelinerMetrics(reg, id -> { lookups.incrementAndGet(); return id.startsWith("net-") ? "net-demand" : null; });

    private long meters(String name) { return reg.getMeters().stream().filter(x -> x.getId().getName().equals(name)).count(); }

    private Gauge gauge(String name, String... kv) {
        io.micrometer.core.instrument.search.Search s = reg.find(name);
        for (int i = 0; i < kv.length; i += 2) s = s.tag(kv[i], kv[i + 1]);
        return s.gauge();
    }

    @Test void countersAndTimersAreTaggedByGraphNeverByRound() {
        for (int i = 0; i < 50; i++) {
            String round = "net-r" + i;
            m.chunkProcessed(round, "R1", "load", 10, 8, 120);
            m.chunkRetried(round, "load");
            m.deadLetter(round, "load");
            m.busyWait(round, "load", 1500);
            m.runFinished(round, "R1", "SUCCEEDED", 2000);
            m.roundFinished("net-demand", round, "SUCCEEDED");
        }
        m.gateTimeout("net-demand", "approve", "FAIL");
        m.stalledChunk("net-demand", "load");
        for (Meter x : reg.getMeters()) {
            if (x instanceof Gauge) continue;
            assertNull(x.getId().getTag("round"), x.getId() + " must not carry a round tag");
        }
        assertEquals(1, meters("pipeliner.chunks.processed"), "one series for 50 rounds");
        assertEquals(50, reg.get("pipeliner.chunks.processed").tag("graph", "net-demand").tag("pipeline", "load").tag("run", "R1").counter().count());
        assertEquals(500, reg.get("pipeliner.rows.in").counter().count());
        assertEquals(400, reg.get("pipeliner.rows.out").counter().count());
        assertEquals(50, reg.get("pipeliner.chunk.retries").counter().count());
        assertEquals(50, reg.get("pipeliner.dead.letters").counter().count());
        assertEquals(75.0, reg.get("pipeliner.busy.wait").counter().count(), 1e-9, "seconds");
        assertEquals(50, reg.get("pipeliner.chunk.duration").tag("graph", "net-demand").timer().count());
        assertEquals(6.0, reg.get("pipeliner.chunk.duration").timer().totalTime(TimeUnit.SECONDS), 1e-9);
        assertEquals(50, reg.get("pipeliner.run.duration").tag("graph", "net-demand").tag("run", "R1").tag("status", "SUCCEEDED").timer().count());
        assertEquals(50, reg.get("pipeliner.rounds.finished").tag("graph", "net-demand").tag("status", "SUCCEEDED").counter().count());
        assertEquals(1, reg.get("pipeliner.gate.timeouts").tag("graph", "net-demand").tag("gate", "approve").tag("policy", "FAIL").counter().count());
        assertEquals(1, reg.get("pipeliner.stalled.chunk.alerts").tag("graph", "net-demand").tag("pipeline", "load").counter().count());
        assertEquals(50, lookups.get(), "graph resolved once per round (cached), and unknown rounds map to 'unknown'");
    }

    @Test void registeredGraphsExportZeroSeriesSoIncreaseSeesTheFirstEvent() {
        m.graphRegistered("net-demand", java.util.Arrays.asList("load", "post"), java.util.Collections.singletonList("approve"));
        assertEquals(0, reg.get("pipeliner.dead.letters").tag("pipeline", "load").counter().count());
        assertEquals(0, reg.get("pipeliner.busy.wait").tag("pipeline", "post").counter().count());
        assertEquals(0, reg.get("pipeliner.stalled.chunk.alerts").tag("pipeline", "post").counter().count());
        assertEquals(0, reg.get("pipeliner.rounds.finished").tag("status", "FAILED").counter().count());
        assertEquals(0, reg.get("pipeliner.gate.timeouts").tag("policy", "FAIL").counter().count());
        m.deadLetter("net-x", "load");   // resolves to the same graph: increments the pre-registered series
        assertEquals(1, reg.get("pipeliner.dead.letters").tag("graph", "net-demand").tag("pipeline", "load").counter().count());
    }

    @Test void unknownRoundsFallBackToUnknownGraph() {
        m.chunkProcessed("other-1", "R1", "p", 1, 1, 1);
        assertEquals(1, reg.get("pipeliner.chunks.processed").tag("graph", "unknown").counter().count());
        m.chunkProcessed(null, null, "p", 1, 1, 1);
        assertNotNull(reg.find("pipeliner.chunks.processed").tag("graph", "none").counter());
    }

    @Test void roundGaugesAreRemovedWhenTheRoundEnds() {
        m.channelLength("net-a", "e1", 40);
        m.channelLength("net-a", "e2", 3);
        m.channelLength("net-b", "e1", 7);
        m.gateWaiting("net-a", "approve", 90_000);
        m.oldestPending("net-a", "load", 12_500);
        assertEquals(40, gauge("pipeliner.channel.length", "round", "net-a", "edge", "e1").value());
        assertEquals("net-demand", gauge("pipeliner.channel.length", "round", "net-a", "edge", "e1").getId().getTag("graph"));
        assertEquals(90.0, gauge("pipeliner.gate.waiting", "round", "net-a", "gate", "approve").value(), 1e-9, "seconds");
        assertEquals(12.5, gauge("pipeliner.oldest.pending", "round", "net-a", "pipeline", "load").value(), 1e-9);
        m.channelLength("net-a", "e1", 41);
        assertEquals(41, gauge("pipeliner.channel.length", "round", "net-a", "edge", "e1").value());
        assertEquals(3, meters("pipeliner.channel.length"));

        m.roundFinished("net-demand", "net-a", "SUCCEEDED");
        assertEquals(1, meters("pipeliner.channel.length"));
        assertNull(reg.find("pipeliner.gate.waiting").gauge());
        assertNull(reg.find("pipeliner.oldest.pending").gauge());
        assertNotNull(gauge("pipeliner.channel.length", "round", "net-b", "edge", "e1"));

        m.channelLength("net-a", "e1", 5);   // a late sample re-registers; the next end of round removes it again
        assertEquals(2, meters("pipeliner.channel.length"));
        m.roundFinished("net-demand", "net-a", "CANCELLED");
        assertEquals(1, meters("pipeliner.channel.length"));
    }

    @Test void leaderGaugeAndLossOfLeadershipClearsRoundGauges() {
        m.liveInstances("pod1", 2);
        assertEquals(0, gauge("pipeliner.leader", "pod", "pod1").value(), "every pod exports the gauge");
        assertEquals(2, gauge("pipeliner.live.instances", "pod", "pod1").value());
        m.leaderChanged("pod1", 7);
        m.activeRounds(3);
        m.channelLength("net-a", "e1", 40);
        assertEquals(1, gauge("pipeliner.leader", "pod", "pod1").value());
        assertEquals(3, reg.get("pipeliner.active.rounds").gauge().value());
        assertEquals(1, reg.get("pipeliner.leader.changes").counter().count());
        m.leaderLost("pod1");
        assertEquals(0, gauge("pipeliner.leader", "pod", "pod1").value());
        assertEquals(0, reg.get("pipeliner.active.rounds").gauge().value());
        assertNull(reg.find("pipeliner.channel.length").gauge(), "a pod that is no longer leader must not keep stale round gauges");
        assertEquals(2, gauge("pipeliner.live.instances", "pod", "pod1").value(), "process gauges stay");
    }

    @Test void retentionAndRedisGauges() {
        m.redisUsedMemory(123456);
        m.highWatermark(64);
        m.retentionPass(2, 3, 1);
        m.retentionPass(0, 4, 0);
        assertEquals(123456, reg.get("pipeliner.redis.used.memory").gauge().value());
        assertEquals(64, reg.get("pipeliner.channel.high.watermark").gauge().value());
        assertEquals(7, reg.get("pipeliner.retention.purged.rounds").counter().count());
        assertEquals(2, reg.get("pipeliner.retention.compacted.rounds").counter().count());
        assertTrue(reg.get("pipeliner.retention.last.run.timestamp").gauge().value() > 1_700_000_000.0);
    }
}
