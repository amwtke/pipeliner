package io.github.amwtke.pipeliner.runtime;

import static io.github.amwtke.pipeliner.runtime.fake.TestKit.*;
import static org.junit.jupiter.api.Assertions.*;

import io.github.amwtke.pipeliner.api.Row;
import io.github.amwtke.pipeliner.core.engine.MasterTickMode;
import io.github.amwtke.pipeliner.core.engine.RuntimeConfig;
import io.github.amwtke.pipeliner.core.engine.TickResult;
import io.github.amwtke.pipeliner.core.graph.ExecMode;
import io.github.amwtke.pipeliner.core.graph.PipelineGraph;
import io.github.amwtke.pipeliner.core.store.RoundStatus;
import io.github.amwtke.pipeliner.core.util.SimpleDataSourceRegistry;
import io.github.amwtke.pipeliner.runtime.fake.InMemoryControlPlaneStore;
import io.github.amwtke.pipeliner.runtime.fake.InMemoryDataPlane;
import java.util.*;
import java.util.concurrent.*;
import org.junit.jupiter.api.Test;

class MasterTickTest {
    static PipelineGraph graph(String sink) {
        return PipelineGraph.builder("pipe")
                .pipeline("a", p -> p.source(new RangeSource(6, 4)).chunkRows(3)
                        .batch((rs, out, ctx) -> { for (Row r : rs) out.emit(r.copy().set("a", 1L)); }))
                .pipeline("b", p -> p.row((row, out, ctx) -> out.emit(row.copy().set("v", row.getLong("id") * 2))).sink(collect(sink)))
                .edge("a", "b").build();
    }

    static RuntimeConfig ext(String pod) { return cfg(pod).masterTickMode(MasterTickMode.EXTERNAL).externalTickIntervalMillis(100).build(); }

    static boolean masterThreadAlive() {
        for (Thread t : Thread.getAllStackTraces().keySet()) if ("pg-master".equals(t.getName()) && t.isAlive()) return true;
        return false;
    }

    /** Ticks the pods in turn until the round is terminal; returns all results. */
    static List<TickResult> drive(String round, io.github.amwtke.pipeliner.core.engine.PipelinerEngine viewer, DefaultPipelinerEngine... pods) throws Exception {
        List<TickResult> all = new ArrayList<>();
        long end = System.currentTimeMillis() + 20000;
        int i = 0;
        while (System.currentTimeMillis() < end) {
            all.add(pods[i++ % pods.length].tick());
            if (viewer.view(round).getRound().getStatus().terminal()) return all;
            Thread.sleep(10);
        }
        fail("round did not finish");
        return all;
    }

    @Test void externalModeStartsNoMasterThreadAndNothingAdvancesWithoutTick() throws Exception {
        String sink = uniqueName("s");
        try (Harness h = new Harness(graph(sink), Collections.<String, ExecMode>emptyMap())) {
            DefaultPipelinerEngine e = h.pod(ext("x1"), true);
            String round = e.submit("pipe", "k", null);
            Thread.sleep(400);
            assertFalse(masterThreadAlive());
            assertFalse(e.isLeader());
            assertEquals(RoundStatus.CREATED, e.view(round).getRound().getStatus());
        }
    }

    @Test void tickAdvancesRoundStepByStep() throws Exception {
        String sink = uniqueName("s");
        try (Harness h = new Harness(graph(sink), Collections.<String, ExecMode>emptyMap())) {
            DefaultPipelinerEngine e = h.pod(ext("x1"), true);
            String round = e.submit("pipe", "k", null);
            TickResult first = e.tick();
            assertTrue(first.isLeader());
            assertFalse(first.isSkipped());
            assertTrue(first.getEpoch() > 0);
            assertEquals(1, first.getRoundsTicked());
            List<TickResult> rs = drive(round, e, e);
            assertEquals(RoundStatus.SUCCEEDED, e.view(round).getRound().getStatus());
            assertTrue(rs.size() > 1, "needed several ticks");
            assertEquals(24, sinkRows(sink).size());
        }
    }

    @Test void concurrentTickOnOnePodRunsOnceAndSkipsTheOther() throws Exception {
        CountDownLatch entered = new CountDownLatch(1), release = new CountDownLatch(1);
        InMemoryDataPlane plane = new InMemoryDataPlane() {
            @Override public Set<String> activeRounds() {
                if (!Thread.currentThread().getName().equals("tick-ex")) return super.activeRounds();
                entered.countDown();
                try { release.await(10, TimeUnit.SECONDS); } catch (InterruptedException ignored) { Thread.currentThread().interrupt(); }
                return super.activeRounds();
            }
        };
        DefaultPipelinerEngine e = new DefaultPipelinerEngine(ext("x1"), new InMemoryControlPlaneStore(), plane,
                new SimpleDataSourceRegistry(new HashMap<String, javax.sql.DataSource>()), null);
        e.start();
        ExecutorService ex = Executors.newSingleThreadExecutor(r -> new Thread(r, "tick-ex"));
        try {
            Future<TickResult> slow = ex.submit(e::tick);
            assertTrue(entered.await(5, TimeUnit.SECONDS));
            TickResult second = e.tick();
            assertTrue(second.isSkipped());
            assertEquals("tick in progress", second.getSkipReason());
            release.countDown();
            TickResult r = slow.get(10, TimeUnit.SECONDS);
            assertFalse(r.isSkipped());
            assertTrue(r.isLeader());
        } finally {
            release.countDown();
            ex.shutdownNow();
            e.halt();
        }
    }

    @Test void twoPodsTickingAlternatelyOnlyLeaseHolderAdvancesAndMatchesLocalMode() throws Exception {
        String sinkExt = uniqueName("s"), sinkLocal = uniqueName("s");
        try (Harness h = new Harness(graph(sinkExt), Collections.<String, ExecMode>emptyMap())) {
            RuntimeConfig c = ext("x1").toBuilder().leaderTtlMillis(5000).build();
            DefaultPipelinerEngine a = h.pod(c, true), b = h.pod(c.toBuilder().podId("x2").build(), true);
            String round = a.submit("pipe", "k", null);
            List<TickResult> rs = drive(round, a, a, b);
            int notLeader = 0;
            for (TickResult r : rs) {
                if (r.isSkipped()) { assertEquals("not leader", r.getSkipReason()); notLeader++; }
            }
            assertTrue(a.isLeader());
            assertFalse(b.isLeader());
            assertTrue(notLeader > 0);
            assertEquals(RoundStatus.SUCCEEDED, a.view(round).getRound().getStatus());
        }
        try (Harness h = new Harness(graph(sinkLocal), Collections.<String, ExecMode>emptyMap())) {
            DefaultPipelinerEngine e = h.pod("local1");
            String round = e.submit("pipe", "k", null);
            assertEquals(RoundStatus.SUCCEEDED, e.awaitRound(round, 15000));
        }
        assertEquals(sinkRows(sinkLocal), sinkRows(sinkExt));
    }

    @Test void leadershipHandsOverWhenTriggerHitsAnotherPodAfterTtl() throws Exception {
        String sink = uniqueName("s");
        try (Harness h = new Harness(graph(sink), Collections.<String, ExecMode>emptyMap())) {
            DefaultPipelinerEngine a = h.pod(ext("x1"), true), b = h.pod(ext("x2"), true);
            TickResult ra = a.tick();
            assertTrue(ra.isLeader());
            assertTrue(b.tick().isSkipped());
            Thread.sleep(800);   // leaderTtl 600: the lease keeper of the live leader keeps it, no handover
            assertTrue(b.tick().isSkipped(), "a live leader keeps its lease between triggers");
            a.halt();   // the leader dies (no release): after the TTL the next trigger on another pod takes over
            Thread.sleep(800);
            TickResult rb = b.tick();
            assertTrue(rb.isLeader());
            assertFalse(rb.isSkipped());
            assertTrue(rb.getEpoch() > ra.getEpoch());
            assertFalse(a.isLeader());
            assertTrue(b.isLeader());
        }
    }

    @Test void masterDisabledSkips() throws Exception {
        try (Harness h = new Harness(graph(uniqueName("s")), Collections.<String, ExecMode>emptyMap())) {
            DefaultPipelinerEngine e = h.pod(ext("x1").toBuilder().masterEnabled(false).build(), true);
            TickResult r = e.tick();
            assertTrue(r.isSkipped());
            assertEquals("master disabled", r.getSkipReason());
            assertFalse(r.isLeader());
        }
    }
}
