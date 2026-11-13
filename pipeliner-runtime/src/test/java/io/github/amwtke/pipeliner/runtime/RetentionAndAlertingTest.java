package io.github.amwtke.pipeliner.runtime;

import static io.github.amwtke.pipeliner.runtime.fake.TestKit.*;
import static org.junit.jupiter.api.Assertions.*;

import io.github.amwtke.pipeliner.api.Row;
import io.github.amwtke.pipeliner.core.engine.RuntimeConfig;
import io.github.amwtke.pipeliner.core.graph.ExecMode;
import io.github.amwtke.pipeliner.core.graph.PipelineGraph;
import io.github.amwtke.pipeliner.core.store.*;
import io.github.amwtke.pipeliner.core.util.ReflectionComponentFactory;
import io.github.amwtke.pipeliner.core.util.SimpleDataSourceRegistry;
import io.github.amwtke.pipeliner.runtime.fake.InMemoryControlPlaneStore;
import io.github.amwtke.pipeliner.runtime.fake.InMemoryDataPlane;
import io.github.amwtke.pipeliner.runtime.fake.RecordingMetrics;
import io.github.amwtke.pipeliner.runtime.master.RetentionJanitor;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

/** Retention janitor and stalled-chunk detection against the in-memory fakes. */
class RetentionAndAlertingTest {
    private static final long HOUR = 3_600_000L;

    private static Services services(RuntimeConfig cfg, InMemoryControlPlaneStore store, InMemoryDataPlane plane, RecordingMetrics m) {
        return new Services(cfg, store, plane, new SimpleDataSourceRegistry(new HashMap<String, javax.sql.DataSource>()), m,
                new GraphRegistry(null), new ReflectionComponentFactory());
    }

    private static List<String> finishedRounds(InMemoryControlPlaneStore store, int n) {
        List<String> ids = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            String rid = "jr-" + i + "-" + System.nanoTime();
            store.createRound(RoundRecord.builder().roundId(rid).graphName("g").bizKey("b" + i).planHash("h").params(new HashMap<String, String>())
                            .status(RoundStatus.CREATED).createdAt(1L).build(),
                    Collections.singletonList(RunRecord.builder().roundId(rid).runId("r").status(RunStatus.PENDING).build()),
                    Collections.<GateRecord>emptyList(),
                    Collections.singletonList(PipelineRecord.builder().roundId(rid).pipelineId("p").runId("r").status(PipelineStatus.PENDING).build()));
            assertTrue(store.casRoundStatus(rid, RoundStatus.CREATED, RoundStatus.RUNNING, null, 1));
            assertTrue(store.casRoundStatus(rid, RoundStatus.RUNNING, RoundStatus.SUCCEEDED, null, 1));
            ids.add(rid);
        }
        return ids;
    }

    private static RuntimeConfig.RuntimeConfigBuilder rcfg() {
        return cfg("janitor").retentionEnabled(true).chunkRetentionMillis(0).roundRetentionMillis(Long.MAX_VALUE / 4).retentionIntervalMillis(600_000)
                .retentionBatch(2);
    }

    @Test void onlyTheLeaderRunsTheJanitor() throws Exception {
        InMemoryControlPlaneStore store = new InMemoryControlPlaneStore();
        InMemoryDataPlane plane = new InMemoryDataPlane();
        RecordingMetrics m = new RecordingMetrics();
        finishedRounds(store, 3);
        Thread.sleep(5);
        AtomicBoolean leader = new AtomicBoolean(false);
        RetentionJanitor j = new RetentionJanitor(services(rcfg().build(), store, plane, m), leader::get);
        assertFalse(j.runIfDue(System.currentTimeMillis()));
        assertEquals(0, store.purgeCalls.get() + store.findCalls.get() + store.compactCalls.get());
        assertEquals(0, m.retentionPasses.get());
        leader.set(true);
        assertTrue(j.runIfDue(System.currentTimeMillis()));
        assertTrue(store.findCalls.get() > 0);
    }

    @Test void passesAreBoundedAndRateLimited() throws Exception {
        InMemoryControlPlaneStore store = new InMemoryControlPlaneStore();
        InMemoryDataPlane plane = new InMemoryDataPlane();
        RecordingMetrics m = new RecordingMetrics();
        List<String> ids = finishedRounds(store, 5);
        Thread.sleep(5);
        RetentionJanitor j = new RetentionJanitor(services(rcfg().build(), store, plane, m), () -> true);
        long t0 = System.currentTimeMillis();
        assertTrue(j.runIfDue(t0));
        assertEquals(2, m.compacted.get(), "batch of 2 per pass");
        assertEquals(2, store.compactCalls.get());
        assertFalse(j.runIfDue(t0 + 1000), "interval not elapsed");
        assertEquals(2, store.compactCalls.get());
        assertTrue(j.runIfDue(t0 + 600_001));
        assertTrue(j.runIfDue(t0 + 1_200_002));
        assertEquals(5, m.compacted.get());
        for (String id : ids) assertTrue(store.loadRoundRecord(id).isCompacted());
        assertTrue(j.runIfDue(t0 + 1_800_003));
        assertEquals(5, m.compacted.get(), "nothing left to compact");
        assertEquals(0, m.purged.get(), "round retention not reached");
        assertEquals(4, m.retentionPasses.get());
    }

    @Test void purgesOldRoundsPrunesWorkersAndHonoursTheSwitch() throws Exception {
        InMemoryControlPlaneStore store = new InMemoryControlPlaneStore();
        InMemoryDataPlane plane = new InMemoryDataPlane();
        RecordingMetrics m = new RecordingMetrics();
        List<String> ids = finishedRounds(store, 3);
        plane.putWorker("ancient", System.currentTimeMillis() - HOUR);
        plane.putWorker("fresh", System.currentTimeMillis());
        Thread.sleep(5);
        RuntimeConfig on = rcfg().roundRetentionMillis(0).workerDeadMillis(1000).build();
        RetentionJanitor off = new RetentionJanitor(services(on.toBuilder().retentionEnabled(false).build(), store, plane, m), () -> true);
        assertFalse(off.runIfDue(System.currentTimeMillis()));
        assertEquals(3, store.roundCount());
        RetentionJanitor j = new RetentionJanitor(services(on, store, plane, m), () -> true);
        long t0 = System.currentTimeMillis();
        assertTrue(j.runIfDue(t0));
        assertEquals(2, m.purged.get(), "batch of 2");
        assertEquals(1, store.roundCount());
        assertEquals(1, m.prunedWorkers.get());
        assertEquals(2, plane.purgedRoundData.size(), "leftover data plane keys of the purged rounds are removed");
        assertTrue(plane.liveWorkers(HOUR * 2).containsKey("fresh"));
        assertFalse(plane.liveWorkers(HOUR * 2).containsKey("ancient"));
        assertTrue(j.runIfDue(t0 + 600_001));
        assertEquals(3, m.purged.get());
        assertEquals(0, store.roundCount());
        for (String id : ids) assertNull(store.loadRoundRecord(id));
    }

    @Test void aFailingStoreNeverBreaksThePass() {
        InMemoryControlPlaneStore store = new InMemoryControlPlaneStore() {
            @Override public synchronized List<String> purgeRounds(long cutoff, int max) { throw new IllegalStateException("db down"); }
        };
        RecordingMetrics m = new RecordingMetrics();
        RetentionJanitor j = new RetentionJanitor(services(rcfg().build(), store, new InMemoryDataPlane(), m), () -> true);
        assertTrue(j.runIfDue(System.currentTimeMillis()));
        assertEquals(1, m.retentionPasses.get());
    }

    @Test void leaderPurgesFinishedRoundsEndToEnd() throws Exception {
        String sink = uniqueName("s");
        PipelineGraph g = PipelineGraph.builder("pipe")
                .pipeline("a", p -> p.source(new RangeSource(3, 4)).chunkRows(3).batch((rs, out, ctx) -> { for (Row r : rs) out.emit(r.copy()); }).sink(collect(sink)))
                .build();
        RecordingMetrics m = new RecordingMetrics();
        try (Harness h = new Harness(g, Collections.<String, ExecMode>emptyMap())) {
            RuntimeConfig c = cfg("pod1").retentionEnabled(true).chunkRetentionMillis(0).roundRetentionMillis(HOUR).retentionIntervalMillis(50).build();
            DefaultPipelinerEngine e = new DefaultPipelinerEngine(c, h.store, h.plane, new SimpleDataSourceRegistry(new HashMap<String, javax.sql.DataSource>()), m);
            e.registerPlan(h.plan());
            e.start();
            try {
                String round = e.submit("pipe", "k", null);
                assertEquals(RoundStatus.SUCCEEDED, e.awaitRound(round, 15000));
                assertTrue(waitFor(() -> h.store.loadRoundRecord(round).isCompacted(), 5000), "compacted after the chunk retention");
                assertEquals(0, h.store.countChunks(round, null, null));
                assertEquals(Long.valueOf(3), e.view(round).getChunkCounts().get("a").get(ChunkStatus.DONE));
                assertEquals(1, m.roundsFinished.size());
                assertTrue(m.roundsFinished.get(0).endsWith("|SUCCEEDED") && m.roundsFinished.get(0).startsWith("pipe|"));
                assertTrue(m.leaderChanges.get() >= 1);
                assertTrue(m.chunks.get() >= 3);
                assertEquals(0, m.purged.get(), "round retention is 1h");
            } finally {
                e.halt();
            }
        }
    }

    @Test void stalledPendingChunkIsReportedOncePerEntry() throws Exception {
        AtomicBoolean hung = new AtomicBoolean();
        CountDownLatch hungLatch = new CountDownLatch(1);
        PipelineGraph g = PipelineGraph.builder("pipe")
                .pipeline("a", p -> p.source(new RangeSource(2, 2)).chunkRows(2)
                        .batch((rs, out, ctx) -> {
                            if (hung.compareAndSet(false, true)) { hungLatch.countDown(); Thread.sleep(Long.MAX_VALUE); }
                            for (Row r : rs) out.emit(r.copy());
                        }).sink(collect(uniqueName("s"))))
                .build();
        RecordingMetrics m = new RecordingMetrics();
        try (Harness h = new Harness(g, Collections.<String, ExecMode>emptyMap())) {
            // no renewals and no autoclaim within the test: the hung entry keeps idling
            RuntimeConfig c = cfg("pod1").stalledChunkAlertMillis(300).renewIntervalMillis(600_000).claimIdleMillis(600_000)
                    .autoClaimIntervalMillis(600_000).build();
            DefaultPipelinerEngine e = new DefaultPipelinerEngine(c, h.store, h.plane, new SimpleDataSourceRegistry(new HashMap<String, javax.sql.DataSource>()), m);
            e.registerPlan(h.plan());
            e.start();
            try {
                e.submit("pipe", "k", null);
                assertTrue(hungLatch.await(10, TimeUnit.SECONDS));
                assertTrue(waitFor(() -> m.stalled.size() >= 1, 10000), "stall alert");
                Thread.sleep(1000);
                assertEquals(Collections.singletonList("pipe|a"), m.stalled, "one alert per (round, pipeline, entry)");
                assertTrue(m.maxOldestPendingMillis.get() >= 300);
                assertEquals(c.getHighWatermark(), m.highWatermark);
            } finally {
                e.halt();
            }
        }
    }

    @Test void healthyRoundDoesNotRaiseStalledAlerts() throws Exception {
        PipelineGraph g = PipelineGraph.builder("pipe")
                .pipeline("a", p -> p.source(new RangeSource(4, 2)).chunkRows(2).batch((rs, out, ctx) -> { for (Row r : rs) out.emit(r.copy()); })
                        .sink(collect(uniqueName("s"))))
                .build();
        RecordingMetrics m = new RecordingMetrics();
        try (Harness h = new Harness(g, Collections.<String, ExecMode>emptyMap())) {
            RuntimeConfig c = cfg("pod1").stalledChunkAlertMillis(60_000).build();
            DefaultPipelinerEngine e = new DefaultPipelinerEngine(c, h.store, h.plane, new SimpleDataSourceRegistry(new HashMap<String, javax.sql.DataSource>()), m);
            e.registerPlan(h.plan());
            e.start();
            try {
                assertEquals(RoundStatus.SUCCEEDED, e.awaitRound(e.submit("pipe", "k", null), 15000));
                assertTrue(m.stalled.isEmpty());
                assertTrue(waitFor(() -> m.liveInstancesSeen >= 0 && m.activeRoundsSeen >= 0, 3000));
            } finally {
                e.halt();
            }
        }
    }
}
