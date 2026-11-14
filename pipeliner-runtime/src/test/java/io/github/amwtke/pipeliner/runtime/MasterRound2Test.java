package io.github.amwtke.pipeliner.runtime;

import static io.github.amwtke.pipeliner.runtime.fake.TestKit.*;
import static org.junit.jupiter.api.Assertions.*;

import io.github.amwtke.pipeliner.api.GateCondition;
import io.github.amwtke.pipeliner.api.GateDecision;
import io.github.amwtke.pipeliner.api.Row;
import io.github.amwtke.pipeliner.core.dataplane.Keys;
import io.github.amwtke.pipeliner.core.dataplane.LocalRunAssignment;
import io.github.amwtke.pipeliner.core.dataplane.StreamEntry;
import io.github.amwtke.pipeliner.core.engine.MasterTickMode;
import io.github.amwtke.pipeliner.core.engine.RuntimeConfig;
import io.github.amwtke.pipeliner.core.engine.TickResult;
import io.github.amwtke.pipeliner.core.graph.ExecMode;
import io.github.amwtke.pipeliner.core.graph.Gates;
import io.github.amwtke.pipeliner.core.graph.PipelineGraph;
import io.github.amwtke.pipeliner.core.store.*;
import io.github.amwtke.pipeliner.core.util.ReflectionComponentFactory;
import io.github.amwtke.pipeliner.core.util.SimpleDataSourceRegistry;
import io.github.amwtke.pipeliner.runtime.context.GateFallbackCache;
import io.github.amwtke.pipeliner.runtime.fake.InMemoryControlPlaneStore;
import io.github.amwtke.pipeliner.runtime.fake.InMemoryDataPlane;
import io.github.amwtke.pipeliner.runtime.fake.RecordingMetrics;
import io.github.amwtke.pipeliner.runtime.fake.TestPlanner;
import io.github.amwtke.pipeliner.runtime.master.RetentionJanitor;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

/** Review round 2, master side (g1b): lease keeper, fairness, enumerate failures, data-plane loss, local siblings, retention, caches. */
class MasterRound2Test {
    private static RuntimeConfig ext(String pod) { return cfg(pod).masterTickMode(MasterTickMode.EXTERNAL).externalTickIntervalMillis(100).build(); }

    private static DefaultPipelinerEngine engine(RuntimeConfig c, InMemoryControlPlaneStore store, InMemoryDataPlane plane, PipelineGraph g, Map<String, ExecMode> modes) {
        DefaultPipelinerEngine e = new DefaultPipelinerEngine(c, store, plane, new SimpleDataSourceRegistry(new HashMap<String, javax.sql.DataSource>()), null, new ReflectionComponentFactory());
        e.registerPlan(TestPlanner.plan(g, modes));
        e.start();
        return e;
    }

    private static PipelineGraph simple(String name) {
        return PipelineGraph.builder(name).pipeline("p", p -> p.source(new RangeSource(3, 2)).sink(collect(uniqueName("s")))).build();
    }

    // F031 / F032
    @Test void leaseKeeperRenewsTheLeaseOfALeadingPodBetweenExternalTicks() throws Exception {
        InMemoryDataPlane plane = new InMemoryDataPlane();
        DefaultPipelinerEngine e = engine(ext("x1"), new InMemoryControlPlaneStore(), plane, simple("lk"), Collections.<String, ExecMode>emptyMap());
        try {
            TickResult t = e.tick();
            assertTrue(t.isLeader());
            Thread.sleep(2000);   // ttl 600 ms: without the keeper the lease would be long gone
            assertEquals("x1", plane.currentLeader());
            assertTrue(e.isLeader());
            assertEquals(t.getEpoch(), e.tick().getEpoch(), "no new epoch: leadership never lapsed");
        } finally { e.halt(); }
    }

    @Test void leaseKeeperNeverAcquiresLeadership() throws Exception {
        InMemoryDataPlane plane = new InMemoryDataPlane();
        DefaultPipelinerEngine e = engine(ext("x1"), new InMemoryControlPlaneStore(), plane, simple("lk2"), Collections.<String, ExecMode>emptyMap());
        try {
            Thread.sleep(900);
            assertNull(plane.currentLeader());
            assertFalse(e.isLeader());
        } finally { e.halt(); }
    }

    // F007
    @Test void anErrorFromAGateConditionDoesNotKillOrWedgeTheMaster() throws Exception {
        GateCondition boom = ctx -> { throw new NoClassDefFoundError("boom"); };
        PipelineGraph g = PipelineGraph.builder("er")
                .pipeline("p1", p -> p.source(new RangeSource(1, 1)).sink(collect(uniqueName("a"))))
                .pipeline("p2", p -> p.source(new RangeSource(1, 1)).sink(collect(uniqueName("b"))))
                .gate("G", Gates.condition(boom), gb -> gb.after("p1").before("p2")).build();
        InMemoryDataPlane plane = new InMemoryDataPlane();
        DefaultPipelinerEngine e = engine(ext("x1"), new InMemoryControlPlaneStore(), plane, g, Collections.<String, ExecMode>emptyMap());
        try {
            e.submit("er", "k", null);
            for (int i = 0; i < 40; i++) { TickResult t = assertDoesNotThrow(e::tick); assertFalse(t.isSkipped()); Thread.sleep(20); }
            assertTrue(e.isLeader());
            assertEquals("x1", plane.currentLeader());
        } finally { e.halt(); }
    }

    // F029
    @Test void aSlowRoundDoesNotStarveTheRoundsBehindIt() throws Exception {
        AtomicBoolean slow = new AtomicBoolean();
        String[] slowId = new String[1];
        InMemoryControlPlaneStore store = new InMemoryControlPlaneStore() {
            @Override public RoundRecord loadRoundRecord(String roundId) {
                if (slow.get() && roundId.equals(slowId[0])) try { Thread.sleep(450); } catch (InterruptedException ignored) { Thread.currentThread().interrupt(); }
                return super.loadRoundRecord(roundId);
            }
        };
        InMemoryDataPlane plane = new InMemoryDataPlane();
        // LOCAL mode with a very long tick interval: pg-master ticks once at start, then sleeps; the test drives the ticks (no lease keeper in this mode)
        DefaultPipelinerEngine e = engine(cfg("x1").tickMillis(600_000).build(), store, plane, simple("fair"), Collections.<String, ExecMode>emptyMap());
        Thread.sleep(300);
        try {
            String r1 = e.submit("fair", "k1", null), r2 = e.submit("fair", "k2", null);
            List<String> ids = new ArrayList<>(Arrays.asList(r1, r2));
            Collections.sort(ids);
            slowId[0] = ids.get(0);
            slow.set(true);
            e.tick();
            assertNotEquals(RoundStatus.CREATED, store.loadRoundRecord(ids.get(0)).getStatus());
            assertEquals(RoundStatus.CREATED, store.loadRoundRecord(ids.get(1)).getStatus(), "tick 1 aborted after the slow round");
            e.tick();
            assertNotEquals(RoundStatus.CREATED, store.loadRoundRecord(ids.get(1)).getStatus(), "tick 2 resumes behind the slow round");
        } finally { e.halt(); }
    }

    // F030
    @Test void transientEnqueueFailureIsRetriedAndNeverFailsThePipeline() throws Exception {
        AtomicInteger fails = new AtomicInteger(2);
        InMemoryDataPlane plane = new InMemoryDataPlane() {
            @Override public boolean enqueueSplit(String r, String p, int attempt, io.github.amwtke.pipeliner.api.Split s, long epoch) {
                if (fails.getAndDecrement() > 0) throw new IllegalStateException("redis timeout");
                return super.enqueueSplit(r, p, attempt, s, epoch);
            }
        };
        InMemoryControlPlaneStore store = new InMemoryControlPlaneStore();
        DefaultPipelinerEngine e = engine(cfg("x1").build(), store, plane, simple("enq"), Collections.<String, ExecMode>emptyMap());
        try {
            String round = e.submit("enq", "k", null);
            assertEquals(RoundStatus.SUCCEEDED, e.awaitRound(round, 15000));
            assertTrue(fails.get() < 0, "enqueue was attempted again after the failures");
            for (PipelineRecord p : e.view(round).getPipelines()) assertNull(p.getError());
        } finally { e.halt(); }
    }

    @Test void userEnumerateFailureStillFailsThePipeline() throws Exception {
        PipelineGraph g = PipelineGraph.builder("enf").pipeline("p", p -> p.source(new io.github.amwtke.pipeliner.api.SplitSource() {
            @Override public String datasetName() { return null; }
            @Override public List<io.github.amwtke.pipeliner.api.Split> enumerate(io.github.amwtke.pipeliner.api.SourceContext ctx) { throw new IllegalArgumentException("bad source"); }
            @Override public List<Row> read(io.github.amwtke.pipeliner.api.Split s, io.github.amwtke.pipeliner.api.SourceContext ctx) { return Collections.emptyList(); }
        }).sink(collect(uniqueName("s")))).build();
        try (Harness h = new Harness(g, Collections.<String, ExecMode>emptyMap())) {
            DefaultPipelinerEngine e = h.pod("pod");
            String round = e.submit("enf", "k", null);
            assertEquals(RoundStatus.FAILED, e.awaitRound(round, 15000));
            assertTrue(e.view(round).getPipelines().get(0).getError().contains("enumerate failed"));
        }
    }

    // F057
    @Test void lostDataPlaneFailsTheRunWithAClearError() throws Exception {
        InMemoryDataPlane plane = new InMemoryDataPlane() {   // nobody consumes: the chunks stay PENDING
            @Override public List<StreamEntry> read(String consumer, List<String> keys, int count, long block) {
                try { Thread.sleep(Math.max(10, Math.min(block, 50))); } catch (InterruptedException ignored) { Thread.currentThread().interrupt(); }
                return new ArrayList<>();
            }
        };
        InMemoryControlPlaneStore store = new InMemoryControlPlaneStore();
        DefaultPipelinerEngine e = engine(ext("x1"), store, plane, simple("lost"), Collections.<String, ExecMode>emptyMap());
        try {
            String round = e.submit("lost", "k", null);
            long epoch = 0;
            for (int i = 0; i < 6; i++) epoch = e.tick().getEpoch();
            assertTrue(store.loadRoundState(round).getPipelines().get(0).isEnumDone());
            assertEquals(PipelineStatus.RUNNING, e.view(round).getPipelines().get(0).getStatus(), "queued splits: not lost");
            plane.deleteKeys(round, Arrays.asList(Keys.splitQueue(round, "p", 0), Keys.splitIndex(round, "p", 0)), epoch);   // Redis flushed
            for (int i = 0; i < 4; i++) e.tick();
            PipelineRecord p = e.view(round).getPipelines().get(0);
            assertEquals(PipelineStatus.FAILED, p.getStatus());
            assertTrue(p.getError().contains("data plane lost") && p.getError().contains("rerunFrom"), p.getError());
            assertEquals(RoundStatus.FAILED, e.view(round).getRound().getStatus());
        } finally { e.halt(); }
    }

    // F064
    @Test void localRunFailureNamingOnePipelineFailsItsSiblingsToo() throws Exception {
        PipelineGraph g = PipelineGraph.builder("sib")
                .pipeline("a", p -> p.source(new RangeSource(2, 2)).row((row, out, ctx) -> { throw new IllegalStateException("bad row"); }))
                .pipeline("b", p -> p.row((row, out, ctx) -> out.emit(row)).sink(collect(uniqueName("s"))))
                .edge("a", "b").build();
        try (Harness h = new Harness(g, mode(ExecMode.LOCAL, "a", "b"))) {
            DefaultPipelinerEngine e = h.pod("pod");
            String round = e.submit("sib", "k", null);
            assertEquals(RoundStatus.FAILED, e.awaitRound(round, 15000));
            for (PipelineRecord p : e.view(round).getPipelines()) assertEquals(PipelineStatus.FAILED, p.getStatus(), p.getPipelineId());
        }
    }

    // F127
    @Test void localRunAssignmentCodecIsRobust() {
        assertEquals("pod-1|3", LocalRunAssignment.format("pod-1", 3));
        assertEquals("pod-1", LocalRunAssignment.parse("pod-1|3").pod);
        assertEquals(3, LocalRunAssignment.parse("pod-1|3").attempt);
        assertEquals("a|b", LocalRunAssignment.parse("a|b|7").pod);
        for (String bad : new String[] {null, "", "nobar", "|3", "pod|", "pod|x"}) assertNull(LocalRunAssignment.parse(bad), String.valueOf(bad));
        assertNull(LocalRunAssignment.podOf("garbage"));
    }

    // F018
    @Test void fallbackCacheNeverServesAStaleNegativeAnswer() {
        InMemoryControlPlaneStore store = new InMemoryControlPlaneStore();
        String rid = "fc-" + System.nanoTime();
        store.createRound(RoundRecord.builder().roundId(rid).graphName("g").bizKey("b").planHash("h").params(new HashMap<String, String>())
                        .status(RoundStatus.CREATED).createdAt(1L).build(),
                Collections.singletonList(RunRecord.builder().roundId(rid).runId("r").status(RunStatus.PENDING).build()),
                Collections.singletonList(GateRecord.builder().roundId(rid).gateId("G").status(GateStatus.CLOSED).build()),
                Collections.singletonList(PipelineRecord.builder().roundId(rid).pipelineId("p").runId("r").status(PipelineStatus.PENDING).build()));
        GateFallbackCache cache = new GateFallbackCache(store);
        assertFalse(cache.isFallback(rid, "G"));
        Map<String, String> ctx = new HashMap<>();
        ctx.put("fallback", "true");
        assertTrue(store.casGateStatus(rid, "G", GateStatus.CLOSED, GateStatus.OPEN, "timeout", ctx, 1));
        assertTrue(cache.isFallback(rid, "G"), "the negative answer must not have been cached");
        cache.evict(rid);
        assertTrue(cache.isFallback(rid, "G"));
    }

    // F075 / F076
    private static Services services(RuntimeConfig cfg, InMemoryControlPlaneStore store, InMemoryDataPlane plane, RecordingMetrics m) {
        return new Services(cfg, store, plane, new SimpleDataSourceRegistry(new HashMap<String, javax.sql.DataSource>()), m, new GraphRegistry(null), new ReflectionComponentFactory());
    }

    private static String finishedRound(InMemoryControlPlaneStore store, int i) {
        String rid = "jr-" + i + "-" + System.nanoTime();
        store.createRound(RoundRecord.builder().roundId(rid).graphName("g").bizKey("b" + i).planHash("h").params(new HashMap<String, String>())
                        .status(RoundStatus.CREATED).createdAt(1L).build(),
                Collections.singletonList(RunRecord.builder().roundId(rid).runId("r").status(RunStatus.PENDING).build()),
                Collections.<GateRecord>emptyList(),
                Collections.singletonList(PipelineRecord.builder().roundId(rid).pipelineId("p").runId("r").status(PipelineStatus.PENDING).build()));
        assertTrue(store.casRoundStatus(rid, RoundStatus.CREATED, RoundStatus.RUNNING, null, 1));
        assertTrue(store.casRoundStatus(rid, RoundStatus.RUNNING, RoundStatus.SUCCEEDED, null, 1));
        return rid;
    }

    @Test void dataPlanePurgeIsRetriedUntilItSucceeds() throws Exception {
        InMemoryControlPlaneStore store = new InMemoryControlPlaneStore();
        AtomicInteger fails = new AtomicInteger(1);
        InMemoryDataPlane plane = new InMemoryDataPlane() {
            @Override public synchronized int purgeRoundData(Collection<String> ids) {
                if (fails.getAndDecrement() > 0) throw new IllegalStateException("scan timeout");
                return super.purgeRoundData(ids);
            }
        };
        RuntimeConfig c = cfg("j").retentionEnabled(true).chunkRetentionMillis(Long.MAX_VALUE / 4).roundRetentionMillis(0).retentionIntervalMillis(1).retentionBatch(5).build();
        String rid = finishedRound(store, 1);
        Thread.sleep(5);
        RetentionJanitor j = new RetentionJanitor(services(c, store, plane, new RecordingMetrics()), () -> true);
        assertTrue(j.runIfDue(System.currentTimeMillis()));
        assertNull(store.loadRoundRecord(rid), "records are gone");
        assertFalse(plane.purgedRoundData.contains(rid), "first purge failed");
        Thread.sleep(5);
        assertTrue(j.runIfDue(System.currentTimeMillis()));
        assertTrue(plane.purgedRoundData.contains(rid), "retried on the next pass");
    }

    @Test void pendingPurgeSurvivesAMasterRestart() throws Exception {
        InMemoryControlPlaneStore store = new InMemoryControlPlaneStore();
        AtomicInteger fails = new AtomicInteger(1);
        InMemoryDataPlane plane = new InMemoryDataPlane() {
            @Override public synchronized int purgeRoundData(Collection<String> ids) {
                if (fails.getAndDecrement() > 0) throw new IllegalStateException("scan timeout");
                return super.purgeRoundData(ids);
            }
        };
        RuntimeConfig c = cfg("j").retentionEnabled(true).chunkRetentionMillis(Long.MAX_VALUE / 4).roundRetentionMillis(0).retentionIntervalMillis(1).retentionBatch(5).build();
        String rid = finishedRound(store, 1);
        Thread.sleep(5);
        assertTrue(new RetentionJanitor(services(c, store, plane, new RecordingMetrics()), () -> true).runIfDue(System.currentTimeMillis()));
        assertTrue(store.pendingPurges(100).contains(rid), "the id is kept in the store, not in the janitor");
        Thread.sleep(5);
        // a different janitor instance (new leader / restarted pod) finishes the job
        assertTrue(new RetentionJanitor(services(c, store, plane, new RecordingMetrics()), () -> true).runIfDue(System.currentTimeMillis()));
        assertTrue(plane.purgedRoundData.contains(rid));
        assertFalse(store.pendingPurges(100).contains(rid), "cleared after the data plane purge succeeded");
    }

    @Test void onlyReallyCompactedRoundsAreCountedAndRefusedOnesAreNotRepeated() throws Exception {
        AtomicInteger calls = new AtomicInteger();
        InMemoryControlPlaneStore store = new InMemoryControlPlaneStore() {
            @Override public synchronized int compactRound(String id, long cutoff) { calls.incrementAndGet(); return -2; }
        };
        RuntimeConfig c = cfg("j").retentionEnabled(true).chunkRetentionMillis(0).roundRetentionMillis(Long.MAX_VALUE / 4).retentionIntervalMillis(1).retentionBatch(5).build();
        finishedRound(store, 1);
        Thread.sleep(5);
        RecordingMetrics m = new RecordingMetrics();
        RetentionJanitor j = new RetentionJanitor(services(c, store, new InMemoryDataPlane(), m), () -> true);
        assertTrue(j.runIfDue(System.currentTimeMillis()));
        Thread.sleep(5);
        assertTrue(j.runIfDue(System.currentTimeMillis()));
        assertEquals(0, m.compacted.get());
        assertEquals(1, calls.get(), "the refused round is not retried every pass");

        InMemoryControlPlaneStore plain = new InMemoryControlPlaneStore();
        String rid = finishedRound(plain, 2);
        Thread.sleep(5);
        RecordingMetrics m2 = new RecordingMetrics();
        assertTrue(new RetentionJanitor(services(c, plain, new InMemoryDataPlane(), m2), () -> true).runIfDue(System.currentTimeMillis()));
        assertTrue(plain.loadRoundRecord(rid).isCompacted());
        assertEquals(1, m2.compacted.get(), "a compaction of a round without chunk records still counts");
    }
}
