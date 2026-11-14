package io.github.amwtke.pipeliner.core.store;

import static org.junit.jupiter.api.Assertions.*;

import io.github.amwtke.pipeliner.core.graph.ExecMode;
import io.github.amwtke.pipeliner.core.graph.GateType;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

/**
 * Behavioural contract every ControlPlaneStore must satisfy. Published once as pipeliner-core's test-jar and run against the MySQL store, the
 * Redis store and the runtime's in-memory fake (each module subclasses it and sets {@link #store}).
 */
public abstract class AbstractControlPlaneStoreContractTest {
    protected ControlPlaneStore store;

    private void savePending(String rid, List<ChunkRecord> chunks) { store.savePendingChunks(rid, chunks, 1); }

    private static int seq = 0;
    private static synchronized int next() { return ++seq; }

    private final String tag = Long.toString(System.nanoTime(), 36) + next();

    private String rid(String n) { return "r-" + n + "-" + tag; }

    private RoundRecord newRound(String rid, String biz) {
        Map<String, String> p = new LinkedHashMap<>();
        p.put("dt", "2024-01-01");
        return RoundRecord.builder().roundId(rid).graphName("g-" + tag).bizKey(biz).planHash("h1").planJson("{\"x\":1}").params(p)
                .status(RoundStatus.CREATED).createdAt(1000L).build();
    }

    private RunRecord run(String rid, String id, int stage) {
        return RunRecord.builder().roundId(rid).runId(id).stage(stage).mode(ExecMode.CLUSTER).status(RunStatus.PENDING).build();
    }

    private PipelineRecord pl(String rid, String id, String run) {
        return PipelineRecord.builder().roundId(rid).pipelineId(id).runId(run).status(PipelineStatus.PENDING).parallelism(2).build();
    }

    private GateRecord gate(String rid, String id) {
        return GateRecord.builder().roundId(rid).gateId(id).type(GateType.EXTERNAL).beforeRuns(Arrays.asList("run2", "run3")).status(GateStatus.CLOSED).build();
    }

    private String mk(String n) {
        String rid = rid(n);
        store.createRound(newRound(rid, "biz-" + n), Arrays.asList(run(rid, "run1", 0), run(rid, "run2", 1)),
                Collections.singletonList(gate(rid, "gate1")), Arrays.asList(pl(rid, "p1", "run1"), pl(rid, "p2", "run1"), pl(rid, "p3", "run2")));
        return rid;
    }

    private ChunkRecord pending(String rid, String p, String c, String run) {
        return ChunkRecord.builder().roundId(rid).pipelineId(p).chunkId(c).runId(run).status(ChunkStatus.PENDING).inputEdge("split").splitSpec("{\"a\":1}").build();
    }

    private ChunkEvent ev(String rid, String type, String p, String c, String run, String edge, long in, Map<String, Long> out) {
        return ChunkEvent.builder().type(type).roundId(rid).runId(run).pipelineId(p).chunkId(c).inputEdge(edge).owner("w1").rowsIn(in).rowsOut(out)
                .costMs(12).error("DEAD".equals(type) ? "boom" : null).attempt(1).ts(5000L).build();
    }

    private static Map<String, Long> m(Object... kv) {
        Map<String, Long> r = new LinkedHashMap<>();
        for (int i = 0; i < kv.length; i += 2) r.put((String) kv[i], ((Number) kv[i + 1]).longValue());
        return r;
    }

    @Test
    void createRoundIsIdempotentOnBizKey() {
        String rid = mk("create");
        RoundRecord r = store.loadRoundRecord(rid);
        assertEquals(RoundStatus.CREATED, r.getStatus());
        assertEquals("0-0", r.getJournalOffset());
        assertEquals("2024-01-01", r.getParams().get("dt"));
        assertEquals("{\"x\":1}", store.loadRound(rid).getRound().getPlanJson(), "the full view carries the plan");
        assertEquals(1000L, r.getCreatedAt());
        assertFalse(r.isIncomplete());
        assertEquals(rid, store.findRoundByBizKey("g-" + tag, "biz-create").getRoundId());
        assertNull(store.findRoundByBizKey("g-" + tag, "nope"));
        assertNull(store.loadRoundRecord("missing-" + tag));
        assertNull(store.loadRound("missing-" + tag));

        RoundRecord other = store.createRound(newRound(rid("create2"), "biz-create"), Collections.singletonList(run(rid("create2"), "runX", 0)),
                Collections.<GateRecord>emptyList(), Collections.<PipelineRecord>emptyList());
        assertEquals(rid, other.getRoundId());
        assertNull(store.loadRoundRecord(rid("create2")));
        assertEquals(2, store.loadRound(rid).getRuns().size());
    }

    @Test
    void createRoundRefusesAnExistingRoundIdAndKeepsTheLiveRound() {
        String rid = mk("dupid");
        assertTrue(store.casRoundStatus(rid, RoundStatus.CREATED, RoundStatus.RUNNING, null, 3));
        RoundRecord clash = newRound(rid, "biz-dupid-other");
        assertThrows(DuplicateRoundIdException.class, () -> store.createRound(clash, Collections.singletonList(run(rid, "runZ", 0)),
                Collections.<GateRecord>emptyList(), Collections.<PipelineRecord>emptyList()));
        RoundRecord live = store.loadRoundRecord(rid);
        assertEquals("biz-dupid", live.getBizKey(), "the live round is untouched");
        assertEquals(RoundStatus.RUNNING, live.getStatus());
        assertEquals(3, live.getMasterEpoch());
        assertEquals(2, store.loadRound(rid).getRuns().size());
        assertNull(store.findRoundByBizKey("g-" + tag, "biz-dupid-other"), "no biz key entry was left behind");
    }

    @Test
    void concurrentCreateRoundYieldsOneRound() throws Exception {
        ExecutorService ex = Executors.newFixedThreadPool(6);
        List<Future<String>> fs = new ArrayList<>();
        CountDownLatch go = new CountDownLatch(1);
        for (int i = 0; i < 6; i++) {
            final String rid = rid("cc" + i);
            fs.add(ex.submit(() -> {
                go.await();
                return store.createRound(newRound(rid, "biz-cc"), Collections.singletonList(run(rid, "run1", 0)), Collections.<GateRecord>emptyList(),
                        Collections.singletonList(pl(rid, "p1", "run1"))).getRoundId();
            }));
        }
        go.countDown();
        Set<String> ids = new HashSet<>();
        for (Future<String> f : fs) ids.add(f.get(30, TimeUnit.SECONDS));
        ex.shutdown();
        assertEquals(1, ids.size());
        assertEquals(1, store.loadRound(ids.iterator().next()).getRuns().size());
    }

    @Test
    void loadRoundViewAndActiveRounds() {
        String rid = mk("view");
        RoundView v = store.loadRound(rid);
        assertEquals(2, v.getRuns().size());
        assertEquals(1, v.getGates().size());
        assertEquals(3, v.getPipelines().size());
        assertEquals(Arrays.asList("run2", "run3"), v.getGates().get(0).getBeforeRuns());
        assertEquals(ExecMode.CLUSTER, v.getRuns().get(0).getMode());
        assertTrue(store.findActiveRounds().contains(rid));
        assertTrue(store.casRoundStatus(rid, RoundStatus.CREATED, RoundStatus.RUNNING, null, 1));
        assertTrue(store.findActiveRounds().contains(rid));
        assertTrue(store.casRoundStatus(rid, RoundStatus.RUNNING, RoundStatus.PAUSED, null, 1));
        assertTrue(store.findActiveRounds().contains(rid));
        assertTrue(store.casRoundStatus(rid, RoundStatus.PAUSED, RoundStatus.CANCELLED, "x", 1));
        assertFalse(store.findActiveRounds().contains(rid));
    }

    @Test
    void listRecentRoundsActiveFirstThenNewest() {
        String g = "g-list-" + tag, other = "g-other-" + tag;
        String[] ids = new String[5];
        for (int i = 0; i < 5; i++) {
            ids[i] = rid("list" + i);
            RoundRecord rr = newRound(ids[i], "biz-list" + i);
            rr.setGraphName(i == 4 ? other : g);
            rr.setCreatedAt(100_000L + i * 10);   // list0 oldest
            store.createRound(rr, Collections.singletonList(run(ids[i], "run1", 0)), Collections.<GateRecord>emptyList(),
                    Collections.singletonList(pl(ids[i], "p1", "run1")));
        }
        // list0 stays active (oldest); list1..3 finish; list4 is another graph
        for (int i = 1; i < 4; i++) {
            assertTrue(store.casRoundStatus(ids[i], RoundStatus.CREATED, RoundStatus.RUNNING, null, 1));
            assertTrue(store.casRoundStatus(ids[i], RoundStatus.RUNNING, RoundStatus.SUCCEEDED, null, 1));
        }
        List<RoundRecord> l = store.listRecentRounds(g, 10);
        assertEquals(Arrays.asList(ids[0], ids[3], ids[2], ids[1]), l.stream().map(RoundRecord::getRoundId).collect(java.util.stream.Collectors.toList()),
                "active first, then newest created first, other graph excluded");
        assertEquals(g, l.get(0).getGraphName());
        assertEquals(RoundStatus.CREATED, l.get(0).getStatus());
        assertEquals(RoundStatus.SUCCEEDED, l.get(1).getStatus());
        assertEquals(Arrays.asList(ids[0], ids[3]), store.listRecentRounds(g, 2).stream().map(RoundRecord::getRoundId).collect(java.util.stream.Collectors.toList()));
        assertEquals(1, store.listRecentRounds(other, 5).size());
        assertTrue(store.listRecentRounds(null, 1000).stream().anyMatch(r -> r.getRoundId().equals(ids[4])), "no graph filter spans graphs");
        assertTrue(store.listRecentRounds(g, 0).isEmpty());
    }

    @Test
    void casRoundWithEpochFencing() {
        String rid = mk("cas");
        assertFalse(store.casRoundStatus(rid, RoundStatus.RUNNING, RoundStatus.PAUSED, null, 5), "wrong expect");
        assertTrue(store.casRoundStatus(rid, RoundStatus.CREATED, RoundStatus.RUNNING, null, 5));
        RoundRecord r = store.loadRoundRecord(rid);
        assertEquals(RoundStatus.RUNNING, r.getStatus());
        assertEquals(5, r.getMasterEpoch());
        assertNotNull(r.getStartedAt());
        assertNull(r.getFinishedAt());
        assertFalse(store.casRoundStatus(rid, RoundStatus.RUNNING, RoundStatus.PAUSED, null, 4), "lower epoch rejected");
        assertEquals(RoundStatus.RUNNING, store.loadRoundRecord(rid).getStatus());
        assertTrue(store.casRoundStatus(rid, RoundStatus.RUNNING, RoundStatus.PAUSED, null, 5), "same epoch ok");
        assertTrue(store.casRoundStatus(rid, RoundStatus.PAUSED, RoundStatus.RUNNING, null, 7), "higher epoch wins");
        assertEquals(7, store.loadRoundRecord(rid).getMasterEpoch());
        assertFalse(store.casRoundStatus(rid, RoundStatus.RUNNING, RoundStatus.FAILED, "e", 6));
        assertTrue(store.casRoundStatus(rid, RoundStatus.RUNNING, RoundStatus.FAILED, "oops", 7));
        r = store.loadRoundRecord(rid);
        assertEquals("oops", r.getError());
        assertNotNull(r.getFinishedAt());
        assertTrue(store.setRoundIncomplete(rid, 7));
        assertTrue(store.loadRoundRecord(rid).isIncomplete());
    }

    @Test
    void concurrentCasExactlyOneWins() throws Exception {
        String rid = mk("race");
        for (int round = 0; round < 5; round++) {
            final String run = "run" + (round % 2 + 1);
            final RunStatus from = RunStatus.PENDING;
            final long epoch = 1000 + round * 10;
            if (round >= 2) store.resetRuns(rid, Collections.singletonList(run), Collections.<String>emptyList(), 1000 + round * 10 - 5); // back to PENDING
            AtomicInteger wins = new AtomicInteger();
            CountDownLatch go = new CountDownLatch(1);
            ExecutorService ex = Executors.newFixedThreadPool(2);
            List<Future<?>> fs = new ArrayList<>();
            for (int t = 0; t < 2; t++) {
                fs.add(ex.submit(() -> {
                    go.await();
                    if (store.casRunStatus(rid, run, from, RunStatus.RUNNING, null, epoch)) wins.incrementAndGet();
                    return null;
                }));
            }
            go.countDown();
            for (Future<?> f : fs) f.get(30, TimeUnit.SECONDS);
            ex.shutdown();
            assertEquals(1, wins.get(), "round " + round);
        }
        // pipeline + gate races too
        AtomicInteger wins = new AtomicInteger();
        ExecutorService ex = Executors.newFixedThreadPool(4);
        CountDownLatch go = new CountDownLatch(1);
        List<Future<?>> fs = new ArrayList<>();
        for (int t = 0; t < 4; t++) {
            fs.add(ex.submit(() -> {
                go.await();
                if (store.casGateStatus(rid, "gate1", GateStatus.CLOSED, GateStatus.OPEN, "t", null, 2000)) wins.incrementAndGet();
                return null;
            }));
        }
        go.countDown();
        for (Future<?> f : fs) f.get(30, TimeUnit.SECONDS);
        ex.shutdown();
        assertEquals(1, wins.get());
    }

    @Test
    void runPipelineCasAndUpdates() {
        String rid = mk("runpl");
        assertTrue(store.casRunStatus(rid, "run1", RunStatus.PENDING, RunStatus.RUNNING, null, 3));
        RunRecord r = store.loadRound(rid).getRuns().stream().filter(x -> x.getRunId().equals("run1")).findFirst().get();
        assertEquals(RunStatus.RUNNING, r.getStatus());
        assertNotNull(r.getStartedAt());
        assertEquals(3, r.getMasterEpoch());
        assertFalse(store.casRunStatus(rid, "run1", RunStatus.RUNNING, RunStatus.SUCCEEDED, null, 2));
        assertFalse(store.updateRunAssignment(rid, "run1", "w9", 1, 2));
        assertTrue(store.updateRunAssignment(rid, "run1", "w9", 1, 3));
        assertTrue(store.updateRunAssignment(rid, "run1", "w9", 1, 3), "writing the same values again is not 'fenced'");
        assertTrue(store.casRunStatus(rid, "run1", RunStatus.RUNNING, RunStatus.FAILED, "bad", 3));
        r = store.loadRound(rid).getRuns().stream().filter(x -> x.getRunId().equals("run1")).findFirst().get();
        assertEquals("w9", r.getAssignee());
        assertEquals(1, r.getAttempt());
        assertEquals("bad", r.getError());
        assertNotNull(r.getFinishedAt());

        assertTrue(store.casPipelineStatus(rid, "p1", PipelineStatus.PENDING, PipelineStatus.RUNNING, null, 3));
        assertFalse(store.casPipelineStatus(rid, "p1", PipelineStatus.PENDING, PipelineStatus.RUNNING, null, 3));
        assertFalse(store.casPipelineStatus(rid, "p1", PipelineStatus.RUNNING, PipelineStatus.SUCCEEDED, null, 2));
        store.setPipelineEnumDone(rid, "p1", true, 3);
        store.setPipelineParallelism(rid, "p1", 8, 3);
        store.setPipelineParallelism(rid, "p2", 9, 1); // pipeline epoch 0 <= 1 -> allowed
        store.setPipelineEnumDone(rid, "p1", false, 2); // fenced
        PipelineRecord p = store.loadRound(rid).getPipelines().stream().filter(x -> x.getPipelineId().equals("p1")).findFirst().get();
        assertTrue(p.isEnumDone());
        assertEquals(8, p.getParallelism());
        assertNotNull(p.getStartedAt());
        assertTrue(store.casPipelineStatus(rid, "p1", PipelineStatus.RUNNING, PipelineStatus.SUCCEEDED, null, 3));
        p = store.loadRound(rid).getPipelines().stream().filter(x -> x.getPipelineId().equals("p1")).findFirst().get();
        assertNotNull(p.getFinishedAt());
    }

    @Test
    void gateArmOpenResetAndSignals() {
        String rid = mk("gate");
        store.armGate(rid, "gate1", 111L, 999L, 2);
        GateRecord g = store.loadRound(rid).getGates().get(0);
        assertEquals(111L, g.getArmedAt());
        assertEquals(999L, g.getDeadlineAt());
        assertEquals(2, g.getMasterEpoch());
        store.armGate(rid, "gate1", 5L, null, 1); // fenced
        assertEquals(111L, store.loadRound(rid).getGates().get(0).getArmedAt());
        store.markGateAlerted(rid, "gate1");
        assertTrue(store.loadRound(rid).getGates().get(0).isAlerted());

        Map<String, String> ctx = new HashMap<>();
        ctx.put("k", "v");
        assertFalse(store.casGateStatus(rid, "gate1", GateStatus.CLOSED, GateStatus.OPEN, "me", ctx, 1));
        assertTrue(store.casGateStatus(rid, "gate1", GateStatus.CLOSED, GateStatus.OPEN, "me", ctx, 2));
        g = store.loadRound(rid).getGates().get(0);
        assertEquals(GateStatus.OPEN, g.getStatus());
        assertEquals("me", g.getOpenedBy());
        assertNotNull(g.getOpenedAt());
        assertEquals("v", g.getContext().get("k"));

        store.saveGateSignal(rid, "gate1", "s1", "payload1");
        store.saveGateSignal(rid, "gate1", "s1", "other-payload");   // idempotent per key: the first payload is kept
        assertEquals("payload1", store.loadGateSignals(rid, "gate1").get("s1"));
        store.saveGateSignal(rid, "gate1", "s2", "payload2");
        assertEquals(2, store.loadGateSignals(rid, "gate1").size());
        assertEquals("payload2", store.loadGateSignals(rid, "gate1").get("s2"));
        assertTrue(store.loadGateSignals(rid, "nogate").isEmpty());

        assertTrue(store.fenceRound(rid, 3));
        assertThrows(StaleEpochException.class, () -> store.resetRuns(rid, Collections.<String>emptyList(), Collections.singletonList("gate1"), 1)); // fenced
        assertEquals(GateStatus.OPEN, store.loadRound(rid).getGates().get(0).getStatus());
        assertEquals(2, store.loadGateSignals(rid, "gate1").size());
        store.resetRuns(rid, Collections.<String>emptyList(), Collections.singletonList("gate1"), 3);
        g = store.loadRound(rid).getGates().get(0);
        assertEquals(GateStatus.CLOSED, g.getStatus());
        assertNull(g.getArmedAt());
        assertNull(g.getDeadlineAt());
        assertNull(g.getOpenedBy());
        assertNull(g.getOpenedAt());
        assertTrue(g.getContext() == null || g.getContext().isEmpty());
        assertFalse(g.isAlerted());
        assertTrue(store.loadGateSignals(rid, "gate1").isEmpty(), "a gate reset deletes its signals");
        assertTrue(store.casGateStatus(rid, "gate1", GateStatus.CLOSED, GateStatus.TIMEOUT, null, null, 3));
    }

    @Test
    void savePendingNeverOverwrites() {
        String rid = mk("pend");
        savePending(rid, Arrays.asList(pending(rid, "p1", "S0000", "run1"), pending(rid, "p1", "S0001", "run1")));
        assertEquals(2, store.countChunks(rid, "p1", ChunkStatus.PENDING));
        store.applyChunkEvents(rid, Collections.singletonList(ev(rid, "DONE", "p1", "S0000", "run1", "split", 10, m("e1", 10))), "1-0", 1);
        savePending(rid, Arrays.asList(pending(rid, "p1", "S0000", "run1"), pending(rid, "p1", "S0002", "run1")));
        List<ChunkRecord> all = store.queryChunks(ChunkQuery.builder().roundId(rid).build());
        assertEquals(3, all.size());
        assertEquals(ChunkStatus.DONE, all.get(0).getStatus());
        assertEquals(10L, all.get(0).getRowsIn());
        assertEquals("{\"a\":1}", all.get(1).getSplitSpec());
        assertEquals(ChunkStatus.PENDING, all.get(2).getStatus());
        savePending(rid, Collections.<ChunkRecord>emptyList());
    }

    @Test
    void applyEventsIdempotentDeadRulesAndOffset() {
        String rid = mk("apply");
        savePending(rid, Arrays.asList(pending(rid, "p1", "S0000", "run1"), pending(rid, "p1", "S0001", "run1"), pending(rid, "p1", "S0003", "run1")));
        List<ChunkEvent> batch = Arrays.asList(
                ev(rid, "DONE", "p1", "S0000", "run1", "split", 100, m("e1", 90, "sink:main", 5)),
                ev(rid, "DEAD", "p1", "S0001", "run1", "split", 0, null),
                ev(rid, "DONE", "p1", "S0002", "run1", "split", 7, m("e1", 7)),
                ev(rid, "LOCAL_RUN_DONE", "p1", "S0009", "run1", null, 0, null));
        store.applyChunkEvents(rid, batch, "10-1", 1);
        List<ChunkRecord> first = store.queryChunks(ChunkQuery.builder().roundId(rid).build());
        store.applyChunkEvents(rid, batch, "10-1", 1);
        List<ChunkRecord> second = store.queryChunks(ChunkQuery.builder().roundId(rid).build());
        assertEquals(first, second);
        assertEquals(4, first.size());
        assertEquals("10-1", store.loadRoundRecord(rid).getJournalOffset());
        ChunkRecord c0 = first.get(0);
        assertEquals(ChunkStatus.DONE, c0.getStatus());
        assertEquals(100L, c0.getRowsIn());
        assertEquals(90L, c0.getRowsOut().get("e1"));
        assertEquals(5L, c0.getRowsOut().get("sink:main"));
        assertEquals(12, c0.getCostMs());
        assertEquals("w1", c0.getOwner());
        assertEquals("{\"a\":1}", c0.getSplitSpec(), "split spec preserved");
        assertEquals(ChunkStatus.DEAD, first.get(1).getStatus());
        assertEquals("boom", first.get(1).getError());
        assertEquals(ChunkStatus.DONE, first.get(2).getStatus(), "DONE inserted without pending");
        assertEquals(ChunkStatus.PENDING, first.get(3).getStatus());

        // DEAD must not overwrite DONE (in a later batch and in the same batch)
        store.applyChunkEvents(rid, Arrays.asList(ev(rid, "DEAD", "p1", "S0000", "run1", "split", 0, null),
                ev(rid, "DEAD", "p1", "S0002", "run1", "split", 0, null)), "11-0", 1);
        ChunkRecord d = store.queryChunks(ChunkQuery.builder().roundId(rid).build()).get(0);
        assertEquals(ChunkStatus.DONE, d.getStatus());
        assertNull(d.getError());
        assertEquals(100L, d.getRowsIn());
        assertEquals(ChunkStatus.DONE, store.queryChunks(ChunkQuery.builder().roundId(rid).build()).get(2).getStatus());
        // DONE overwrites DEAD
        store.applyChunkEvents(rid, Collections.singletonList(ev(rid, "DONE", "p1", "S0001", "run1", "split", 3, m("e1", 3))), "12-0", 1);
        ChunkRecord e = store.queryChunks(ChunkQuery.builder().roundId(rid).build()).get(1);
        assertEquals(ChunkStatus.DONE, e.getStatus());
        assertNull(e.getError());
        assertEquals(3L, e.getRowsOut().get("e1"));
        assertEquals("12-0", store.loadRoundRecord(rid).getJournalOffset());
        // offset-only batch
        store.applyChunkEvents(rid, Collections.<ChunkEvent>emptyList(), "13-0", 1);
        assertEquals("13-0", store.loadRoundRecord(rid).getJournalOffset());
    }

    @Test
    void skippedResetQueryCountAndSums() {
        String rid = mk("misc");
        savePending(rid, Arrays.asList(pending(rid, "p1", "S0004", "run1"), pending(rid, "p3", "S0000", "run2")));
        store.applyChunkEvents(rid, Arrays.asList(
                ev(rid, "DONE", "p1", "S0000", "run1", "split", 10, m("e1", 8, "e2", 2)),
                ev(rid, "DONE", "p1", "S0001", "run1", "split", 20, m("e1", 15, "sink:main", 4)),
                ev(rid, "DONE", "p2", "S0000>1.0", "run1", "e1", 8, m("sink:main", 8)),
                ev(rid, "DONE", "p2", "S0001>1.0", "run1", "e1", 15, m("sink:main", 15)),
                ev(rid, "DEAD", "p1", "S0002", "run1", "split", 0, null),
                ev(rid, "DEAD", "p1", "S0003", "run1", "split", 0, null),
                ev(rid, "DONE", "p3", "S0001", "run2", "split", 1000, m("e9", 1000))), "5-0", 1);

        assertEquals(m("e1", 23, "e2", 2), store.sumRowsOutByEdge(rid, "run1"));
        assertEquals(m("e1", 23), store.sumRowsInByEdge(rid, "run1"));
        assertEquals(m("e9", 1000), store.sumRowsOutByEdge(rid, "run2"));
        assertTrue(store.sumRowsInByEdge(rid, "run2").isEmpty());

        assertEquals(2, store.countChunks(rid, "p1", ChunkStatus.DONE));
        assertEquals(2, store.countChunks(rid, "p1", ChunkStatus.DEAD));
        assertEquals(1, store.countChunks(rid, "p1", ChunkStatus.PENDING));

        // queryChunks filters / ordering / paging
        assertEquals(5, store.countChunks(rid, "p1", null));
        assertEquals(9, store.queryChunks(ChunkQuery.builder().roundId(rid).build()).size());
        List<ChunkRecord> p1 = store.queryChunks(ChunkQuery.builder().roundId(rid).pipelineId("p1").build());
        assertEquals(Arrays.asList("S0000", "S0001", "S0002", "S0003", "S0004"), ids(p1));
        assertEquals(Arrays.asList("S0001", "S0002"), ids(store.queryChunks(ChunkQuery.builder().roundId(rid).pipelineId("p1").limit(2).offset(1).build())));
        assertEquals(Arrays.asList("S0002", "S0003"), ids(store.queryChunks(ChunkQuery.builder().roundId(rid).status(ChunkStatus.DEAD).build())));
        assertEquals(2, store.queryChunks(ChunkQuery.builder().roundId(rid).runId("run2").build()).size());
        List<ChunkRecord> all = store.queryChunks(ChunkQuery.builder().roundId(rid).build());
        assertEquals("p1", all.get(0).getPipelineId());
        assertEquals("p3", all.get(all.size() - 1).getPipelineId());

        // view aggregation
        RoundView v = store.loadRound(rid);
        assertEquals(2L, v.getChunkCounts().get("p1").get(ChunkStatus.DONE));
        assertEquals(2L, v.getChunkCounts().get("p1").get(ChunkStatus.DEAD));
        assertEquals(1L, v.getChunkCounts().get("p1").get(ChunkStatus.PENDING));
        assertEquals(2L, v.getChunkCounts().get("p2").get(ChunkStatus.DONE));
        assertEquals(30L, v.getRowsIn().get("p1"));
        assertEquals(1000L, v.getRowsIn().get("p3"));
        assertEquals(29L, v.getRowsOut().get("p1"));
        assertEquals(23L, v.getRowsOut().get("p2"));

        // markChunksSkipped affects run1 only
        store.markChunksSkipped(rid, "run1", 1);
        assertEquals(2, store.countChunks(rid, "p1", ChunkStatus.SKIPPED));
        assertEquals(0, store.countChunks(rid, "p1", ChunkStatus.DEAD));
        assertEquals(2, store.countChunks(rid, "p1", ChunkStatus.DONE));

        // resetRun
        store.casRunStatus(rid, "run1", RunStatus.PENDING, RunStatus.RUNNING, null, 4);
        store.casPipelineStatus(rid, "p1", PipelineStatus.PENDING, PipelineStatus.RUNNING, null, 4);
        store.setPipelineEnumDone(rid, "p1", true, 4);
        store.updateRunAssignment(rid, "run1", "w1", 2, 4);
        assertThrows(StaleEpochException.class, () -> store.resetRuns(rid, Collections.singletonList("run1"), Collections.<String>emptyList(), 3)); // run1 was written by epoch 4
        assertEquals(2, store.countChunks(rid, "p1", ChunkStatus.DONE));
        store.resetRuns(rid, Collections.singletonList("run1"), Collections.<String>emptyList(), 5);
        assertEquals(0, store.queryChunks(ChunkQuery.builder().roundId(rid).runId("run1").build()).size());
        assertEquals(2, store.queryChunks(ChunkQuery.builder().roundId(rid).runId("run2").build()).size());
        v = store.loadRound(rid);
        RunRecord r1 = v.getRuns().stream().filter(x -> x.getRunId().equals("run1")).findFirst().get();
        assertEquals(RunStatus.PENDING, r1.getStatus());
        assertEquals(3, r1.getAttempt());
        assertNull(r1.getAssignee());
        for (PipelineRecord p : v.getPipelines()) {
            if (p.getRunId().equals("run1")) {
                assertEquals(PipelineStatus.PENDING, p.getStatus());
                assertFalse(p.isEnumDone());
            }
        }
        assertTrue(store.sumRowsOutByEdge(rid, "run1").isEmpty());
    }

    private static List<String> ids(List<ChunkRecord> l) {
        List<String> out = new ArrayList<>();
        for (ChunkRecord c : l) out.add(c.getChunkId());
        return out;
    }

    @Test
    void fenceRoundFencesEveryMasterWrite() {
        String rid = mk("fence");
        // a DEAD chunk the deposed master must not be able to skip
        store.applyChunkEvents(rid, Collections.singletonList(ev(rid, "DEAD", "p1", "S0009", "run1", "split", 0, null)), "0-0", 1);
        assertFalse(store.fenceRound("missing-" + tag, 1), "unknown round");
        assertTrue(store.fenceRound(rid, 5));
        assertEquals(5, store.loadRoundRecord(rid).getMasterEpoch());
        assertTrue(store.fenceRound(rid, 5), "same epoch is fine");
        assertFalse(store.fenceRound(rid, 4), "a higher epoch holds the round");

        // the deposed master (epoch 4) cannot write any row, not even rows nobody has touched yet
        assertFalse(store.casRunStatus(rid, "run1", RunStatus.PENDING, RunStatus.RUNNING, null, 4));
        assertFalse(store.updateRunAssignment(rid, "run1", "w1", 1, 4));
        assertFalse(store.casPipelineStatus(rid, "p1", PipelineStatus.PENDING, PipelineStatus.RUNNING, null, 4));
        store.setPipelineEnumDone(rid, "p1", true, 4);
        store.setPipelineParallelism(rid, "p1", 9, 4);
        store.armGate(rid, "gate1", 111L, null, 4);
        assertFalse(store.casGateStatus(rid, "gate1", GateStatus.CLOSED, GateStatus.OPEN, "x", new HashMap<String, String>(), 4));
        assertThrows(StaleEpochException.class, () -> store.resetRuns(rid, Collections.singletonList("run1"), Collections.singletonList("gate1"), 4));
        store.markChunksSkipped(rid, "run1", 4);
        savePending(rid, Collections.singletonList(pending(rid, "p1", "S0007", "run1")));   // epoch 1 < 5: fenced
        store.savePendingChunks(rid, Collections.singletonList(pending(rid, "p1", "S0008", "run1")), 4);
        assertThrows(StaleEpochException.class, () -> store.applyChunkEvents(rid,
                Collections.singletonList(ev(rid, "DONE", "p1", "S0000", "run1", "split", 1, m("e1", 1))), "1-0", 4));
        RoundView v = store.loadRoundState(rid);
        assertEquals("0-0", v.getRound().getJournalOffset());
        assertEquals(RunStatus.PENDING, v.getRuns().get(0).getStatus());
        assertNull(v.getRuns().get(0).getAssignee());
        PipelineRecord p1 = v.getPipelines().stream().filter(x -> x.getPipelineId().equals("p1")).findFirst().get();
        assertEquals(PipelineStatus.PENDING, p1.getStatus());
        assertEquals(2, p1.getParallelism());
        assertFalse(p1.isEnumDone());
        assertNull(v.getGates().get(0).getArmedAt());
        assertEquals(GateStatus.CLOSED, v.getGates().get(0).getStatus());
        assertEquals(1, store.countChunks(rid, "p1", null), "the fenced saves stored nothing");
        assertEquals(1, store.countChunks(rid, "p1", ChunkStatus.DEAD), "the deposed master's skip did not flip DEAD to SKIPPED");
        assertEquals(0, store.countChunks(rid, "p1", ChunkStatus.SKIPPED));

        // the current master works, and a newer one deposes it
        assertTrue(store.casRunStatus(rid, "run1", RunStatus.PENDING, RunStatus.RUNNING, null, 5));
        store.applyChunkEvents(rid, Collections.singletonList(ev(rid, "DONE", "p1", "S0000", "run1", "split", 1, m("e1", 1))), "1-0", 5);
        assertEquals(1, store.countChunks(rid, "p1", ChunkStatus.DONE));
        assertTrue(store.fenceRound(rid, 6));
        assertFalse(store.casRunStatus(rid, "run1", RunStatus.RUNNING, RunStatus.FAILED, "x", 5));
        assertThrows(StaleEpochException.class, () -> store.applyChunkEvents(rid, Collections.<ChunkEvent>emptyList(), "2-0", 5));
    }

    @Test
    void maxMasterEpochTracksFencedRounds() {
        String rid = mk("maxepoch");
        long base = store.maxMasterEpoch();
        long e = base + 100;
        assertTrue(store.fenceRound(rid, e));
        assertEquals(e, store.maxMasterEpoch());
        assertFalse(store.fenceRound(rid, e - 1), "refused fence does not lower the maximum");
        assertEquals(e, store.maxMasterEpoch());
        String other = mk("maxepoch2");
        assertTrue(store.fenceRound(other, e - 50));
        assertEquals(e, store.maxMasterEpoch(), "a lower epoch on another round does not change the maximum");
    }

    @Test
    void loadRoundStateHasNoChunkStats() {
        String rid = mk("state");
        store.applyChunkEvents(rid, Collections.singletonList(ev(rid, "DONE", "p1", "S0000", "run1", "split", 7, m("e1", 7))), "1-0", 1);
        RoundView s = store.loadRoundState(rid);
        assertEquals(2, s.getRuns().size());
        assertEquals(3, s.getPipelines().size());
        assertEquals(1, s.getGates().size());
        assertTrue(s.getChunkCounts().isEmpty());
        assertTrue(s.getRowsIn().isEmpty());
        assertEquals(1L, store.loadRound(rid).getChunkCounts().get("p1").get(ChunkStatus.DONE));
        assertNull(store.loadRoundState("missing-" + tag));
    }

    @Test
    void resetRunsIsAtomicAndClearsGateSignals() {
        String rid = mk("resetruns");
        assertTrue(store.casRoundStatus(rid, RoundStatus.CREATED, RoundStatus.RUNNING, null, 5));
        for (String r : new String[]{"run1", "run2"}) {
            assertTrue(store.casRunStatus(rid, r, RunStatus.PENDING, RunStatus.RUNNING, null, 5));
            assertTrue(store.casRunStatus(rid, r, RunStatus.RUNNING, RunStatus.SUCCEEDED, null, 5));
        }
        for (String p : new String[]{"p1", "p2", "p3"}) {
            assertTrue(store.casPipelineStatus(rid, p, PipelineStatus.PENDING, PipelineStatus.RUNNING, null, 5));
            store.setPipelineEnumDone(rid, p, true, 5);
            assertTrue(store.casPipelineStatus(rid, p, PipelineStatus.RUNNING, PipelineStatus.SUCCEEDED, null, 5));
        }
        store.updateRunAssignment(rid, "run2", "w1", 2, 5);
        store.armGate(rid, "gate1", 111L, 999L, 5);
        assertTrue(store.casGateStatus(rid, "gate1", GateStatus.CLOSED, GateStatus.OPEN, "ext", m2("k", "v"), 5));
        store.markGateAlerted(rid, "gate1");
        store.saveGateSignal(rid, "gate1", "S0000@1", "a");
        store.saveGateSignal(rid, "gate1", "S0001@1", "b");
        store.applyChunkEvents(rid, Arrays.asList(ev(rid, "DONE", "p1", "S0000", "run1", "split", 10, m("e1", 10)),
                ev(rid, "DONE", "p3", "S0000", "run2", "split", 7, m("sink:out", 7))), "1-0", 5);
        assertTrue(store.casRoundStatus(rid, RoundStatus.RUNNING, RoundStatus.FAILED, "x", 5));

        // fenced: nothing at all is applied (one atomic unit)
        assertThrows(StaleEpochException.class, () -> store.resetRuns(rid, Arrays.asList("run1", "run2"), Collections.singletonList("gate1"), 4));
        assertEquals(RoundStatus.FAILED, store.loadRoundRecord(rid).getStatus());
        assertEquals(2, store.loadGateSignals(rid, "gate1").size());
        assertEquals(1, store.countChunks(rid, "p1", ChunkStatus.DONE));
        assertThrows(StaleEpochException.class, () -> store.resetRuns("missing-" + tag, Collections.singletonList("run1"), Collections.<String>emptyList(), 9));

        store.resetRuns(rid, Collections.singletonList("run2"), Collections.singletonList("gate1"), 6);
        RoundView v = store.loadRound(rid);
        assertEquals(RoundStatus.RUNNING, v.getRound().getStatus(), "a terminal round is revived");
        assertNull(v.getRound().getFinishedAt());
        assertNull(v.getRound().getError());
        assertEquals(6, v.getRound().getMasterEpoch());
        assertTrue(store.findActiveRounds().contains(rid));
        for (RunRecord r : v.getRuns()) {
            if (r.getRunId().equals("run2")) {
                assertEquals(RunStatus.PENDING, r.getStatus());
                assertEquals(3, r.getAttempt(), "attempt 2 + 1");
                assertNull(r.getAssignee());
                assertNull(r.getFinishedAt());
            } else {
                assertEquals(RunStatus.SUCCEEDED, r.getStatus(), "run1 untouched");
                assertEquals(0, r.getAttempt());
            }
        }
        for (PipelineRecord p : v.getPipelines()) {
            if (p.getRunId().equals("run2")) { assertEquals(PipelineStatus.PENDING, p.getStatus()); assertFalse(p.isEnumDone()); }
            else { assertEquals(PipelineStatus.SUCCEEDED, p.getStatus()); assertTrue(p.isEnumDone()); }
        }
        GateRecord g = v.getGates().get(0);
        assertEquals(GateStatus.CLOSED, g.getStatus());
        assertNull(g.getArmedAt());
        assertNull(g.getDeadlineAt());
        assertNull(g.getOpenedBy());
        assertNull(g.getOpenedAt());
        assertTrue(g.getContext() == null || g.getContext().isEmpty());
        assertFalse(g.isAlerted());
        assertTrue(store.loadGateSignals(rid, "gate1").isEmpty(), "signals of the old attempt are gone");
        assertEquals(1, store.countChunks(rid, "p1", ChunkStatus.DONE), "run1's chunk records survive");
        assertEquals(0, store.countChunks(rid, "p3", null));
        // a later signal of the new attempt is kept
        store.saveGateSignal(rid, "gate1", "S0000@3", "c");
        assertEquals(1, store.loadGateSignals(rid, "gate1").size());
    }

    private static Map<String, String> m2(String k, String v) { Map<String, String> r = new HashMap<>(); r.put(k, v); return r; }

    @Test
    void perfApplyTenBatchesOf500() {
        String rid = mk("perf");
        long t0 = System.currentTimeMillis();
        for (int b = 0; b < 10; b++) {
            List<ChunkEvent> evs = new ArrayList<>();
            for (int i = 0; i < 500; i++) evs.add(ev(rid, i % 50 == 0 ? "DEAD" : "DONE", "p1", String.format("S%05d", b * 500 + i), "run1", "split", i, m("e1", i)));
            store.applyChunkEvents(rid, evs, (b + 1) + "-0", 1);
        }
        long ms = System.currentTimeMillis() - t0;
        assertEquals(5000, store.countChunks(rid, "p1", null));
        assertEquals(4900, store.countChunks(rid, "p1", ChunkStatus.DONE));
        assertTrue(ms < 10_000, "took " + ms + "ms");
    }

    // ------------------------------------------------------------------ retention

    private static final long FUTURE = System.currentTimeMillis() + 3_600_000L;

    private String finished(String n, RoundStatus end) {
        String rid = mk(n);
        assertTrue(store.casRoundStatus(rid, RoundStatus.CREATED, RoundStatus.RUNNING, null, 1));
        assertTrue(store.casRoundStatus(rid, RoundStatus.RUNNING, end, end == RoundStatus.FAILED ? "x" : null, 1));
        return rid;
    }

    private void fill(String rid) {
        savePending(rid, Arrays.asList(pending(rid, "p1", "c1", "run1"), pending(rid, "p1", "c2", "run1"), pending(rid, "p1", "c3", "run1"),
                pending(rid, "p2", "c1", "run1")));
        store.applyChunkEvents(rid, Arrays.asList(
                ev(rid, "DONE", "p1", "c1", "run1", "split", 10, m("e1", 7, "sink:out", 3)),
                ev(rid, "DONE", "p1", "c2", "run1", "split", 5, m("e1", 3)),
                ev(rid, "DEAD", "p1", "c3", "run1", "split", 0, null)), "5-0", 1);
        store.saveGateSignal(rid, "gate1", "k1", "payload");
    }

    @Test
    void compactKeepsSummariesAndViewCounts() {
        String rid = mk("compact");
        fill(rid);
        assertEquals(0, store.compactRound(rid, FUTURE), "a non-terminal round is never compacted");
        assertEquals(4, store.countChunks(rid, null, null));
        assertTrue(store.casRoundStatus(rid, RoundStatus.CREATED, RoundStatus.RUNNING, null, 1));
        assertTrue(store.casRoundStatus(rid, RoundStatus.RUNNING, RoundStatus.SUCCEEDED, null, 1));
        RoundView before = store.loadRound(rid);
        assertEquals(0, store.compactRound(rid, 1L), "finishedAt is not older than the cutoff");
        assertFalse(store.findTerminalRounds(1L, 1000).contains(rid));
        assertTrue(store.findTerminalRounds(FUTURE, 1000).contains(rid));

        assertEquals(4, store.compactRound(rid, FUTURE));
        assertEquals(0, store.compactRound(rid, FUTURE), "already compacted");
        assertFalse(store.findTerminalRounds(FUTURE, 1000).contains(rid));
        assertEquals(0, store.countChunks(rid, null, null));
        assertTrue(store.queryChunks(ChunkQuery.builder().roundId(rid).limit(100).build()).isEmpty());
        RoundView after = store.loadRound(rid);
        assertTrue(after.getRound().isCompacted());
        assertEquals(RoundStatus.SUCCEEDED, after.getRound().getStatus());
        assertEquals(before.getChunkCounts(), after.getChunkCounts());
        assertEquals(before.getRowsIn(), after.getRowsIn());
        assertEquals(before.getRowsOut(), after.getRowsOut());
        assertEquals(Long.valueOf(2), after.getChunkCounts().get("p1").get(ChunkStatus.DONE));
        assertEquals(Long.valueOf(1), after.getChunkCounts().get("p1").get(ChunkStatus.DEAD));
        assertEquals(Long.valueOf(15), after.getRowsIn().get("p1"));
        assertEquals(Long.valueOf(13), after.getRowsOut().get("p1"));
        assertEquals(Long.valueOf(1), after.getChunkCounts().get("p2").get(ChunkStatus.PENDING));
        for (PipelineRecord p : after.getPipelines()) if (p.getPipelineId().equals("p1")) assertNotNull(p.getSummaryCounts());
        assertEquals(1, store.loadGateSignals(rid, "gate1").size(), "compaction keeps gate signals");
    }

    @Test
    void rerunAfterCompactionDropsStaleSummaries() {
        String rid = finished("recompact", RoundStatus.FAILED);
        fill(rid);
        assertEquals(4, store.compactRound(rid, FUTURE));
        store.resetRuns(rid, Collections.singletonList("run1"), Collections.singletonList("gate1"), 2);
        RoundView v = store.loadRound(rid);
        assertEquals(RoundStatus.RUNNING, v.getRound().getStatus());
        assertFalse(v.getRound().isCompacted());
        assertTrue(v.getChunkCounts().isEmpty());
        assertEquals(0, store.compactRound(rid, FUTURE), "running again");
    }

    @Test
    void purgeDeletesEverythingAndTheBizKeyCanBeResubmitted() {
        String rid = finished("purge", RoundStatus.SUCCEEDED);
        fill(rid);
        String other = finished("purge-compacted", RoundStatus.CANCELLED);
        fill(other);
        store.compactRound(other, FUTURE);
        String keep = mk("purge-keep");
        List<String> purged = store.purgeRounds(FUTURE, 1000);
        assertTrue(purged.contains(rid));
        assertTrue(purged.contains(other));
        assertFalse(purged.contains(keep));
        for (String id : Arrays.asList(rid, other)) {
            assertNull(store.loadRoundRecord(id));
            assertNull(store.loadRound(id));
            assertNull(store.loadRoundState(id));
            assertTrue(store.queryChunks(ChunkQuery.builder().roundId(id).limit(100).build()).isEmpty());
            assertTrue(store.loadGateSignals(id, "gate1").isEmpty());
            assertEquals(0, store.countChunks(id, null, null));
            assertFalse(store.findActiveRounds().contains(id));
        }
        assertNull(store.findRoundByBizKey("g-" + tag, "biz-purge"));
        assertNotNull(store.loadRoundRecord(keep));
        assertFalse(store.purgeRounds(FUTURE, 1000).contains(rid), "purge is idempotent");
        String again = rid("purge-again");
        RoundRecord created = store.createRound(newRound(again, "biz-purge"), Collections.singletonList(run(again, "run1", 0)),
                Collections.<GateRecord>emptyList(), Collections.<PipelineRecord>emptyList());
        assertEquals(again, created.getRoundId());
    }

    @Test
    void purgeRefusesNonTerminalAndTooRecentRounds() {
        String created = mk("purge-created");
        String running = mk("purge-running");
        assertTrue(store.casRoundStatus(running, RoundStatus.CREATED, RoundStatus.RUNNING, null, 1));
        String paused = mk("purge-paused");
        assertTrue(store.casRoundStatus(paused, RoundStatus.CREATED, RoundStatus.RUNNING, null, 1));
        assertTrue(store.casRoundStatus(paused, RoundStatus.RUNNING, RoundStatus.PAUSED, null, 1));
        String recent = finished("purge-recent", RoundStatus.SUCCEEDED);
        List<String> purged = store.purgeRounds(FUTURE, 1000);
        for (String id : Arrays.asList(created, running, paused)) { assertFalse(purged.contains(id)); assertNotNull(store.loadRoundRecord(id)); }
        assertTrue(purged.contains(recent));
        String young = finished("purge-young", RoundStatus.FAILED);
        assertFalse(store.purgeRounds(1L, 1000).contains(young));
        assertNotNull(store.loadRoundRecord(young));
        assertEquals(0, store.purgeRounds(FUTURE, 0).size());
    }

    @Test
    void roundFlippedBackToRunningBetweenFindAndPurgeIsNotPurged() {
        String rid = finished("purge-flip", RoundStatus.FAILED);
        fill(rid);
        assertTrue(store.findTerminalRounds(FUTURE, 1000).contains(rid));
        store.resetRuns(rid, Collections.singletonList("run1"), Collections.singletonList("gate1"), 2);   // what rerunFrom/retryRun do
        assertEquals(RoundStatus.RUNNING, store.loadRoundRecord(rid).getStatus());
        assertFalse(store.purgeRounds(FUTURE, 1000).contains(rid));
        assertEquals(0, store.compactRound(rid, FUTURE));
        assertNotNull(store.loadRound(rid));
        assertNotNull(store.findRoundByBizKey("g-" + tag, "biz-purge-flip"));
        // and it can finish and be purged later
        assertTrue(store.casRoundStatus(rid, RoundStatus.RUNNING, RoundStatus.SUCCEEDED, null, 2));
        assertTrue(store.purgeRounds(FUTURE, 1000).contains(rid));
    }

    @Test
    void retentionBatchesAreBoundedAndOldestFirst() {
        List<String> ids = new ArrayList<>();
        for (int i = 0; i < 3; i++) { ids.add(finished("batch" + i, RoundStatus.SUCCEEDED)); try { Thread.sleep(3); } catch (InterruptedException e) { Thread.currentThread().interrupt(); } }
        List<String> found = store.findTerminalRounds(FUTURE, 1000);
        assertTrue(found.indexOf(ids.get(0)) < found.indexOf(ids.get(1)) && found.indexOf(ids.get(1)) < found.indexOf(ids.get(2)));
        assertEquals(1, store.findTerminalRounds(FUTURE, 1).size());
        assertTrue(store.purgeRounds(FUTURE, 1).size() <= 1);
    }

    @Test
    void reserveMasterEpochIsAtomicCompareAndSet() throws Exception {
        long base = store.maxMasterEpoch();
        long e = base + 10;
        assertTrue(store.reserveMasterEpoch(e));
        assertEquals(e, store.maxMasterEpoch());
        assertFalse(store.reserveMasterEpoch(e), "the same epoch cannot be owned twice");
        assertFalse(store.reserveMasterEpoch(e - 5), "a lower epoch is refused");
        assertEquals(e, store.maxMasterEpoch(), "a refused reservation does not lower the maximum");
        assertTrue(store.reserveMasterEpoch(e + 1));
        // a round fenced by a higher epoch also blocks a lower reservation
        String rid = mk("reserve");
        long high = e + 100;
        assertTrue(store.fenceRound(rid, high));
        assertFalse(store.reserveMasterEpoch(high), "round epochs count");
        assertTrue(store.reserveMasterEpoch(high + 1));
        // two masters handed the same epoch: exactly one owns it
        long raced = high + 50;
        ExecutorService pool = Executors.newFixedThreadPool(8);
        try {
            List<Future<Boolean>> fs = new ArrayList<>();
            for (int i = 0; i < 8; i++) fs.add(pool.submit(() -> store.reserveMasterEpoch(raced)));
            int owners = 0;
            for (Future<Boolean> f : fs) if (f.get()) owners++;
            assertEquals(1, owners);
        } finally {
            pool.shutdownNow();
        }
        assertEquals(raced, store.maxMasterEpoch());
    }

    @Test
    void setRoundIncompleteIsFencedByTheRoundEpoch() {
        String rid = mk("incfence");
        assertTrue(store.fenceRound(rid, 9));
        assertFalse(store.setRoundIncomplete(rid, 8), "a deposed master (lower epoch) is rejected");
        assertFalse(store.loadRoundRecord(rid).isIncomplete());
        assertEquals(9, store.loadRoundRecord(rid).getMasterEpoch());
        assertTrue(store.setRoundIncomplete(rid, 9));
        assertTrue(store.loadRoundRecord(rid).isIncomplete());
        assertFalse(store.setRoundIncomplete(rid("no-such-round"), 9), "a missing round");
    }

    @Test
    void maxRoundSeqSurvivesRetentionPurgingEveryRound() {
        long base = store.maxRoundSeq();
        String rid = rid("seqpurge");
        RoundRecord a = newRound(rid, "biz-seqpurge");
        a.setSeq(base + 11);
        store.createRound(a, Collections.singletonList(run(rid, "run1", 0)), Collections.<GateRecord>emptyList(), Collections.singletonList(pl(rid, "p1", "run1")));
        assertTrue(store.casRoundStatus(rid, RoundStatus.CREATED, RoundStatus.RUNNING, null, 1));
        assertTrue(store.casRoundStatus(rid, RoundStatus.RUNNING, RoundStatus.SUCCEEDED, null, 1));
        assertTrue(store.purgeRounds(FUTURE, 1000).contains(rid));
        assertNull(store.loadRoundRecord(rid));
        assertTrue(store.maxRoundSeq() >= base + 11, "the highest issued seq is persisted, not derived from the rounds that exist: " + store.maxRoundSeq());
    }

    @Test
    void purgedRoundsAreTrackedAsPendingUntilCleared() {
        String rid = mk("pendpurge");
        assertTrue(store.casRoundStatus(rid, RoundStatus.CREATED, RoundStatus.RUNNING, null, 1));
        assertTrue(store.casRoundStatus(rid, RoundStatus.RUNNING, RoundStatus.SUCCEEDED, null, 1));
        assertFalse(store.pendingPurges(10000).contains(rid));
        assertTrue(store.purgeRounds(FUTURE, 1000).contains(rid));
        assertTrue(store.pendingPurges(10000).contains(rid), "recorded by purgeRounds itself");
        assertTrue(store.pendingPurges(10000).contains(rid), "reading does not remove it");
        store.clearPendingPurges(Collections.singletonList(rid));
        assertFalse(store.pendingPurges(10000).contains(rid));
        store.clearPendingPurges(Collections.singletonList(rid));   // idempotent
        assertEquals(0, store.pendingPurges(0).size());
    }

    @Test
    void maxRoundSeqTracksCreatedRounds() {
        long base = store.maxRoundSeq();
        RoundRecord a = newRound(rid("seqa"), "biz-seqa");
        a.setSeq(base + 7);
        store.createRound(a, Collections.singletonList(run(a.getRoundId(), "run1", 0)), Collections.<GateRecord>emptyList(), Collections.singletonList(pl(a.getRoundId(), "p1", "run1")));
        assertEquals(base + 7, store.maxRoundSeq());
        RoundRecord b = newRound(rid("seqb"), "biz-seqb");
        b.setSeq(base + 3);
        store.createRound(b, Collections.singletonList(run(b.getRoundId(), "run1", 0)), Collections.<GateRecord>emptyList(), Collections.singletonList(pl(b.getRoundId(), "p1", "run1")));
        assertEquals(base + 7, store.maxRoundSeq(), "a lower seq does not lower the maximum");
    }

    @Test
    void countSinkChunksCountsOnlyDoneChunksThatWroteSinkRows() {
        String rid = mk("sinkchunks");
        assertEquals(0, store.countSinkChunks(rid, "p1"));
        savePending(rid, Collections.singletonList(pending(rid, "p1", "S0004", "run1")));
        store.applyChunkEvents(rid, Arrays.asList(
                ev(rid, "DONE", "p1", "S0000", "run1", "split", 10, m("sink:out", 10)),                 // wrote rows
                ev(rid, "DONE", "p1", "S0001", "run1", "split", 5, m("e1", 5, "sink:out", 0)),          // empty sink output: sink never called
                ev(rid, "DONE", "p1", "S0002", "run1", "split", 0, m("sink:out", 0)),                   // empty chunk
                ev(rid, "DONE", "p1", "S0003", "run1", "split", 6, m("sink:a", 2, "sink:b", 3, "e1", 1)), // two ports: still one chunk
                ev(rid, "DEAD", "p1", "S0005", "run1", "split", 0, null),
                ev(rid, "DONE", "p2", "S0000>1.0", "run1", "e1", 3, m("sink:out", 3))), "1-0", 1);
        assertEquals(2, store.countSinkChunks(rid, "p1"), "S0000 and S0003");
        assertEquals(4, store.countChunks(rid, "p1", ChunkStatus.DONE), "all DONE chunks, empty or not");
        assertEquals(1, store.countSinkChunks(rid, "p2"));
        // events are replayed after a failover: counting stays idempotent
        store.applyChunkEvents(rid, Arrays.asList(ev(rid, "DONE", "p1", "S0000", "run1", "split", 10, m("sink:out", 10)),
                ev(rid, "DONE", "p1", "S0003", "run1", "split", 6, m("sink:a", 2, "sink:b", 3, "e1", 1))), "2-0", 1);
        assertEquals(2, store.countSinkChunks(rid, "p1"));
        // a later DONE with other numbers replaces the record, and the count follows
        store.applyChunkEvents(rid, Collections.singletonList(ev(rid, "DONE", "p1", "S0000", "run1", "split", 10, m("sink:out", 0))), "3-0", 1);
        assertEquals(1, store.countSinkChunks(rid, "p1"));
        store.applyChunkEvents(rid, Collections.singletonList(ev(rid, "DONE", "p1", "S0004", "run1", "split", 1, m("sink:out", 1))), "4-0", 1);
        assertEquals(2, store.countSinkChunks(rid, "p1"), "a pending chunk completing counts once");
        // a rerun deletes the chunk records, and the count with them
        store.resetRuns(rid, Collections.singletonList("run1"), Collections.<String>emptyList(), 2);
        assertEquals(0, store.countSinkChunks(rid, "p1"));
        assertEquals(0, store.countSinkChunks(rid, "p2"));
    }

    @Test
    void gateSignalKeyLongerThanTheLimitIsRejectedNotTruncated() {
        String rid = mk("sigkey");
        StringBuilder sb = new StringBuilder();
        while (sb.length() < ControlPlaneStore.MAX_SIGNAL_KEY_LENGTH - 2) sb.append('k');
        String stem = sb.toString();
        String ka = stem + "@0", kb = stem + "@1";                  // exactly at the limit, differing only in the last characters
        assertEquals(ControlPlaneStore.MAX_SIGNAL_KEY_LENGTH, ka.length());
        store.saveGateSignal(rid, "gate1", ka, "a");
        store.saveGateSignal(rid, "gate1", kb, "b");
        Map<String, String> got = store.loadGateSignals(rid, "gate1");
        assertEquals(2, got.size(), "keys past the old 190 character limit stay distinct");
        assertEquals("a", got.get(ka));
        assertEquals("b", got.get(kb));
        String tooLong = stem + "@10";
        assertThrows(IllegalArgumentException.class, () -> store.saveGateSignal(rid, "gate1", tooLong, "x"));
        assertThrows(IllegalArgumentException.class, () -> store.saveGateSignal(rid, "gate1", "", "x"));
        assertEquals(2, store.loadGateSignals(rid, "gate1").size(), "nothing was stored for the rejected keys");
    }

    // ------------------------------------------------------------------ round 2: atomic reset, revive, paging

    private RunRecord runOf(String rid, String runId) {
        return store.loadRound(rid).getRuns().stream().filter(x -> x.getRunId().equals(runId)).findFirst().get();
    }

    @Test
    void resetRunsRejectedByALaterRunAppliesNothing() {
        String rid = mk("resetatomic");
        assertTrue(store.casRoundStatus(rid, RoundStatus.CREATED, RoundStatus.RUNNING, null, 5));
        assertTrue(store.casRunStatus(rid, "run1", RunStatus.PENDING, RunStatus.RUNNING, null, 5));
        assertTrue(store.casRunStatus(rid, "run2", RunStatus.PENDING, RunStatus.RUNNING, null, 8));   // run2 was written by a newer epoch; the round's stays 5
        store.setPipelineEnumDone(rid, "p1", true, 5);
        store.applyChunkEvents(rid, Collections.singletonList(ev(rid, "DONE", "p1", "S0000", "run1", "split", 3, m("e1", 3))), "1-0", 5);
        store.saveGateSignal(rid, "gate1", "k", "v");
        assertTrue(store.setRoundIncomplete(rid, 5));
        assertEquals(5, store.loadRoundRecord(rid).getMasterEpoch());

        // run1 passes, run2 is rejected: the whole reset (including run1, the gate and the round epoch bump) is rolled back
        assertThrows(StaleEpochException.class, () -> store.resetRuns(rid, Arrays.asList("run1", "run2"), Collections.singletonList("gate1"), 6));
        RoundView v = store.loadRound(rid);
        assertEquals(5, v.getRound().getMasterEpoch(), "the fence bump was rolled back");
        assertTrue(v.getRound().isIncomplete());
        assertEquals(RunStatus.RUNNING, runOf(rid, "run1").getStatus());
        assertEquals(0, runOf(rid, "run1").getAttempt(), "no attempt was bumped");
        assertEquals(RunStatus.RUNNING, runOf(rid, "run2").getStatus());
        assertTrue(v.getPipelines().stream().filter(x -> x.getPipelineId().equals("p1")).findFirst().get().isEnumDone());
        assertEquals(1, store.countChunks(rid, "p1", ChunkStatus.DONE), "chunk records survive");
        assertEquals(1, store.loadGateSignals(rid, "gate1").size(), "signals survive");
    }

    @Test
    void resetRunsSkipsAnUnknownRunLikeEveryStore() {
        String rid = finished("resetunknown", RoundStatus.FAILED);
        store.saveGateSignal(rid, "gate1", "k", "v");
        store.resetRuns(rid, Arrays.asList("run1", "runX"), Arrays.asList("gate1", "gateX"), 6);   // runX / gateX do not exist: skipped
        assertEquals(RunStatus.PENDING, runOf(rid, "run1").getStatus());
        assertEquals(1, runOf(rid, "run1").getAttempt());
        assertEquals(RoundStatus.RUNNING, store.loadRoundRecord(rid).getStatus());
        assertEquals(6, store.loadRoundRecord(rid).getMasterEpoch());
        assertTrue(store.loadGateSignals(rid, "gate1").isEmpty());
        assertEquals(2, store.loadRound(rid).getRuns().size(), "no run was created for the unknown id");
    }

    @Test
    void resetRunsClearsTheIncompleteFlag() {
        String rid = mk("resetincomplete");
        assertTrue(store.setRoundIncomplete(rid, 1));
        assertTrue(store.loadRoundRecord(rid).isIncomplete());
        store.resetRuns(rid, Collections.singletonList("run1"), Collections.<String>emptyList(), 2);
        assertFalse(store.loadRoundRecord(rid).isIncomplete());
    }

    @Test
    void failedToRunningReviveClearsErrorAndFinishedAtButKeepsStartedAt() {
        String rid = mk("revive");
        assertTrue(store.casRoundStatus(rid, RoundStatus.CREATED, RoundStatus.RUNNING, null, 1));
        assertTrue(store.casRunStatus(rid, "run1", RunStatus.PENDING, RunStatus.RUNNING, null, 1));
        assertTrue(store.casPipelineStatus(rid, "p1", PipelineStatus.PENDING, PipelineStatus.RUNNING, null, 1));
        Long roundStart = store.loadRoundRecord(rid).getStartedAt();
        Long runStart = runOf(rid, "run1").getStartedAt();
        Long plStart = store.loadRound(rid).getPipelines().get(0).getStartedAt();
        assertNotNull(roundStart);
        assertTrue(store.casRunStatus(rid, "run1", RunStatus.RUNNING, RunStatus.FAILED, "run boom", 1));
        assertTrue(store.casPipelineStatus(rid, "p1", PipelineStatus.RUNNING, PipelineStatus.FAILED, "pl boom", 1));
        assertTrue(store.casRoundStatus(rid, RoundStatus.RUNNING, RoundStatus.FAILED, "failed runs: [run1]", 1));
        assertNotNull(store.loadRoundRecord(rid).getFinishedAt());

        assertTrue(store.casRoundStatus(rid, RoundStatus.FAILED, RoundStatus.RUNNING, null, 2));
        assertTrue(store.casRunStatus(rid, "run1", RunStatus.FAILED, RunStatus.RUNNING, null, 2));
        assertTrue(store.casPipelineStatus(rid, "p1", PipelineStatus.FAILED, PipelineStatus.RUNNING, null, 2));
        RoundRecord r = store.loadRoundRecord(rid);
        assertNull(r.getError());
        assertNull(r.getFinishedAt());
        assertEquals(roundStart, r.getStartedAt(), "startedAt is kept");
        RunRecord run = runOf(rid, "run1");
        assertNull(run.getError());
        assertNull(run.getFinishedAt());
        assertEquals(runStart, run.getStartedAt());
        PipelineRecord p = store.loadRound(rid).getPipelines().stream().filter(x -> x.getPipelineId().equals("p1")).findFirst().get();
        assertNull(p.getError());
        assertNull(p.getFinishedAt());
        assertEquals(plStart, p.getStartedAt());
    }

    @Test
    void aNullErrorKeepsTheStoredErrorOnOtherTransitions() {
        String rid = mk("keeperror");
        assertTrue(store.casRoundStatus(rid, RoundStatus.CREATED, RoundStatus.RUNNING, null, 1));
        assertTrue(store.casRoundStatus(rid, RoundStatus.RUNNING, RoundStatus.PAUSED, "warn", 1));
        assertTrue(store.casRoundStatus(rid, RoundStatus.PAUSED, RoundStatus.RUNNING, null, 1));
        assertEquals("warn", store.loadRoundRecord(rid).getError());
    }

    @Test
    void queryChunksPagesAcrossPipelinesWithFilters() {
        String rid = mk("page");
        savePending(rid, Arrays.asList(pending(rid, "p1", "c00", "run1"), pending(rid, "p1", "c01", "run1"), pending(rid, "p1", "c02", "run1"),
                pending(rid, "p1", "c03", "run1"), pending(rid, "p1", "c04", "run1"), pending(rid, "p2", "d00", "run1"), pending(rid, "p2", "d01", "run1"),
                pending(rid, "p2", "d02", "run1"), pending(rid, "p2", "d03", "run1"), pending(rid, "p3", "e00", "run2"), pending(rid, "p3", "e01", "run2"),
                pending(rid, "p3", "e02", "run2")));
        store.applyChunkEvents(rid, Arrays.asList(ev(rid, "DONE", "p1", "c01", "run1", "split", 1, m("e1", 1)), ev(rid, "DONE", "p1", "c03", "run1", "split", 1, m("e1", 1)),
                ev(rid, "DONE", "p2", "d00", "run1", "split", 1, m("e1", 1)), ev(rid, "DONE", "p2", "d02", "run1", "split", 1, m("e1", 1)),
                ev(rid, "DONE", "p2", "d03", "run1", "split", 1, m("e1", 1)), ev(rid, "DONE", "p3", "e01", "run2", "split", 1, m("e9", 1))), "1-0", 1);
        assertEquals(12, store.queryChunks(ChunkQuery.builder().roundId(rid).limit(1000).build()).size());
        assertEquals(Arrays.asList("c03", "c04", "d00", "d01", "d02"), ids(store.queryChunks(ChunkQuery.builder().roundId(rid).limit(5).offset(3).build())));
        assertEquals(Arrays.asList("c01", "c03", "d00", "d02", "d03", "e01"), ids(store.queryChunks(ChunkQuery.builder().roundId(rid).status(ChunkStatus.DONE).limit(100).build())));
        assertEquals(Arrays.asList("d00", "d02", "d03"), ids(store.queryChunks(ChunkQuery.builder().roundId(rid).status(ChunkStatus.DONE).limit(3).offset(2).build())));
        assertEquals(Arrays.asList("e00", "e01", "e02"), ids(store.queryChunks(ChunkQuery.builder().roundId(rid).runId("run2").limit(100).build())));
        assertEquals(Arrays.asList("c00", "c02", "c04", "d01"), ids(store.queryChunks(ChunkQuery.builder().roundId(rid).runId("run1").status(ChunkStatus.PENDING).limit(100).build())));
        assertEquals(Arrays.asList("d00", "d01", "d02", "d03"), ids(store.queryChunks(ChunkQuery.builder().roundId(rid).pipelineId("p2").runId("run1").limit(100).build())));
        assertTrue(store.queryChunks(ChunkQuery.builder().roundId(rid).pipelineId("p2").runId("run2").limit(100).build()).isEmpty());
        assertTrue(store.queryChunks(ChunkQuery.builder().roundId(rid).limit(0).build()).isEmpty());
        assertTrue(store.queryChunks(ChunkQuery.builder().roundId(rid).limit(10).offset(500).build()).isEmpty());
    }

    @Test
    void edgeSumsFollowReplacedRecordsAndResets() {
        String rid = mk("edges");
        store.applyChunkEvents(rid, Arrays.asList(ev(rid, "DONE", "p1", "S0000", "run1", "split", 10, m("e1", 8, "sink:x", 2)),
                ev(rid, "DONE", "p2", "S0000>1.0", "run1", "e1", 8, m("sink:out", 8))), "1-0", 1);
        assertEquals(m("e1", 8), store.sumRowsOutByEdge(rid, "run1"));
        assertEquals(m("e1", 8), store.sumRowsInByEdge(rid, "run1"));
        store.applyChunkEvents(rid, Collections.singletonList(ev(rid, "DONE", "p1", "S0000", "run1", "split", 10, m("e1", 5))), "2-0", 1);   // replaced by a retry
        assertEquals(5L, store.sumRowsOutByEdge(rid, "run1").get("e1"));
        store.applyChunkEvents(rid, Collections.singletonList(ev(rid, "DEAD", "p1", "S0000", "run1", "split", 0, null)), "3-0", 1);   // DEAD never replaces DONE
        assertEquals(5L, store.sumRowsOutByEdge(rid, "run1").get("e1"));
        store.resetRuns(rid, Collections.singletonList("run1"), Collections.<String>emptyList(), 2);
        assertTrue(store.sumRowsOutByEdge(rid, "run1").values().stream().allMatch(v -> v == 0));
        assertTrue(store.sumRowsInByEdge(rid, "run1").values().stream().allMatch(v -> v == 0));
    }

    @Test
    void resetRunsAtTheRoundsOwnEpochIsNotFenced() {
        String rid = mk("resetsame");
        assertTrue(store.fenceRound(rid, 7));
        assertTrue(store.fenceRound(rid, 7));
        store.resetRuns(rid, Collections.singletonList("run1"), Collections.<String>emptyList(), 7);   // same epoch: allowed, even though nothing about the epoch changes
        assertEquals(1, runOf(rid, "run1").getAttempt());
        assertTrue(store.casRoundStatus(rid, RoundStatus.CREATED, RoundStatus.RUNNING, null, 7));
        assertTrue(store.casRunStatus(rid, "run1", RunStatus.PENDING, RunStatus.RUNNING, null, 7));
    }
}
