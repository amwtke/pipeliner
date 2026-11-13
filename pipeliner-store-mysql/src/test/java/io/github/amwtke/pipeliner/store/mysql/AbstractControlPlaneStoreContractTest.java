package io.github.amwtke.pipeliner.store.mysql;

import static org.junit.jupiter.api.Assertions.*;

import io.github.amwtke.pipeliner.core.graph.ExecMode;
import io.github.amwtke.pipeliner.core.graph.GateType;
import io.github.amwtke.pipeliner.core.store.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

/** Behavioural contract every ControlPlaneStore must satisfy. */
abstract class AbstractControlPlaneStoreContractTest {
    protected ControlPlaneStore store;

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
        assertEquals("{\"x\":1}", r.getPlanJson());
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
        store.setRoundIncomplete(rid);
        assertTrue(store.loadRoundRecord(rid).isIncomplete());
    }

    @Test
    void concurrentCasExactlyOneWins() throws Exception {
        String rid = mk("race");
        for (int round = 0; round < 5; round++) {
            final String run = "run" + (round % 2 + 1);
            final RunStatus from = RunStatus.PENDING;
            final long epoch = 1000 + round * 10;
            if (round >= 2) store.resetRun(rid, run, 1000 + round * 10 - 5); // back to PENDING
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
                if (store.casGateStatus(rid, "gate1", GateStatus.CLOSED, GateStatus.OPEN, "t", null, 1)) wins.incrementAndGet();
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
        store.saveGateSignal(rid, "gate1", "s1", "payload1");
        store.saveGateSignal(rid, "gate1", "s2", "payload2");
        assertEquals(2, store.loadGateSignals(rid, "gate1").size());
        assertEquals("payload2", store.loadGateSignals(rid, "gate1").get("s2"));
        assertTrue(store.loadGateSignals(rid, "nogate").isEmpty());

        store.resetGate(rid, "gate1", 1); // fenced
        assertEquals(GateStatus.OPEN, store.loadRound(rid).getGates().get(0).getStatus());
        store.resetGate(rid, "gate1", 3);
        g = store.loadRound(rid).getGates().get(0);
        assertEquals(GateStatus.CLOSED, g.getStatus());
        assertNull(g.getArmedAt());
        assertNull(g.getDeadlineAt());
        assertNull(g.getOpenedBy());
        assertNull(g.getOpenedAt());
        assertTrue(g.getContext() == null || g.getContext().isEmpty());
        assertFalse(g.isAlerted());
        assertEquals(2, store.loadGateSignals(rid, "gate1").size());
        assertTrue(store.casGateStatus(rid, "gate1", GateStatus.CLOSED, GateStatus.TIMEOUT, null, null, 3));
    }

    @Test
    void savePendingNeverOverwrites() {
        String rid = mk("pend");
        store.savePendingChunks(rid, Arrays.asList(pending(rid, "p1", "S0000", "run1"), pending(rid, "p1", "S0001", "run1")));
        assertEquals(2, store.countChunks(rid, "p1", ChunkStatus.PENDING));
        store.applyChunkEvents(rid, Collections.singletonList(ev(rid, "DONE", "p1", "S0000", "run1", "split", 10, m("e1", 10))), "1-0", 1);
        store.savePendingChunks(rid, Arrays.asList(pending(rid, "p1", "S0000", "run1"), pending(rid, "p1", "S0002", "run1")));
        List<ChunkRecord> all = store.queryChunks(ChunkQuery.builder().roundId(rid).build());
        assertEquals(3, all.size());
        assertEquals(ChunkStatus.DONE, all.get(0).getStatus());
        assertEquals(10L, all.get(0).getRowsIn());
        assertEquals("{\"a\":1}", all.get(1).getSplitSpec());
        assertEquals(ChunkStatus.PENDING, all.get(2).getStatus());
        store.savePendingChunks(rid, Collections.<ChunkRecord>emptyList());
    }

    @Test
    void applyEventsIdempotentDeadRulesAndOffset() {
        String rid = mk("apply");
        store.savePendingChunks(rid, Arrays.asList(pending(rid, "p1", "S0000", "run1"), pending(rid, "p1", "S0001", "run1"), pending(rid, "p1", "S0003", "run1")));
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
        store.savePendingChunks(rid, Arrays.asList(pending(rid, "p1", "S0004", "run1"), pending(rid, "p3", "S0000", "run2")));
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
        store.resetRun(rid, "run1", 3); // fenced
        assertEquals(2, store.countChunks(rid, "p1", ChunkStatus.DONE));
        store.resetRun(rid, "run1", 5);
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
        store.resetGate(rid, "gate1", 4);
        store.resetRun(rid, "run1", 4);
        store.markChunksSkipped(rid, "run1", 4);
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
        assertEquals(0, store.countChunks(rid, "p1", null));

        // the current master works, and a newer one deposes it
        assertTrue(store.casRunStatus(rid, "run1", RunStatus.PENDING, RunStatus.RUNNING, null, 5));
        store.applyChunkEvents(rid, Collections.singletonList(ev(rid, "DONE", "p1", "S0000", "run1", "split", 1, m("e1", 1))), "1-0", 5);
        assertEquals(1, store.countChunks(rid, "p1", ChunkStatus.DONE));
        assertTrue(store.fenceRound(rid, 6));
        assertFalse(store.casRunStatus(rid, "run1", RunStatus.RUNNING, RunStatus.FAILED, "x", 5));
        assertThrows(StaleEpochException.class, () -> store.applyChunkEvents(rid, Collections.<ChunkEvent>emptyList(), "2-0", 5));
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
}
