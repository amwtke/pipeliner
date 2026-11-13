package io.github.amwtke.pipeliner.runtime;

import static io.github.amwtke.pipeliner.runtime.fake.TestKit.*;
import static org.junit.jupiter.api.Assertions.*;

import io.github.amwtke.pipeliner.api.PipelinerException;
import io.github.amwtke.pipeliner.core.dataplane.Keys;
import io.github.amwtke.pipeliner.core.graph.ExecMode;
import io.github.amwtke.pipeliner.core.graph.Gates;
import io.github.amwtke.pipeliner.core.graph.PipelineGraph;
import io.github.amwtke.pipeliner.core.store.*;
import io.github.amwtke.pipeliner.runtime.process.Events;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

/** Regression tests for the correctness fixes: journal batches, fencing, admin command queue, local-run recovery, poison chunks, gates. */
class ResilienceTest {
    private static PipelineGraph localGraph(String sink) {
        return PipelineGraph.builder("lt").pipeline("a", p -> p.source(new RangeSource(3, 2)).sink(collect(sink))).build();
    }

    private static PipelineGraph slowGraph() {
        return PipelineGraph.builder("slow")
                .pipeline("p", p -> p.source(new RangeSource(20, 1)).row((row, out, ctx) -> { Thread.sleep(40); out.emit(row); }).sink(collect(uniqueName("s"))))
                .build();
    }

    private static List<String> rows(int n, long perSplit) {
        List<String> l = new ArrayList<>();
        for (long id = 0; id < n; id++) l.add(io.github.amwtke.pipeliner.api.Row.of("id", id, "g", id / perSplit).toString());
        Collections.sort(l);
        return l;
    }

    /** State a master leaves behind when it dies right after starting a local run: round and run RUNNING, nothing else done. */
    private static String crashedAfterRunRunning(Harness h, DefaultPipelinerEngine e, String graph) {
        String round = e.submit(graph, "k", null);
        assertTrue(h.store.casRoundStatus(round, RoundStatus.CREATED, RoundStatus.RUNNING, null, 0));
        assertTrue(h.store.casRunStatus(round, "R1", RunStatus.PENDING, RunStatus.RUNNING, null, 0));
        return round;
    }

    @Test void localRunDoneInEarlierBatchSurvivesFailureInLaterBatch() throws Exception {
        String sink = uniqueName("s");
        try (Harness h = new Harness(localGraph(sink), mode(ExecMode.LOCAL, "a"))) {
            DefaultPipelinerEngine m = h.pod(cfg("m").workerEnabled(false).workerDeadMillis(60000).journalBatch(1).build(), false);
            String round = crashedAfterRunRunning(h, m, "lt");
            assertTrue(h.store.casPipelineStatus(round, "a", PipelineStatus.PENDING, PipelineStatus.RUNNING, null, 0));
            assertTrue(h.store.updateRunAssignment(round, "R1", "ghost", 1, 0));
            h.plane.assignLocalRun(round, "R1", "ghost", 1, 0);
            h.plane.heartbeat("ghost", Collections.singletonMap("graph.lt", "test-lt"));
            h.plane.appendJournal(round, Events.localRun(true, round, "R1", "a", "ghost", 1, null));
            h.plane.appendJournal(round, ChunkEvent.builder().type("DONE").roundId(round).runId("R1").pipelineId("a").chunkId("S0000")
                    .inputEdge("split").owner("ghost").rowsIn(2L).rowsOut(new HashMap<String, Long>()).ts(1L).build());
            h.store.failApplyOnCall = 2;   // batch 1 = LOCAL_RUN_DONE, batch 2 fails once
            m.start();
            assertEquals(RoundStatus.SUCCEEDED, m.awaitRound(round, 10000));
            assertEquals(1, h.store.countChunks(round, "a", ChunkStatus.DONE));
        }
    }

    @Test void fencedMasterStepsDownAndComesBackWithAHigherEpoch() throws Exception {
        try (Harness h = new Harness(slowGraph(), Collections.<String, ExecMode>emptyMap())) {
            DefaultPipelinerEngine e = h.pod("pod1");
            String round = e.submit("slow", "k", null);
            assertTrue(h.store.fenceRound(round, 5));   // as if a newer master (epoch 5) had taken the round
            assertEquals(RoundStatus.SUCCEEDED, e.awaitRound(round, 20000));
            assertTrue(h.plane.currentEpoch() >= 5, "the returning leader took fresh epochs until it was newer: " + h.plane.currentEpoch());
        }
    }

    @Test void adminCommandsRunOnTheLeaderAndReportErrors() throws Exception {
        try (Harness h = new Harness(slowGraph(), Collections.<String, ExecMode>emptyMap())) {
            h.pod(cfg("m").workerEnabled(false).build(), true);
            DefaultPipelinerEngine w = h.pod(cfg("w").masterEnabled(false).build(), true);
            assertFalse(w.isLeader());
            String round = w.submit("slow", "k", null);
            w.pause(round);   // returns only after the leader executed it
            assertEquals(RoundStatus.PAUSED, w.view(round).getRound().getStatus());
            assertTrue(h.plane.isPaused(round));
            w.resume(round);
            assertEquals(RoundStatus.RUNNING, w.view(round).getRound().getStatus());
            assertFalse(h.plane.isPaused(round));
            PipelinerException ex = assertThrows(PipelinerException.class, () -> w.rerunFrom(round, "NOPE"));
            assertTrue(ex.getMessage().contains("unknown run"), ex.getMessage());
            w.cancel(round);
            assertEquals(RoundStatus.CANCELLED, w.view(round).getRound().getStatus());
            assertTrue(h.plane.activeRounds().isEmpty());
        }
    }

    @Test void adminCommandTimesOutWithoutLeader() throws Exception {
        try (Harness h = new Harness(slowGraph(), Collections.<String, ExecMode>emptyMap())) {
            DefaultPipelinerEngine w = h.pod(cfg("w").masterEnabled(false).adminTimeoutMillis(300).build(), true);
            String round = w.submit("slow", "k", null);
            PipelinerException ex = assertThrows(PipelinerException.class, () -> w.pause(round));
            assertTrue(ex.getMessage().contains("timed out"), ex.getMessage());
            assertEquals(RoundStatus.CREATED, w.view(round).getRound().getStatus());
        }
    }

    @Test void localRunRecoversWhenMasterDiedBeforePipelinesWereStarted() throws Exception {
        String sink = uniqueName("s");
        try (Harness h = new Harness(localGraph(sink), mode(ExecMode.LOCAL, "a"))) {
            DefaultPipelinerEngine e = h.pod(cfg("pod").build(), false);
            String round = crashedAfterRunRunning(h, e, "lt");   // pipelines still PENDING, no assignment anywhere
            e.start();
            assertEquals(RoundStatus.SUCCEEDED, e.awaitRound(round, 15000));
            assertEquals(rows(6, 2), sinkRows(sink));
        }
    }

    @Test void localRunRecoversWhenAssignEntryIsMissing() throws Exception {
        String sink = uniqueName("s");
        try (Harness h = new Harness(localGraph(sink), mode(ExecMode.LOCAL, "a"))) {
            DefaultPipelinerEngine e = h.pod(cfg("pod").build(), false);
            String round = crashedAfterRunRunning(h, e, "lt");
            assertTrue(h.store.casPipelineStatus(round, "a", PipelineStatus.PENDING, PipelineStatus.RUNNING, null, 0));
            assertTrue(h.store.updateRunAssignment(round, "R1", "pod", 1, 0));   // store says pod|1, the data plane never got it
            assertTrue(h.plane.localRunAssignments(round).isEmpty());
            e.start();
            assertEquals(RoundStatus.SUCCEEDED, e.awaitRound(round, 15000));
            assertEquals(rows(6, 2), sinkRows(sink));
            assertEquals(1, h.store.loadRound(round).getRuns().get(0).getAttempt(), "republished, not reassigned");
        }
    }

    @Test void localRunIsNotAssignedToAPodWithoutThePlan() throws Exception {
        String sink = uniqueName("s");
        try (Harness h = new Harness(localGraph(sink), mode(ExecMode.LOCAL, "a"))) {
            h.plane.heartbeat("other", Collections.singletonMap("graph.else", "x"));   // live, but cannot run graph 'lt'
            DefaultPipelinerEngine m = h.pod(cfg("m").workerEnabled(false).workerDeadMillis(60000).build(), true);
            String round = m.submit("lt", "k", null);
            assertTrue(waitFor(() -> h.store.loadRound(round).getRuns().get(0).getStatus() == RunStatus.RUNNING, 5000));
            Thread.sleep(300);
            assertNull(h.store.loadRound(round).getRuns().get(0).getAssignee(), "left unassigned");
            h.pod(cfg("w").masterEnabled(false).build(), true);
            assertEquals(RoundStatus.SUCCEEDED, m.awaitRound(round, 15000));
            assertEquals("w", h.store.loadRound(round).getRuns().get(0).getAssignee());
        }
    }

    @Test void chunkDeliveredTooOftenIsDeadLetteredWithoutProcessing() throws Exception {
        String sink = uniqueName("s");
        AtomicInteger processed = new AtomicInteger();
        PipelineGraph g = PipelineGraph.builder("md")
                .pipeline("p", p -> p.source(new RangeSource(4, 1)).row((r, o, c) -> { processed.incrementAndGet(); o.emit(r); }).sink(collect(sink)))
                .build();
        try (Harness h = new Harness(g, Collections.<String, ExecMode>emptyMap())) {
            DefaultPipelinerEngine m = h.pod(cfg("m").workerEnabled(false).build(), true);
            String round = m.submit("md", "k", null);
            String queue = Keys.splitQueue(round, "p", 0);
            assertTrue(waitFor(() -> h.plane.streamLength(queue) == 4, 5000));
            h.plane.read("ghost#0", Collections.singletonList(queue), 1, 0);          // delivery 1, consumer dies mid-chunk ...
            for (int i = 1; i <= 3; i++) h.plane.autoClaim("ghost#" + i, queue, 0, 1);   // ... and so do three more claimers
            h.pod(cfg("w").masterEnabled(false).maxDeliveries(2).build(), true);
            assertEquals(RoundStatus.FAILED, m.awaitRound(round, 15000));
            List<ChunkRecord> dead = h.store.queryChunks(ChunkQuery.builder().roundId(round).status(ChunkStatus.DEAD).build());
            assertEquals(1, dead.size());
            assertTrue(dead.get(0).getError().contains("exceeded max deliveries"), dead.get(0).getError());
            assertEquals(3, processed.get(), "the poison chunk was never processed");
        }
    }

    @Test void gateStaysClosedUntilItsAfterRunsSucceeded() throws Exception {
        PipelineGraph g = PipelineGraph.builder("gr")
                .pipeline("p1", p -> p.source(new RangeSource(1, 1)).sink(collect(uniqueName("a"))))
                .pipeline("p2", p -> p.source(new RangeSource(1, 1)).sink(collect(uniqueName("b"))))
                .gate("G", Gates.barrier(), gb -> gb.after("p1").before("p2"))
                .build();
        try (Harness h = new Harness(g, Collections.<String, ExecMode>emptyMap())) {
            DefaultPipelinerEngine e = h.pod(cfg("pod").build(), false);
            String round = e.submit("gr", "k", null);
            // R1's pipeline succeeded but the run itself failed (e.g. row reconciliation): the gate must not open
            assertTrue(h.store.casRoundStatus(round, RoundStatus.CREATED, RoundStatus.RUNNING, null, 0));
            assertTrue(h.store.casRunStatus(round, "R1", RunStatus.PENDING, RunStatus.RUNNING, null, 0));
            assertTrue(h.store.casPipelineStatus(round, "p1", PipelineStatus.PENDING, PipelineStatus.RUNNING, null, 0));
            assertTrue(h.store.casPipelineStatus(round, "p1", PipelineStatus.RUNNING, PipelineStatus.SUCCEEDED, null, 0));
            assertTrue(h.store.casRunStatus(round, "R1", RunStatus.RUNNING, RunStatus.FAILED, "mismatch", 0));
            e.start();
            assertEquals(RoundStatus.FAILED, e.awaitRound(round, 10000));
            RoundView v = e.view(round);
            assertEquals(GateStatus.CLOSED, v.getGates().get(0).getStatus());
            for (RunRecord r : v.getRuns()) if (r.getRunId().equals("R2")) assertEquals(RunStatus.PENDING, r.getStatus());
        }
    }
}
