package io.github.amwtke.pipeliner.runtime;

import static io.github.amwtke.pipeliner.runtime.fake.TestKit.*;
import static org.junit.jupiter.api.Assertions.*;

import io.github.amwtke.pipeliner.api.GateCondition;
import io.github.amwtke.pipeliner.api.GateContext;
import io.github.amwtke.pipeliner.api.GateDecision;
import io.github.amwtke.pipeliner.api.PipelinerException;
import io.github.amwtke.pipeliner.core.codec.ChunkCodec;
import io.github.amwtke.pipeliner.core.graph.ExecMode;
import io.github.amwtke.pipeliner.core.graph.GateDef;
import io.github.amwtke.pipeliner.core.graph.Gates;
import io.github.amwtke.pipeliner.core.graph.PipelineGraph;
import io.github.amwtke.pipeliner.core.graph.TimeoutPolicy;
import io.github.amwtke.pipeliner.core.plan.ExecutionPlan;
import io.github.amwtke.pipeliner.core.store.*;
import io.github.amwtke.pipeliner.runtime.fake.TestPlanner;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;

/** Round 2 admin findings: refusal rules, reliable command queue, attempt-scoped signals. */
class AdminHardeningTest {
    private static final Map<String, ExecMode> NO_MODES = Collections.<String, ExecMode>emptyMap();

    private static PipelineGraph twoRuns(String s1, String s2, AtomicBoolean poison, GateDef gate, Duration timeout) {
        PipelineGraph.Builder b = PipelineGraph.builder("ah")
                .pipeline("p1", p -> p.source(new RangeSource(4, 3)).row((row, out, ctx) -> {
                    if (poison.get() && row.getLong("id") == 5) throw new IllegalStateException("poison row");
                    out.emit(row);
                }).sink(collect(s1)))
                .pipeline("p2", p -> p.source(new RangeSource(3, 3)).sink(collect(s2)));
        if (gate != null) b.gate("G", gate, gb -> { gb.after("p1").before("p2"); if (timeout != null) gb.timeout(timeout, TimeoutPolicy.FAIL); });
        return b.build();
    }

    private static PipelineGraph gated(GateDef gate, Duration timeout) {
        return twoRuns(uniqueName("s"), uniqueName("s"), new AtomicBoolean(false), gate, timeout);
    }

    private static PipelinerException fails(org.junit.jupiter.api.function.Executable r) {
        return assertThrows(PipelinerException.class, r);
    }

    private static GateRecord gate(DefaultPipelinerEngine e, String round) {
        for (GateRecord g : e.view(round).getGates()) if (g.getGateId().equals("G")) return g;
        return null;
    }

    private static RunRecord run(DefaultPipelinerEngine e, String round, String id) {
        for (RunRecord r : e.view(round).getRuns()) if (r.getRunId().equals(id)) return r;
        return null;
    }

    // F009
    @Test void unknownRoundIsAnErrorNotADeposition() throws Exception {
        try (Harness h = new Harness(gated(null, null), NO_MODES)) {
            DefaultPipelinerEngine e = h.pod("pod");
            assertTrue(waitFor(e::isLeader, 5000));
            long epoch = h.plane.currentEpoch();
            PipelinerException ex = fails(() -> e.pause("no-such-round"));
            assertTrue(ex.getMessage().contains("unknown round"), ex.getMessage());
            assertTrue(e.isLeader());
            assertEquals(epoch, h.plane.currentEpoch(), "no step-down, no new epoch");
            String round = e.submit("ah", "k", null);
            e.pause(round);
            assertEquals(RoundStatus.PAUSED, e.view(round).getRound().getStatus());
        }
    }

    // F095
    @Test void adminCommandFencesTheDataPlane() throws Exception {
        try (Harness h = new Harness(gated(null, null), NO_MODES)) {
            DefaultPipelinerEngine e = h.pod(cfg("a").workerEnabled(false).build(), true);
            String round = e.submit("ah", "k", null);
            assertTrue(waitFor(e::isLeader, 5000));
            long epoch = h.plane.currentEpoch();
            assertTrue(h.plane.fenceRound(round, epoch + 100), "a newer master owns the data plane of the round");
            PipelinerException ex = fails(() -> e.cancel(round));
            assertTrue(ex.getMessage().contains("deposed"), ex.getMessage());
            assertNotEquals(RoundStatus.CANCELLED, e.view(round).getRound().getStatus());
        }
    }

    // F096
    @Test void planHashMismatchIsRefusedWithoutNpe() throws Exception {
        try (Harness h = new Harness(gated(null, null), NO_MODES)) {
            DefaultPipelinerEngine e = h.pod(cfg("a").workerEnabled(false).build(), true);
            String round = e.submit("ah", "k", null);
            java.lang.reflect.Field f = ExecutionPlan.class.getDeclaredField("hash");   // the leader now runs a different plan version
            f.setAccessible(true);
            f.set(h.plan(), "other-plan");
            PipelinerException ex = fails(() -> e.rerunFrom(round, "R1"));
            assertTrue(ex.getMessage().contains("different plan"), ex.getMessage());
            assertTrue(fails(() -> e.retryRun(round, "R2")).getMessage().contains("different plan"));
            assertTrue(fails(() -> e.cancel(round)).getMessage().contains("different plan"));
        }
    }

    // F001
    @Test void retryRefusedWhenItsInGateTimedOut() throws Exception {
        try (Harness h = new Harness(gated(Gates.manual(), Duration.ofMillis(300)), NO_MODES)) {
            DefaultPipelinerEngine e = h.pod("pod");
            String round = e.submit("ah", "k", null);
            assertEquals(RoundStatus.FAILED, e.awaitRound(round, 10000));
            assertEquals(GateStatus.TIMEOUT, gate(e, round).getStatus());
            PipelinerException ex = fails(() -> e.retryRun(round, "R2"));
            assertTrue(ex.getMessage().contains("in-gate G"), ex.getMessage());
            assertEquals(RoundStatus.FAILED, e.view(round).getRound().getStatus());
        }
    }

    // F015
    @Test void openGateChecksAfterRunsAndReportsFailures() throws Exception {
        CountDownLatch release = new CountDownLatch(1);
        PipelineGraph g = PipelineGraph.builder("ah")
                .pipeline("p1", p -> p.source(new RangeSource(2, 2)).row((r, o, c) -> {
                    try { release.await(); } catch (InterruptedException ex) { Thread.currentThread().interrupt(); }
                    o.emit(r);
                }).sink(collect(uniqueName("s"))))
                .pipeline("p2", p -> p.source(new RangeSource(2, 2)).sink(collect(uniqueName("s"))))
                .gate("G", Gates.manual(), gb -> gb.after("p1").before("p2"))
                .build();
        try (Harness h = new Harness(g, NO_MODES)) {
            DefaultPipelinerEngine e = h.pod("pod");
            String round = e.submit("ah", "k", null);
            assertTrue(waitFor(() -> run(e, round, "R1").getStatus() == RunStatus.RUNNING, 5000));
            assertTrue(fails(() -> e.openGate(round, "G", "op")).getMessage().contains("not SUCCEEDED"));
            assertTrue(fails(() -> e.openGate(round, "typo", "op")).getMessage().contains("unknown gate"));
            assertEquals(GateStatus.CLOSED, gate(e, round).getStatus());
            e.openGate(round, "G", "op", true);
            assertEquals(GateStatus.OPEN, gate(e, round).getStatus());
            assertTrue(fails(() -> e.openGate(round, "G", "op", true)).getMessage().contains("not CLOSED"));
            release.countDown();
            assertEquals(RoundStatus.SUCCEEDED, e.awaitRound(round, 10000));
        }
    }

    // F012 / F053 / F011
    @Test void skipAndRetryGuards() throws Exception {
        try (Harness h = new Harness(twoRuns(uniqueName("s"), uniqueName("s"), new AtomicBoolean(false), null, null), mode(ExecMode.LOCAL, "p2"))) {
            DefaultPipelinerEngine e = h.pod("pod");
            String round = e.submit("ah", "k", null);
            assertEquals(RoundStatus.SUCCEEDED, e.awaitRound(round, 10000));
            assertTrue(fails(() -> e.skipDeadLetters(round, "R2")).getMessage().contains("CLUSTER"), "local skip is refused, not turned into retry");
            assertTrue(fails(() -> e.skipDeadLetters(round, "R1")).getMessage().contains("no dead letters"));
            assertFalse(e.view(round).getRound().isIncomplete(), "nothing skipped: not incomplete");
        }
        try (Harness h = new Harness(twoRuns(uniqueName("s"), uniqueName("s"), new AtomicBoolean(true), null, null), NO_MODES)) {
            DefaultPipelinerEngine e = h.pod("pod");
            String round = e.submit("ah", "k", null);
            assertEquals(RoundStatus.FAILED, e.awaitRound(round, 15000));
            assertTrue(h.store.compactRound(round, System.currentTimeMillis() + 10000) > 0);
            assertTrue(fails(() -> e.retryRun(round, "R1")).getMessage().contains("compacted"));
            assertTrue(fails(() -> e.skipDeadLetters(round, "R1")).getMessage().contains("compacted"));
        }
        try (Harness h = new Harness(gated(Gates.manual(), null), NO_MODES)) {
            DefaultPipelinerEngine e = h.pod("pod");
            String round = e.submit("ah", "k", null);
            e.cancel(round);
            assertTrue(fails(() -> e.retryRun(round, "R1")).getMessage().contains("cancelled"));
            assertTrue(fails(() -> e.skipDeadLetters(round, "R1")).getMessage().contains("cancelled"));
        }
    }

    // F054 / F012: skip drains first and still ends with the poisoned chunk SKIPPED (not DEAD again)
    @Test void skipEndsWithSkippedChunksNotDeadOnes() throws Exception {
        try (Harness h = new Harness(twoRuns(uniqueName("s"), uniqueName("s"), new AtomicBoolean(true), null, null), NO_MODES)) {
            DefaultPipelinerEngine e = h.pod("pod");
            String round = e.submit("ah", "k", null);
            assertEquals(RoundStatus.FAILED, e.awaitRound(round, 15000));
            e.skipDeadLetters(round, "R1");
            assertEquals(RoundStatus.SUCCEEDED, e.awaitRound(round, 15000));
            assertEquals(0, h.store.countChunks(round, "p1", ChunkStatus.DEAD));
            assertEquals(1, h.store.countChunks(round, "p1", ChunkStatus.SKIPPED));
            assertTrue(e.view(round).getRound().isIncomplete());
        }
    }

    // F013 / F014
    @Test void timedOutCommandIsWithdrawnAndNeverRunsLater() throws Exception {
        try (Harness h = new Harness(gated(null, null), NO_MODES)) {
            DefaultPipelinerEngine client = h.pod(cfg("client").masterEnabled(false).workerEnabled(false).adminTimeoutMillis(300).build(), true);
            String round = client.submit("ah", "k", null);
            PipelinerException ex = fails(() -> client.pause(round));
            assertTrue(ex.getMessage().contains("withdrawn"), ex.getMessage());
            assertTrue(h.plane.popCommands(10).isEmpty(), "queue is clean");
        }
    }

    @Test void expiredCommandIsSkippedAndAnswered() throws Exception {
        try (Harness h = new Harness(gated(null, null), NO_MODES)) {
            Map<String, String> cmd = new LinkedHashMap<>();
            cmd.put("id", "c1");
            cmd.put("op", "pause");
            cmd.put("roundId", "x");
            cmd.put("deadline", String.valueOf(System.currentTimeMillis() - 1000));
            h.plane.pushCommand(ChunkCodec.toJson(cmd));
            h.pod("pod");
            assertTrue(waitFor(() -> h.plane.getCommandResult("c1") != null, 5000));
            Map<String, String> r = ChunkCodec.toStringMap(h.plane.getCommandResult("c1"));
            assertEquals("false", r.get("ok"));
            assertEquals("true", r.get("expired"));
        }
    }

    @Test void withdrawOnlyRemovesQueuedCommands() throws Exception {
        try (Harness h = new Harness(gated(null, null), NO_MODES)) {
            h.plane.pushCommand("{\"id\":\"1\"}");
            h.plane.pushCommand("{\"id\":\"2\"}");
            assertTrue(h.plane.withdrawCommand("{\"id\":\"2\"}"));
            assertFalse(h.plane.withdrawCommand("{\"id\":\"2\"}"));
            assertEquals(Collections.singletonList("{\"id\":\"1\"}"), h.plane.popCommands(1));
            assertFalse(h.plane.withdrawCommand("{\"id\":\"1\"}"), "already popped");
        }
    }

    // F010
    @Test void rerunResetsOnlyGatesWhoseAfterRunIsRerun() {
        PipelineGraph g = PipelineGraph.builder("gr")
                .pipeline("pa", p -> p.source(new RangeSource(1, 1)).sink(collect(uniqueName("s"))))
                .pipeline("pb", p -> p.source(new RangeSource(1, 1)).sink(collect(uniqueName("s"))))
                .pipeline("pc", p -> p.source(new RangeSource(1, 1)).sink(collect(uniqueName("s"))))
                .gate("GA", Gates.barrier(), gb -> gb.after("pa").before("pc"))
                .gate("GB", Gates.barrier(), gb -> gb.after("pb").before("pc"))
                .build();
        ExecutionPlan plan = TestPlanner.plan(g, NO_MODES);
        List<String> runs = new ArrayList<>(Arrays.asList("R1"));
        runs.addAll(plan.downstreamRuns("R1"));
        assertEquals(Collections.singletonList("GA"), AdminOps.gatesToReset(plan, "R1", runs));
    }

    // F017
    private static GateContext ctx(Map<String, String> signals, int attempt) {
        return new GateContext() {
            @Override public String roundId() { return "r"; }
            @Override public String gateId() { return "G"; }
            @Override public Map<String, String> params() { return Collections.emptyMap(); }
            @Override public Map<String, String> signals() { return signals; }
            @Override public long armedAtMillis() { return 0; }
            @Override public long nowMillis() { return 0; }
            @Override public long doneChunkCount(String pipelineId) { return 0; }
            @Override public io.github.amwtke.pipeliner.api.DataSourceRegistry dataSources() { return null; }
            @Override public int runAttemptOf(String pipelineId) { return attempt; }
        };
    }

    @Test void externalCountOnlyCountsTheCurrentAttempt() throws Exception {
        Map<String, String> sig = new LinkedHashMap<>();
        sig.put("r:p:S0000@1", "x");
        sig.put("r:p:S0001@1", "x");
        sig.put("r:p:S0000@3", "x");
        GateCondition c = Gates.externalCount(3).getCondition();
        assertEquals(GateDecision.WAIT, c.evaluate(ctx(sig, 3)));
        sig.put("r:p:S0001@3", "x");
        sig.put("r:p:S0002@3", "x");
        assertEquals(GateDecision.OPEN, c.evaluate(ctx(sig, 3)));
        Map<String, String> plain = new LinkedHashMap<>();
        plain.put("a", "1"); plain.put("b", "1"); plain.put("c", "1");
        assertEquals(GateDecision.OPEN, c.evaluate(ctx(plain, 5)), "keys without @ count as before");
    }

    @Test void lateSignalOfASupersededAttemptIsRejected() throws Exception {
        try (Harness h = new Harness(gated(Gates.externalCount(100), null), NO_MODES)) {
            DefaultPipelinerEngine e = h.pod("pod");
            String round = e.submit("ah", "k", null);
            assertTrue(waitFor(() -> gate(e, round).getArmedAt() != null, 10000));
            assertTrue(e.signalGate(round, "G", round + ":p1:S0000@0", "x"));
            e.rerunFrom(round, "R1");
            assertTrue(waitFor(() -> run(e, round, "R1").getAttempt() > 0, 5000));
            int attempt = run(e, round, "R1").getAttempt();
            assertFalse(e.signalGate(round, "G", round + ":p1:S0001@" + (attempt - 1), "late"));
            assertTrue(e.signalGate(round, "G", round + ":p1:S0001@" + attempt, "ok"));
            assertTrue(e.signalGate(round, "G", "free-form-key", "ok"));
        }
    }
}
