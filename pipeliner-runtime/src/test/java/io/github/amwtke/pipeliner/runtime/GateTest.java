package io.github.amwtke.pipeliner.runtime;

import static io.github.amwtke.pipeliner.runtime.fake.TestKit.*;
import static org.junit.jupiter.api.Assertions.*;

import io.github.amwtke.pipeliner.api.GateCondition;
import io.github.amwtke.pipeliner.api.GateDecision;
import io.github.amwtke.pipeliner.core.graph.*;
import io.github.amwtke.pipeliner.core.store.*;
import java.time.Duration;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;

class GateTest {
    private final AtomicBoolean fallbackSeen = new AtomicBoolean();

    /** p1 -> gate G -> p2 (independent runs R1, R2). */
    private PipelineGraph graph(GateDef def, Duration timeout, TimeoutPolicy policy) {
        return PipelineGraph.builder("gt")
                .pipeline("p1", p -> p.source(new RangeSource(2, 2)).sink(collect(uniqueName("a"))))
                .pipeline("p2", p -> p.source(new RangeSource(2, 2)).row((r, o, c) -> {
                    if (c.gateFallback("G")) fallbackSeen.set(true);
                    o.emit(r);
                }).sink(collect(uniqueName("b"))))
                .gate("G", def, gb -> { gb.after("p1").before("p2"); if (timeout != null) gb.timeout(timeout, policy); })
                .build();
    }

    private GateRecord gate(DefaultPipelinerEngine e, String round) {
        for (GateRecord g : e.view(round).getGates()) if (g.getGateId().equals("G")) return g;
        return null;
    }

    private RunStatus run(DefaultPipelinerEngine e, String round, String id) {
        for (RunRecord r : e.view(round).getRuns()) if (r.getRunId().equals(id)) return r.getStatus();
        return null;
    }

    @Test void manualGateWaitsUntilOpened() throws Exception {
        try (Harness h = new Harness(graph(Gates.manual(), null, null), Collections.<String, ExecMode>emptyMap())) {
            DefaultPipelinerEngine e = h.pod("pod");
            String round = e.submit("gt", "k", null);
            assertTrue(waitFor(() -> run(e, round, "R1") == RunStatus.SUCCEEDED && gate(e, round).getArmedAt() != null, 10000));
            Thread.sleep(300);
            assertEquals(RunStatus.PENDING, run(e, round, "R2"));
            assertEquals(RoundStatus.RUNNING, e.view(round).getRound().getStatus());
            e.openGate(round, "G", "alice");
            assertEquals(RoundStatus.SUCCEEDED, e.awaitRound(round, 10000));
            assertEquals("alice", gate(e, round).getOpenedBy());
        }
    }

    @Test void externalGateOpensOnSignals() throws Exception {
        try (Harness h = new Harness(graph(Gates.externalCount(2), null, null), Collections.<String, ExecMode>emptyMap())) {
            DefaultPipelinerEngine e = h.pod("pod");
            String round = e.submit("gt", "k", null);
            assertTrue(waitFor(() -> gate(e, round).getArmedAt() != null, 10000));
            e.signalGate(round, "G", "s1", "x");
            Thread.sleep(300);
            assertEquals(GateStatus.CLOSED, gate(e, round).getStatus());
            e.signalGate(round, "G", "s2", "y");
            assertEquals(RoundStatus.SUCCEEDED, e.awaitRound(round, 10000));
        }
    }

    @Test void conditionGateIsPolledAndExceptionsMeanWait() throws Exception {
        AtomicBoolean ok = new AtomicBoolean();
        AtomicBoolean boom = new AtomicBoolean(true);
        GateCondition cond = ctx -> {
            if (boom.get()) throw new IllegalStateException("db down");
            return ok.get() ? GateDecision.OPEN : GateDecision.WAIT;
        };
        try (Harness h = new Harness(graph(Gates.condition(cond), null, null), Collections.<String, ExecMode>emptyMap())) {
            DefaultPipelinerEngine e = h.pod("pod");
            String round = e.submit("gt", "k", null);
            assertTrue(waitFor(() -> gate(e, round).getArmedAt() != null, 10000));
            Thread.sleep(200);
            assertEquals(GateStatus.CLOSED, gate(e, round).getStatus());
            boom.set(false);
            Thread.sleep(200);
            assertEquals(GateStatus.CLOSED, gate(e, round).getStatus());
            ok.set(true);
            assertEquals(RoundStatus.SUCCEEDED, e.awaitRound(round, 10000));
        }
    }

    @Test void timerGateOpensAfterDelay() throws Exception {
        try (Harness h = new Harness(graph(Gates.delay(Duration.ofMillis(400)), null, null), Collections.<String, ExecMode>emptyMap())) {
            DefaultPipelinerEngine e = h.pod("pod");
            String round = e.submit("gt", "k", null);
            assertEquals(RoundStatus.SUCCEEDED, e.awaitRound(round, 10000));
            GateRecord g = gate(e, round);
            assertTrue(g.getOpenedAt() - g.getArmedAt() >= 400);
        }
    }

    @Test void rejectFailsDownstreamRun() throws Exception {
        try (Harness h = new Harness(graph(Gates.condition(ctx -> GateDecision.REJECT), null, null), Collections.<String, ExecMode>emptyMap())) {
            DefaultPipelinerEngine e = h.pod("pod");
            String round = e.submit("gt", "k", null);
            assertEquals(RoundStatus.FAILED, e.awaitRound(round, 10000));
            assertEquals(GateStatus.REJECTED, gate(e, round).getStatus());
            assertEquals(RunStatus.FAILED, run(e, round, "R2"));
            assertEquals(RunStatus.SUCCEEDED, run(e, round, "R1"));
        }
    }

    @Test void timeoutFailPolicy() throws Exception {
        try (Harness h = new Harness(graph(Gates.manual(), Duration.ofMillis(300), TimeoutPolicy.FAIL), Collections.<String, ExecMode>emptyMap())) {
            DefaultPipelinerEngine e = h.pod("pod");
            String round = e.submit("gt", "k", null);
            assertEquals(RoundStatus.FAILED, e.awaitRound(round, 10000));
            assertEquals(GateStatus.TIMEOUT, gate(e, round).getStatus());
            assertEquals(RunStatus.FAILED, run(e, round, "R2"));
        }
    }

    @Test void timeoutOpenWithFallbackIsVisibleToRunners() throws Exception {
        try (Harness h = new Harness(graph(Gates.manual(), Duration.ofMillis(300), TimeoutPolicy.OPEN_WITH_FALLBACK), Collections.<String, ExecMode>emptyMap())) {
            DefaultPipelinerEngine e = h.pod("pod");
            String round = e.submit("gt", "k", null);
            assertEquals(RoundStatus.SUCCEEDED, e.awaitRound(round, 10000));
            assertEquals("true", gate(e, round).getContext().get("fallback"));
            assertTrue(fallbackSeen.get(), "RunContext.gateFallback(G) is true in p2");
        }
    }

    @Test void timeoutAlertAndWaitKeepsWaiting() throws Exception {
        try (Harness h = new Harness(graph(Gates.manual(), Duration.ofMillis(300), TimeoutPolicy.ALERT_AND_WAIT), Collections.<String, ExecMode>emptyMap())) {
            DefaultPipelinerEngine e = h.pod("pod");
            String round = e.submit("gt", "k", null);
            assertTrue(waitFor(() -> gate(e, round).isAlerted(), 10000));
            assertEquals(GateStatus.CLOSED, gate(e, round).getStatus());
            assertEquals(RoundStatus.RUNNING, e.view(round).getRound().getStatus());
            e.openGate(round, "G", "bob");
            assertEquals(RoundStatus.SUCCEEDED, e.awaitRound(round, 10000));
            assertFalse(fallbackSeen.get());
        }
    }
}
