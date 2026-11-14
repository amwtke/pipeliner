package io.github.amwtke.pipeliner.runtime;

import static io.github.amwtke.pipeliner.runtime.fake.TestKit.*;
import static org.junit.jupiter.api.Assertions.*;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import io.github.amwtke.pipeliner.api.PipelinerException;
import io.github.amwtke.pipeliner.core.codec.ChunkCodec;
import io.github.amwtke.pipeliner.core.dataplane.DeadLetterRequest;
import io.github.amwtke.pipeliner.core.dataplane.Keys;
import io.github.amwtke.pipeliner.core.engine.RuntimeConfig;
import io.github.amwtke.pipeliner.core.graph.ExecMode;
import io.github.amwtke.pipeliner.core.graph.PipelineGraph;
import io.github.amwtke.pipeliner.core.plan.ExecutionPlan;
import io.github.amwtke.pipeliner.core.store.*;
import io.github.amwtke.pipeliner.core.util.ReflectionComponentFactory;
import io.github.amwtke.pipeliner.core.util.SimpleDataSourceRegistry;
import io.github.amwtke.pipeliner.runtime.fake.InMemoryControlPlaneStore;
import io.github.amwtke.pipeliner.runtime.fake.InMemoryDataPlane;
import io.github.amwtke.pipeliner.runtime.fake.RecordingMetrics;
import io.github.amwtke.pipeliner.runtime.fake.TestPlanner;
import io.github.amwtke.pipeliner.runtime.process.ChunkProcessor;
import io.github.amwtke.pipeliner.runtime.worker.ClusterPipelineWorker;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

/** Round 2 integration follow-ups: dead-letter bookkeeping, partial retry, commands behind a deposition, attempt limit, lost consumer group, warned pruning. */
class Round2FollowupTest {
    private static final Map<String, ExecMode> NO_MODES = Collections.<String, ExecMode>emptyMap();

    private static DefaultPipelinerEngine engine(RuntimeConfig c, InMemoryControlPlaneStore store, InMemoryDataPlane plane, RecordingMetrics m, PipelineGraph g) {
        DefaultPipelinerEngine e = new DefaultPipelinerEngine(c, store, plane, new SimpleDataSourceRegistry(new HashMap<String, javax.sql.DataSource>()), m, new ReflectionComponentFactory());
        e.registerPlan(TestPlanner.plan(g, NO_MODES));
        e.start();
        return e;
    }

    private static PipelineGraph poisoned(String sink, AtomicBoolean poison) {
        return PipelineGraph.builder("fu").pipeline("p1", p -> p.source(new RangeSource(4, 3)).row((row, out, ctx) -> {
            if (poison.get() && row.getLong("id") == 5) throw new IllegalStateException("poison row");
            out.emit(row);
        }).sink(collect(sink))).build();
    }

    // ---- C3: the worker's dead-letter metric and ERROR log only when the entry was really moved ----
    private static List<String> deadLetterErrors(ListAppender<ILoggingEvent> app) {
        List<String> out = new ArrayList<>();
        for (ILoggingEvent ev : new ArrayList<>(app.list)) if (ev.getLevel() == Level.ERROR && ev.getFormattedMessage().contains("dead-lettered")) out.add(ev.getFormattedMessage());
        return out;
    }

    private static ListAppender<ILoggingEvent> capture() {
        Logger l = (Logger) org.slf4j.LoggerFactory.getLogger(ClusterPipelineWorker.class);
        ListAppender<ILoggingEvent> app = new ListAppender<>();
        app.start();
        l.addAppender(app);
        return app;
    }

    @Test void deadLetterMetricAndErrorLogAreSkippedWhenTheEntryWasNotMoved() throws Exception {
        AtomicInteger calls = new AtomicInteger();
        InMemoryDataPlane plane = new InMemoryDataPlane() {
            @Override public boolean deadLetter(DeadLetterRequest req) { calls.incrementAndGet(); return false; }   // already committed / claimed elsewhere
        };
        RecordingMetrics m = new RecordingMetrics();
        ListAppender<ILoggingEvent> app = capture();
        DefaultPipelinerEngine e = engine(cfg("dl").build(), new InMemoryControlPlaneStore(), plane, m, poisoned(uniqueName("s"), new AtomicBoolean(true)));
        try {
            e.submit("fu", "k", null);
            assertTrue(waitFor(() -> calls.get() >= 1, 15000), "the worker tried to dead-letter the poison chunk");
            Thread.sleep(300);
            assertEquals(0, m.deadLetters.get(), "no metric: nothing was moved to the DLQ");
            assertTrue(deadLetterErrors(app).isEmpty(), "no ERROR log: " + deadLetterErrors(app));
        } finally { e.halt(); }
    }

    @Test void deadLetterMetricAndErrorLogArePresentWhenTheEntryWasMoved() throws Exception {
        RecordingMetrics m = new RecordingMetrics();
        ListAppender<ILoggingEvent> app = capture();
        InMemoryDataPlane plane = new InMemoryDataPlane();
        DefaultPipelinerEngine e = engine(cfg("dl2").build(), new InMemoryControlPlaneStore(), plane, m, poisoned(uniqueName("s"), new AtomicBoolean(true)));
        try {
            String round = e.submit("fu", "k", null);
            assertEquals(RoundStatus.FAILED, e.awaitRound(round, 15000));
            assertEquals(1, m.deadLetters.get());
            assertEquals(1, deadLetterErrors(app).size());
        } finally { e.halt(); }
    }

    // ---- F055: requeueDeadLetters fails partway ----
    @Test void retryFailingPartwayLeavesTheRunFailedAndAReissuedRetryCompletesIt() throws Exception {
        AtomicInteger calls = new AtomicInteger();
        InMemoryDataPlane plane = new InMemoryDataPlane() {
            @Override public synchronized int requeueDeadLetters(String r, String p, int attempt, long epoch) {
                int n = super.requeueDeadLetters(r, p, attempt, epoch);
                if (calls.incrementAndGet() == 1) throw new IllegalStateException("redis connection reset after requeue");   // partway: requeued, nothing else
                return n;
            }
        };
        AtomicBoolean poison = new AtomicBoolean(true);
        String sink = uniqueName("s");
        InMemoryControlPlaneStore store = new InMemoryControlPlaneStore();
        DefaultPipelinerEngine e = engine(cfg("f55").build(), store, plane, new RecordingMetrics(), poisoned(sink, poison));
        try {
            String round = e.submit("fu", "k", null);
            assertEquals(RoundStatus.FAILED, e.awaitRound(round, 15000));
            poison.set(false);
            assertThrows(PipelinerException.class, () -> e.retryRun(round, "R1"));
            assertEquals(1, calls.get());
            assertEquals(RoundStatus.FAILED, e.view(round).getRound().getStatus(), "a failed retry changes nothing in the store");
            assertEquals(RunStatus.FAILED, e.view(round).getRuns().get(0).getStatus());
            e.retryRun(round, "R1");   // re-issued
            assertEquals(RoundStatus.SUCCEEDED, e.awaitRound(round, 15000));
            assertEquals(12, sinkRows(sink).size(), "every row arrived once the retry went through");
            assertEquals(0, store.countChunks(round, "p1", ChunkStatus.DEAD));
        } finally { e.halt(); }
    }

    // ---- C4: a deposition in the middle of the queue never drops the commands behind it ----
    @Test void commandsQueuedBehindADepositionAreNotLost() throws Exception {
        PipelineGraph g = PipelineGraph.builder("fu").pipeline("p1", p -> p.source(new RangeSource(2, 2)).sink(collect(uniqueName("s")))).build();
        try (Harness h = new Harness(g, NO_MODES)) {
            DefaultPipelinerEngine idle = h.pod(cfg("sub").masterEnabled(false).workerEnabled(false).build(), true);
            String fencedRound = idle.submit("fu", "k1", null), okRound = idle.submit("fu", "k2", null), okRound2 = idle.submit("fu", "k3", null);
            assertTrue(h.plane.fenceRound(fencedRound, 1_000_000L), "a newer master owns this round: executing a command on it deposes the leader");
            long deadline = System.currentTimeMillis() + 60_000;
            List<String> ids = new ArrayList<>();
            for (String[] c : new String[][] {{"cancel", fencedRound}, {"pause", okRound}, {"pause", okRound2}}) {
                Map<String, String> cmd = new LinkedHashMap<>();
                String id = UUID.randomUUID().toString();
                ids.add(id);
                cmd.put("id", id); cmd.put("op", c[0]); cmd.put("roundId", c[1]); cmd.put("deadline", String.valueOf(deadline));
                h.plane.pushCommand(ChunkCodec.toJson(cmd));
            }
            DefaultPipelinerEngine master = h.pod(cfg("m").workerEnabled(false).build(), false);
            master.start();
            assertTrue(waitFor(() -> h.plane.getCommandResult(ids.get(2)) != null, 20000), "the command behind the deposed one was still executed");
            Map<String, String> r0 = ChunkCodec.toStringMap(h.plane.getCommandResult(ids.get(0)));
            assertEquals("false", r0.get("ok"));
            assertTrue(r0.get("error").contains("deposed"), r0.get("error"));
            assertEquals("true", ChunkCodec.toStringMap(h.plane.getCommandResult(ids.get(1))).get("ok"));
            assertEquals("true", ChunkCodec.toStringMap(h.plane.getCommandResult(ids.get(2))).get("ok"));
            assertEquals(RoundStatus.PAUSED, h.store.loadRoundRecord(okRound).getStatus());
            assertEquals(RoundStatus.PAUSED, h.store.loadRoundRecord(okRound2).getStatus());
        }
    }

    // ---- F058 master side: attempts are limited to [0, VERSION_RADIX) ----
    @Test void rerunFromRefusedWhenTheNextAttemptWouldReachTheRadix() throws Exception {
        PipelineGraph g = PipelineGraph.builder("fu").pipeline("p1", p -> p.source(new RangeSource(2, 2)).sink(collect(uniqueName("s")))).build();
        try (Harness h = new Harness(g, NO_MODES)) {
            DefaultPipelinerEngine e = h.pod("pod");
            String round = e.submit("fu", "k", null);
            assertEquals(RoundStatus.SUCCEEDED, e.awaitRound(round, 15000));
            long epoch = h.plane.currentEpoch();
            assertTrue(h.store.updateRunAssignment(round, "R1", null, ChunkProcessor.VERSION_RADIX - 2, epoch));
            e.rerunFrom(round, "R1");   // attempt 998 -> 999 is still fine
            assertEquals(RoundStatus.SUCCEEDED, e.awaitRound(round, 15000));
            assertEquals(ChunkProcessor.VERSION_RADIX - 1, h.store.loadRound(round).getRuns().get(0).getAttempt());
            PipelinerException ex = assertThrows(PipelinerException.class, () -> e.rerunFrom(round, "R1"));
            assertTrue(ex.getMessage().contains("attempt " + ChunkProcessor.VERSION_RADIX), ex.getMessage());
            assertEquals(ChunkProcessor.VERSION_RADIX - 1, h.store.loadRound(round).getRuns().get(0).getAttempt(), "nothing was reset");
        }
    }

    @Test void localRunAtTheAttemptLimitFailsWithAClearErrorInsteadOfBeingReassigned() throws Exception {
        PipelineGraph g = PipelineGraph.builder("fu").pipeline("p1", p -> p.source(new RangeSource(2, 2)).sink(collect(uniqueName("s")))).build();
        try (Harness h = new Harness(g, mode(ExecMode.LOCAL, "p1"))) {
            DefaultPipelinerEngine sub = h.pod(cfg("sub").masterEnabled(false).workerEnabled(false).build(), true);
            String round = sub.submit("fu", "k", null);
            assertTrue(h.store.updateRunAssignment(round, "R1", null, ChunkProcessor.VERSION_RADIX - 1, 0));
            DefaultPipelinerEngine e = h.pod("pod");
            assertEquals(RoundStatus.FAILED, e.awaitRound(round, 15000));
            RunRecord r = e.view(round).getRuns().get(0);
            assertEquals(RunStatus.FAILED, r.getStatus());
            assertTrue(r.getError().contains("attempts are limited"), r.getError());
        }
    }

    // ---- F057 remainder: the consumer group is gone while the stream survives ----
    @Test void lostConsumerGroupWithTheStreamStillPresentFailsTheRunWithTheDataPlaneLostError() throws Exception {
        InMemoryDataPlane plane = new InMemoryDataPlane() {   // nobody consumes: the chunks stay queued
            @Override public List<io.github.amwtke.pipeliner.core.dataplane.StreamEntry> read(String consumer, List<String> keys, int count, long block) {
                try { Thread.sleep(Math.max(10, Math.min(block, 50))); } catch (InterruptedException ignored) { Thread.currentThread().interrupt(); }
                return new ArrayList<>();
            }
        };
        InMemoryControlPlaneStore store = new InMemoryControlPlaneStore();
        RuntimeConfig c = cfg("x1").masterTickMode(io.github.amwtke.pipeliner.core.engine.MasterTickMode.EXTERNAL).externalTickIntervalMillis(100).build();
        DefaultPipelinerEngine e = engine(c, store, plane, new RecordingMetrics(), PipelineGraph.builder("fu").pipeline("p", p -> p.source(new RangeSource(3, 2)).sink(collect(uniqueName("s")))).build());
        try {
            String round = e.submit("fu", "k", null);
            for (int i = 0; i < 6; i++) e.tick();
            assertTrue(store.loadRoundState(round).getPipelines().get(0).isEnumDone());
            assertEquals(PipelineStatus.RUNNING, e.view(round).getPipelines().get(0).getStatus());
            String q = Keys.splitQueue(round, "p", 0);
            assertTrue(plane.groupExists(q));
            plane.destroyGroup(q);   // XGROUP DESTROY: the stream and its entries survive
            assertTrue(plane.streamLength(q) > 0, "the stream is still there");
            assertFalse(plane.groupExists(q));
            for (int i = 0; i < 4; i++) e.tick();
            PipelineRecord p = e.view(round).getPipelines().get(0);
            assertEquals(PipelineStatus.FAILED, p.getStatus());
            assertTrue(p.getError().contains("data plane lost") && p.getError().contains("rerunFrom required") && p.getError().contains("consumer group"), p.getError());
            assertEquals(RoundStatus.FAILED, e.view(round).getRound().getStatus());
        } finally { e.halt(); }
    }

    // ---- F138 remainder: the worker's per-round warned set follows the active set ----
    @Test void workerPrunesItsWarnedSetWhenARoundLeavesTheActiveSet() throws Exception {
        PipelineGraph g = PipelineGraph.builder("fu").pipeline("p1", p -> p.source(new RangeSource(2, 2)).sink(collect(uniqueName("s")))).build();
        try (Harness h = new Harness(g, NO_MODES)) {
            DefaultPipelinerEngine sub = h.pod(cfg("sub").masterEnabled(false).workerEnabled(false).build(), true);
            String round = sub.submit("fu", "k", null);
            ExecutionPlan other = TestPlanner.plan(g, NO_MODES);
            java.lang.reflect.Field f = ExecutionPlan.class.getDeclaredField("hash");
            f.setAccessible(true);
            f.set(other, "another-version");
            DefaultPipelinerEngine w = new DefaultPipelinerEngine(cfg("w").masterEnabled(false).build(), h.store, h.plane,
                    new SimpleDataSourceRegistry(new HashMap<String, javax.sql.DataSource>()), null, new ReflectionComponentFactory());
            w.registerPlan(other);
            w.start();
            try {
                Set<String> warned = warnedOf(w);
                assertTrue(waitFor(() -> warned.contains(round), 5000), "the worker warned about the round it cannot run");
                h.plane.removeActiveRound(round, h.plane.currentEpoch());
                assertTrue(waitFor(() -> !warned.contains(round), 5000), "left the active set: the entry is gone");
            } finally { w.halt(); }
        }
    }

    @SuppressWarnings("unchecked")
    private static Set<String> warnedOf(DefaultPipelinerEngine e) throws Exception {
        java.lang.reflect.Field wf = DefaultPipelinerEngine.class.getDeclaredField("worker");
        wf.setAccessible(true);
        Object agent = wf.get(e);
        java.lang.reflect.Field f = agent.getClass().getDeclaredField("warned");
        f.setAccessible(true);
        return (Set<String>) f.get(agent);
    }
}
