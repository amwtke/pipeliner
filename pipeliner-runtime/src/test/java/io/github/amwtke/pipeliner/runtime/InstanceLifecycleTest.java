package io.github.amwtke.pipeliner.runtime;

import static io.github.amwtke.pipeliner.runtime.fake.TestKit.*;
import static org.junit.jupiter.api.Assertions.*;

import io.github.amwtke.pipeliner.api.*;
import io.github.amwtke.pipeliner.core.engine.LiveInstance;
import io.github.amwtke.pipeliner.core.graph.ExecMode;
import io.github.amwtke.pipeliner.core.graph.PipelineGraph;
import io.github.amwtke.pipeliner.core.store.RoundStatus;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Lazy, on-demand instantiation of runners (and open/close of Lifecycle components) on a pod. */
class InstanceLifecycleTest {
    static final AtomicInteger CREATED = new AtomicInteger(), OPENED = new AtomicInteger(), CLOSED = new AtomicInteger(),
            DESTROYED = new AtomicInteger(), OPEN_FAILURES = new AtomicInteger(), SINK_OPENED = new AtomicInteger(), SINK_CLOSED = new AtomicInteger();
    static volatile boolean processedBeforeOpen;
    static volatile CountDownLatch inProcess = new CountDownLatch(1);
    static volatile boolean block;
    static volatile CountDownLatch gate = new CountDownLatch(1);   // blocked runners wait on it (interruptible)

    /** Counts its own lifecycle; optionally blocks (until interrupted) or fails its first N opens. */
    static class Probe implements BatchRunner, Lifecycle {
        volatile boolean open;
        @Override public void open(PipelineContext ctx) throws Exception {
            if (OPEN_FAILURES.getAndDecrement() > 0) throw new IllegalStateException("boom");
            assertEquals("b", ctx.pipelineId());
            assertNotNull(ctx.podId());
            open = true;
            OPENED.incrementAndGet();
        }
        @Override public void close() { CLOSED.incrementAndGet(); }
        @Override public void process(List<Row> rows, Collector out, RunContext ctx) throws Exception {
            if (!open) processedBeforeOpen = true;
            inProcess.countDown();
            if (block) gate.await();
            for (Row r : rows) out.emit(r);
        }
    }

    static class LcSink implements Sink, Lifecycle {
        final Sink delegate;
        LcSink(Sink d) { delegate = d; }
        @Override public String datasetName() { return null; }
        @Override public void open(PipelineContext ctx) { SINK_OPENED.incrementAndGet(); }
        @Override public void close() { SINK_CLOSED.incrementAndGet(); }
        @Override public void write(Chunk chunk, SinkContext ctx) throws Exception { delegate.write(chunk, ctx); }
    }

    /** Counting factory (no reflection: the test classes are package-private). */
    static final ComponentFactory FACTORY = new ComponentFactory() {
        @Override public <T> T create(Class<T> type) {
            CREATED.incrementAndGet();
            return type.cast(new Probe());
        }
        @Override public void destroy(Object c) { DESTROYED.incrementAndGet(); }
    };

    @BeforeEach void reset() {
        for (AtomicInteger a : new AtomicInteger[] {CREATED, OPENED, CLOSED, DESTROYED, OPEN_FAILURES, SINK_OPENED, SINK_CLOSED}) a.set(0);
        processedBeforeOpen = false;
        inProcess = new CountDownLatch(1);
        block = false;
        gate = new CountDownLatch(1);
    }

    static PipelineGraph graph(String sink, int parallelism, boolean perThread, boolean lcSink) {
        Sink s = lcSink ? new LcSink(collect(sink)) : collect(sink);
        return PipelineGraph.builder("lazy")
                .pipeline("a", p -> p.source(new RangeSource(6, 4)).chunkRows(3))
                .pipeline("b", p -> {
                    p.batch(Probe.class).parallelism(parallelism).sink(s);
                    if (perThread) p.runnerPerThread();
                })
                .edge("a", "b").build();
    }

    static LiveInstance liveB(DefaultPipelinerEngine e) {
        for (LiveInstance l : e.liveInstances()) if (l.getPipelineId().equals("b")) return l;
        return null;
    }

    @Test void nothingIsInstantiatedBeforeARunStarts() throws Exception {
        try (Harness h = new Harness(graph(uniqueName("s"), 2, false, false), Collections.<String, ExecMode>emptyMap(), FACTORY)) {
            DefaultPipelinerEngine e = h.pod("pod1");
            Thread.sleep(300);   // several agent ticks
            assertEquals(0, CREATED.get());
            assertTrue(e.liveInstances().isEmpty());
        }
    }

    @Test void createdOpenedOnceSharedByThreadsThenClosedAndDestroyed() throws Exception {
        String sink = uniqueName("s");
        block = true;   // hold the pipeline open so the live instance can be observed
        try (Harness h = new Harness(graph(sink, 3, false, true), Collections.<String, ExecMode>emptyMap(), FACTORY)) {
            DefaultPipelinerEngine e = h.pod("pod1");
            String round = e.submit("lazy", "k", new HashMap<String, String>());
            assertTrue(inProcess.await(10, TimeUnit.SECONDS));
            assertTrue(waitFor(() -> liveB(e) != null && liveB(e).getThreads() == 3, 5000), String.valueOf(e.liveInstances().size()));
            LiveInstance li = liveB(e);
            assertEquals("b", li.getPipelineId());
            assertEquals(round, li.getRoundId());
            assertFalse(li.isPerThread());
            assertEquals(1, CREATED.get(), "one runner shared by the pod's threads");
            assertEquals(1, OPENED.get());
            assertEquals(0, CLOSED.get());
            assertEquals(1, SINK_OPENED.get());
            // the heartbeat publishes the live instances compactly: [[round,pipeline,attempt,threads],...]
            String expect = "[[\"" + round + "\",\"b\",0,3]]";
            assertTrue(waitFor(() -> expect.equals(h.plane.workerInfo("pod1").get("live")), 5000), String.valueOf(h.plane.workerInfo("pod1")));
            gate.countDown();
            assertEquals(RoundStatus.SUCCEEDED, e.awaitRound(round, 10000));
            assertTrue(waitFor(() -> e.liveInstances().isEmpty(), 10000), "released after the round ended");
            assertTrue(waitFor(() -> "[]".equals(h.plane.workerInfo("pod1").get("live")), 5000), "heartbeat drops released instances");
            assertEquals(1, CLOSED.get());
            assertEquals(1, DESTROYED.get());
            assertEquals(1, SINK_CLOSED.get());
            assertFalse(processedBeforeOpen);
        }
    }

    @Test void runnerIsReleasedWhenThePipelineFinishes() throws Exception {
        String sink = uniqueName("s");
        try (Harness h = new Harness(graph(sink, 2, false, false), Collections.<String, ExecMode>emptyMap(), FACTORY)) {
            DefaultPipelinerEngine e = h.pod("pod1");
            assertEquals(RoundStatus.SUCCEEDED, e.awaitRound(e.submit("lazy", "k", null), 15000));
            assertEquals(24, sinkRows(sink).size());
            assertTrue(waitFor(() -> e.liveInstances().isEmpty() && DESTROYED.get() == 1, 10000));
            assertEquals(1, CREATED.get());
            assertEquals(1, OPENED.get());
            assertEquals(1, CLOSED.get());
            assertFalse(processedBeforeOpen);
        }
    }

    @Test void perThreadCreatesOneRunnerPerThread() throws Exception {
        String sink = uniqueName("s");
        try (Harness h = new Harness(graph(sink, 3, true, false), Collections.<String, ExecMode>emptyMap(), FACTORY)) {
            DefaultPipelinerEngine e = h.pod("pod1");
            assertEquals(RoundStatus.SUCCEEDED, e.awaitRound(e.submit("lazy", "k", null), 15000));
            assertEquals(24, sinkRows(sink).size());
            assertTrue(waitFor(() -> DESTROYED.get() == CREATED.get() && e.liveInstances().isEmpty(), 10000));
            assertTrue(CREATED.get() >= 1 && CREATED.get() <= 3, "created " + CREATED.get());
            assertEquals(CREATED.get(), OPENED.get());
            assertEquals(CREATED.get(), CLOSED.get());
        }
        reset();
        block = true;
        try (Harness h = new Harness(graph(uniqueName("s"), 3, true, false), Collections.<String, ExecMode>emptyMap(), FACTORY)) {
            DefaultPipelinerEngine e = h.pod("pod1");
            String round = e.submit("lazy", "k2", null);
            assertTrue(waitFor(() -> CREATED.get() == 3 && OPENED.get() == 3, 10000), "one runner per thread: " + CREATED.get());
            assertTrue(e.liveInstances().stream().anyMatch(l -> l.getPipelineId().equals("b") && l.isPerThread()));
            gate.countDown();
            assertEquals(RoundStatus.SUCCEEDED, e.awaitRound(round, 10000));
            assertTrue(waitFor(() -> CLOSED.get() == 3 && DESTROYED.get() == 3, 10000));
        }
    }

    @Test void openFailureDoesNotKillTheAgentAndIsRetried() throws Exception {
        String sink = uniqueName("s");
        OPEN_FAILURES.set(2);
        try (Harness h = new Harness(graph(sink, 1, false, false), Collections.<String, ExecMode>emptyMap(), FACTORY)) {
            DefaultPipelinerEngine e = h.pod("pod1");
            assertEquals(RoundStatus.SUCCEEDED, e.awaitRound(e.submit("lazy", "k", null), 20000));
            assertEquals(24, sinkRows(sink).size());
            assertTrue(CREATED.get() >= 3, "two failed opens, then a good one: " + CREATED.get());
            assertTrue(waitFor(() -> DESTROYED.get() == CREATED.get(), 5000), "failed instances are destroyed too");
            assertEquals(1, OPENED.get());
        }
    }

    @Test void localAndInlineRunsReleaseOnCompletion() throws Exception {
        for (ExecMode m : new ExecMode[] {ExecMode.LOCAL, ExecMode.INLINE}) {
            reset();
            String sink = uniqueName("s");
            try (Harness h = new Harness(graph(sink, 1, false, true), mode(m, "a", "b"), FACTORY)) {
                DefaultPipelinerEngine e = h.pod("pod-" + m);
                assertEquals(RoundStatus.SUCCEEDED, e.awaitRound(e.submit("lazy", "k", null), 15000), m.toString());
                assertTrue(waitFor(() -> DESTROYED.get() == 1, 5000), m.toString());
                assertEquals(1, CREATED.get(), m.toString());
                assertEquals(1, OPENED.get(), m.toString());
                assertEquals(1, CLOSED.get(), m.toString());
                assertEquals(1, SINK_OPENED.get(), m.toString());
                assertEquals(1, SINK_CLOSED.get(), m.toString());
                assertTrue(e.liveInstances().isEmpty());
            }
        }
    }

    @Test void localAndInlineRunsReleaseOnCancel() throws Exception {
        for (ExecMode m : new ExecMode[] {ExecMode.LOCAL, ExecMode.INLINE}) {
            reset();
            block = true;
            try (Harness h = new Harness(graph(uniqueName("s"), 1, false, true), mode(m, "a", "b"), FACTORY)) {
                DefaultPipelinerEngine e = h.pod("pod-" + m);
                String round = e.submit("lazy", "k", null);
                assertTrue(inProcess.await(10, TimeUnit.SECONDS), m.toString());
                assertTrue(e.liveInstances().stream().anyMatch(l -> l.getPipelineId().equals("b")), m.toString());
                e.cancel(round);
                assertEquals(RoundStatus.CANCELLED, e.awaitRound(round, 10000), m.toString());
                assertTrue(waitFor(() -> e.liveInstances().isEmpty(), 10000), m.toString());
                assertEquals(1, CLOSED.get(), m.toString());
                assertEquals(1, DESTROYED.get(), m.toString());
                assertEquals(1, SINK_CLOSED.get(), m.toString());
            }
        }
    }

    /** Instance-based runners stay supported: shared singleton, opened/closed per activation, never destroyed. */
    @Test void instanceRunnerIsOpenedAndClosedButNotDestroyed() throws Exception {
        String sink = uniqueName("s");
        Probe shared = new Probe();
        PipelineGraph g = PipelineGraph.builder("inst")
                .pipeline("a", p -> p.source(new RangeSource(3, 4)).chunkRows(3))
                .pipeline("b", p -> p.batch(shared).sink(collect(sink)))
                .edge("a", "b").build();
        try (Harness h = new Harness(g, Collections.<String, ExecMode>emptyMap(), FACTORY)) {
            DefaultPipelinerEngine e = h.pod("pod1");
            assertEquals(RoundStatus.SUCCEEDED, e.awaitRound(e.submit("inst", "k", null), 15000));
            assertTrue(waitFor(() -> CLOSED.get() == 1, 5000));
            assertEquals(1, OPENED.get());
            assertEquals(0, CREATED.get());
            assertEquals(0, DESTROYED.get());
        }
    }
}
