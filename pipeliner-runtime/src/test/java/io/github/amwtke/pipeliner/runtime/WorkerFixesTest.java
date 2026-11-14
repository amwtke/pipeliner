package io.github.amwtke.pipeliner.runtime;

import static io.github.amwtke.pipeliner.runtime.fake.TestKit.*;
import static org.junit.jupiter.api.Assertions.*;

import io.github.amwtke.pipeliner.api.*;
import io.github.amwtke.pipeliner.core.dataplane.Keys;
import io.github.amwtke.pipeliner.core.engine.RuntimeConfig;
import io.github.amwtke.pipeliner.core.graph.ExecMode;
import io.github.amwtke.pipeliner.core.graph.PipelineGraph;
import io.github.amwtke.pipeliner.core.plan.ExecutionPlan;
import io.github.amwtke.pipeliner.core.store.RoundStatus;
import io.github.amwtke.pipeliner.core.util.SimpleDataSourceRegistry;
import io.github.amwtke.pipeliner.runtime.fake.InMemoryControlPlaneStore;
import io.github.amwtke.pipeliner.runtime.fake.InMemoryDataPlane;
import io.github.amwtke.pipeliner.runtime.fake.RecordingMetrics;
import io.github.amwtke.pipeliner.runtime.fake.TestPlanner;
import io.github.amwtke.pipeliner.runtime.instance.PipelineInstance;
import io.github.amwtke.pipeliner.runtime.instance.PipelineInstances;
import io.github.amwtke.pipeliner.runtime.process.ChunkProcessor;
import io.github.amwtke.pipeliner.runtime.process.ChunkRetryLoop;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

/** Regression tests of review round 2, group g2-worker (local executor, instances, retry policy, engine lifecycle). */
class WorkerFixesTest {
    private static final RowRunner PASS = (row, out, ctx) -> out.emit(row);

    private static PipelineGraph chain(String name, SplitSource src, RowRunner mid, Sink sink) {
        return PipelineGraph.builder(name)
                .pipeline("a", p -> p.source(src))
                .pipeline("b", p -> p.row(mid))
                .pipeline("c", p -> p.row(PASS).sink(sink))
                .edge("a", "b").edge("b", "c").build();
    }

    private static Map<String, ExecMode> all(ExecMode m) { return mode(m, "a", "b", "c"); }

    // ---- F005: a failing pipeline thread stops its siblings ----
    @Test void localRunFailsWhenTheSourceFailsInsteadOfHanging() throws Exception {
        SplitSource bad = new SplitSource() {
            @Override public String datasetName() { return null; }
            @Override public List<Split> enumerate(SourceContext ctx) { throw new IllegalStateException("bad param"); }
            @Override public List<Row> read(Split split, SourceContext ctx) { return Collections.emptyList(); }
        };
        try (Harness h = new Harness(chain("f5", bad, PASS, collect(uniqueName("s"))), all(ExecMode.LOCAL))) {
            DefaultPipelinerEngine e = h.pod("pod");
            String round = e.submit("f5", "k", null);
            assertEquals(RoundStatus.FAILED, e.awaitRound(round, 15000));
            assertTrue(waitFor(() -> e.liveInstances().isEmpty(), 5000), "instances released: no thread is left polling");
        }
    }

    // ---- F033: a CancellationException from user code is a failure ----
    @Test void cancellationExceptionFromUserCodeFailsTheRun() throws Exception {
        for (ExecMode m : new ExecMode[] {ExecMode.INLINE, ExecMode.LOCAL}) {
            RowRunner boom = (row, out, ctx) -> { throw new CancellationException("future cancelled by its own timeout"); };
            try (Harness h = new Harness(chain("f33", new RangeSource(2, 2), boom, collect(uniqueName("s"))), all(m))) {
                DefaultPipelinerEngine e = h.pod("pod");
                String round = e.submit("f33", "k", null);
                assertEquals(RoundStatus.FAILED, e.awaitRound(round, 15000), m.toString());
            }
        }
    }

    // ---- F036: INLINE/LOCAL retry a failing open() ----
    static final class FlakyOpenRunner implements RowRunner, Lifecycle {
        final AtomicInteger opens = new AtomicInteger();
        @Override public void open(PipelineContext ctx) { if (opens.incrementAndGet() == 1) throw new IllegalStateException("db blip"); }
        @Override public void process(Row row, Collector out, RunContext ctx) { out.emit(row); }
    }

    @Test void localAndInlineRunsRetryAFailingOpen() throws Exception {
        for (ExecMode m : new ExecMode[] {ExecMode.INLINE, ExecMode.LOCAL}) {
            FlakyOpenRunner r = new FlakyOpenRunner();
            String sink = uniqueName("s");
            try (Harness h = new Harness(chain("f36", new RangeSource(2, 2), r, collect(sink)), all(m))) {
                DefaultPipelinerEngine e = h.pod("pod");
                String round = e.submit("f36", "k", null);
                assertEquals(RoundStatus.SUCCEEDED, e.awaitRound(round, 15000), m.toString());
                assertEquals(2, r.opens.get(), m.toString());
                assertEquals(4, sinkRows(sink).size());
            }
        }
    }

    // ---- F037: a retry sees fresh rows ----
    @Test void retryOfAnInlineOrLocalChunkDoesNotSeeTheFailedAttemptsEdits() throws Exception {
        for (ExecMode m : new ExecMode[] {ExecMode.INLINE, ExecMode.LOCAL}) {
            AtomicInteger failed = new AtomicInteger();
            RowRunner editThenFail = (row, out, ctx) -> {
                row.set("g", row.getLong("g") + 100);
                if (row.getLong("id") % 2 == 1 && failed.getAndIncrement() == 0) throw new IllegalStateException("once");
                out.emit(row);
            };
            String sink = uniqueName("s");
            try (Harness h = new Harness(chain("f37", new RangeSource(2, 2), editThenFail, collect(sink)), all(m))) {
                DefaultPipelinerEngine e = h.pod("pod");
                String round = e.submit("f37", "k", null);
                assertEquals(RoundStatus.SUCCEEDED, e.awaitRound(round, 15000), m.toString());
                assertEquals(3, failed.get(), m + ": the odd rows were seen 3 times: once failing, then again on the retry");
                for (String r : sinkRows(sink)) assertTrue(r.contains("g=100") || r.contains("g=101"), m + " edited exactly once: " + r);
            }
        }
    }

    // ---- F040: the completion event is appended with retries ----
    @Test void localRunCompletionIsRetriedWhileTheJournalRejectsIt() throws Exception {
        AtomicInteger rejected = new AtomicInteger();
        String sink = uniqueName("s");
        try (Harness h = new Harness(chain("f40", new RangeSource(2, 2), PASS, collect(sink)), all(ExecMode.LOCAL))) {
            h.plane.failAppendWhen = ev -> ev.getType().startsWith("LOCAL_RUN") && rejected.incrementAndGet() <= 3;
            DefaultPipelinerEngine e = h.pod("pod");
            String round = e.submit("f40", "k", null);
            assertEquals(RoundStatus.SUCCEEDED, e.awaitRound(round, 20000));
            assertTrue(rejected.get() > 3, "the executor retried the append: " + rejected.get());
            assertEquals(4, sinkRows(sink).size());
        }
    }

    // ---- F061: StackOverflowError is a failed attempt, not a worker death ----
    @Test void stackOverflowIsRetriedInClusterMode() throws Exception {
        AtomicInteger thrown = new AtomicInteger();
        RowRunner so = (row, out, ctx) -> { if (thrown.getAndIncrement() == 0) throw new StackOverflowError("deep"); out.emit(row); };
        String sink = uniqueName("s");
        try (Harness h = new Harness(chain("f61", new RangeSource(2, 2), so, collect(sink)), all(ExecMode.CLUSTER))) {
            DefaultPipelinerEngine e = h.pod("pod");
            String round = e.submit("f61", "k", null);
            assertEquals(RoundStatus.SUCCEEDED, e.awaitRound(round, 15000));
            assertEquals(4, sinkRows(sink).size());
            for (String k : h.plane.keys()) assertFalse(k.contains(":dlq:") && h.plane.streamLength(k) > 0, "nothing dead-lettered: " + k);
        }
    }

    @Test void retryLoopRetriesErrorsButRethrowsOutOfMemory() throws Exception {
        RuntimeConfig cfg = RuntimeConfig.builder().podId("p").maxAttempts(3).retryBackoffMillis(0).build();
        AtomicInteger calls = new AtomicInteger();
        ChunkRetryLoop.Outcome<String> o = ChunkRetryLoop.run(cfg, io.github.amwtke.pipeliner.core.engine.PipelinerMetrics.NOOP, "r", "p", "c",
                () -> false, t -> false, () -> { if (calls.incrementAndGet() < 3) throw new StackOverflowError(); return "ok"; });
        assertTrue(o.ok());
        assertEquals(3, calls.get());
        calls.set(0);
        assertThrows(OutOfMemoryError.class, () -> ChunkRetryLoop.run(cfg, io.github.amwtke.pipeliner.core.engine.PipelinerMetrics.NOOP, "r", "p", "c",
                () -> false, t -> false, () -> { calls.incrementAndGet(); throw new OutOfMemoryError(); }));
        assertEquals(1, calls.get(), "OOM is not retried");
        ChunkRetryLoop.Outcome<String> failed = ChunkRetryLoop.run(cfg, io.github.amwtke.pipeliner.core.engine.PipelinerMetrics.NOOP, "r", "p", "c",
                () -> false, t -> false, () -> { throw new IllegalStateException("x"); });
        assertFalse(failed.ok());
        assertTrue(failed.failure instanceof IllegalStateException);
    }

    // ---- F063: rows out are reported for INLINE/LOCAL, and counted once per port ----
    @Test void rowsOutIsReportedForLocalAndInlineRuns() throws Exception {
        for (ExecMode m : new ExecMode[] {ExecMode.INLINE, ExecMode.LOCAL}) {
            RecordingMetrics metrics = new RecordingMetrics();
            try (Harness h = new Harness(chain("f63", new RangeSource(2, 3), PASS, collect(uniqueName("s"))), all(m))) {
                DefaultPipelinerEngine e = h.pod(cfg("pod").build(), true, metrics);
                String round = e.submit("f63", "k", null);
                assertEquals(RoundStatus.SUCCEEDED, e.awaitRound(round, 15000), m.toString());
                assertEquals(18, metrics.rowsOutTotal.get(), m + ": a -> b, b -> c, c -> sink, 6 rows each");
            }
        }
    }

    @Test void aPortWithASinkAndAnEdgeCountsItsRowsOnce() throws Exception {
        RecordingMetrics metrics = new RecordingMetrics();
        PipelineGraph g = PipelineGraph.builder("f63b")
                .pipeline("a", p -> p.source(new RangeSource(2, 3)).sink(collect(uniqueName("s"))))
                .pipeline("b", p -> p.row(PASS).sink(collect(uniqueName("s"))))
                .edge("a", "b").build();
        try (Harness h = new Harness(g, Collections.<String, ExecMode>emptyMap())) {
            DefaultPipelinerEngine e = h.pod(cfg("pod").build(), true, metrics);
            String round = e.submit("f63b", "k", null);
            assertEquals(RoundStatus.SUCCEEDED, e.awaitRound(round, 15000));
            assertEquals(12, metrics.rowsOutTotal.get(), "6 rows leave a (sink + edge, once), 6 rows leave b");
        }
    }

    // ---- F094 / F102: stop waits for the threads; a cancelled executor stays silent ----
    static final AtomicInteger CLOSED = new AtomicInteger();

    static final class ClosingSink implements Sink, Lifecycle {
        @Override public String datasetName() { return null; }
        @Override public void write(Chunk chunk, SinkContext ctx) {}
        @Override public void close() { CLOSED.incrementAndGet(); }
    }

    @Test void engineStopWaitsForTheLocalRunToReleaseItsInstances() throws Exception {
        CLOSED.set(0);
        CountDownLatch inRunner = new CountDownLatch(1);
        RowRunner block = (row, out, ctx) -> { inRunner.countDown(); new CountDownLatch(1).await(); };
        try (Harness h = new Harness(chain("f94", new RangeSource(2, 2), block, new ClosingSink()), all(ExecMode.LOCAL))) {
            DefaultPipelinerEngine e = h.pod("pod");
            e.submit("f94", "k", null);
            assertTrue(inRunner.await(10, TimeUnit.SECONDS));
            e.stop();
            assertEquals(1, CLOSED.get(), "the sink was closed before stop() returned");
            assertTrue(e.liveInstances().isEmpty());
        }
    }

    @Test void cancelledLocalRunDoesNotRecreateTheJournal() throws Exception {
        CountDownLatch inRunner = new CountDownLatch(1);
        RowRunner slow = (row, out, ctx) -> { inRunner.countDown(); Thread.sleep(200); out.emit(row); };
        try (Harness h = new Harness(chain("f102", new RangeSource(6, 2), slow, collect(uniqueName("s"))), all(ExecMode.LOCAL))) {
            DefaultPipelinerEngine e = h.pod("pod");
            String round = e.submit("f102", "k", null);
            assertTrue(inRunner.await(10, TimeUnit.SECONDS));
            e.cancel(round);
            assertTrue(waitFor(() -> e.liveInstances().isEmpty(), 10000));
            Thread.sleep(300);
            assertFalse(h.plane.keys().contains(Keys.journal(round)), "journal stays deleted after cancel: " + h.plane.keys());
        }
    }

    // ---- F035 / F059 / F093 / F092: config validation and start() ----
    @Test void invalidRuntimeConfigIsRejectedAtStart() {
        InMemoryControlPlaneStore store = new InMemoryControlPlaneStore();
        InMemoryDataPlane plane = new InMemoryDataPlane();
        SimpleDataSourceRegistry ds = new SimpleDataSourceRegistry(new HashMap<String, javax.sql.DataSource>());
        for (RuntimeConfig bad : Arrays.asList(
                cfg("p").maxAttempts(0).build(),
                cfg("p").highWatermark(16).lowWatermark(32).build(),
                cfg("p").lowWatermark(0).build(),
                cfg("p").readBlockMillis(0).build(),
                cfg("p").maxParallelism(0).build())) {
            DefaultPipelinerEngine e = new DefaultPipelinerEngine(bad, store, plane, ds, null);
            PipelinerException ex = assertThrows(PipelinerException.class, e::start);
            assertTrue(ex.getMessage().contains("invalid RuntimeConfig"), ex.getMessage());
            assertFalse(e.isLeader());
        }
        cfg("p").build().validate();
    }

    @Test void startCanBeRetriedAfterAFailedInit() {
        AtomicInteger inits = new AtomicInteger();
        InMemoryControlPlaneStore store = new InMemoryControlPlaneStore() {
            @Override public void init() { if (inits.incrementAndGet() == 1) throw new IllegalStateException("redis down"); }
        };
        DefaultPipelinerEngine e = new DefaultPipelinerEngine(cfg("p").build(), store, new InMemoryDataPlane(),
                new SimpleDataSourceRegistry(new HashMap<String, javax.sql.DataSource>()), null);
        assertThrows(IllegalStateException.class, e::start);
        e.start();
        assertEquals(2, inits.get());
        assertTrue(waitFor(e::isLeader, 5000));
        e.stop();
        assertFalse(e.isLeader());
    }

    // ---- F058 ----
    @Test void runAttemptAtTheVersionRadixIsRefused() {
        assertThrows(PipelinerException.class, () -> new ChunkProcessor(null, null, "r", null, "p", null, ChunkProcessor.VERSION_RADIX, 7));
    }

    // ---- F006 / F039 / F060: instance registry ----
    static final class CountingSource implements SplitSource, Lifecycle {
        final AtomicInteger opens = new AtomicInteger(), closes = new AtomicInteger();
        volatile CountDownLatch openGate = new CountDownLatch(0);
        @Override public String datasetName() { return null; }
        @Override public List<Split> enumerate(SourceContext ctx) { return Collections.emptyList(); }
        @Override public List<Row> read(Split split, SourceContext ctx) { return Collections.emptyList(); }
        @Override public void open(PipelineContext ctx) throws Exception { opens.incrementAndGet(); openGate.await(); }
        @Override public void close() { closes.incrementAndGet(); }
    }

    private static PipelineInstances instances(ComponentFactory f) {
        return new PipelineInstances(f, "pod", new SimpleDataSourceRegistry(new HashMap<String, javax.sql.DataSource>()));
    }

    @Test void sharedSourceIsOpenedOnceAndClosedByTheLastInstanceAcrossRounds() throws Exception {
        CountingSource src = new CountingSource();
        ExecutionPlan plan = TestPlanner.plan(PipelineGraph.builder("sh").pipeline("a", p -> p.source(src)).build(), Collections.<String, ExecMode>emptyMap());
        PipelineInstances pi = instances(new io.github.amwtke.pipeliner.core.util.ReflectionComponentFactory());
        PipelineInstance.Lease l1 = pi.acquire(plan, "round1", new HashMap<String, String>(), "a", 0);
        PipelineInstance.Lease l2 = pi.acquire(plan, "round2", new HashMap<String, String>(), "a", 0);
        assertEquals(1, src.opens.get(), "second round's instance does not reopen the shared source");
        l1.release();
        assertEquals(0, src.closes.get(), "still used by round2");
        l2.release();
        assertEquals(1, src.closes.get());
        PipelineInstance.Lease l3 = pi.acquire(plan, "round3", new HashMap<String, String>(), "a", 0);
        assertEquals(2, src.opens.get());
        l3.release();
        assertEquals(2, src.closes.get());
    }

    @Test void slowOpenNeverBlocksTheSnapshotAndConcurrentAcquirersWaitForOneOpen() throws Exception {
        CountingSource src = new CountingSource();
        src.openGate = new CountDownLatch(1);
        ExecutionPlan plan = TestPlanner.plan(PipelineGraph.builder("sl").pipeline("a", p -> p.source(src)).build(), Collections.<String, ExecMode>emptyMap());
        PipelineInstances pi = instances(new io.github.amwtke.pipeliner.core.util.ReflectionComponentFactory());
        ExecutorService ex = Executors.newFixedThreadPool(3);
        try {
            Future<PipelineInstance.Lease> f1 = ex.submit(() -> pi.acquire(plan, "r", new HashMap<String, String>(), "a", 0));
            assertTrue(waitFor(() -> src.opens.get() == 1, 5000));
            Future<PipelineInstance.Lease> f2 = ex.submit(() -> pi.acquire(plan, "r", new HashMap<String, String>(), "a", 0));
            Thread.sleep(100);
            assertFalse(f2.isDone(), "waits for the one open");
            assertNotNull(ex.submit(() -> pi.snapshot()).get(2, TimeUnit.SECONDS), "the heartbeat snapshot does not wait for user open()");
            src.openGate.countDown();
            PipelineInstance.Lease a = f1.get(5, TimeUnit.SECONDS), b = f2.get(5, TimeUnit.SECONDS);
            assertEquals(1, src.opens.get());
            a.release();
            assertEquals(0, src.closes.get());
            b.release();
            assertEquals(1, src.closes.get());
        } finally {
            ex.shutdownNow();
        }
    }

    static final AtomicInteger PT_CLOSED = new AtomicInteger(), PT_DESTROYED = new AtomicInteger();

    public static class FailingOpenRunner implements RowRunner, Lifecycle {
        @Override public void open(PipelineContext ctx) { throw new IllegalStateException("before taking a permit"); }
        @Override public void close() { PT_CLOSED.incrementAndGet(); }
        @Override public void process(Row row, Collector out, RunContext ctx) { out.emit(row); }
    }

    @Test void aComponentWhoseOpenFailedIsDestroyedButNeverClosed() throws Exception {
        PT_CLOSED.set(0);
        PT_DESTROYED.set(0);
        ComponentFactory f = new ComponentFactory() {
            @Override public <T> T create(Class<T> type) { return type.cast(new FailingOpenRunner()); }
            @Override public void destroy(Object c) { PT_DESTROYED.incrementAndGet(); }
        };
        for (boolean perThread : new boolean[] {true, false}) {
            PipelineGraph g = PipelineGraph.builder("pt").pipeline("a", p -> {
                p.source(new RangeSource(1, 1)).row(FailingOpenRunner.class);
                if (perThread) p.runnerPerThread();
            }).build();
            ExecutionPlan plan = TestPlanner.plan(g, Collections.<String, ExecMode>emptyMap());
            assertThrows(IllegalStateException.class, () -> instances(f).acquire(plan, "r", new HashMap<String, String>(), "a", 0));
        }
        assertEquals(0, PT_CLOSED.get(), "close() is only called on components that opened");
        assertEquals(2, PT_DESTROYED.get(), "the factory still gets destroy() for what it created");
    }
}
