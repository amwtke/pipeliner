package io.github.amwtke.pipeliner.runtime;

import static io.github.amwtke.pipeliner.runtime.fake.TestKit.*;
import static org.junit.jupiter.api.Assertions.*;

import io.github.amwtke.pipeliner.api.*;
import io.github.amwtke.pipeliner.core.dataplane.DesiredState;
import io.github.amwtke.pipeliner.core.dataplane.Keys;
import io.github.amwtke.pipeliner.core.engine.LiveInstance;
import io.github.amwtke.pipeliner.core.graph.ExecMode;
import io.github.amwtke.pipeliner.core.graph.PipelineGraph;
import io.github.amwtke.pipeliner.core.store.*;
import io.github.amwtke.pipeliner.runtime.fake.InMemoryDataPlane;
import io.github.amwtke.pipeliner.runtime.worker.ClaimRenewer;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;

/** Multi-pod correctness: zombie masters/workers, atomic rerun with attempt-scoped data, fresh instances per attempt, flags. */
class ZombieAndRerunTest {
    private static List<String> rowsOf(int total, long perSplit) {
        List<String> l = new ArrayList<>();
        for (long id = 0; id < total; id++) l.add(Row.of("id", id, "g", id / perSplit).toString());
        Collections.sort(l);
        return l;
    }

    private static PipelineGraph clusterGraph(String name, String sink, RowRunner runner, int splits) {
        return PipelineGraph.builder(name)
                .pipeline("p", p -> p.source(new RangeSource(splits, 3)).row(runner).sink(collect(sink)))
                .build();
    }

    // ---- zombie master ----

    @Test void zombieMasterCannotMutateTheDataPlaneOfAFencedRound() throws Exception {
        try (Harness h = new Harness(clusterGraph("zm", uniqueName("s"), (r, o, c) -> o.emit(r), 3), Collections.<String, ExecMode>emptyMap())) {
            DefaultPipelinerEngine a = h.pod(cfg("a").workerEnabled(false).build(), true);   // no workers: the round stays in flight
            String round = a.submit("zm", "k", null);
            String queue = Keys.splitQueue(round, "p", 0);
            assertTrue(waitFor(() -> h.plane.streamLength(queue) == 3 && !h.plane.desiredState(round).isEmpty(), 10000));
            long epochA = h.plane.currentEpoch();
            Set<String> keys = h.plane.keys();
            Map<String, DesiredState> desired = h.plane.desiredState(round);
            String journalBefore = String.valueOf(h.plane.readJournal(round, "0-0", 100).size());

            assertTrue(h.plane.fenceRound(round, epochA + 100), "master B takes the round");
            Thread.sleep(500);   // A keeps ticking (and keeps getting deposed)

            assertEquals(desired, h.plane.desiredState(round));
            assertEquals(3, h.plane.streamLength(queue));
            assertEquals(keys, h.plane.keys());
            assertEquals(journalBefore, String.valueOf(h.plane.readJournal(round, "0-0", 100).size()));
            assertFalse(h.plane.isPaused(round));
            // every mutation A could still attempt is rejected and changes nothing
            assertThrows(StaleEpochException.class, () -> h.plane.setDesiredParallelism(round, "p", 0, 0, epochA));
            assertThrows(StaleEpochException.class, () -> h.plane.deleteKeys(round, Arrays.asList(queue), epochA));
            assertThrows(StaleEpochException.class, () -> h.plane.setPaused(round, true, epochA));
            assertThrows(StaleEpochException.class, () -> h.plane.enqueueSplit(round, "p", 0, new Split("S0009", Collections.<String, String>emptyMap()), epochA));
            assertThrows(StaleEpochException.class, () -> h.plane.trimJournal(round, "9999999-0", epochA));
            assertThrows(StaleEpochException.class, () -> h.plane.clearLocalRun(round, "R1", epochA));
            assertEquals(desired, h.plane.desiredState(round));
            assertEquals(3, h.plane.streamLength(queue));
        }
    }

    // ---- atomic rerun ----

    /** Fails row 5 on attempt 0 only. */
    private static final RowRunner POISON_ATTEMPT0 = (row, out, ctx) -> {
        if (ctx.attempt() == 0 && row.getLong("id") == 5) throw new IllegalStateException("poison row");
        out.emit(row);
    };

    @Test void rerunCrashedBetweenStoreResetAndCleanupIsFinishedByTheNextTick() throws Exception {
        String sink = uniqueName("s");
        try (Harness h = new Harness(clusterGraph("rc", sink, POISON_ATTEMPT0, 4), Collections.<String, ExecMode>emptyMap())) {
            DefaultPipelinerEngine e = h.pod("pod");
            String round = e.submit("rc", "k", null);
            assertEquals(RoundStatus.FAILED, e.awaitRound(round, 15000));
            assertTrue(h.plane.keys().contains(Keys.dlq(round, "p", 0)), "the failed attempt keeps its DLQ");
            assertEquals(3, sinkChunks(sink).size());

            // a rerun that died right after the atomic store reset: attempt 1 is recorded, attempt 0's keys are still there
            h.store.resetRuns(round, Collections.singletonList("R1"), Collections.<String>emptyList(), h.plane.currentEpoch());
            // a late journal event of the old attempt must not leak into the new one
            h.plane.appendJournal(round, ChunkEvent.builder().type("DONE").roundId(round).runId("R1").pipelineId("p").chunkId("S0099")
                    .inputEdge("split").owner("zombie").rowsIn(3L).rowsOut(new HashMap<String, Long>()).attempt(0).ts(1L).build());
            assertEquals(RoundStatus.RUNNING, h.store.loadRoundRecord(round).getStatus());
            h.plane.addActiveRound(round);

            assertEquals(RoundStatus.SUCCEEDED, e.awaitRound(round, 15000));
            assertEquals(rowsOf(12, 3), sinkRows(sink), "same result as an undisturbed run");
            RoundView v = e.view(round);
            assertEquals(1, v.getRuns().get(0).getAttempt());
            assertEquals(0, h.store.countChunks(round, "p", ChunkStatus.DEAD));
            assertEquals(4, h.store.countChunks(round, "p", ChunkStatus.DONE));
            assertTrue(h.store.queryChunks(ChunkQuery.builder().roundId(round).build()).stream().noneMatch(c -> c.getChunkId().equals("S0099")),
                    "stale event of attempt 0 was dropped");
            for (String k : h.plane.keys()) assertFalse(k.contains(round), "leaked key " + k);
        }
    }

    @Test void sourceCannotSucceedWhileASplitChunkRecordIsStillPending() throws Exception {
        try (Harness h = new Harness(clusterGraph("sp", uniqueName("s"), (r, o, c) -> o.emit(r), 3), Collections.<String, ExecMode>emptyMap())) {
            DefaultPipelinerEngine m = h.pod(cfg("m").workerEnabled(false).build(), true);
            String round = m.submit("sp", "k", null);
            String queue = Keys.splitQueue(round, "p", 0);
            assertTrue(waitFor(() -> h.plane.streamLength(queue) == 3 && h.store.countChunks(round, "p", ChunkStatus.PENDING) == 3, 10000));
            h.plane.deleteKeys(round, Collections.singletonList(queue), h.plane.currentEpoch());   // queue empty, but nothing was processed
            h.plane.ensureGroup(round, queue, h.plane.currentEpoch());   // an empty queue WITH its consumer group (without the group the master reports a lost data plane)
            Thread.sleep(600);
            assertEquals(0, h.plane.streamLength(queue));
            assertEquals(3, h.store.countChunks(round, "p", ChunkStatus.PENDING));
            for (PipelineRecord p : m.view(round).getPipelines()) assertEquals(PipelineStatus.RUNNING, p.getStatus());
            assertEquals(RoundStatus.RUNNING, m.view(round).getRound().getStatus());
        }
    }

    /** Write-version aware stand-in for a database: keeps the highest version per chunk, like JdbcSink.versionColumn does. */
    static final class VersionedSink implements Sink {
        final Map<String, Long> latest = new java.util.concurrent.ConcurrentHashMap<>();
        final List<long[]> writes = Collections.synchronizedList(new ArrayList<long[]>());
        @Override public String datasetName() { return null; }
        @Override public void write(Chunk chunk, SinkContext ctx) {
            writes.add(new long[] {ctx.writeVersion(), ctx.attempt()});
            latest.merge(chunk.getChunkId(), ctx.writeVersion(), Math::max);
        }
    }

    @Test void zombieWorkerOfTheOldAttemptWritesOnlyItsOwnKeysAndLowerVersions() throws Exception {
        VersionedSink sink = new VersionedSink();
        CountDownLatch entered = new CountDownLatch(1), hold = new CountDownLatch(1);
        PipelineGraph g = PipelineGraph.builder("zw")
                .pipeline("p", p -> p.source(new RangeSource(4, 3)).row((row, out, ctx) -> {
                    if (ctx.attempt() == 0 && row.getLong("id") == 0) { entered.countDown(); hold.await(); }   // the frozen worker
                    out.emit(row);
                }).parallelism(1).sink(sink))   // one attempt-0 thread: it is the frozen one, so no attempt-0 write can happen before it resumes
                .build();
        try (Harness h = new Harness(g, Collections.<String, ExecMode>emptyMap())) {
            DefaultPipelinerEngine e = h.pod("pod");
            String round = e.submit("zw", "k", null);
            assertTrue(entered.await(10, TimeUnit.SECONDS));
            String oldChannelOrQueue = Keys.splitQueue(round, "p", 0);
            e.rerunFrom(round, "R1");
            assertEquals(RoundStatus.SUCCEEDED, e.awaitRound(round, 15000), "attempt 1 runs while the zombie is frozen");
            long seq = e.view(round).getRound().getSeq();
            assertTrue(seq > 0);
            assertEquals(1, e.view(round).getRuns().get(0).getAttempt());
            assertEquals(0, h.plane.streamLength(oldChannelOrQueue), "attempt 0's keys are gone");

            assertTrue(sink.writes.stream().noneMatch(w -> w[1] == 0), "nothing of attempt 0 was written before the zombie resumed");
            long conflictsBefore = h.plane.conflictCount.get();
            hold.countDown();   // the zombie resumes: runs the sink, then fails to commit
            assertTrue(waitFor(() -> sink.writes.stream().anyMatch(w -> w[1] == 0), 10000), "the zombie did write");
            assertTrue(waitFor(() -> h.plane.conflictCount.get() > conflictsBefore, 10000), "the zombie's commit was rejected");
            for (Map.Entry<String, Long> en : sink.latest.entrySet())
                assertEquals(seq * 1000 + 1, (long) en.getValue(), "chunk " + en.getKey() + " keeps the attempt-1 version");
            for (long[] w : sink.writes) assertEquals(seq * 1000 + w[1], w[0]);
            assertEquals(4, sink.latest.size());
            assertEquals(4, h.store.countChunks(round, "p", ChunkStatus.DONE));
            for (String k : h.plane.keys()) assertFalse(k.contains(round), "the zombie leaked key " + k);
            assertEquals(RoundStatus.SUCCEEDED, e.view(round).getRound().getStatus());
        }
    }

    // ---- fresh instance per attempt ----

    static final AtomicInteger OPENS = new AtomicInteger();
    static volatile int blockAttempt = -1;
    static volatile CountDownLatch entered = new CountDownLatch(1), hold = new CountDownLatch(1);

    static class LcProbe implements RowRunner, Lifecycle {
        @Override public void open(PipelineContext ctx) { OPENS.incrementAndGet(); }
        @Override public void close() {}
        @Override public void process(Row row, Collector out, RunContext ctx) throws Exception {
            if (ctx.attempt() == blockAttempt) { entered.countDown(); hold.await(); }
            out.emit(row);
        }
    }

    private static final ComponentFactory FACTORY = new ComponentFactory() {
        @Override public <T> T create(Class<T> type) { return type.cast(new LcProbe()); }
        @Override public void destroy(Object c) {}
    };

    private static void resetProbe(int attemptToBlock) {
        OPENS.set(0);
        blockAttempt = attemptToBlock;
        entered = new CountDownLatch(1);
        hold = new CountDownLatch(1);
    }

    private static PipelineGraph probeGraph(String name) {
        return PipelineGraph.builder(name).pipeline("p", p -> p.source(new RangeSource(3, 2)).row(LcProbe.class).sink(collect(uniqueName("s")))).build();
    }

    @Test void rerunOfAClusterRunOpensAFreshInstance() throws Exception {
        resetProbe(0);
        try (Harness h = new Harness(probeGraph("fi"), Collections.<String, ExecMode>emptyMap(), FACTORY)) {
            DefaultPipelinerEngine e = h.pod("pod");
            String round = e.submit("fi", "k", null);
            assertTrue(entered.await(10, TimeUnit.SECONDS));
            assertEquals(1, OPENS.get());
            e.rerunFrom(round, "R1");
            assertTrue(waitFor(() -> OPENS.get() == 2, 10000), "open() ran again for attempt 1: " + OPENS.get());
            Set<Integer> attempts = new HashSet<>();
            for (LiveInstance l : e.liveInstances()) attempts.add(l.getAttempt());
            assertEquals(new HashSet<>(Arrays.asList(0, 1)), attempts, "the frozen old instance and the new one are separate");
            hold.countDown();
            assertEquals(RoundStatus.SUCCEEDED, e.awaitRound(round, 15000));
            assertTrue(waitFor(() -> e.liveInstances().isEmpty(), 10000));
        }
    }

    @Test void rerunOfALocalRunOpensAFreshInstance() throws Exception {
        resetProbe(1);   // a local run's first attempt is 1
        try (Harness h = new Harness(probeGraph("fl"), mode(ExecMode.LOCAL, "p"), FACTORY)) {
            DefaultPipelinerEngine e = h.pod("pod");
            String round = e.submit("fl", "k", null);
            assertTrue(entered.await(10, TimeUnit.SECONDS));
            e.rerunFrom(round, "R1");
            assertTrue(waitFor(() -> OPENS.get() == 2, 10000), "open() ran again for the new attempt: " + OPENS.get());
            assertEquals(RoundStatus.SUCCEEDED, e.awaitRound(round, 15000));
            assertTrue(e.view(round).getRuns().get(0).getAttempt() > 1);
            hold.countDown();
        }
    }

    // ---- flags ----

    @Test void zombiesLatePausedFlagIsCorrectedByTheMaster() throws Exception {
        try (Harness h = new Harness(clusterGraph("pf", uniqueName("s"), (r, o, c) -> o.emit(r), 2), Collections.<String, ExecMode>emptyMap())) {
            DefaultPipelinerEngine m = h.pod(cfg("m").workerEnabled(false).build(), true);
            String round = m.submit("pf", "k", null);
            assertTrue(waitFor(() -> !h.plane.desiredState(round).isEmpty(), 10000));
            h.plane.setPaused(round, true, h.plane.currentEpoch());   // an old master's pause that landed late
            assertTrue(waitFor(() -> !h.plane.isPaused(round), 5000), "RUNNING round: the flag is cleared");
            m.pause(round);
            assertTrue(h.plane.isPaused(round));
            h.plane.setPaused(round, false, h.plane.currentEpoch());  // and a late resume of a zombie
            assertTrue(waitFor(() -> h.plane.isPaused(round), 5000), "PAUSED round: the flag is set again");
            m.resume(round);
            assertTrue(waitFor(() -> !h.plane.isPaused(round), 5000));
        }
    }

    @Test void memoryThrottleCarriesATtlThatIsRefreshedWhileOverTheLimit() throws Exception {
        try (Harness h = new Harness(clusterGraph("th", uniqueName("s"), (r, o, c) -> o.emit(r), 2), Collections.<String, ExecMode>emptyMap())) {
            h.plane.usedMemory = 5000;
            DefaultPipelinerEngine m = h.pod(cfg("m").workerEnabled(false).redisMemoryLimitBytes(1000).build(), true);
            m.submit("th", "k", null);
            assertTrue(waitFor(() -> h.plane.globalThrottle(), 5000));
            assertEquals(5000, h.plane.lastThrottleTtl, "max(3 x tick, 5 s)");
            long n = h.plane.throttleSets.get();
            assertTrue(waitFor(() -> h.plane.throttleSets.get() >= n + 3, 3000), "refreshed every tick while over the limit");
            h.plane.usedMemory = 0;
            assertTrue(waitFor(() -> !h.plane.globalThrottle(), 5000));
        }
    }

    // ---- claim renewer ----

    @Test void claimRenewerSilentlyDropsClaimsOfDeletedStreamsAndAckedEntries() throws Exception {
        AtomicLong renews = new AtomicLong();
        InMemoryDataPlane plane = new InMemoryDataPlane() {
            @Override public synchronized boolean renew(String consumer, String key, String entryId) {
                renews.incrementAndGet();
                return super.renew(consumer, key, entryId);
            }
        };
        ClaimRenewer renewer = new ClaimRenewer(plane, 20);
        renewer.start();
        try {
            renewer.register("c", Keys.channel("gone", "e1", 0), "1-0");   // stream/group does not exist: NOGROUP
            Thread.sleep(250);
            assertEquals(1, renews.get(), "renewed once, then forgotten");
            String key = Keys.channel("r", "e1", 0);
            plane.ensureGroup("r", key, 1);
            renewer.register("c", key, "5-0");   // never delivered / already acked: not pending
            Thread.sleep(250);
            assertEquals(2, renews.get());
        } finally {
            renewer.stop();
        }
    }
}
