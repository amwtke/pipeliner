package io.github.amwtke.pipeliner.dataplane.redis;

import static org.junit.jupiter.api.Assertions.*;

import io.github.amwtke.pipeliner.api.Split;
import io.github.amwtke.pipeliner.core.dataplane.*;
import io.github.amwtke.pipeliner.core.store.ChunkEvent;
import io.github.amwtke.pipeliner.core.store.StaleEpochException;
import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.*;

/** Integration tests against Redis on 127.0.0.1:6380 DB 2. */
class RedisDataPlaneTest {
    static RedisClient client;
    static RedisDataPlane dp;
    static StatefulRedisConnection<String, String> raw;

    static final String R = "r1";
    static final long E = 5;   // master epoch used by the tests
    static final String IN = Keys.channel(R, "e1", 0);
    static final String OUT = Keys.channel(R, "e2", 0);

    @BeforeAll
    static void up() {
        try {
            client = RedisClient.create("redis://127.0.0.1:6380/2");
            raw = client.connect();
            raw.sync().ping();
        } catch (Exception e) {
            raw = null;
        }
        Assumptions.assumeTrue(raw != null, "redis not reachable");
        dp = new RedisDataPlane(client);
    }

    @AfterAll
    static void down() {
        if (dp != null) dp.close();
        if (raw != null) raw.close();
        if (client != null) client.shutdown();
    }

    @BeforeEach
    void flush() { raw.sync().flushdb(); }

    private static ChunkEvent ev(String chunk) {
        return ChunkEvent.builder().type("DONE").roundId(R).pipelineId("p1").chunkId(chunk).ts(1L).build();
    }

    private String add(String key, String payload) {
        return raw.sync().xadd(key, Collections.singletonMap("c", payload));
    }

    private long pendingCount(String key) { return raw.sync().xpending(key, Keys.GROUP).getCount(); }

    @Test
    void commitOk() {
        dp.ensureGroup(R, IN, E);
        dp.ensureGroup(R, OUT, E);
        add(IN, "x");
        StreamEntry e = dp.read("c1", Collections.singletonList(IN), 10, 0).get(0);
        assertEquals("x", e.getFields().get("c"));
        assertEquals(IN, e.getStreamKey());
        CommitResult r = dp.commit(CommitRequest.builder().roundId(R).inputStreamKey(IN).inputEntryId(e.getEntryId())
                .highWatermark(100).outputs(Arrays.asList(new OutputChunk(OUT, "o1"), new OutputChunk(OUT, "o2")))
                .event(ev("c0")).build());
        assertEquals(CommitResult.OK, r);
        assertEquals(0, dp.streamLength(IN));
        assertEquals(0, pendingCount(IN));
        assertEquals(2, dp.streamLength(OUT));
        List<ChunkEvent> j = dp.readJournal(R, "0-0", 10);
        assertEquals(1, j.size());
        assertEquals("c0", j.get(0).getChunkId());
        assertNotNull(j.get(0).getEventId());
    }

    @Test
    void commitBusy() {
        dp.ensureGroup(R, IN, E);
        add(IN, "x");
        add(OUT, "a");
        add(OUT, "b");
        StreamEntry e = dp.read("c1", Collections.singletonList(IN), 10, 0).get(0);
        CommitResult r = dp.commit(CommitRequest.builder().roundId(R).inputStreamKey(IN).inputEntryId(e.getEntryId())
                .highWatermark(2).outputs(Collections.singletonList(new OutputChunk(OUT, "o"))).event(ev("c")).build());
        assertEquals(CommitResult.BUSY, r);
        assertEquals(1, dp.streamLength(IN));
        assertEquals(1, pendingCount(IN));
        assertEquals(2, dp.streamLength(OUT));
        assertEquals(0, dp.readJournal(R, "0-0", 10).size());
    }

    @Test
    void commitConflictAfterAutoClaim() throws Exception {
        dp.ensureGroup(R, IN, E);
        add(IN, "x");
        StreamEntry e = dp.read("A", Collections.singletonList(IN), 10, 0).get(0);
        Thread.sleep(120);
        List<StreamEntry> claimed = dp.autoClaim("B", IN, 100, 10);
        assertEquals(1, claimed.size());
        assertEquals(e.getEntryId(), claimed.get(0).getEntryId());
        assertEquals("x", claimed.get(0).getFields().get("c"));
        CommitRequest req = CommitRequest.builder().roundId(R).inputStreamKey(IN).inputEntryId(e.getEntryId())
                .highWatermark(10).outputs(Collections.<OutputChunk>emptyList()).event(ev("c")).build();
        assertEquals(CommitResult.OK, dp.commit(req));
        assertEquals(CommitResult.CONFLICT, dp.commit(req));
    }

    @Test
    void autoClaimRespectsMinIdleAndRenewResetsIdle() throws Exception {
        dp.ensureGroup(R, IN, E);
        add(IN, "x");
        StreamEntry e = dp.read("A", Collections.singletonList(IN), 10, 0).get(0);
        assertTrue(dp.autoClaim("B", IN, 60_000, 10).isEmpty());
        Thread.sleep(300);
        assertTrue(idle(IN) >= 250);
        dp.renew("A", IN, e.getEntryId());
        assertTrue(idle(IN) < 150, "idle=" + idle(IN));
        assertEquals(1, pendingCount(IN));
    }

    private long idle(String key) {
        return raw.sync().xpending(key, io.lettuce.core.Consumer.from(Keys.GROUP, "A"),
                io.lettuce.core.Range.unbounded(), io.lettuce.core.Limit.from(10)).get(0).getMsSinceLastDelivery();
    }

    @Test
    void enqueueSplitIdempotent() {
        Split s = new Split("S0000", Collections.singletonMap("k", "v"));
        assertTrue(dp.enqueueSplit(R, "p1", 0, s, E));
        assertFalse(dp.enqueueSplit(R, "p1", 0, s, E));
        assertEquals(1, dp.streamLength(Keys.splitQueue(R, "p1", 0)));
        dp.ensureGroup(R, Keys.splitQueue(R, "p1", 0), E);
        StreamEntry e = dp.read("c", Collections.singletonList(Keys.splitQueue(R, "p1", 0)), 5, 0).get(0);
        assertEquals("S0000", io.github.amwtke.pipeliner.core.codec.ChunkCodec.decodeSplit(e.getFields().get("s")).getSplitId());
    }

    @Test
    void deadLetterRequeueClear() {
        dp.ensureGroup(R, IN, E);
        add(IN, "bad");
        StreamEntry e = dp.read("c", Collections.singletonList(IN), 5, 0).get(0);
        dp.deadLetter(DeadLetterRequest.builder().roundId(R).pipelineId("p1").inputStreamKey(IN)
                .inputEntryId(e.getEntryId()).inputFields(e.getFields()).error("boom").event(ev("cx")).build());
        assertEquals(0, dp.streamLength(IN));
        assertEquals(0, pendingCount(IN));
        String dlq = Keys.dlq(R, "p1", 0);
        assertEquals(1, dp.streamLength(dlq));
        Map<String, String> f = raw.sync().xrange(dlq, io.lettuce.core.Range.unbounded()).get(0).getBody();
        assertEquals(IN, f.get("src"));
        assertEquals("bad", f.get("c"));
        assertEquals("boom", f.get("err"));
        assertEquals(1, dp.readJournal(R, "0-0", 10).size());
        assertEquals(1, dp.requeueDeadLetters(R, "p1", 0, E));
        assertEquals(0, dp.streamLength(dlq));
        assertEquals(1, dp.streamLength(IN));
        List<StreamEntry> again = dp.read("c", Collections.singletonList(IN), 5, 0);
        assertEquals(1, again.size());
        assertEquals("bad", again.get(0).getFields().get("c"));
        assertEquals(1, again.get(0).getFields().size());
        // clear
        dp.deadLetter(DeadLetterRequest.builder().roundId(R).pipelineId("p1").inputStreamKey(IN)
                .inputEntryId(again.get(0).getEntryId()).inputFields(again.get(0).getFields()).error("e2").build());
        assertEquals(1, dp.clearDeadLetters(R, "p1", 0, E));
        assertEquals(0, dp.streamLength(dlq));
    }

    @Test
    void duplicateDeadLetterIsNoOp() {
        dp.ensureGroup(R, IN, E);
        add(IN, "bad");
        StreamEntry e = dp.read("c", Collections.singletonList(IN), 5, 0).get(0);
        DeadLetterRequest req = DeadLetterRequest.builder().roundId(R).pipelineId("p1").inputStreamKey(IN)
                .inputEntryId(e.getEntryId()).inputFields(e.getFields()).error("boom").event(ev("cx")).build();
        dp.deadLetter(req);
        dp.deadLetter(req);   // e.g. a second worker that autoclaimed the same entry
        assertEquals(1, dp.streamLength(Keys.dlq(R, "p1", 0)));
        assertEquals(1, dp.readJournal(R, "0-0", 10).size());
    }

    @Test
    void autoClaimReportsDeliveryCount() throws Exception {
        dp.ensureGroup(R, IN, E);
        add(IN, "x");
        StreamEntry e = dp.read("A", Collections.singletonList(IN), 10, 0).get(0);
        assertEquals(0, e.getDeliveryCount(), "plain reads do not fill it");
        Thread.sleep(30);
        assertEquals(2, dp.autoClaim("B", IN, 10, 10).get(0).getDeliveryCount());
        Thread.sleep(30);
        assertEquals(3, dp.autoClaim("C", IN, 10, 10).get(0).getDeliveryCount());
    }

    @Test
    void commandQueueAndWorkerInfo() {
        assertTrue(dp.popCommands(5).isEmpty());
        dp.pushCommand("{\"id\":\"1\"}");
        dp.pushCommand("{\"id\":\"2\"}");
        dp.pushCommand("{\"id\":\"3\"}");
        assertEquals(Arrays.asList("{\"id\":\"1\"}", "{\"id\":\"2\"}"), dp.popCommands(2));
        assertEquals(Collections.singletonList("{\"id\":\"3\"}"), dp.popCommands(5));
        assertNull(dp.getCommandResult("1"));
        dp.putCommandResult("1", "{\"ok\":\"true\"}");
        assertEquals("{\"ok\":\"true\"}", dp.getCommandResult("1"));
        assertTrue(raw.sync().ttl(Keys.commandResult("1")) > 0);
        assertTrue(dp.workerInfo("nobody").isEmpty());
        Map<String, String> info = new HashMap<>();
        info.put("graph.g", "h1");
        dp.heartbeat("w9", info);
        assertEquals("h1", dp.workerInfo("w9").get("graph.g"));
    }

    @Test
    void journalPagingAndTrim() {
        for (int i = 0; i < 5; i++) dp.appendJournal(R, ev("c" + i));
        List<ChunkEvent> p1 = dp.readJournal(R, "0-0", 2);
        assertEquals(Arrays.asList("c0", "c1"), Arrays.asList(p1.get(0).getChunkId(), p1.get(1).getChunkId()));
        List<ChunkEvent> p2 = dp.readJournal(R, p1.get(1).getEventId(), 2);
        assertEquals("c2", p2.get(0).getChunkId());
        assertEquals("c3", p2.get(1).getChunkId());
        List<ChunkEvent> p3 = dp.readJournal(R, p2.get(1).getEventId(), 10);
        assertEquals(1, p3.size());
        assertTrue(dp.readJournal(R, p3.get(0).getEventId(), 10).isEmpty());
        dp.trimJournal(R, p2.get(0).getEventId(), E);
        List<ChunkEvent> rest = dp.readJournal(R, "0-0", 10);
        assertEquals(3, rest.size());
        assertEquals("c2", rest.get(0).getChunkId());
    }

    @Test
    void leadership() throws Exception {
        assertNull(dp.currentLeader());
        assertTrue(dp.tryAcquireOrRenewLeadership("a", 400));
        assertFalse(dp.tryAcquireOrRenewLeadership("b", 400));
        assertEquals("a", dp.currentLeader());
        Thread.sleep(250);
        assertTrue(dp.tryAcquireOrRenewLeadership("a", 400));
        Thread.sleep(250);
        assertEquals("a", dp.currentLeader());
        Thread.sleep(300);
        assertNull(dp.currentLeader());
        assertTrue(dp.tryAcquireOrRenewLeadership("b", 5000));
        dp.releaseLeadership("a");
        assertEquals("b", dp.currentLeader());
        dp.releaseLeadership("b");
        assertNull(dp.currentLeader());
        assertEquals(0, dp.currentEpoch());
        assertEquals(1, dp.nextEpoch());
        assertEquals(2, dp.nextEpoch());
        assertEquals(2, dp.currentEpoch());
    }

    @Test
    void nextRoundSeqRespectsFloor() {
        assertEquals(1, dp.nextRoundSeq());
        assertEquals(58, dp.nextRoundSeq(57), "a counter below the floor (Redis wiped) is raised to it, then incremented");
        assertEquals(59, dp.nextRoundSeq(3), "a lower floor changes nothing");
        assertEquals(60, dp.nextRoundSeq());
        assertEquals("60", raw.sync().get(Keys.roundSeq()), "the raised counter is persisted");
    }

    @Test
    void nextEpochRespectsFloor() {
        assertEquals(0, dp.currentEpoch());
        assertEquals(8, dp.nextEpoch(7), "counter below the floor is raised to it, then incremented");
        assertEquals(9, dp.nextEpoch(3), "a lower floor changes nothing");
        assertEquals(10, dp.nextEpoch(0));
        assertEquals(10, dp.currentEpoch());
    }

    @Test
    void stateRoundTrips() {
        dp.addActiveRound("r1");
        dp.addActiveRound("r2");
        assertEquals(new HashSet<>(Arrays.asList("r1", "r2")), dp.activeRounds());
        dp.removeActiveRound("r1", E);
        assertEquals(Collections.singleton("r2"), dp.activeRounds());
        dp.setDesiredParallelism(R, "p1", 3, 4, E);
        dp.setDesiredParallelism(R, "p2", 2, 4, E);
        dp.setDesiredParallelism(R, "p2", 0, 4, E);
        assertEquals(Collections.singletonMap("p1", new DesiredState(3, 4)), dp.desiredState(R));
        dp.assignLocalRun(R, "run1", "podA", 2, E);
        assertEquals(Collections.singletonMap("run1", "podA|2"), dp.localRunAssignments(R));
        dp.clearLocalRun(R, "run1", E);
        assertTrue(dp.localRunAssignments(R).isEmpty());
        assertFalse(dp.isPaused(R));
        dp.setPaused(R, true, E);
        assertTrue(dp.isPaused(R));
        dp.setPaused(R, false, E);
        assertFalse(dp.isPaused(R));
        assertFalse(dp.globalThrottle());
        dp.setGlobalThrottle(true, 0);
        assertTrue(dp.globalThrottle());
        dp.setGlobalThrottle(false, 0);
        assertFalse(dp.globalThrottle());
        dp.heartbeat("w1", Collections.singletonMap("host", "h"));
        assertTrue(dp.liveWorkers(5000).containsKey("w1"));
        assertTrue(dp.liveWorkers(5000).get("w1") > 0);
        assertTrue(dp.usedMemoryBytes() > 0);
        raw.sync().del(Keys.workers(), Keys.workerInfo("w1"));
        assertTrue(dp.liveWorkers(5000).isEmpty());
    }

    @Test
    void zombieMasterIsFencedOnEveryDataPlaneMutation() {
        // master A (epoch 5) is mid-tick: the round has live data
        assertTrue(dp.fenceRound(R, 5));
        dp.ensureGroup(R, IN, 5);
        dp.setDesiredParallelism(R, "p1", 3, 1, 5);
        dp.assignLocalRun(R, "run1", "podA", 1, 5);
        dp.setPaused(R, true, 5);
        dp.addActiveRound(R);
        Split sp = new Split("S0000", Collections.singletonMap("k", "v"));
        assertTrue(dp.enqueueSplit(R, "p1", 1, sp, 5));
        dp.appendJournal(R, ev("c1"));
        dp.appendJournal(R, ev("c2"));
        String eid = dp.readJournal(R, "0-0", 10).get(1).getEventId();
        add(IN, "x");
        add(IN, "y");
        StreamEntry bad = dp.read("c", Collections.singletonList(IN), 1, 0).get(0);
        dp.deadLetter(DeadLetterRequest.builder().roundId(R).pipelineId("p1").attempt(1).inputStreamKey(IN).inputEntryId(bad.getEntryId())
                .inputFields(bad.getFields()).error("e").event(ev("c3")).build());
        String dlq = Keys.dlq(R, "p1", 1);

        // master B takes over (epoch 6); A is a zombie that resumes
        assertTrue(dp.fenceRound(R, 6));
        assertFalse(dp.fenceRound(R, 5), "A cannot take the fence back");
        assertThrows(StaleEpochException.class, () -> dp.setDesiredParallelism(R, "p1", 0, 1, 5));
        assertThrows(StaleEpochException.class, () -> dp.setDesiredParallelism(R, "p9", 2, 1, 5));
        assertThrows(StaleEpochException.class, () -> dp.assignLocalRun(R, "run1", "podZ", 9, 5));
        assertThrows(StaleEpochException.class, () -> dp.clearLocalRun(R, "run1", 5));
        assertThrows(StaleEpochException.class, () -> dp.setPaused(R, false, 5));
        assertThrows(StaleEpochException.class, () -> dp.enqueueSplit(R, "p1", 1, new Split("S0001", Collections.singletonMap("k", "w")), 5));
        assertThrows(StaleEpochException.class, () -> dp.trimJournal(R, eid, 5));
        assertThrows(StaleEpochException.class, () -> dp.deleteKeys(R, Arrays.asList(IN, Keys.splitQueue(R, "p1", 1), dlq, Keys.journal(R)), 5));
        assertThrows(StaleEpochException.class, () -> dp.requeueDeadLetters(R, "p1", 1, 5));
        assertThrows(StaleEpochException.class, () -> dp.clearDeadLetters(R, "p1", 1, 5));
        assertThrows(StaleEpochException.class, () -> dp.ensureGroup(R, Keys.channel(R, "e9", 1), 5));
        assertThrows(StaleEpochException.class, () -> dp.removeActiveRound(R, 5));

        // nothing changed
        assertEquals(Collections.singletonMap("p1", new DesiredState(3, 1)), dp.desiredState(R));
        assertEquals(Collections.singletonMap("run1", "podA|1"), dp.localRunAssignments(R));
        assertTrue(dp.isPaused(R));
        assertTrue(dp.activeRounds().contains(R));
        assertEquals(1, dp.streamLength(Keys.splitQueue(R, "p1", 1)));
        assertEquals(1, raw.sync().scard(Keys.splitIndex(R, "p1", 1)));
        assertEquals(3, dp.readJournal(R, "0-0", 10).size());
        assertEquals(1, dp.streamLength(dlq));
        assertEquals(1, raw.sync().exists(IN));
        assertEquals(0, raw.sync().exists(Keys.channel(R, "e9", 1)));

        // B (and anything at its epoch) can mutate
        dp.setDesiredParallelism(R, "p1", 0, 1, 6);
        assertTrue(dp.desiredState(R).isEmpty());
        assertEquals(1, dp.requeueDeadLetters(R, "p1", 1, 6));
        dp.trimJournal(R, eid, 6);
        assertEquals(2, dp.readJournal(R, "0-0", 10).size());
        dp.deleteKeys(R, Arrays.asList(Keys.splitQueue(R, "p1", 1), Keys.splitIndex(R, "p1", 1)), 6);
        assertEquals(0, dp.streamLength(Keys.splitQueue(R, "p1", 1)));
    }

    @Test
    void attemptsHaveDisjointKeys() {
        assertNotEquals(Keys.channel(R, "e1", 0), Keys.channel(R, "e1", 1));
        assertNotEquals(Keys.splitQueue(R, "p", 0), Keys.splitQueue(R, "p", 1));
        assertNotEquals(Keys.splitIndex(R, "p", 0), Keys.splitIndex(R, "p", 1));
        assertNotEquals(Keys.dlq(R, "p", 0), Keys.dlq(R, "p", 1));
        // a split enqueued under attempt 1 does not touch attempt 0's queue and is not deduplicated against it
        Split s = new Split("S0000", Collections.singletonMap("k", "v"));
        assertTrue(dp.enqueueSplit(R, "p1", 0, s, 1));
        assertTrue(dp.enqueueSplit(R, "p1", 1, s, 1));
        assertEquals(1, dp.streamLength(Keys.splitQueue(R, "p1", 0)));
        assertEquals(1, dp.streamLength(Keys.splitQueue(R, "p1", 1)));
        dp.deleteKeys(R, Arrays.asList(Keys.splitQueue(R, "p1", 0), Keys.splitIndex(R, "p1", 0)), 1);
        assertEquals(0, dp.streamLength(Keys.splitQueue(R, "p1", 0)));
        assertEquals(1, dp.streamLength(Keys.splitQueue(R, "p1", 1)));
    }

    @Test
    void throttleFlagExpiresAndRoundSeqIsMonotonic() throws Exception {
        dp.setGlobalThrottle(true, 300);
        assertTrue(dp.globalThrottle());
        assertTrue(raw.sync().pttl(Keys.throttle()) > 0);
        Thread.sleep(450);
        assertFalse(dp.globalThrottle(), "a throttle nobody refreshes expires");
        dp.setGlobalThrottle(true, 0);
        assertEquals(-1, raw.sync().pttl(Keys.throttle()));
        dp.setGlobalThrottle(false, 0);
        long a = dp.nextRoundSeq(), b = dp.nextRoundSeq();
        assertEquals(a + 1, b);
    }

    @Test
    void renewReportsWhetherTheEntryIsStillPending() {
        dp.ensureGroup(R, IN, 1);
        add(IN, "x");
        StreamEntry e = dp.read("A", Collections.singletonList(IN), 1, 0).get(0);
        assertTrue(dp.renew("A", IN, e.getEntryId()));
        dp.commit(CommitRequest.builder().roundId(R).inputStreamKey(IN).inputEntryId(e.getEntryId()).highWatermark(10)
                .outputs(new ArrayList<OutputChunk>()).event(ev("c")).build());
        assertFalse(dp.renew("A", IN, e.getEntryId()), "acked entry");
        raw.sync().del(IN);
        assertThrows(Exception.class, () -> dp.renew("A", IN, e.getEntryId()), "NOGROUP once the stream is gone");
    }

    @Test
    void blockingReadsDoNotStallSharedConnection() throws Exception {
        dp.ensureGroup(R, IN, E);
        int n = 4;
        ExecutorService ex = Executors.newFixedThreadPool(n);
        CountDownLatch started = new CountDownLatch(n);
        List<Future<Long>> fs = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            final String c = "w" + i;
            fs.add(ex.submit(() -> {
                started.countDown();
                long t = System.currentTimeMillis();
                dp.read(c, Collections.singletonList(IN), 1, 2000);
                dp.releaseThreadResources();
                return System.currentTimeMillis() - t;
            }));
        }
        started.await();
        Thread.sleep(200);
        long worst = 0;
        for (int i = 0; i < 20; i++) {
            long t = System.nanoTime();
            dp.streamLength(IN);
            dp.isPaused(R);
            worst = Math.max(worst, (System.nanoTime() - t) / 1_000_000);
        }
        assertTrue(worst < 100, "worst=" + worst);
        for (Future<Long> f : fs) assertTrue(f.get() >= 1500);
        ex.shutdown();
    }

    @Test
    void blockingReadWakesOnData() throws Exception {
        dp.ensureGroup(R, IN, E);
        ExecutorService ex = Executors.newSingleThreadExecutor();
        Future<List<StreamEntry>> f = ex.submit(() -> dp.read("w", Collections.singletonList(IN), 1, 5000));
        Thread.sleep(200);
        add(IN, "hi");
        List<StreamEntry> r = f.get(3, TimeUnit.SECONDS);
        assertEquals(1, r.size());
        ex.shutdown();
    }

    @Test
    void throughput() {
        dp.ensureGroup(R, IN, E);
        dp.ensureGroup(R, OUT, E);
        int n = 2000;
        for (int i = 0; i < n; i++) add(IN, "p" + i);
        long t = System.currentTimeMillis();
        int done = 0;
        while (done < n) {
            for (StreamEntry e : dp.read("c", Collections.singletonList(IN), 100, 0)) {
                assertEquals(CommitResult.OK, dp.commit(CommitRequest.builder().roundId(R).inputStreamKey(IN)
                        .inputEntryId(e.getEntryId()).highWatermark(1_000_000)
                        .outputs(Collections.singletonList(new OutputChunk(OUT, "o"))).event(ev("c")).build()));
                done++;
            }
        }
        long ms = System.currentTimeMillis() - t;
        assertEquals(n, dp.streamLength(OUT));
        assertTrue(ms < 10_000, "ms=" + ms);
    }

    @Test
    void pendingSummaryReportsOldestPendingEntry() throws Exception {
        assertEquals(0, dp.pendingSummary(IN).getPending(), "missing stream");
        dp.ensureGroup(R, IN, E);
        assertEquals(0, dp.pendingSummary(IN).getPending(), "no pending entries");
        assertNull(dp.pendingSummary(IN).getOldestEntryId());
        String first = add(IN, "a");
        add(IN, "b");
        add(IN, "c");
        dp.read("w1", Collections.singletonList(IN), 1, 0);   // a -> w1
        Thread.sleep(150);
        dp.read("w2", Collections.singletonList(IN), 2, 0);   // b, c -> w2
        PendingSummary s = dp.pendingSummary(IN);
        assertEquals(3, s.getPending());
        assertEquals(first, s.getOldestEntryId());
        assertEquals("w1", s.getOldestConsumer());
        assertEquals(1, s.getOldestDeliveryCount());
        assertTrue(s.getOldestIdleMillis() >= 100, "idle " + s.getOldestIdleMillis());
        assertTrue(dp.renew("w1", IN, first));
        assertTrue(dp.pendingSummary(IN).getOldestIdleMillis() < 100, "renew resets the idle clock");
        Thread.sleep(20);
        dp.autoClaim("w3", IN, 10, 10);
        PendingSummary after = dp.pendingSummary(IN);
        assertEquals("w3", after.getOldestConsumer());
        assertEquals(2, after.getOldestDeliveryCount());
    }

    @Test
    void groupExistsTellsAMissingStreamFromAMissingGroup() {
        assertFalse(dp.groupExists(IN), "missing stream (ERR no such key)");
        dp.ensureGroup(R, IN, E);
        assertTrue(dp.groupExists(IN));
        add(IN, "x");
        raw.sync().xgroupDestroy(IN, Keys.GROUP);   // the stream and its entry survive, the group does not
        assertEquals(1, dp.streamLength(IN));
        assertFalse(dp.groupExists(IN), "NOGROUP");
        raw.sync().xgroupCreate(io.lettuce.core.XReadArgs.StreamOffset.from(IN, "0"), "someone-else");
        assertFalse(dp.groupExists(IN), "another group is not ours");
        dp.ensureGroup(R, IN, E);
        assertTrue(dp.groupExists(IN));
    }

    @Test
    void preRegistryRoundsAreFoundByABoundedResumableScanAndOtherRoundsAreUntouched() {
        // keys of a round created before pg:{r}:keys existed: no registry, not even a fixed control key
        String legacy = "legacy1";
        Set<String> mine = new HashSet<>();
        for (String k : Arrays.asList(Keys.channel(legacy, "e1", 0), Keys.splitQueue(legacy, "p", 0), Keys.splitIndex(legacy, "p", 0), Keys.dlq(legacy, "p", 0))) {
            raw.sync().xadd(k, Collections.singletonMap("c", "x"));
            mine.add(k);
        }
        for (int i = 0; i < 300; i++) { String k = "pg:{legacy1}:filler:" + i; raw.sync().set(k, "v"); mine.add(k); }
        for (int i = 0; i < 2000; i++) raw.sync().set("decoy:" + i, "v");   // the SCAN has to walk through unrelated keys too
        seedRound("modern", "e1");
        String other = "pg:{legacy10}:ch:e1:0";   // a round whose id merely starts with the same characters
        raw.sync().set(other, "v");
        dp.setLegacyScan(0, 20);   // budget 0: every call does exactly one SCAN step of 20, then stops
        try {
            dp.purgeRoundData(Collections.singletonList(legacy));
            assertTrue(raw.sync().sismember(RedisDataPlane.LEGACY_PENDING, legacy), "no registry: queued for the scan");
            assertNotNull(raw.sync().hget(RedisDataPlane.LEGACY_CURSOR, legacy), "the cursor is persisted between calls (one step cannot finish 2300+ keys)");
            int calls = 1;
            while (raw.sync().sismember(RedisDataPlane.LEGACY_PENDING, legacy) && calls < 5000) { dp.purgeRoundData(Collections.<String>emptyList()); calls++; }
            assertTrue(calls > 3, "resumed over several calls: " + calls);
            assertFalse(raw.sync().sismember(RedisDataPlane.LEGACY_PENDING, legacy));
            assertNull(raw.sync().hget(RedisDataPlane.LEGACY_CURSOR, legacy));
            for (String k : mine) assertEquals(0, raw.sync().exists(k), k);
            assertEquals(1, raw.sync().exists(other), "pg:{legacy10}:* is another round");
            assertEquals(1, raw.sync().exists(Keys.registry("modern")), "a round with a registry is never scanned for");
            assertEquals(2000, raw.sync().keys("decoy:*").size());
            assertFalse(raw.sync().sismember(RedisDataPlane.LEGACY_PENDING, "modern"));
        } finally { dp.setLegacyScan(50, 500); }
    }

    @Test
    void registryRoundsAreNotQueuedForTheLegacyScanButKeylessOnesAre() {
        seedRound("reg", "e1");
        dp.purgeRoundData(Collections.singletonList("reg"));
        assertFalse(raw.sync().sismember(RedisDataPlane.LEGACY_PENDING, "reg"));
        dp.purgeRoundData(Collections.singletonList("nokeys"));   // finishes within the first call: nothing to find
        assertFalse(raw.sync().sismember(RedisDataPlane.LEGACY_PENDING, "nokeys"));
    }

    @Test
    @SuppressWarnings("unchecked")
    void aReconnectingBlockingConnectionIsReusedNotOrphaned() throws Exception {   // F070
        dp.ensureGroup(R, IN, E);
        dp.read("c1", Collections.singletonList(IN), 1, 50);   // creates this thread's blocking connection
        java.lang.reflect.Field f = RedisDataPlane.class.getDeclaredField("blockingAll");
        f.setAccessible(true);
        int before = ((Collection<Object>) f.get(dp)).size();
        raw.sync().clientKill(io.lettuce.core.KillArgs.Builder.typeNormal().skipme());   // an outage: every other connection is cut
        Thread.sleep(300);
        for (int i = 0; i < 20; i++) {
            try { dp.read("c1", Collections.singletonList(IN), 1, 50); } catch (RuntimeException ignored) { /* the first calls may fail while reconnecting */ }
            Thread.sleep(50);
        }
        assertEquals(before, ((Collection<Object>) f.get(dp)).size(), "no replacement connection was created (the old one reconnected by itself)");
        assertEquals(0, dp.read("c1", Collections.singletonList(IN), 1, 50).size(), "and it works again");
    }

    @Test
    void pruneWorkersRemovesOnlyStaleMembers() {
        dp.heartbeat("fresh", Collections.<String, String>emptyMap());
        raw.sync().zadd(Keys.workers(), (double) (System.currentTimeMillis() - 3_600_000L), "ancient");
        assertEquals(1, dp.pruneWorkers(600_000));
        assertNotNull(raw.sync().zscore(Keys.workers(), "fresh"));
        assertNull(raw.sync().zscore(Keys.workers(), "ancient"));
        assertEquals(0, dp.pruneWorkers(600_000));
    }

    /** Creates the round's data through the dataplane (so the key registry is filled). */
    private void seedRound(String r, String edge) {
        assertTrue(dp.fenceRound(r, E));
        String ch = Keys.channel(r, edge, 0);
        dp.ensureGroup(r, ch, E);
        add(ch, "x");
        dp.setDesiredParallelism(r, "p", 2, 0, E);
        dp.appendJournal(r, ChunkEvent.builder().type("DONE").roundId(r).pipelineId("p").chunkId("c").ts(1L).build());
        dp.enqueueSplit(r, "p", 0, new Split("s0", Collections.singletonMap("i", "0")), E);
    }

    @Test
    void purgeRoundDataDeletesOnlyTheGivenRoundsByRegistry() {
        seedRound("gone", "e1");
        seedRound("gone2", "e1");
        seedRound("kept", "e1");
        long before = raw.sync().dbsize();
        int n = dp.purgeRoundData(Arrays.asList("gone", "gone2"));
        assertTrue(n >= 10, "deleted " + n);
        assertEquals(3, raw.sync().exists(Keys.channel("kept", "e1", 0), Keys.fence("kept"), Keys.registry("kept")));
        for (String r : Arrays.asList("gone", "gone2")) {
            assertEquals(0, raw.sync().exists(Keys.channel(r, "e1", 0), Keys.fence(r), Keys.registry(r), Keys.journal(r), Keys.desired(r),
                    Keys.splitQueue(r, "p", 0), Keys.splitIndex(r, "p", 0)), r);
        }
        assertEquals(before - n, raw.sync().dbsize());
        assertEquals(0, dp.purgeRoundData(Collections.<String>emptyList()));
        assertTrue(dp.purgeRoundData(Collections.singletonList("never-existed")) >= 0);
    }

    @Test
    void fenceKeyHasNoTtlAndSurvivesDeleteKeys() {
        assertTrue(dp.fenceRound(R, 7));
        assertEquals(-1L, raw.sync().pttl(Keys.fence(R)), "no TTL");
        assertTrue(dp.fenceRound(R, 8));
        assertEquals(-1L, raw.sync().pttl(Keys.fence(R)));
        dp.deleteKeys(R, Arrays.asList(IN), 8);
        assertEquals("8", raw.sync().get(Keys.fence(R)));
    }

    @Test
    void ensureGroupIgnoresBusyGroupButRaisesOtherErrors() {
        dp.ensureGroup(R, IN, E);
        dp.ensureGroup(R, IN, E);   // BUSYGROUP is fine
        raw.sync().set("pg:{r1}:ch:wrongtype:0", "str");
        assertThrows(RuntimeException.class, () -> dp.ensureGroup(R, "pg:{r1}:ch:wrongtype:0", E), "WRONGTYPE must not be swallowed");
    }

    @Test
    void heartbeatReplacesTheInfoHash() {
        Map<String, String> first = new LinkedHashMap<>();
        first.put("graph.old", "h1");
        first.put("pods", "1");
        dp.heartbeat("podx", first);
        assertEquals(first, dp.workerInfo("podx"));
        Map<String, String> second = Collections.singletonMap("graph.new", "h2");
        dp.heartbeat("podx", second);
        assertEquals(second, dp.workerInfo("podx"), "stale fields are gone");
        long ttl = raw.sync().pttl(Keys.workerInfo("podx"));
        assertTrue(ttl > 0 && ttl <= 60_000, "ttl " + ttl);
    }

    @Test
    void deadLetterReturnsWhetherItMovedTheEntry() {
        dp.ensureGroup(R, IN, E);
        add(IN, "bad");
        StreamEntry e = dp.read("A", Collections.singletonList(IN), 1, 0).get(0);
        DeadLetterRequest req = DeadLetterRequest.builder().roundId(R).pipelineId("p1").attempt(0).inputStreamKey(IN).inputEntryId(e.getEntryId())
                .inputFields(e.getFields()).error("boom").event(ev("c1")).consumer("A").build();
        assertTrue(dp.deadLetter(req));
        assertFalse(dp.deadLetter(req), "already dead-lettered");
        assertEquals(1, raw.sync().xlen(Keys.dlq(R, "p1", 0)));
    }

    @Test
    void ownershipRenewCommitAndDeadLetterRequireTheOwningConsumer() throws Exception {
        dp.ensureGroup(R, IN, E);
        add(IN, "chunk");
        StreamEntry e = dp.read("A", Collections.singletonList(IN), 1, 0).get(0);
        Thread.sleep(50);
        assertEquals(1, dp.autoClaim("B", IN, 10, 1).size(), "B autoclaims the stalled entry");
        assertFalse(dp.renew("A", IN, e.getEntryId()), "A's renewer must not take it back");
        assertEquals("B", raw.sync().xpending(IN, Keys.GROUP, io.lettuce.core.Range.create("-", "+"), io.lettuce.core.Limit.from(1)).get(0).getConsumer());
        CommitRequest byA = CommitRequest.builder().roundId(R).inputStreamKey(IN).inputEntryId(e.getEntryId()).highWatermark(10)
                .outputs(Collections.<OutputChunk>emptyList()).event(ev("c1")).consumer("A").build();
        assertEquals(CommitResult.CONFLICT, dp.commit(byA), "A lost ownership");
        assertEquals(1, pendingCount(IN), "nothing was acked");
        DeadLetterRequest dlA = DeadLetterRequest.builder().roundId(R).pipelineId("p1").attempt(0).inputStreamKey(IN).inputEntryId(e.getEntryId())
                .inputFields(e.getFields()).error("x").event(ev("c1")).consumer("A").build();
        assertFalse(dp.deadLetter(dlA));
        assertEquals(0, raw.sync().exists(Keys.dlq(R, "p1", 0)));
        assertTrue(dp.renew("B", IN, e.getEntryId()));
        byA.setConsumer("B");
        assertEquals(CommitResult.OK, dp.commit(byA));
        assertEquals(0, pendingCount(IN));
    }

    @Test
    void autoClaimKeepsItsCursorAndReachesEntriesBehindRenewedOnes() throws Exception {
        dp.ensureGroup(R, IN, E);
        int renewed = 450;
        for (int i = 0; i < renewed + 3; i++) add(IN, "c" + i);
        List<StreamEntry> all = new ArrayList<>();
        while (all.size() < renewed + 3) all.addAll(dp.read("A", Collections.singletonList(IN), 100, 0));
        Thread.sleep(200);
        for (int i = 0; i < renewed; i++) assertTrue(dp.renew("A", IN, all.get(i).getEntryId()));   // the head of the PEL stays fresh
        Set<String> stale = new HashSet<>();
        for (int i = renewed; i < all.size(); i++) stale.add(all.get(i).getEntryId());
        Set<String> got = new HashSet<>();
        for (int call = 0; call < 20 && got.size() < stale.size(); call++) {
            for (StreamEntry e : dp.autoClaim("B", IN, 150, 1)) {
                assertTrue(stale.contains(e.getEntryId()), "only a stale entry may be claimed");
                got.add(e.getEntryId());
            }
        }
        assertEquals(stale, got, "every stale entry was reclaimed although 450 renewed entries precede them");
    }

    @Test
    void scriptsSurviveScriptFlushUnderConcurrency() throws Exception {
        raw.sync().scriptFlush();
        ExecutorService ex = Executors.newFixedThreadPool(8);
        List<Future<Long>> fs = new ArrayList<>();
        for (int i = 0; i < 64; i++) fs.add(ex.submit(() -> dp.nextRoundSeq()));
        Set<Long> seen = new HashSet<>();
        for (Future<Long> f : fs) seen.add(f.get(10, TimeUnit.SECONDS));
        ex.shutdown();
        assertEquals(64, seen.size());
    }
}
