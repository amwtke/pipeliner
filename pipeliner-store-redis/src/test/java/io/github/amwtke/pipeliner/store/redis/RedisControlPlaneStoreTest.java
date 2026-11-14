package io.github.amwtke.pipeliner.store.redis;

import static org.junit.jupiter.api.Assertions.*;

import io.github.amwtke.pipeliner.core.store.AbstractControlPlaneStoreContractTest;
import io.lettuce.core.RedisClient;
import java.net.InetSocketAddress;
import java.net.Socket;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;

/** Runs the shared contract against a live Redis (DB 1 on port 6380; skipped when unreachable). */
class RedisControlPlaneStoreTest extends AbstractControlPlaneStoreContractTest {
    private static RedisClient client;
    private static RedisControlPlaneStore shared;

    @BeforeAll
    static void connect() {
        boolean up;
        try (Socket s = new Socket()) {
            s.connect(new InetSocketAddress("127.0.0.1", 6380), 500);
            up = true;
        } catch (Exception e) {
            up = false;
        }
        Assumptions.assumeTrue(up, "Redis not reachable");
        client = RedisClient.create("redis://127.0.0.1:6380/1");
        client.connect().sync().flushdb();
        shared = new RedisControlPlaneStore(client);
        shared.init();
    }

    @AfterAll
    static void close() { if (client != null) client.shutdown(); }

    @BeforeEach
    void setUp() { store = shared; }

    @org.junit.jupiter.api.Test
    void purgeLeavesNoKeyOfTheRoundBehind() {
        String rid = "scan-" + Long.toString(System.nanoTime(), 36);
        java.util.Map<String, String> params = new java.util.LinkedHashMap<>();
        params.put("a", "b");
        shared.createRound(io.github.amwtke.pipeliner.core.store.RoundRecord.builder().roundId(rid).graphName("scan-g").bizKey(rid).planHash("h").planJson("{}")
                        .params(params).status(io.github.amwtke.pipeliner.core.store.RoundStatus.CREATED).createdAt(1L).build(),
                java.util.Collections.singletonList(io.github.amwtke.pipeliner.core.store.RunRecord.builder().roundId(rid).runId("run1").stage(0)
                        .status(io.github.amwtke.pipeliner.core.store.RunStatus.PENDING).build()),
                java.util.Collections.singletonList(io.github.amwtke.pipeliner.core.store.GateRecord.builder().roundId(rid).gateId("g1")
                        .status(io.github.amwtke.pipeliner.core.store.GateStatus.CLOSED).build()),
                java.util.Collections.singletonList(io.github.amwtke.pipeliner.core.store.PipelineRecord.builder().roundId(rid).pipelineId("p1").runId("run1")
                        .status(io.github.amwtke.pipeliner.core.store.PipelineStatus.PENDING).build()));
        shared.savePendingChunks(rid, java.util.Collections.singletonList(io.github.amwtke.pipeliner.core.store.ChunkRecord.builder().pipelineId("p1")
                .chunkId("c1").runId("run1").inputEdge("split").build()), 1);
        shared.saveGateSignal(rid, "g1", "k", "v");
        io.lettuce.core.api.sync.RedisCommands<String, String> c = client.connect().sync();
        c.set("pg:{" + rid + "}:fence", "1");   // data plane leftovers of the round that the store script also removes
        c.set("pg:{" + rid + "}:journal", "x");
        assertTrue(scan(c, rid) > 5);
        assertTrue(shared.casRoundStatus(rid, io.github.amwtke.pipeliner.core.store.RoundStatus.CREATED, io.github.amwtke.pipeliner.core.store.RoundStatus.RUNNING, null, 1));
        assertTrue(shared.casRoundStatus(rid, io.github.amwtke.pipeliner.core.store.RoundStatus.RUNNING, io.github.amwtke.pipeliner.core.store.RoundStatus.FAILED, "x", 1));
        assertTrue(shared.purgeRounds(System.currentTimeMillis() + 10_000, 1000).contains(rid));
        assertEquals(0, scan(c, rid), "no key of the purged round may remain");
        assertNull(c.hget("pg:store:biz", "scan-g|" + rid));
        assertNull(c.zscore("pg:store:graph:scan-g", rid), "the per-graph index entry is gone");
        assertNull(c.zscore("pg:store:rounds", rid));
        assertNull(c.zscore("pg:store:finished", rid));
        assertNull(c.zscore("pg:store:uncompacted", rid));
        assertFalse(c.sismember("pg:store:active", rid));
    }

    @org.junit.jupiter.api.Test
    void initBackfillsMaxEpochAndSeqFromExistingRounds() {
        String rid = "bf-" + Long.toString(System.nanoTime(), 36);
        io.github.amwtke.pipeliner.core.store.RoundRecord r = io.github.amwtke.pipeliner.core.store.RoundRecord.builder().roundId(rid).graphName("bf-g").bizKey(rid)
                .planHash("h").planJson("{}").status(io.github.amwtke.pipeliner.core.store.RoundStatus.CREATED).createdAt(1L).seq(9_000_000L).build();
        shared.createRound(r, java.util.Collections.singletonList(io.github.amwtke.pipeliner.core.store.RunRecord.builder().roundId(rid).runId("run1").stage(0)
                .status(io.github.amwtke.pipeliner.core.store.RunStatus.PENDING).build()), java.util.Collections.<io.github.amwtke.pipeliner.core.store.GateRecord>emptyList(),
                java.util.Collections.singletonList(io.github.amwtke.pipeliner.core.store.PipelineRecord.builder().roundId(rid).pipelineId("p1").runId("run1")
                        .status(io.github.amwtke.pipeliner.core.store.PipelineStatus.PENDING).build()));
        // the round's epoch was written by a path that does not maintain the global max (e.g. an upgraded store)
        io.lettuce.core.api.sync.RedisCommands<String, String> c = client.connect().sync();
        c.hset("pg:{" + rid + "}:round", "epoch", "8000000");
        c.del("pg:store:maxepoch", "pg:store:maxseq", "pg:store:schema");   // the backfill runs once, behind the schema marker
        assertEquals(0, shared.maxMasterEpoch());
        assertEquals(0, shared.maxRoundSeq());
        shared.init();
        assertTrue(shared.maxMasterEpoch() >= 8_000_000L, "init backfilled the max epoch: " + shared.maxMasterEpoch());
        assertTrue(shared.maxRoundSeq() >= 9_000_000L, "init backfilled the max seq: " + shared.maxRoundSeq());
        shared.init();   // idempotent, never lowers
        assertTrue(shared.maxMasterEpoch() >= 8_000_000L);
        assertFalse(shared.reserveMasterEpoch(8_000_000L), "a reservation at the backfilled epoch is refused");
        assertTrue(shared.reserveMasterEpoch(shared.maxMasterEpoch() + 1));
    }

    private static io.github.amwtke.pipeliner.core.store.RoundRecord round(String rid, String graph, long created) {
        return io.github.amwtke.pipeliner.core.store.RoundRecord.builder().roundId(rid).graphName(graph).bizKey(rid).planHash("h").planJson("{}")
                .status(io.github.amwtke.pipeliner.core.store.RoundStatus.CREATED).createdAt(created).build();
    }

    private static String mkRound(String graph, long created) {
        String rid = "rt-" + Long.toString(System.nanoTime(), 36);
        shared.createRound(round(rid, graph, created),
                java.util.Collections.singletonList(io.github.amwtke.pipeliner.core.store.RunRecord.builder().roundId(rid).runId("run1").stage(0)
                        .status(io.github.amwtke.pipeliner.core.store.RunStatus.PENDING).build()),
                java.util.Collections.<io.github.amwtke.pipeliner.core.store.GateRecord>emptyList(),
                java.util.Collections.singletonList(io.github.amwtke.pipeliner.core.store.PipelineRecord.builder().roundId(rid).pipelineId("p1").runId("run1")
                        .status(io.github.amwtke.pipeliner.core.store.PipelineStatus.PENDING).build()));
        return rid;
    }

    @org.junit.jupiter.api.Test
    void initBackfillRunsOnlyOnceBehindTheSchemaMarker() {
        shared.init();
        assertNotNull(client.connect().sync().get("pg:store:schema"));
        io.lettuce.core.api.sync.RedisCommands<String, String> c = client.connect().sync();
        String rid = mkRound("mk-g", 1L);
        c.hset("pg:{" + rid + "}:round", "epoch", "9000000");
        c.del("pg:store:maxepoch");
        shared.init();   // the marker is set: no second pass over the rounds
        assertEquals(0, shared.maxMasterEpoch());
        c.del("pg:store:schema");
        shared.init();
        assertTrue(shared.maxMasterEpoch() >= 9_000_000L, "without the marker the backfill runs again");
    }

    @org.junit.jupiter.api.Test
    void roundStatusIndexesAreUpdatedInsideTheFencedStatusScript() {
        io.lettuce.core.api.sync.RedisCommands<String, String> c = client.connect().sync();
        String rid = mkRound("idx-g", 10L);
        assertTrue(c.sismember("pg:store:active", rid));
        assertTrue(shared.casRoundStatus(rid, io.github.amwtke.pipeliner.core.store.RoundStatus.CREATED, io.github.amwtke.pipeliner.core.store.RoundStatus.RUNNING, null, 5));
        assertTrue(shared.casRoundStatus(rid, io.github.amwtke.pipeliner.core.store.RoundStatus.RUNNING, io.github.amwtke.pipeliner.core.store.RoundStatus.FAILED, "x", 5));
        assertFalse(c.sismember("pg:store:active", rid));
        assertNotNull(c.zscore("pg:store:finished", rid));
        assertNotNull(c.zscore("pg:store:uncompacted", rid));
        // a fenced revive neither changes the status nor the indexes
        assertFalse(shared.casRoundStatus(rid, io.github.amwtke.pipeliner.core.store.RoundStatus.FAILED, io.github.amwtke.pipeliner.core.store.RoundStatus.RUNNING, null, 4));
        assertFalse(c.sismember("pg:store:active", rid));
        assertNotNull(c.zscore("pg:store:finished", rid));
        assertTrue(shared.casRoundStatus(rid, io.github.amwtke.pipeliner.core.store.RoundStatus.FAILED, io.github.amwtke.pipeliner.core.store.RoundStatus.RUNNING, null, 6));
        assertTrue(c.sismember("pg:store:active", rid));
        assertNull(c.zscore("pg:store:finished", rid));
        assertNull(c.zscore("pg:store:uncompacted", rid));
        assertNull(c.hget("pg:{" + rid + "}:round", "finishedAt"));
        assertNull(c.hget("pg:{" + rid + "}:round", "error"));
    }

    @org.junit.jupiter.api.Test
    void listRecentRoundsWithAGraphFilterReadsThePerGraphIndex() {
        io.lettuce.core.api.sync.RedisCommands<String, String> c = client.connect().sync();
        String g = "pg-idx-" + Long.toString(System.nanoTime(), 36);
        String a = mkRound(g, 100L), b = mkRound(g, 200L), other = mkRound(g + "-other", 300L);
        assertEquals(2, c.zcard("pg:store:graph:" + g));
        assertEquals(java.util.Arrays.asList(b, a), ids(shared.listRecentRounds(g, 10)));
        assertEquals(java.util.Collections.singletonList(other), ids(shared.listRecentRounds(g + "-other", 10)));
        // a graph index that is missing (rounds written before it existed) falls back to the global scan
        c.del("pg:store:graph:" + g);
        assertEquals(java.util.Arrays.asList(b, a), ids(shared.listRecentRounds(g, 10)));
    }

    private static java.util.List<String> ids(java.util.List<io.github.amwtke.pipeliner.core.store.RoundRecord> l) {
        java.util.List<String> out = new java.util.ArrayList<>();
        for (io.github.amwtke.pipeliner.core.store.RoundRecord r : l) out.add(r.getRoundId());
        return out;
    }

    /** The journal offset is written after the events: a script that fails midway leaves it behind so the batch is retried. */
    @org.junit.jupiter.api.Test
    void aFailingChunkScriptDoesNotAdvanceTheJournalOffset() {
        io.lettuce.core.api.sync.RedisCommands<String, String> c = client.connect().sync();
        String rid = mkRound("jo-g", 1L);
        io.github.amwtke.pipeliner.core.store.ChunkEvent ok = io.github.amwtke.pipeliner.core.store.ChunkEvent.builder().type("DONE").roundId(rid).runId("run1")
                .pipelineId("p1").chunkId("c1").inputEdge("split").rowsIn(1L).ts(1L).build();
        shared.applyChunkEvents(rid, java.util.Collections.singletonList(ok), "1-0", 1);
        c.hset("pg:{" + rid + "}:chunk:p1", "c2", "{not json");   // corrupt record: decoding it fails the script
        io.github.amwtke.pipeliner.core.store.ChunkEvent bad = io.github.amwtke.pipeliner.core.store.ChunkEvent.builder().type("DONE").roundId(rid).runId("run1")
                .pipelineId("p1").chunkId("c2").inputEdge("split").rowsIn(1L).ts(1L).build();
        assertThrows(RuntimeException.class, () -> shared.applyChunkEvents(rid, java.util.Arrays.asList(ok, bad), "2-0", 1));
        assertEquals("1-0", shared.loadRoundRecord(rid).getJournalOffset(), "the offset stayed behind the failed batch");
    }

    /** Pipelines written before the chunk indexes existed are served from their records, and the init backfill indexes them. */
    @org.junit.jupiter.api.Test
    void pipelinesWithoutChunkIndexesFallBackToScanningAndInitIndexesThem() {
        io.lettuce.core.api.sync.RedisCommands<String, String> c = client.connect().sync();
        String rid = mkRound("legacy-g", 1L);
        java.util.Map<String, Long> out = new java.util.LinkedHashMap<>();
        out.put("e1", 4L);
        java.util.List<io.github.amwtke.pipeliner.core.store.ChunkEvent> evs = new java.util.ArrayList<>();
        for (int i = 0; i < 4; i++)
            evs.add(io.github.amwtke.pipeliner.core.store.ChunkEvent.builder().type(i == 3 ? "DEAD" : "DONE").roundId(rid).runId("run1").pipelineId("p1")
                    .chunkId("c" + i).inputEdge("split").rowsIn(4L).rowsOut(i == 3 ? null : out).ts(1L).build());
        shared.applyChunkEvents(rid, evs, "1-0", 1);
        assertEquals(12L, (long) shared.sumRowsOutByEdge(rid, "run1").get("e1"));
        // simulate a pipeline from the previous version: no index marker, no index keys, no per-edge sums
        c.hdel("pg:{" + rid + "}:cnt:p1", "_ix", "o:e1");
        c.del("pg:{" + rid + "}:cidx:p1", "pg:{" + rid + "}:cst:p1");
        assertEquals(12L, (long) shared.sumRowsOutByEdge(rid, "run1").get("e1"), "summed from the records");
        assertEquals(java.util.Arrays.asList("c1", "c2"), ids2(shared.queryChunks(io.github.amwtke.pipeliner.core.store.ChunkQuery.builder().roundId(rid)
                .status(io.github.amwtke.pipeliner.core.store.ChunkStatus.DONE).limit(2).offset(1).build())));
        shared.markChunksSkipped(rid, "run1", 1);
        assertEquals(1, shared.countChunks(rid, "p1", io.github.amwtke.pipeliner.core.store.ChunkStatus.SKIPPED));
        c.del("pg:{" + rid + "}:cidx:p1", "pg:{" + rid + "}:cst:p1");
        c.hdel("pg:{" + rid + "}:cnt:p1", "_ix");
        c.del("pg:store:schema");
        shared.init();
        assertTrue(c.hexists("pg:{" + rid + "}:cnt:p1", "_ix"), "init rebuilt the indexes");
        assertEquals(4, c.zcard("pg:{" + rid + "}:cidx:p1"));
        assertEquals(12L, (long) shared.sumRowsOutByEdge(rid, "run1").get("e1"));
        assertEquals(java.util.Arrays.asList("c0", "c1", "c2", "c3"), ids2(shared.queryChunks(io.github.amwtke.pipeliner.core.store.ChunkQuery.builder().roundId(rid).limit(10).build())));
    }

    private static java.util.List<String> ids2(java.util.List<io.github.amwtke.pipeliner.core.store.ChunkRecord> l) {
        java.util.List<String> out = new java.util.ArrayList<>();
        for (io.github.amwtke.pipeliner.core.store.ChunkRecord r : l) out.add(r.getChunkId());
        return out;
    }

    private static int scan(io.lettuce.core.api.sync.RedisCommands<String, String> c, String rid) {
        int n = 0;
        io.lettuce.core.ScanArgs args = io.lettuce.core.ScanArgs.Builder.matches("pg:{" + rid + "}:*").limit(1000);
        io.lettuce.core.KeyScanCursor<String> cur = c.scan(args);
        while (true) {
            n += cur.getKeys().size();
            if (cur.isFinished()) return n;
            cur = c.scan(cur, args);
        }
    }
}
