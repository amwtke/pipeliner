package io.github.amwtke.pipeliner.store.redis;

import static org.junit.jupiter.api.Assertions.*;

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
                .chunkId("c1").runId("run1").inputEdge("split").build()));
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
        assertNull(c.zscore("pg:store:rounds", rid));
        assertNull(c.zscore("pg:store:finished", rid));
        assertNull(c.zscore("pg:store:uncompacted", rid));
        assertFalse(c.sismember("pg:store:active", rid));
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
