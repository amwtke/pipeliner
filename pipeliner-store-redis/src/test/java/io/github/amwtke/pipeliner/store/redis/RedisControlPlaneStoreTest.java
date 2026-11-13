package io.github.amwtke.pipeliner.store.redis;

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
}
