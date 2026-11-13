package io.github.amwtke.pipeliner.store.mysql;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.net.InetSocketAddress;
import java.net.Socket;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;

/** Runs the shared contract against a live MySQL (skipped when unreachable). */
class MySqlControlPlaneStoreTest extends AbstractControlPlaneStoreContractTest {
    private static final String URL = "jdbc:mysql://127.0.0.1:3306/pg_store_test?useSSL=false&allowPublicKeyRetrieval=true"
            + "&rewriteBatchedStatements=true&serverTimezone=UTC&createDatabaseIfNotExist=true";
    private static HikariDataSource ds;
    private static MySqlControlPlaneStore shared;

    @BeforeAll
    static void connect() {
        boolean up;
        try (Socket s = new Socket()) {
            s.connect(new InetSocketAddress("127.0.0.1", 3306), 500);
            up = true;
        } catch (Exception e) {
            up = false;
        }
        Assumptions.assumeTrue(up, "MySQL not reachable");
        HikariConfig cfg = new HikariConfig();
        cfg.setJdbcUrl(URL);
        cfg.setUsername("pipeliner");
        cfg.setPassword("pipeliner");
        cfg.setMaximumPoolSize(8);
        ds = new HikariDataSource(cfg);
        shared = new MySqlControlPlaneStore(ds);
        shared.init();
        shared.init(); // idempotent
    }

    @AfterAll
    static void close() { if (ds != null) ds.close(); }

    @BeforeEach
    void setUp() { store = shared; }
}
