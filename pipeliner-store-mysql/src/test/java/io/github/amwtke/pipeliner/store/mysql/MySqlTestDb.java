package io.github.amwtke.pipeliner.store.mysql;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.net.InetSocketAddress;
import java.net.Socket;

/** The shared test MySQL (schema pg_store_test on the Lima VM). */
final class MySqlTestDb {
    private MySqlTestDb() {}

    static final String URL = "jdbc:mysql://127.0.0.1:3306/pg_store_test?useSSL=false&allowPublicKeyRetrieval=true"
            + "&rewriteBatchedStatements=true&serverTimezone=UTC&createDatabaseIfNotExist=true";

    static boolean up() {
        try (Socket s = new Socket()) {
            s.connect(new InetSocketAddress("127.0.0.1", 3306), 500);
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    /** A pool on the test schema; {@code urlSuffix} appends driver properties, {@code autoCommit} is the pool's default mode. */
    static HikariDataSource open(String urlSuffix, boolean autoCommit) {
        HikariConfig cfg = new HikariConfig();
        cfg.setJdbcUrl(URL + urlSuffix);
        cfg.setUsername("pipeliner");
        cfg.setPassword("pipeliner");
        cfg.setMaximumPoolSize(8);
        cfg.setAutoCommit(autoCommit);
        return new HikariDataSource(cfg);
    }
}
