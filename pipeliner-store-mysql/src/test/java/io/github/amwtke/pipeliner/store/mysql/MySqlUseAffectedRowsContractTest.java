package io.github.amwtke.pipeliner.store.mysql;

import com.zaxxer.hikari.HikariDataSource;
import io.github.amwtke.pipeliner.core.store.AbstractControlPlaneStoreContractTest;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;

/** The whole contract with {@code useAffectedRows=true}: an UPDATE that matches a row but changes nothing reports 0, which must not be read as "fenced". */
class MySqlUseAffectedRowsContractTest extends AbstractControlPlaneStoreContractTest {
    private static HikariDataSource ds;
    private static MySqlControlPlaneStore shared;

    @BeforeAll
    static void connect() {
        Assumptions.assumeTrue(MySqlTestDb.up(), "MySQL not reachable");
        ds = MySqlTestDb.open("&useAffectedRows=true", true);
        shared = new MySqlControlPlaneStore(ds);
        shared.init();
    }

    @AfterAll
    static void close() { if (ds != null) ds.close(); }

    @BeforeEach
    void setUp() { store = shared; }
}
