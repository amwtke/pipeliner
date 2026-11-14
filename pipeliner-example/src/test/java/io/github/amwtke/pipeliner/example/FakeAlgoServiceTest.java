package io.github.amwtke.pipeliner.example;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import io.github.amwtke.pipeliner.api.Row;
import io.github.amwtke.pipeliner.core.engine.PipelinerEngine;
import io.github.amwtke.pipeliner.core.util.SimpleDataSourceRegistry;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import javax.sql.DataSource;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.ObjectProvider;

/** The durable ex_algo_request table against real MySQL (own schema, own table): idempotent claim, release of a failed claim, abandoned claims. */
class FakeAlgoServiceTest {
    private static final String MYSQL_DB = "pipeliner_demo_test";
    private static HikariDataSource my;
    private static SimpleDataSourceRegistry registry;

    @BeforeAll
    static void up() {
        boolean ok;
        try (Socket s = new Socket()) { s.connect(new InetSocketAddress("127.0.0.1", 3306), 500); ok = true; } catch (Exception e) { ok = false; }
        Assumptions.assumeTrue(ok, "mysql not reachable");
        HikariConfig c = new HikariConfig();
        c.setJdbcUrl("jdbc:mysql://127.0.0.1:3306/" + MYSQL_DB + "?createDatabaseIfNotExist=true&useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=UTC");
        c.setUsername("pipeliner"); c.setPassword("pipeliner"); c.setDriverClassName("com.mysql.cj.jdbc.Driver"); c.setMaximumPoolSize(4);
        my = new HikariDataSource(c);
        Map<String, DataSource> m = new HashMap<>();
        m.put("mysql", my);
        registry = new SimpleDataSourceRegistry(m);
    }

    @AfterAll
    static void down() { if (my != null) my.close(); }

    @BeforeEach
    void fresh() throws Exception {
        try (Connection c = my.getConnection(); Statement st = c.createStatement()) {
            st.execute("drop table if exists ex_algo_request");
            st.execute(FakeAlgoService.REQUEST_DDL);
        }
    }

    /** A service whose ClickHouse step is replaced by a counter (and an optional failure switch). */
    private static final class Svc extends FakeAlgoService {
        final AtomicInteger computed = new AtomicInteger();
        volatile boolean fail;
        Svc(PipelinerEngine engine, long claimTimeoutMs) {
            super(registry, provider(engine), 0, claimTimeoutMs);
        }
        @Override long insert(String planVersion, String payload) throws Exception {
            if (fail) throw new IllegalStateException("clickhouse down");
            computed.incrementAndGet();
            return 3;
        }
    }

    @SuppressWarnings("unchecked")
    private static ObjectProvider<PipelinerEngine> provider(PipelinerEngine e) {
        ObjectProvider<PipelinerEngine> p = mock(ObjectProvider.class);
        when(p.getObject()).thenReturn(e);
        return p;
    }

    private static void submit(Svc s, String batch) {
        s.submit("round-1", "v1", batch, Collections.singletonList(Row.of("material_id", "M1", "group_id", "G1", "kind", "BIG")));
    }

    private static int count() throws Exception {
        try (Connection c = my.getConnection(); Statement st = c.createStatement(); ResultSet rs = st.executeQuery("select count(*) from ex_algo_request")) { rs.next(); return rs.getInt(1); }
    }

    private static int status(String batch) throws Exception {
        try (Connection c = my.getConnection(); PreparedStatement ps = c.prepareStatement("select status from ex_algo_request where batch_id=?")) {
            ps.setString(1, batch);
            try (ResultSet rs = ps.executeQuery()) { assertTrue(rs.next(), "row " + batch); return rs.getInt(1); }
        }
    }

    private static void exec(String sql, Object... args) throws Exception {
        try (Connection c = my.getConnection(); PreparedStatement ps = c.prepareStatement(sql)) {
            for (int i = 0; i < args.length; i++) ps.setObject(i + 1, args[i]);
            ps.executeUpdate();
        }
    }

    @Test
    void submitIsIdempotentPerBatchIdAndSurvivesAMissingTable() throws Exception {
        PipelinerEngine engine = mock(PipelinerEngine.class);
        Svc s = new Svc(engine, 10_000);
        submit(s, "r:p:S0@0");
        submit(s, "r:p:S0@0");
        assertEquals(1, count());
        exec("drop table ex_algo_request");
        submit(s, "r:p:S1@0");   // the table is recreated on demand
        assertEquals(1, count());
    }

    @Test
    void aRequestIsClaimedAndCompletedByExactlyOnePodAndSignalledOnce() throws Exception {
        PipelinerEngine engine = mock(PipelinerEngine.class);
        Svc a = new Svc(engine, 10_000), b = new Svc(engine, 10_000);
        submit(a, "r:p:S0@0");
        a.poll();
        b.poll();
        a.poll();
        assertEquals(1, a.computed.get() + b.computed.get(), "one pod did the work");
        verify(engine, times(1)).signalGate("round-1", "algo-callback", "r:p:S0@0", "rows=3");
        assertEquals(2, status("r:p:S0@0"), "DONE");
    }

    @Test
    void aFailedClaimIsReleasedAndRetriedByTheNextPoll() throws Exception {
        PipelinerEngine engine = mock(PipelinerEngine.class);
        Svc s = new Svc(engine, 60_000);   // long timeout: only the release can make it claimable again
        submit(s, "r:p:S0@0");
        s.fail = true;
        s.poll();
        assertEquals(0, status("r:p:S0@0"), "released back to PENDING");
        verify(engine, never()).signalGate(anyString(), anyString(), anyString(), anyString());
        s.fail = false;
        s.poll();
        assertEquals(2, status("r:p:S0@0"));
        verify(engine, times(1)).signalGate("round-1", "algo-callback", "r:p:S0@0", "rows=3");
    }

    @Test
    void anAbandonedClaimIsPickedUpOnlyAfterTheTimeout() throws Exception {
        PipelinerEngine engine = mock(PipelinerEngine.class);
        Svc s = new Svc(engine, 400);
        submit(s, "r:p:S0@0");
        // another pod claimed it just now and then died
        exec("update ex_algo_request set status=1, claimed_by='dead-pod', claimed_at=? where batch_id=?", System.currentTimeMillis(), "r:p:S0@0");
        s.poll();
        assertEquals(1, status("r:p:S0@0"), "a live claim is respected");
        assertEquals(0, s.computed.get());
        Thread.sleep(600);
        s.poll();
        assertEquals(2, status("r:p:S0@0"), "the abandoned claim was taken over");
        assertEquals(1, s.computed.get());
        verify(engine, times(1)).signalGate("round-1", "algo-callback", "r:p:S0@0", "rows=3");
    }
}
