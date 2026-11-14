package io.github.amwtke.pipeliner.connectors.jdbc;

import static org.junit.jupiter.api.Assertions.*;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import io.github.amwtke.pipeliner.api.Chunk;
import io.github.amwtke.pipeliner.api.DataSourceRegistry;
import io.github.amwtke.pipeliner.api.Row;
import io.github.amwtke.pipeliner.api.SinkContext;
import io.github.amwtke.pipeliner.api.SourceContext;
import io.github.amwtke.pipeliner.api.Split;
import io.github.amwtke.pipeliner.core.util.SimpleDataSourceRegistry;
import java.math.BigDecimal;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.sql.Connection;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class JdbcConnectorsTest {
    static HikariDataSource my, ch;
    static DataSourceRegistry reg;
    static boolean myUp, chUp;

    static boolean reachable(int port) {
        try (Socket s = new Socket()) {
            s.connect(new InetSocketAddress("127.0.0.1", port), 500);
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    static HikariDataSource pool(String url) {
        HikariConfig c = new HikariConfig();
        c.setJdbcUrl(url);
        c.setUsername("pipeliner");
        c.setPassword("pipeliner");
        c.setMaximumPoolSize(4);
        c.setInitializationFailTimeout(-1);
        return new HikariDataSource(c);
    }

    @BeforeAll
    static void init() throws Exception {
        myUp = reachable(3306);
        chUp = reachable(8123);
        Map<String, javax.sql.DataSource> m = new HashMap<>();
        if (myUp) {
            my = pool("jdbc:mysql://127.0.0.1:3306/pg_conn_test?useSSL=false&allowPublicKeyRetrieval=true&rewriteBatchedStatements=true&serverTimezone=UTC&createDatabaseIfNotExist=true");
            m.put("my", my);
        }
        if (chUp) {
            try (Connection c = DriverManagerHolder.chAdmin(); Statement s = c.createStatement()) {
                s.execute("CREATE DATABASE IF NOT EXISTS pg_conn_test");
            }
            ch = pool("jdbc:clickhouse:http://127.0.0.1:8123/pg_conn_test?compress=0");
            m.put("ch", ch);
        }
        reg = new SimpleDataSourceRegistry(m);
    }

    /** Helper to create the ClickHouse database before the pool targets it. */
    static final class DriverManagerHolder {
        static Connection chAdmin() throws Exception {
            return java.sql.DriverManager.getConnection("jdbc:clickhouse:http://127.0.0.1:8123/default", "pipeliner", "pipeliner");
        }
    }

    @AfterAll
    static void close() {
        if (my != null) my.close();
        if (ch != null) ch.close();
    }

    static void exec(HikariDataSource ds, String... sqls) throws Exception {
        try (Connection c = ds.getConnection(); Statement s = c.createStatement()) {
            for (String q : sqls) s.execute(q);
        }
    }

    static SourceContext sctx(Map<String, String> params) {
        return new SourceContext() {
            public String roundId() { return "r1"; }
            public Map<String, String> params() { return params; }
            public DataSourceRegistry dataSources() { return reg; }
        };
    }

    static SinkContext kctx() {
        return new SinkContext() {
            public String roundId() { return "r1"; }
            public String runId() { return "run1"; }
            public String pipelineId() { return "p1"; }
            public Map<String, String> params() { return Collections.emptyMap(); }
            public DataSourceRegistry dataSources() { return reg; }
        };
    }

    static List<Row> readAll(JdbcSplitSource src, Map<String, String> params) throws Exception {
        SourceContext c = sctx(params);
        List<Row> all = new ArrayList<>();
        for (Split s : src.enumerate(c)) all.addAll(src.read(s, c));
        return all;
    }

    static Chunk chunk(String id, List<Row> rows) { return new Chunk(id, rows, new HashMap<String, String>()); }

    static void need(boolean up) { Assumptions.assumeTrue(up, "service not reachable"); }

    // ---- hash split ----

    private void hashCoverage(String db, HikariDataSource ds, boolean isCh) throws Exception {
        String t = "t_hash";
        exec(ds, "DROP TABLE IF EXISTS " + t);
        exec(ds, isCh ? "CREATE TABLE " + t + " (k String, v Int32) ENGINE=MergeTree ORDER BY k"
                      : "CREATE TABLE " + t + " (k VARCHAR(40) PRIMARY KEY, v INT)");
        List<Row> rows = new ArrayList<>();
        for (int i = 0; i < 10000; i++) rows.add(Row.of("k", "key-" + i, "v", (long) i));
        JdbcSink sink = (isCh ? JdbcSinks.clickhouse(db) : JdbcSinks.mysql(db)).table(t).batchSize(2500).build();
        sink.write(chunk("c1", rows), kctx());
        JdbcSplitSource src = (isCh ? JdbcSources.clickhouse(db) : JdbcSources.mysql(db)).table(t).hashSplit("k", 8).build();
        List<Split> splits = src.enumerate(sctx(new HashMap<String, String>()));
        assertEquals(8, splits.size());
        assertEquals("hash", splits.get(3).getSpec().get("mode"));
        assertEquals(splits.toString(), src.enumerate(sctx(new HashMap<String, String>())).toString());
        Set<String> seen = new HashSet<>();
        int total = 0, nonEmpty = 0;
        for (Split s : splits) {
            List<Row> part = src.read(s, sctx(new HashMap<String, String>()));
            if (!part.isEmpty()) nonEmpty++;
            total += part.size();
            for (Row r : part) assertTrue(seen.add(r.getString("k")), "duplicate " + r);
        }
        assertEquals(10000, total);
        assertEquals(10000, seen.size());
        assertEquals(8, nonEmpty);
        assertEquals("t_hash", src.datasetName());
    }

    @Test void hashSplitMysql() throws Exception { need(myUp); hashCoverage("my", my, false); }
    @Test void hashSplitClickhouse() throws Exception { need(chUp); hashCoverage("ch", ch, true); }

    // ---- distinct / nosplit / params / order ----

    private void basics(String db, HikariDataSource ds, boolean isCh) throws Exception {
        String t = "t_basic";
        exec(ds, "DROP TABLE IF EXISTS " + t);
        exec(ds, isCh ? "CREATE TABLE " + t + " (site String, pv String, qty Int64) ENGINE=MergeTree ORDER BY site"
                      : "CREATE TABLE " + t + " (id INT AUTO_INCREMENT PRIMARY KEY, site VARCHAR(10), pv VARCHAR(10), qty BIGINT)");
        List<Row> rows = new ArrayList<>();
        String[] sites = {"B", "A", "C"};
        for (int i = 0; i < 12; i++) rows.add(Row.of("site", sites[i % 3], "pv", i < 6 ? "v1" : "v2", "qty", (long) (100 - i)));
        (isCh ? JdbcSinks.clickhouse(db) : JdbcSinks.mysql(db)).table(t).build().write(chunk("c", rows), kctx());
        Map<String, String> p = new HashMap<>();
        p.put("pv", "v1");

        JdbcSplitSource dist = (isCh ? JdbcSources.clickhouse(db) : JdbcSources.mysql(db)).table(t)
            .columns("site", "pv", "qty").where("pv = :pv").distinctSplit("site").orderBy("qty").build();
        List<Split> sp = dist.enumerate(sctx(p));
        assertEquals(3, sp.size());
        assertEquals("A", sp.get(0).getSpec().get("value"));
        assertEquals("distinct", sp.get(0).getSpec().get("mode"));
        assertEquals("site", sp.get(0).getSpec().get("col"));
        List<Row> a = dist.read(sp.get(0), sctx(p));
        assertEquals(2, a.size());
        for (Row r : a) { assertEquals("A", r.get("site")); assertEquals("v1", r.get("pv")); }
        assertTrue(a.get(0).getLong("qty") < a.get(1).getLong("qty"));

        JdbcSplitSource all = (isCh ? JdbcSources.clickhouse(db) : JdbcSources.mysql(db)).table(t)
            .columns("site", "qty").where("pv = :pv").orderBy("qty DESC").build();
        List<Split> one = all.enumerate(sctx(p));
        assertEquals(1, one.size());
        assertEquals("all", one.get(0).getSpec().get("mode"));
        List<Row> rs = all.read(one.get(0), sctx(p));
        assertEquals(6, rs.size());
        assertEquals(Arrays.asList("site", "qty"), new ArrayList<>(rs.get(0).columns()));
        assertEquals(Long.valueOf(100), rs.get(0).getLong("qty"));
        assertTrue(rs.get(0).get("qty") instanceof Long);
        for (int i = 1; i < rs.size(); i++) assertTrue(rs.get(i - 1).getLong("qty") > rs.get(i).getLong("qty"));

        // query mode with hash split and a param
        JdbcSplitSource q = (isCh ? JdbcSources.clickhouse(db) : JdbcSources.mysql(db))
            .query("SELECT site, qty FROM " + t + " WHERE pv = :pv").hashSplit("site", 3).orderBy("qty").dataset("ds").build();
        assertEquals("ds", q.datasetName());
        assertEquals(6, readAll(q, p).size());
        JdbcSplitSource q2 = (isCh ? JdbcSources.clickhouse(db) : JdbcSources.mysql(db))
            .query("SELECT site, qty FROM " + t + " WHERE pv = :pv").distinctSplit("site").build();
        assertNull(q2.datasetName());
        assertEquals(3, q2.enumerate(sctx(p)).size());
        assertEquals(6, readAll(q2, p).size());
    }

    @Test void basicsMysql() throws Exception { need(myUp); basics("my", my, false); }
    @Test void basicsClickhouse() throws Exception { need(chUp); basics("ch", ch, true); }

    // ---- idempotency ----

    @Test void mysqlUpsertIdempotent() throws Exception {
        need(myUp);
        exec(my, "DROP TABLE IF EXISTS t_up", "CREATE TABLE t_up (pv VARCHAR(10), id INT, amt DECIMAL(18,4), nm VARCHAR(20), PRIMARY KEY(pv,id))");
        JdbcSink sink = JdbcSinks.mysql("my").table("t_up").upsertKeys("pv", "id").dataset("up").batchSize(7).build();
        assertEquals("up", sink.datasetName());
        List<Row> rows = new ArrayList<>();
        for (int i = 0; i < 20; i++) rows.add(Row.of("pv", "p", "id", (long) i, "amt", new BigDecimal("1.5000").add(BigDecimal.valueOf(i)), "nm", i % 2 == 0 ? null : "n" + i));
        sink.write(chunk("c", rows), kctx());
        sink.write(chunk("c", rows), kctx());
        sink.write(chunk("empty", new ArrayList<Row>()), kctx());
        JdbcSplitSource src = JdbcSources.mysql("my").table("t_up").orderBy("id").build();
        List<Row> back = readAll(src, new HashMap<String, String>());
        assertEquals(20, back.size());
        assertEquals(new BigDecimal("2.5000"), back.get(1).get("amt"));
        assertNull(back.get(0).get("nm"));
        // update path
        rows.set(0, Row.of("pv", "p", "id", 0L, "amt", new BigDecimal("9"), "nm", "z"));
        sink.write(chunk("c2", rows.subList(0, 1)), kctx());
        List<Row> b2 = readAll(src, new HashMap<String, String>());
        assertEquals(20, b2.size());
        assertEquals("z", b2.get(0).get("nm"));
        assertEquals(0, new BigDecimal("9").compareTo(b2.get(0).getDecimal("amt")));
        // no upsertKeys still works
        JdbcSinks.mysql("my").table("t_up").build().write(chunk("c3", rows.subList(0, 2)), kctx());
        assertEquals(20, readAll(src, new HashMap<String, String>()).size());
    }

    @Test void clickhouseFinalDedup() throws Exception {
        need(chUp);
        exec(ch, "DROP TABLE IF EXISTS t_rmt",
            "CREATE TABLE t_rmt (pv String, id Int64, qty Decimal(18,4), ver UInt32) ENGINE=ReplacingMergeTree(ver) ORDER BY (pv, id)");
        JdbcSink sink = JdbcSinks.clickhouse("ch").table("t_rmt").batchSize(4).build();
        assertEquals("t_rmt", sink.datasetName());
        List<Row> rows = new ArrayList<>();
        for (int i = 0; i < 10; i++) rows.add(Row.of("pv", "p", "id", (long) i, "qty", new BigDecimal("1.2500"), "ver", 1L));
        sink.write(chunk("c", rows), kctx());
        sink.write(chunk("c", rows), kctx()); // replay
        List<Row> upd = new ArrayList<>();
        upd.add(Row.of("pv", "p", "id", 0L, "qty", new BigDecimal("7.0000"), "ver", 2L));
        sink.write(chunk("c2", upd), kctx());
        Map<String, String> none = new HashMap<>();
        JdbcSplitSource fin = JdbcSources.clickhouse("ch").table("t_rmt").useFinal().orderBy("id").build();
        List<Row> back = readAll(fin, none);
        assertEquals(10, back.size());
        assertEquals(new BigDecimal("7.0000"), back.get(0).get("qty"));
        JdbcSplitSource fin2 = JdbcSources.clickhouse("ch").table("t_rmt").useFinal().where("id < :n").hashSplit("pv", 2).distinctSplit("pv").build();
        assertEquals(1, fin2.enumerate(sctx(Collections.singletonMap("n", "100"))).size());
        assertEquals(10, readAll(fin2, Collections.singletonMap("n", "100")).size());
        // empty chunk is a no-op
        sink.write(chunk("e", new ArrayList<Row>()), kctx());
    }

    // ---- versioned writes ----

    static SinkContext vctx(long version) {
        return new SinkContext() {
            public String roundId() { return "r1"; }
            public String runId() { return "run1"; }
            public String pipelineId() { return "p1"; }
            public Map<String, String> params() { return Collections.emptyMap(); }
            public DataSourceRegistry dataSources() { return reg; }
            @Override public int attempt() { return (int) (version % 1000); }
            @Override public long writeVersion() { return version; }
        };
    }

    @Test void mysqlVersionedUpsertKeepsTheHighestVersion() throws Exception {
        need(myUp);
        exec(my, "DROP TABLE IF EXISTS t_ver", "CREATE TABLE t_ver (pv VARCHAR(10), id INT, amt DECIMAL(18,4), nm VARCHAR(20), pg_version BIGINT NOT NULL DEFAULT 0, PRIMARY KEY(pv,id))");
        JdbcSink sink = JdbcSinks.mysql("my").table("t_ver").upsertKeys("pv", "id").versionColumn("pg_version").build();
        List<Row> v2 = Arrays.asList(Row.of("pv", "p", "id", 1L, "amt", new BigDecimal("22"), "nm", "new"), Row.of("pv", "p", "id", 2L, "amt", new BigDecimal("22"), "nm", null));
        List<Row> v1 = Arrays.asList(Row.of("pv", "p", "id", 1L, "amt", new BigDecimal("11"), "nm", "old"), Row.of("pv", "p", "id", 2L, "amt", new BigDecimal("11"), "nm", "old"));
        sink.write(chunk("c", v2), vctx(7002));   // the newer attempt/round first ...
        sink.write(chunk("c", v1), vctx(7001));   // ... then a zombie of an older one
        JdbcSplitSource src = JdbcSources.mysql("my").table("t_ver").orderBy("id").build();
        List<Row> back = readAll(src, new HashMap<String, String>());
        assertEquals(2, back.size());
        assertEquals(0, new BigDecimal("22").compareTo(back.get(0).getDecimal("amt")));
        assertEquals("new", back.get(0).get("nm"));
        assertNull(back.get(1).get("nm"), "null of the newer version survives too");
        assertEquals(7002L, back.get(0).getLong("pg_version"));
        sink.write(chunk("c", v2), vctx(7002));   // same version: idempotent replay
        sink.write(chunk("c", v1), vctx(7003));   // newer: wins
        back = readAll(src, new HashMap<String, String>());
        assertEquals("old", back.get(0).get("nm"));
        assertEquals(7003L, back.get(0).getLong("pg_version"));
        // the sink sets the version column itself, even when the rows carry one
        sink.write(chunk("c", Arrays.asList(Row.of("pv", "p", "id", 3L, "amt", BigDecimal.ONE, "nm", "x", "pg_version", 5L))), vctx(8000));
        assertEquals(8000L, readAll(JdbcSources.mysql("my").table("t_ver").where("id = 3").build(), new HashMap<String, String>()).get(0).getLong("pg_version"));
    }

    @Test void clickhouseReplacingMergeTreeVersionKeepsTheHighestVersionUnderFinal() throws Exception {
        need(chUp);
        exec(ch, "DROP TABLE IF EXISTS t_rver",
            "CREATE TABLE t_rver (pv String, id Int64, qty Decimal(18,4), pg_version UInt64) ENGINE=ReplacingMergeTree(pg_version) ORDER BY (pv, id)");
        JdbcSink sink = JdbcSinks.clickhouse("ch").table("t_rver").versionColumn("pg_version").build();
        List<Row> v2 = Arrays.asList(Row.of("pv", "p", "id", 1L, "qty", new BigDecimal("22")));
        List<Row> v1 = Arrays.asList(Row.of("pv", "p", "id", 1L, "qty", new BigDecimal("11")));
        sink.write(chunk("c", v2), vctx(7002));
        sink.write(chunk("c", v1), vctx(7001));   // zombie writes later, with the lower version
        List<Row> back = readAll(JdbcSources.clickhouse("ch").table("t_rver").useFinal().build(), new HashMap<String, String>());
        assertEquals(1, back.size());
        assertEquals(0, new BigDecimal("22").compareTo(back.get(0).getDecimal("qty")));
        assertEquals(7002L, back.get(0).getLong("pg_version"));
    }

    // ---- type consistency ----

    @Test void typeMappingConsistent() throws Exception {
        need(myUp && chUp);
        exec(my, "DROP TABLE IF EXISTS t_types", "CREATE TABLE t_types (id INT PRIMARY KEY, amt DECIMAL(18,4), flag TINYINT UNSIGNED, nm VARCHAR(20), n2 BIGINT)");
        exec(ch, "DROP TABLE IF EXISTS t_types", "CREATE TABLE t_types (id Int32, amt Decimal(18,4), flag UInt8, nm String, n2 Int64) ENGINE=MergeTree ORDER BY id");
        List<Row> rows = Arrays.asList(
            Row.of("id", 1L, "amt", new BigDecimal("123.4500"), "flag", 1L, "nm", "hello", "n2", 9000000000L),
            Row.of("id", 2L, "amt", new BigDecimal("-0.0001"), "flag", 0L, "nm", "", "n2", -5L));
        JdbcSinks.mysql("my").table("t_types").build().write(chunk("c", rows), kctx());
        JdbcSinks.clickhouse("ch").table("t_types").build().write(chunk("c", rows), kctx());
        List<Row> a = readAll(JdbcSources.mysql("my").table("t_types").orderBy("id").build(), new HashMap<String, String>());
        List<Row> b = readAll(JdbcSources.clickhouse("ch").table("t_types").orderBy("id").build(), new HashMap<String, String>());
        assertEquals(2, a.size());
        for (String c : a.get(0).columns()) assertEquals(a.get(0).get(c).getClass(), b.get(0).get(c).getClass(), c);
        assertEquals(a, b);
        assertEquals(rows, a);
    }

    // ---- builder validation ----

    @Test void builderValidation() {
        assertThrows(IllegalArgumentException.class, () -> JdbcSources.mysql("x").build());
        assertThrows(IllegalArgumentException.class, () -> JdbcSources.mysql("x").table("t").query("select 1").build());
        assertThrows(IllegalArgumentException.class, () -> JdbcSources.mysql("x").table("t").useFinal().build());
        assertThrows(IllegalArgumentException.class, () -> JdbcSources.clickhouse("x").query("select 1").useFinal().build());
        assertThrows(IllegalArgumentException.class, () -> JdbcSources.clickhouse("x").query("select 1").where("a=1").build());
        assertThrows(IllegalArgumentException.class, () -> JdbcSources.mysql("x").table("t").hashSplit("k", 0).build());
        assertThrows(IllegalArgumentException.class, () -> JdbcSources.mysql("x").table("t").distinctSplit(" ").build());
        assertThrows(IllegalArgumentException.class, () -> JdbcSinks.mysql("x").build());
        assertThrows(IllegalArgumentException.class, () -> JdbcSinks.mysql("x").table("t").versionColumn(" ").build());
        assertThrows(IllegalArgumentException.class, () -> JdbcSinks.mysql("x").table("t").batchSize(0).build());
        assertThrows(IllegalArgumentException.class, () -> JdbcSinks.clickhouse("x").table("t").upsertKeys("a").build());
        assertEquals("t", JdbcSources.mysql("x").table("t").build().datasetName());
        assertEquals("d", JdbcSinks.mysql("x").table("t").dataset("d").build().datasetName());
    }

    // ---- review round 2: NULL hash bucket, typed distinct values ----

    private void hashNullRows(HikariDataSource ds, boolean isCh) throws Exception {
        String t = "t_hash_null";
        exec(ds, "DROP TABLE IF EXISTS " + t);
        exec(ds, isCh ? "CREATE TABLE " + t + " (id Int32, k Nullable(String)) ENGINE=MergeTree ORDER BY id"
                      : "CREATE TABLE " + t + " (id INT PRIMARY KEY, k VARCHAR(40) NULL)");
        for (int i = 0; i < 200; i++) exec(ds, "INSERT INTO " + t + " VALUES (" + i + ", " + (i % 10 == 0 ? "NULL" : "'key-" + i + "'") + ")");
        JdbcSplitSource src = (isCh ? JdbcSources.clickhouse(isCh ? "ch" : "my") : JdbcSources.mysql("my")).table(t).hashSplit("k", 4).orderBy("id").build();
        List<Row> all = readAll(src, new HashMap<String, String>());
        assertEquals(200, all.size());   // NULL rows used to fall in no bucket
        Set<Long> ids = new HashSet<>();
        for (Row r : all) assertTrue(ids.add(r.getLong("id")));
        List<Row> first = src.read(src.enumerate(sctx(new HashMap<String, String>())).get(0), sctx(new HashMap<String, String>()));
        int nulls = 0;
        for (Row r : first) if (r.get("k") == null) nulls++;
        assertEquals(20, nulls);   // all NULL rows are in bucket 0
    }

    @Test void hashSplitCoversNullMysql() throws Exception { need(myUp); hashNullRows(my, false); }
    @Test void hashSplitCoversNullClickhouse() throws Exception { need(chUp); hashNullRows(ch, true); }

    @Test void distinctSplitRoundTripsTypedValuesMysql() throws Exception {
        need(myUp);
        String t = "t_distinct_typed";
        exec(my, "DROP TABLE IF EXISTS " + t);
        exec(my, "CREATE TABLE " + t + " (id INT AUTO_INCREMENT PRIMARY KEY, f FLOAT, d DOUBLE, flag TINYINT(1), b BOOLEAN, ts TIMESTAMP NULL, dt DATETIME, dec_ DECIMAL(10,2), dd DATE, s VARCHAR(10))");
        exec(my, "SET time_zone = '+00:00'");
        exec(my, "INSERT INTO " + t + " (f, d, flag, b, ts, dt, dec_, dd, s) VALUES "
                + "(0.1, 0.1, 1, 1, '2024-03-10 01:02:03', '2024-03-10 01:02:03', 1.10, '2024-03-10', 'a'),"
                + "(0.1, 0.1, 1, 1, '2024-03-10 01:02:03', '2024-03-10 01:02:03', 1.10, '2024-03-10', 'a'),"
                + "(0.2, 0.2, 0, 0, '2024-03-11 04:00:00', '2024-03-11 04:00:00', 2.20, '2024-03-11', 'b')");
        java.util.TimeZone old = java.util.TimeZone.getDefault();
        try {
            java.util.TimeZone.setDefault(java.util.TimeZone.getTimeZone("Asia/Tokyo"));   // JVM zone differs from the session's UTC
            for (String col : Arrays.asList("f", "d", "flag", "b", "ts", "dt", "dec_", "dd", "s")) {
                JdbcSplitSource src = JdbcSources.mysql("my").table(t).distinctSplit(col).orderBy("id").build();
                List<Split> sp = src.enumerate(sctx(new HashMap<String, String>()));
                assertEquals(2, sp.size(), col);
                int total = 0;
                for (Split s : sp) {
                    List<Row> part = src.read(s, sctx(new HashMap<String, String>()));
                    assertFalse(part.isEmpty(), col + " split read nothing: " + s);
                    total += part.size();
                }
                assertEquals(3, total, col);   // every row exactly once
            }
        } finally {
            java.util.TimeZone.setDefault(old);
        }
    }

    @Test void distinctValueEncodingIsTyped() {
        Map<String, String> m = new java.util.LinkedHashMap<>();
        JdbcSplitSource.encodeValue(0.1f, m);
        assertEquals("double", m.get("vtype"));
        assertEquals(Double.valueOf((double) 0.1f), JdbcSplitSource.decodeValue(m));
        m.clear();
        JdbcSplitSource.encodeValue(Boolean.TRUE, m);
        assertEquals(Boolean.TRUE, JdbcSplitSource.decodeValue(m));
        m.clear();
        java.sql.Timestamp ts = java.sql.Timestamp.valueOf("2024-03-10 01:02:03.5");
        JdbcSplitSource.encodeValue(ts, m);
        assertEquals(ts, JdbcSplitSource.decodeValue(m));
        m.clear();
        JdbcSplitSource.encodeValue(new BigDecimal("1.10"), m);
        assertEquals(new BigDecimal("1.10"), JdbcSplitSource.decodeValue(m));
        m.clear();
        JdbcSplitSource.encodeValue(42L, m);
        assertEquals(42L, JdbcSplitSource.decodeValue(m));
        m.clear();
        JdbcSplitSource.encodeValue("x", m);
        assertEquals("x", JdbcSplitSource.decodeValue(m));
        assertThrows(io.github.amwtke.pipeliner.api.PipelinerException.class, () -> JdbcSplitSource.encodeValue(Double.NaN, new java.util.LinkedHashMap<String, String>()));
    }
}
