package io.github.amwtke.pipeliner.example;

import io.github.amwtke.pipeliner.api.DataSourceRegistry;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.function.Consumer;
import org.springframework.stereotype.Component;

/** Recreates demo tables in MySQL and ClickHouse and fills the master data deterministically from a seed. */
@Component
public class Seeder {
    private static final List<String> MYSQL_TABLES = Arrays.asList("ex_material", "ex_inventory", "ex_in_transit", "ex_safety_stock",
            "ex_bom", "ex_site_capacity", "ex_nd1_big", "ex_nd_final", "ex_publish_log", "ex_fault");
    private static final String[] MYSQL_DDL = {
        "create table ex_material(material_id varchar(16) primary key, group_id varchar(16) not null, kind varchar(8) not null, eol_month int not null)",
        "create table ex_inventory(material_id varchar(16), node varchar(8), qty decimal(18,4) not null, primary key(material_id,node))",
        "create table ex_in_transit(material_id varchar(16), node varchar(8), month int, qty decimal(18,4) not null, primary key(material_id,node,month))",
        "create table ex_safety_stock(material_id varchar(16), node varchar(8), qty decimal(18,4) not null, primary key(material_id,node))",
        "create table ex_bom(big_id varchar(16), fine_id varchar(16), usage_qty decimal(18,4) not null, recovery_rate decimal(18,4) not null, primary key(big_id,fine_id))",
        "create table ex_site_capacity(site varchar(16), month int, capacity decimal(18,4) not null, primary key(site,month))",
        "create table ex_nd1_big(plan_version varchar(64), material_id varchar(16), node varchar(8), month int, gd decimal(18,4) not null, nd1 decimal(18,4) not null, pg_version bigint not null default 0, primary key(plan_version,material_id,node,month))",
        "create table ex_nd_final(plan_version varchar(64), material_id varchar(16), node varchar(8), month int, kind varchar(8) not null, qty decimal(18,4) not null, pg_version bigint not null default 0, primary key(plan_version,material_id,node,month))",
        "create table ex_publish_log(plan_version varchar(64), kind varchar(8), materials bigint not null, total_qty decimal(18,4) not null, pg_version bigint not null default 0, primary key(plan_version,kind))",
        "create table ex_fault(pipeline varchar(32) primary key, modv int not null)"
    };
    private static final String RMT = " ENGINE = ReplacingMergeTree(pg_version) ORDER BY (plan_version, material_id, node, month)";
    private static final Map<String, String> CH_TABLES = new LinkedHashMap<>();
    static {
        CH_TABLES.put("ex_algo_gd", "(plan_version String, material_id String, group_id String, kind String, node String, month Int32, qty Decimal(18,4), pg_version UInt64)");
        CH_TABLES.put("ex_fine_gd", "(plan_version String, material_id String, node String, month Int32, qty Decimal(18,4), pg_version UInt64)");
        CH_TABLES.put("ex_grade_result", "(plan_version String, material_id String, site String, node String, month Int32, gd Decimal(18,4), nd1 Decimal(18,4), core Decimal(18,4), refurbishable Decimal(18,4), scrap Decimal(18,4), pg_version UInt64)");
        CH_TABLES.put("ex_refurb_plan", "(plan_version String, material_id String, site String, node String, month Int32, refurb_in Decimal(18,4), out_month Int32, refurb_out Decimal(18,4), pg_version UInt64)");
        CH_TABLES.put("ex_scrap", "(plan_version String, material_id String, node String, month Int32, scrap Decimal(18,4), pg_version UInt64)");
    }

    private final DataSourceRegistry dataSources;

    public Seeder(DataSourceRegistry dataSources) {
        this.dataSources = dataSources;
    }

    /** Drops and recreates all demo tables, then inserts master data; returns row counts. */
    public Map<String, Object> seed(int materials, long seed) throws Exception {
        try (Connection c = dataSources.get("ch").getConnection(); Statement st = c.createStatement()) {
            st.execute("CREATE DATABASE IF NOT EXISTS " + Demo.DB);
            for (Map.Entry<String, String> e : CH_TABLES.entrySet()) {
                st.execute("DROP TABLE IF EXISTS " + Demo.DB + "." + e.getKey());
                st.execute("CREATE TABLE " + Demo.DB + "." + e.getKey() + " " + e.getValue() + RMT);
            }
        }
        Map<String, Object> counts = new LinkedHashMap<>();
        try (Connection c = dataSources.get("mysql").getConnection()) {
            try (Statement st = c.createStatement()) {
                for (String t : MYSQL_TABLES) st.execute("drop table if exists " + t);
                for (String ddl : MYSQL_DDL) st.execute(ddl);
            }
            generate(c, materials, new Random(seed), counts);
        }
        return counts;
    }

    private void generate(Connection c, int n, Random rnd, Map<String, Object> counts) throws Exception {
        int nBig = (int) (n * 0.4);
        int nFine = n - nBig;
        List<Object[]> mat = new ArrayList<>();
        List<String> ids = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            String id = String.format("M%05d", i + 1);
            ids.add(id);
            int eol = rnd.nextInt(10) == 0 ? 8 + rnd.nextInt(8) : 99;
            mat.add(new Object[]{id, "G" + (i / 3), i < nBig ? "BIG" : "FINE", eol});
        }
        batch(c, "insert into ex_material values (?,?,?,?)", mat);
        counts.put("ex_material", mat.size());

        List<Object[]> bom = new ArrayList<>();
        if (nFine >= 2) {
            for (int i = 0; i < nBig; i++) {
                for (int j = 0; j < 2; j++) {
                    String fine = ids.get(nBig + (i * 2 + j) % nFine);
                    bom.add(new Object[]{ids.get(i), fine, BigDecimal.valueOf(1 + rnd.nextInt(3)), BigDecimal.valueOf(5 + rnd.nextInt(5), 1)});
                }
            }
        }
        batch(c, "insert into ex_bom values (?,?,?,?)", bom);
        counts.put("ex_bom", bom.size());

        List<Object[]> inv = new ArrayList<>(), ss = new ArrayList<>(), tr = new ArrayList<>();
        for (String id : ids) {
            for (String node : Demo.NODES) {
                inv.add(new Object[]{id, node, BigDecimal.valueOf(rnd.nextInt(51))});
                ss.add(new Object[]{id, node, BigDecimal.valueOf(rnd.nextInt(11))});
                if (rnd.nextInt(5) == 0) tr.add(new Object[]{id, node, 1 + rnd.nextInt(4), BigDecimal.valueOf(1 + rnd.nextInt(20))});
            }
        }
        batch(c, "insert into ex_inventory values (?,?,?)", inv);
        batch(c, "insert into ex_safety_stock values (?,?,?)", ss);
        batch(c, "insert into ex_in_transit values (?,?,?,?)", tr);
        counts.put("ex_inventory", inv.size());
        counts.put("ex_safety_stock", ss.size());
        counts.put("ex_in_transit", tr.size());

        List<Object[]> cap = new ArrayList<>();
        for (String site : Demo.SITES) {
            for (int m = 1; m <= Demo.MONTHS; m++) cap.add(new Object[]{site, m, BigDecimal.valueOf(50 + rnd.nextInt(351))});
        }
        batch(c, "insert into ex_site_capacity values (?,?,?)", cap);
        counts.put("ex_site_capacity", cap.size());
    }

    private static void batch(Connection c, String sql, List<Object[]> rows) throws Exception {
        if (rows.isEmpty()) return;
        try (PreparedStatement ps = c.prepareStatement(sql)) {
            for (Object[] r : rows) {
                for (int i = 0; i < r.length; i++) ps.setObject(i + 1, r[i]);
                ps.addBatch();
            }
            ps.executeBatch();
        }
    }

    /** Order-independent per-kind fingerprint of ex_nd_final for one plan version. */
    public Map<String, Object> checksum(String planVersion) throws Exception {
        Map<String, long[]> acc = new LinkedHashMap<>();
        Map<String, BigDecimal> sums = new LinkedHashMap<>();
        for (String k : Arrays.asList("BIG", "FINE")) { acc.put(k, new long[2]); sums.put(k, Demo.zero()); }
        String sql = "select material_id, node, month, kind, qty from ex_nd_final where plan_version = ?";
        try (Connection c = dataSources.get("mysql").getConnection(); PreparedStatement ps = c.prepareStatement(sql)) {
            ps.setString(1, planVersion);
            try (java.sql.ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    String kind = rs.getString("kind");
                    BigDecimal qty = rs.getBigDecimal("qty").setScale(4, java.math.RoundingMode.HALF_UP);
                    long[] a = acc.computeIfAbsent(kind, x -> new long[2]);
                    a[0]++;
                    a[1] += rowHash(rs.getString("material_id") + "|" + rs.getString("node") + "|" + rs.getInt("month") + "|" + qty.toPlainString());
                    sums.merge(kind, qty, BigDecimal::add);
                }
            }
        }
        Map<String, Object> kinds = new LinkedHashMap<>();
        for (Map.Entry<String, long[]> e : acc.entrySet()) {
            Map<String, Object> k = new LinkedHashMap<>();
            k.put("rows", e.getValue()[0]);
            k.put("sumQty", sums.get(e.getKey()).toPlainString());
            k.put("hash", Long.toHexString(e.getValue()[1]));
            kinds.put(e.getKey(), k);
        }
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("planVersion", planVersion);
        out.put("kinds", kinds);
        return out;
    }

    /** First 8 bytes of MD5 as a long; summed (wrapping) over rows for order independence. */
    static long rowHash(String s) {
        try {
            byte[] d = java.security.MessageDigest.getInstance("MD5").digest(s.getBytes(java.nio.charset.StandardCharsets.UTF_8));
            long h = 0;
            for (int i = 0; i < 8; i++) h = (h << 8) | (d[i] & 0xff);
            return h;
        } catch (java.security.NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }
}
