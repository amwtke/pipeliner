package io.github.amwtke.pipeliner.example;

import io.github.amwtke.pipeliner.api.DataSourceRegistry;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/** Chaos-test verification: per-table row counts (raw and FINAL), order-independent hashes and row-level diffs against a base planVersion. */
@RestController
@RequestMapping("/demo")
public class VerifyController {
    private static final List<String> MYSQL = Arrays.asList("ex_nd1_big", "ex_nd_final", "ex_publish_log");
    private static final List<String> CH = Arrays.asList("ex_algo_gd", "ex_fine_gd", "ex_grade_result", "ex_refurb_plan", "ex_scrap");
    private final DataSourceRegistry ds;

    public VerifyController(DataSourceRegistry ds) {
        this.ds = ds;
    }

    /** Rows of one table for a plan version as signature strings (plan_version column dropped) to occurrence count. */
    private Map<String, Integer> rows(String db, String table, String pv, boolean fin) throws Exception {
        String sql = "select * from " + (db.equals("ch") ? Demo.DB + "." : "") + table + (fin ? " FINAL" : "") + " where plan_version = ?";
        Map<String, Integer> out = new HashMap<>();
        try (Connection c = ds.get(db).getConnection(); PreparedStatement ps = c.prepareStatement(sql)) {
            ps.setString(1, pv);
            try (ResultSet rs = ps.executeQuery()) {
                ResultSetMetaData md = rs.getMetaData();
                while (rs.next()) {
                    StringBuilder sb = new StringBuilder();
                    for (int i = 1; i <= md.getColumnCount(); i++) {
                        if (md.getColumnName(i).equals("plan_version") || md.getColumnName(i).equals("pg_version")) continue;   // pg_version differs per round by design
                        Object o = rs.getObject(i);
                        if (o instanceof java.math.BigDecimal) o = ((java.math.BigDecimal) o).setScale(4, java.math.RoundingMode.HALF_UP).toPlainString();
                        sb.append(o).append('|');
                    }
                    out.merge(sb.toString(), 1, Integer::sum);
                }
            }
        }
        return out;
    }

    private long scalar(String db, String sql, String pv) throws Exception {
        try (Connection c = ds.get(db).getConnection(); PreparedStatement ps = c.prepareStatement(sql)) {
            ps.setString(1, pv);
            try (ResultSet rs = ps.executeQuery()) { rs.next(); return rs.getLong(1); }
        }
    }

    @GetMapping("/verify")
    public Map<String, Object> verify(@RequestParam String planVersion, @RequestParam(required = false) String base) throws Exception {
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("planVersion", planVersion);
        Map<String, Object> tables = new LinkedHashMap<>();
        Map<String, Object> diffs = new LinkedHashMap<>();
        List<String> all = new ArrayList<>();
        for (String t : MYSQL) all.add("mysql:" + t);
        for (String t : CH) all.add("ch:" + t);
        for (String key : all) {
            String db = key.substring(0, key.indexOf(':')), t = key.substring(key.indexOf(':') + 1);
            boolean ch = db.equals("ch");
            Map<String, Object> m = new LinkedHashMap<>();
            Map<String, Integer> fin = rows(db, t, planVersion, ch);
            long finalRows = 0, hash = 0, dupRows = 0;
            for (Map.Entry<String, Integer> e : fin.entrySet()) {
                finalRows += e.getValue();
                if (e.getValue() > 1) dupRows += e.getValue() - 1;
                hash += Seeder.rowHash(e.getKey()) * e.getValue();
            }
            m.put("rows", finalRows);   // FINAL for ClickHouse
            m.put("hash", Long.toHexString(hash));
            m.put("identicalDuplicateRowsInFinal", dupRows);
            if (ch) m.put("rawRows", scalar("ch", "select count() from " + Demo.DB + "." + t + " where plan_version = ?", planVersion));
            if (t.equals("ex_nd_final") || t.equals("ex_nd1_big")) {
                m.put("duplicateKeys", scalar("mysql", "select count(*) from (select 1 from " + t
                        + " where plan_version = ? group by material_id, node, month having count(*) > 1) d", planVersion));
            }
            if (t.equals("ex_nd_final")) {
                m.put("negativeQty", scalar("mysql", "select count(*) from ex_nd_final where plan_version = ? and qty < 0", planVersion));
            }
            tables.put(key, m);
            if (base != null) {
                Map<String, Integer> b = rows(db, t, base, ch);
                if (!b.equals(fin)) {
                    TreeSet<String> onlyX = new TreeSet<>(), onlyB = new TreeSet<>();
                    for (Map.Entry<String, Integer> e : fin.entrySet()) if (!e.getValue().equals(b.get(e.getKey()))) onlyX.add(e.getKey());
                    for (Map.Entry<String, Integer> e : b.entrySet()) if (!e.getValue().equals(fin.get(e.getKey()))) onlyB.add(e.getKey());
                    Map<String, Object> d = new LinkedHashMap<>();
                    d.put("onlyInVersionCount", onlyX.size());
                    d.put("onlyInBaseCount", onlyB.size());
                    d.put("onlyInVersionSample", new ArrayList<>(onlyX).subList(0, Math.min(25, onlyX.size())));
                    d.put("onlyInBaseSample", new ArrayList<>(onlyB).subList(0, Math.min(25, onlyB.size())));
                    diffs.put(key, d);
                }
            }
        }
        out.put("tables", tables);
        if (base != null) out.put("diffsVsBase", diffs);
        return out;
    }

    /** Distinct pg_version values per result table for one plan version: {table: {"[kind/]version": rows}} (ClickHouse: FINAL and raw). */
    @GetMapping("/versions")
    public Map<String, Object> versions(@RequestParam String planVersion) throws Exception {
        Map<String, Object> out = new LinkedHashMap<>();
        for (String t : MYSQL) out.put("mysql:" + t, versionHist("mysql", t, planVersion, false, t.equals("ex_nd_final") ? "kind" : null));
        for (String t : CH) {
            out.put("ch:" + t + ":final", versionHist("ch", t, planVersion, true, null));
            out.put("ch:" + t + ":raw", versionHist("ch", t, planVersion, false, null));
        }
        return out;
    }

    private Map<String, Long> versionHist(String db, String table, String pv, boolean fin, String groupCol) throws Exception {
        String sql = "select " + (groupCol == null ? "" : groupCol + ", ") + "pg_version, count(*) from " + (db.equals("ch") ? Demo.DB + "." : "")
                + table + (fin ? " FINAL" : "") + " where plan_version = ? group by " + (groupCol == null ? "" : groupCol + ", ") + "pg_version";
        Map<String, Long> out = new java.util.TreeMap<>();
        try (Connection c = ds.get(db).getConnection(); PreparedStatement ps = c.prepareStatement(sql)) {
            ps.setString(1, pv);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    int n = groupCol == null ? 0 : 1;
                    out.put((groupCol == null ? "" : rs.getString(1) + "/") + rs.getLong(1 + n), rs.getLong(2 + n));
                }
            }
        }
        return out;
    }

    /** Deterministic input change: qty += delta in ex_inventory for the first nBig BIG and first nFine FINE materials (negative delta undoes it). */
    @org.springframework.web.bind.annotation.PostMapping("/bump")
    public Map<String, Object> bump(@RequestParam int nBig, @RequestParam int nFine, @RequestParam int delta) throws Exception {
        Map<String, Object> out = new LinkedHashMap<>();
        try (Connection c = ds.get("mysql").getConnection(); PreparedStatement ps = c.prepareStatement(
                "update ex_inventory i join (select material_id from ex_material where kind = ? order by material_id limit ?) m on i.material_id = m.material_id set i.qty = i.qty + ?")) {
            for (Object[] a : new Object[][]{{"BIG", nBig}, {"FINE", nFine}}) {
                ps.setString(1, (String) a[0]);
                ps.setInt(2, (Integer) a[1]);
                ps.setInt(3, delta);
                out.put((String) a[0], ps.executeUpdate());
            }
        }
        return out;
    }
}
