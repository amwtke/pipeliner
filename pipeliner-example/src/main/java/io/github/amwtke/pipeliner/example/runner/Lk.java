package io.github.amwtke.pipeliner.example.runner;

import io.github.amwtke.pipeliner.api.Row;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;

/** Small helpers for grouping rows and building lookup maps. */
final class Lk {
    private Lk() {}

    static List<String> distinct(List<Row> rows, String col) {
        TreeSet<String> s = new TreeSet<>();
        for (Row r : rows) s.add(r.getString(col));
        return new ArrayList<>(s);
    }

    static String key(Object... parts) {
        StringBuilder sb = new StringBuilder();
        for (Object p : parts) sb.append(sb.length() == 0 ? "" : "|").append(p);
        return sb.toString();
    }

    /** key(cols...) to decimal value column. */
    static Map<String, BigDecimal> decimals(List<Row> rows, String valueCol, String... keyCols) {
        Map<String, BigDecimal> m = new HashMap<>();
        for (Row r : rows) {
            Object[] k = new Object[keyCols.length];
            for (int i = 0; i < k.length; i++) k[i] = r.getString(keyCols[i]);
            BigDecimal v = r.getDecimal(valueCol);
            m.merge(key(k), v == null ? BigDecimal.ZERO : v, BigDecimal::add);
        }
        return m;
    }

    /** Groups rows by (material_id, node) into month to decimal value maps, sorted by key. */
    static Map<String, Map<Integer, BigDecimal>> series(List<Row> rows, String valueCol) {
        Map<String, Map<Integer, BigDecimal>> m = new TreeMap<>();
        for (Row r : rows) {
            m.computeIfAbsent(key(r.getString("material_id"), r.getString("node")), k -> new TreeMap<>())
                    .put(r.getInt("month"), r.getDecimal(valueCol));
        }
        return m;
    }

    static BigDecimal get(Map<String, BigDecimal> m, String key) {
        BigDecimal v = m.get(key);
        return v == null ? BigDecimal.ZERO : v;
    }

    /** In-transit quantities as month to qty for one (material,node). */
    static Map<String, Map<Integer, BigDecimal>> transit(List<Row> rows) {
        Map<String, Map<Integer, BigDecimal>> m = new HashMap<>();
        for (Row r : rows) {
            m.computeIfAbsent(key(r.getString("material_id"), r.getString("node")), k -> new HashMap<>())
                    .merge(r.getInt("month"), r.getDecimal("qty"), BigDecimal::add);
        }
        return m;
    }
}
