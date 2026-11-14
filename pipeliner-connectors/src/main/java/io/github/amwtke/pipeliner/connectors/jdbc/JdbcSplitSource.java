package io.github.amwtke.pipeliner.connectors.jdbc;

import io.github.amwtke.pipeliner.api.PipelinerException;
import io.github.amwtke.pipeliner.api.Row;
import io.github.amwtke.pipeliner.api.SourceContext;
import io.github.amwtke.pipeliner.api.Split;
import io.github.amwtke.pipeliner.api.SplitSource;
import io.github.amwtke.pipeliner.core.util.NamedSql;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Locale;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import javax.sql.DataSource;

/** JDBC split source for MySQL and ClickHouse (hash, distinct or no split). */
@lombok.extern.slf4j.Slf4j
public class JdbcSplitSource implements SplitSource {
    private static final String VALUE_PARAM = "__split_value";
    private static final java.util.Set<String> WARNED = java.util.concurrent.ConcurrentHashMap.newKeySet();

    private final boolean clickhouse;
    private final String dsName, table, dataset, where, query, orderBy, splitMode, splitColumn, columns;
    private final boolean useFinal;
    private final int buckets;

    JdbcSplitSource(JdbcSourceBuilder b) {
        this.clickhouse = "clickhouse".equals(b.dialect);
        this.dsName = b.dataSourceName;
        this.table = b.table;
        this.dataset = b.dataset;
        this.where = b.where;
        this.query = b.query;
        this.orderBy = b.orderBy;
        this.splitMode = b.splitMode;
        this.splitColumn = b.splitColumn;
        this.useFinal = b.useFinal;
        this.buckets = b.buckets;
        this.columns = b.columns == null || b.columns.length == 0 ? "*" : String.join(", ", Arrays.asList(b.columns));
    }

    @Override public String datasetName() { return dataset != null ? dataset : table; }

    @Override public List<Split> enumerate(SourceContext ctx) throws Exception {
        if (orderBy == null && !"none".equals(splitMode) && WARNED.add(String.valueOf(datasetName() != null ? datasetName() : query)))
            log.warn("JdbcSplitSource {}: {} split without orderBy(...); a split's rows must come back in a deterministic (total) order "
                    + "or chunks differ between executions of the same split", datasetName() != null ? datasetName() : query, splitMode);
        List<Split> out = new ArrayList<>();
        if ("hash".equals(splitMode)) {
            for (int i = 0; i < buckets; i++) {
                Map<String, String> spec = new LinkedHashMap<>();
                spec.put("mode", "hash");
                spec.put("col", splitColumn);
                spec.put("bucket", String.valueOf(i));
                spec.put("buckets", String.valueOf(buckets));
                out.add(new Split(null, spec));
            }
        } else if ("distinct".equals(splitMode)) {
            String sql = "SELECT DISTINCT " + splitColumn + " AS v FROM " + source() + whereClause(null)
                + " ORDER BY " + splitColumn;
            for (Object v : NamedSql.queryFirstColumnRaw(ds(ctx), sql, ctx.params())) {
                Map<String, String> spec = new LinkedHashMap<>();
                spec.put("mode", "distinct");
                spec.put("col", splitColumn);
                if (v == null) {
                    spec.put("null", "true");
                } else {
                    encodeValue(v, spec);
                }
                out.add(new Split(null, spec));
            }
        } else {
            Map<String, String> spec = new LinkedHashMap<>();
            spec.put("mode", "all");
            out.add(new Split(null, spec));
        }
        return out;
    }

    @Override public List<Row> read(Split split, SourceContext ctx) throws Exception {
        Map<String, String> spec = split.getSpec();
        String mode = spec.get("mode");
        Map<String, Object> params = new HashMap<>();
        if (ctx.params() != null) params.putAll(ctx.params());
        String pred = null;
        if ("hash".equals(mode)) {
            String col = spec.get("col");
            int n = Integer.parseInt(spec.get("buckets")), i = Integer.parseInt(spec.get("bucket"));
            String hashPred = clickhouse ? "cityHash64(" + col + ") % " + n + " = " + i : "MOD(CRC32(" + col + "), " + n + ") = " + i;
            // the hash of NULL is NULL and matches no bucket: bucket 0 also takes the NULL rows
            pred = i == 0 ? "(" + col + " IS NULL OR " + hashPred + ")" : hashPred;
        } else if ("distinct".equals(mode)) {
            String col = spec.get("col");
            if ("true".equals(spec.get("null"))) {
                pred = col + " IS NULL";
            } else {
                pred = col + " = :" + VALUE_PARAM;
                params.put(VALUE_PARAM, decodeValue(spec));
            }
        } else if (!"all".equals(mode)) {
            throw new PipelinerException("unknown split mode: " + mode);
        }
        String sql = (query != null ? "SELECT * FROM " : "SELECT " + columns + " FROM ") + source() + whereClause(pred);
        if (orderBy != null) sql += " ORDER BY " + orderBy;
        return NamedSql.query(ds(ctx), sql, params);
    }

    // ---- distinct split values: stored as text plus a type tag, rebound with the matching JDBC type

    static void encodeValue(Object v, Map<String, String> spec) {
        String type, val;
        if (v instanceof Boolean) { type = "bool"; val = v.toString(); }
        else if (v instanceof Long || v instanceof Integer || v instanceof Short || v instanceof Byte) { type = "long"; val = String.valueOf(((Number) v).longValue()); }
        else if (v instanceof BigInteger) { type = "decimal"; val = v.toString(); }
        else if (v instanceof BigDecimal) { type = "decimal"; val = ((BigDecimal) v).toPlainString(); }
        else if (v instanceof Float || v instanceof Double) {
            double d = ((Number) v).doubleValue();
            if (Double.isNaN(d) || Double.isInfinite(d))
                throw new PipelinerException("distinct split column " + splitColumnName(spec) + " has a NaN/Infinity value, which cannot be selected by equality; use hash split");
            type = "double"; val = Double.toString(d);   // a float widened to double compares equal to the column in MySQL and ClickHouse
        }
        else if (v instanceof Timestamp) { type = "ts"; val = ((Timestamp) v).toInstant().toString(); }   // UTC instant, JVM-zone independent
        else if (v instanceof java.time.LocalDateTime) { type = "ldt"; val = v.toString(); }
        else if (v instanceof java.sql.Date) { type = "date"; val = ((java.sql.Date) v).toLocalDate().toString(); }
        else if (v instanceof java.time.LocalDate) { type = "date"; val = v.toString(); }
        else if (v instanceof java.sql.Time) { type = "time"; val = v.toString(); }
        else if (v instanceof byte[]) { type = "bytes"; val = Base64.getEncoder().encodeToString((byte[]) v); }
        else if (v instanceof Number) {   // driver wrappers such as ClickHouse UnsignedLong
            BigDecimal d = new BigDecimal(v.toString());
            type = "decimal"; val = d.toPlainString();
        }
        else { type = "string"; val = String.valueOf(v); }
        spec.put("vtype", type);
        spec.put("value", val);
    }

    private static String splitColumnName(Map<String, String> spec) { return spec.get("col"); }

    static Object decodeValue(Map<String, String> spec) {
        String val = spec.get("value");
        String type = spec.get("vtype");
        if (type == null) return val;   // splits created before typed values
        switch (type) {
            case "bool": return Boolean.valueOf(val);
            case "long": return Long.valueOf(val);
            case "decimal": return new BigDecimal(val);
            case "double": return Double.valueOf(val);
            case "ts": return Timestamp.from(java.time.Instant.parse(val));
            case "ldt": return java.time.LocalDateTime.parse(val);
            case "date": return java.time.LocalDate.parse(val);   // zone-free binding
            case "time": return java.sql.Time.valueOf(val);
            case "bytes": return Base64.getDecoder().decode(val);
            case "string": return val;
            default: throw new PipelinerException("unknown distinct split value type: " + type);
        }
    }

    /** FROM-clause source: table (optionally FINAL) or aliased subquery. */
    private String source() {
        if (query != null) return "(" + query + ") t";
        return table + (useFinal ? " FINAL" : "");
    }

    private String whereClause(String pred) {
        List<String> parts = new ArrayList<>();
        if (where != null && !where.trim().isEmpty()) parts.add("(" + where + ")");
        if (pred != null) parts.add(pred);
        return parts.isEmpty() ? "" : " WHERE " + String.join(" AND ", parts);
    }

    private DataSource ds(SourceContext ctx) { return ctx.dataSources().get(dsName); }
}
