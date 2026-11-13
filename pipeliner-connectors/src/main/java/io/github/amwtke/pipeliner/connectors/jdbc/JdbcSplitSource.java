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
            for (Row r : NamedSql.query(ds(ctx), sql, ctx.params())) {
                Object v = r.get("v");
                Map<String, String> spec = new LinkedHashMap<>();
                spec.put("mode", "distinct");
                spec.put("col", splitColumn);
                if (v == null) spec.put("null", "true"); else spec.put("value", String.valueOf(v));
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
            pred = clickhouse ? "cityHash64(" + col + ") % " + n + " = " + i : "MOD(CRC32(" + col + "), " + n + ") = " + i;
        } else if ("distinct".equals(mode)) {
            String col = spec.get("col");
            if ("true".equals(spec.get("null"))) {
                pred = col + " IS NULL";
            } else {
                pred = col + " = :" + VALUE_PARAM;
                params.put(VALUE_PARAM, spec.get("value"));
            }
        } else if (!"all".equals(mode)) {
            throw new PipelinerException("unknown split mode: " + mode);
        }
        String sql = (query != null ? "SELECT * FROM " : "SELECT " + columns + " FROM ") + source() + whereClause(pred);
        if (orderBy != null) sql += " ORDER BY " + orderBy;
        return NamedSql.query(ds(ctx), sql, params);
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
