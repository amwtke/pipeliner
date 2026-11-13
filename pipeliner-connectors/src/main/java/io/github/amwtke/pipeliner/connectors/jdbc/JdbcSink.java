package io.github.amwtke.pipeliner.connectors.jdbc;

import io.github.amwtke.pipeliner.api.Chunk;
import io.github.amwtke.pipeliner.api.Row;
import io.github.amwtke.pipeliner.api.Sink;
import io.github.amwtke.pipeliner.api.SinkContext;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/** JDBC sink: MySQL upsert or ClickHouse batch insert; idempotent per chunk. */
public class JdbcSink implements Sink {
    private final boolean clickhouse;
    private final String dsName, table, dataset;
    private final Set<String> keys = new HashSet<>();
    private final int batchSize;

    JdbcSink(JdbcSinkBuilder b) {
        this.clickhouse = "clickhouse".equals(b.dialect);
        this.dsName = b.dataSourceName;
        this.table = b.table;
        this.dataset = b.dataset;
        this.batchSize = b.batchSize;
        if (b.upsertKeys != null) for (String k : b.upsertKeys) keys.add(k.toLowerCase());
    }

    @Override public String datasetName() { return dataset != null ? dataset : table; }

    @Override public void write(Chunk chunk, SinkContext ctx) throws Exception {
        List<Row> rows = chunk.getRows();
        if (rows == null || rows.isEmpty()) return;
        Set<String> colSet = new LinkedHashSet<>();
        for (Row r : rows) colSet.addAll(r.columns());
        List<String> cols = new ArrayList<>(colSet);
        String sql = clickhouse ? chSql(cols) : mysqlSql(cols);
        try (Connection con = ctx.dataSources().get(dsName).getConnection()) {
            boolean oldAuto = con.getAutoCommit();
            if (!clickhouse) con.setAutoCommit(false);
            try (PreparedStatement ps = con.prepareStatement(sql)) {
                int pending = 0;
                for (Row r : rows) {
                    for (int i = 0; i < cols.size(); i++) bind(ps, i + 1, r.get(cols.get(i)));
                    ps.addBatch();
                    if (++pending >= batchSize) { ps.executeBatch(); pending = 0; }
                }
                if (pending > 0) ps.executeBatch();
                if (!clickhouse) con.commit();
            } catch (Exception e) {
                if (!clickhouse) { try { con.rollback(); } catch (Exception ignore) { /* best effort */ } }
                throw e;
            } finally {
                if (!clickhouse) { try { con.setAutoCommit(oldAuto); } catch (Exception ignore) { /* best effort */ } }
            }
        }
    }

    private static void bind(PreparedStatement ps, int idx, Object v) throws java.sql.SQLException {
        if (v == null) ps.setNull(idx, Types.NULL);
        else if (v instanceof Boolean || v instanceof Number || v instanceof String) ps.setObject(idx, v);
        else ps.setObject(idx, v.toString());
    }

    private String insertHead(List<String> cols) {
        StringBuilder names = new StringBuilder(), marks = new StringBuilder();
        for (int i = 0; i < cols.size(); i++) {
            if (i > 0) { names.append(", "); marks.append(", "); }
            names.append('`').append(cols.get(i)).append('`');
            marks.append('?');
        }
        return "INSERT INTO " + table + " (" + names + ") VALUES (" + marks + ")";
    }

    private String chSql(List<String> cols) { return insertHead(cols); }

    private String mysqlSql(List<String> cols) {
        List<String> upd = new ArrayList<>();
        for (String c : cols) if (!keys.contains(c.toLowerCase())) upd.add(c);
        if (upd.isEmpty()) upd = Arrays.asList(cols.get(0)); // no-op update keeps the statement valid
        StringBuilder sb = new StringBuilder(insertHead(cols)).append(" ON DUPLICATE KEY UPDATE ");
        for (int i = 0; i < upd.size(); i++) {
            if (i > 0) sb.append(", ");
            sb.append('`').append(upd.get(i)).append("`=VALUES(`").append(upd.get(i)).append("`)");
        }
        return sb.toString();
    }
}
