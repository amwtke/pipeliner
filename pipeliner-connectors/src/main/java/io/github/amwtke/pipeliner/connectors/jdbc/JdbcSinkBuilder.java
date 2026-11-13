package io.github.amwtke.pipeliner.connectors.jdbc;

/** Fluent builder for JdbcSink (fields stored; build() is a stub). */
public class JdbcSinkBuilder {
    final String dialect;
    final String dataSourceName;
    String table, dataset;
    String[] upsertKeys;
    int batchSize = 1000;

    JdbcSinkBuilder(String dialect, String dataSourceName) {
        this.dialect = dialect;
        this.dataSourceName = dataSourceName;
    }

    public JdbcSinkBuilder table(String t) { this.table = t; return this; }
    public JdbcSinkBuilder upsertKeys(String... k) { this.upsertKeys = k; return this; }
    public JdbcSinkBuilder dataset(String d) { this.dataset = d; return this; }
    public JdbcSinkBuilder batchSize(int n) { this.batchSize = n; return this; }

    public JdbcSink build() {
        if (dataSourceName == null || dataSourceName.trim().isEmpty()) throw new IllegalArgumentException("datasource name is required");
        if (table == null || table.trim().isEmpty()) throw new IllegalArgumentException("table is required");
        if (batchSize < 1) throw new IllegalArgumentException("batchSize must be >= 1, got " + batchSize);
        if (upsertKeys != null && "clickhouse".equals(dialect))
            throw new IllegalArgumentException("upsertKeys is not supported for ClickHouse (use a ReplacingMergeTree table)");
        return new JdbcSink(this);
    }
}
