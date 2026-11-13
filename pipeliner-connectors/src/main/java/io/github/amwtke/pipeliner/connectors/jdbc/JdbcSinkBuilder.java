package io.github.amwtke.pipeliner.connectors.jdbc;

/** Fluent builder for JdbcSink (fields stored; build() is a stub). */
public class JdbcSinkBuilder {
    final String dialect;
    final String dataSourceName;
    String table, dataset;
    String[] upsertKeys;
    int batchSize = 1000;
    String versionColumn;

    JdbcSinkBuilder(String dialect, String dataSourceName) {
        this.dialect = dialect;
        this.dataSourceName = dataSourceName;
    }

    public JdbcSinkBuilder table(String t) { this.table = t; return this; }
    public JdbcSinkBuilder upsertKeys(String... k) { this.upsertKeys = k; return this; }
    public JdbcSinkBuilder dataset(String d) { this.dataset = d; return this; }
    public JdbcSinkBuilder batchSize(int n) { this.batchSize = n; return this; }
    /**
     * Versioned writes: the sink sets this (BIGINT / UInt64) column to {@code ctx.writeVersion()} on every row. MySQL: the upsert only
     * overwrites a row whose stored version is &lt;= the new one. ClickHouse: declare the table {@code ReplacingMergeTree(<col>)} and
     * read with FINAL, which keeps the highest version. A zombie of an older attempt/round can then never overwrite newer results.
     */
    public JdbcSinkBuilder versionColumn(String column) { this.versionColumn = column; return this; }

    public JdbcSink build() {
        if (dataSourceName == null || dataSourceName.trim().isEmpty()) throw new IllegalArgumentException("datasource name is required");
        if (table == null || table.trim().isEmpty()) throw new IllegalArgumentException("table is required");
        if (batchSize < 1) throw new IllegalArgumentException("batchSize must be >= 1, got " + batchSize);
        if (upsertKeys != null && "clickhouse".equals(dialect))
            throw new IllegalArgumentException("upsertKeys is not supported for ClickHouse (use a ReplacingMergeTree table)");
        if (versionColumn != null && versionColumn.trim().isEmpty()) throw new IllegalArgumentException("versionColumn must not be blank");
        return new JdbcSink(this);
    }
}
