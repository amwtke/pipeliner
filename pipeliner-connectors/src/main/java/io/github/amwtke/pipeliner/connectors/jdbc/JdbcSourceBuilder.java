package io.github.amwtke.pipeliner.connectors.jdbc;

/** Fluent builder for JdbcSplitSource (fields stored; build() is a stub). */
public class JdbcSourceBuilder {
    final String dialect;
    final String dataSourceName;
    String table, dataset, where, query, orderBy, splitColumn, splitMode = "none";
    String[] columns;
    boolean useFinal;
    int buckets;

    JdbcSourceBuilder(String dialect, String dataSourceName) {
        this.dialect = dialect;
        this.dataSourceName = dataSourceName;
    }

    public JdbcSourceBuilder table(String t) { this.table = t; return this; }
    public JdbcSourceBuilder dataset(String d) { this.dataset = d; return this; }
    public JdbcSourceBuilder columns(String... c) { this.columns = c; return this; }
    public JdbcSourceBuilder where(String w) { this.where = w; return this; }
    public JdbcSourceBuilder query(String q) { this.query = q; return this; }
    public JdbcSourceBuilder useFinal() { this.useFinal = true; return this; }
    public JdbcSourceBuilder hashSplit(String column, int buckets) { this.splitMode = "hash"; this.splitColumn = column; this.buckets = buckets; return this; }
    public JdbcSourceBuilder distinctSplit(String column) { this.splitMode = "distinct"; this.splitColumn = column; return this; }
    public JdbcSourceBuilder noSplit() { this.splitMode = "none"; return this; }
    public JdbcSourceBuilder orderBy(String o) { this.orderBy = o; return this; }

    public JdbcSplitSource build() {
        boolean ch = "clickhouse".equals(dialect);
        if (dataSourceName == null || dataSourceName.trim().isEmpty()) throw new IllegalArgumentException("datasource name is required");
        if ((table == null) == (query == null)) throw new IllegalArgumentException("exactly one of table(...) or query(...) must be set");
        if (table != null && table.trim().isEmpty()) throw new IllegalArgumentException("table must not be blank");
        if (query != null && query.trim().isEmpty()) throw new IllegalArgumentException("query must not be blank");
        if (query != null && (columns != null || where != null || useFinal))
            throw new IllegalArgumentException("columns/where/useFinal cannot be combined with query(...); put them in the query");
        if (useFinal && !ch) throw new IllegalArgumentException("useFinal() is only supported for ClickHouse");
        if ("hash".equals(splitMode)) {
            if (splitColumn == null || splitColumn.trim().isEmpty()) throw new IllegalArgumentException("hashSplit requires a column");
            if (buckets < 1) throw new IllegalArgumentException("hashSplit buckets must be >= 1, got " + buckets);
        }
        if ("distinct".equals(splitMode) && (splitColumn == null || splitColumn.trim().isEmpty()))
            throw new IllegalArgumentException("distinctSplit requires a column");
        return new JdbcSplitSource(this);
    }
}
