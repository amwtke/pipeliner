package io.github.amwtke.pipeliner.connectors.jdbc;

/** Entry points for JDBC split sources. */
public final class JdbcSources {
    private JdbcSources() {}

    public static JdbcSourceBuilder mysql(String dataSourceName) { return new JdbcSourceBuilder("mysql", dataSourceName); }

    public static JdbcSourceBuilder clickhouse(String dataSourceName) { return new JdbcSourceBuilder("clickhouse", dataSourceName); }
}
