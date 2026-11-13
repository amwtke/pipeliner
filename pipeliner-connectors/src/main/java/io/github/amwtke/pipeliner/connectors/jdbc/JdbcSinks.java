package io.github.amwtke.pipeliner.connectors.jdbc;

/** Entry points for JDBC sinks. */
public final class JdbcSinks {
    private JdbcSinks() {}

    public static JdbcSinkBuilder mysql(String dataSourceName) { return new JdbcSinkBuilder("mysql", dataSourceName); }

    public static JdbcSinkBuilder clickhouse(String dataSourceName) { return new JdbcSinkBuilder("clickhouse", dataSourceName); }
}
