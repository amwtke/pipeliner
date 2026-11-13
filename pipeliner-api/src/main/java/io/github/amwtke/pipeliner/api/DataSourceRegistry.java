package io.github.amwtke.pipeliner.api;

import java.util.Set;
import javax.sql.DataSource;

/** Named JDBC datasources. */
public interface DataSourceRegistry {
    /** @throws IllegalArgumentException if the name is unknown */
    DataSource get(String name);
    Set<String> names();
}
