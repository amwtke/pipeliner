package io.github.amwtke.pipeliner.core.util;

import io.github.amwtke.pipeliner.api.DataSourceRegistry;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import javax.sql.DataSource;

/** Map-backed DataSourceRegistry. */
public final class SimpleDataSourceRegistry implements DataSourceRegistry {
    private final Map<String, DataSource> map;

    public SimpleDataSourceRegistry(Map<String, DataSource> map) {
        this.map = Collections.unmodifiableMap(new LinkedHashMap<>(map));
    }

    @Override public DataSource get(String name) {
        DataSource ds = map.get(name);
        if (ds == null) throw new IllegalArgumentException("unknown datasource: " + name + " (known: " + map.keySet() + ")");
        return ds;
    }

    @Override public Set<String> names() { return map.keySet(); }
}
