package io.github.amwtke.pipeliner.spring;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import io.github.amwtke.pipeliner.api.DataSourceRegistry;
import io.github.amwtke.pipeliner.core.util.SimpleDataSourceRegistry;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.sql.DataSource;
import org.springframework.beans.factory.DisposableBean;

/** DataSourceRegistry holding Hikari pools from properties plus Spring DataSource beans; closes its own pools. */
public class PipelinerDataSources implements DataSourceRegistry, DisposableBean {
    private final List<HikariDataSource> owned = new ArrayList<>();
    private final SimpleDataSourceRegistry delegate;

    public PipelinerDataSources(Map<String, PipelinerProperties.Ds> configured, Map<String, DataSource> beans) {
        Map<String, DataSource> all = new LinkedHashMap<>(beans);
        configured.forEach((name, c) -> {
            HikariConfig hc = new HikariConfig();
            hc.setPoolName("pg-" + name);
            hc.setJdbcUrl(c.getUrl());
            hc.setUsername(c.getUsername());
            hc.setPassword(c.getPassword());
            if (c.getDriverClassName() != null && !c.getDriverClassName().isEmpty()) hc.setDriverClassName(c.getDriverClassName());
            hc.setMaximumPoolSize(c.getMaximumPoolSize());
            hc.setInitializationFailTimeout(-1);   // do not fail startup when a source is temporarily down
            HikariDataSource ds = new HikariDataSource(hc);
            owned.add(ds);
            all.put(name, ds);
        });
        this.delegate = new SimpleDataSourceRegistry(all);
    }

    @Override public DataSource get(String name) { return delegate.get(name); }

    @Override public Set<String> names() { return delegate.names(); }

    @Override public void destroy() {
        for (HikariDataSource ds : owned) ds.close();
    }
}
