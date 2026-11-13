package io.github.amwtke.pipeliner.spring;

import io.github.amwtke.pipeliner.api.ComponentFactory;
import io.github.amwtke.pipeliner.api.DataSourceRegistry;
import io.github.amwtke.pipeliner.core.dataplane.DataPlane;
import io.github.amwtke.pipeliner.core.engine.PipelinerEngine;
import io.github.amwtke.pipeliner.core.engine.PipelinerMetrics;
import io.github.amwtke.pipeliner.core.engine.RuntimeConfig;
import io.github.amwtke.pipeliner.core.graph.PipelineGraph;
import io.github.amwtke.pipeliner.core.store.ControlPlaneStore;
import io.github.amwtke.pipeliner.dataplane.redis.RedisDataPlane;
import io.github.amwtke.pipeliner.runtime.DefaultPipelinerEngine;
import io.github.amwtke.pipeliner.store.mysql.MySqlControlPlaneStore;
import io.github.amwtke.pipeliner.store.redis.RedisControlPlaneStore;
import io.lettuce.core.RedisClient;
import io.micrometer.core.instrument.MeterRegistry;
import java.util.Map;
import javax.sql.DataSource;
import org.springframework.beans.factory.ListableBeanFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.config.AutowireCapableBeanFactory;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;

/** Wires Redis, data sources, stores, data plane, metrics and the engine. */
@AutoConfiguration
@EnableConfigurationProperties(PipelinerProperties.class)
@Import(PipelinerAdminConfiguration.class)
public class PipelinerAutoConfiguration {

    @Bean(destroyMethod = "shutdown")
    @ConditionalOnMissingBean
    public RedisClient pipelinerRedisClient(PipelinerProperties props) {
        return RedisClient.create(props.getRedis().getUri());
    }

    @Bean
    @ConditionalOnMissingBean(DataSourceRegistry.class)
    public DataSourceRegistry pipelinerDataSourceRegistry(PipelinerProperties props, ListableBeanFactory bf) {
        Map<String, DataSource> beans = bf.getBeansOfType(DataSource.class);
        return new PipelinerDataSources(props.getDatasources(), beans);
    }

    @Bean
    @ConditionalOnMissingBean(ControlPlaneStore.class)
    public ControlPlaneStore pipelinerControlPlaneStore(PipelinerProperties props, DataSourceRegistry registry,
                                                        ObjectProvider<RedisClient> redis) {
        String type = props.getStore().getType();
        ControlPlaneStore store;
        if ("redis".equalsIgnoreCase(type)) {
            store = new RedisControlPlaneStore(redis.getObject());
        } else if ("mysql".equalsIgnoreCase(type)) {
            store = new MySqlControlPlaneStore(registry.get(props.getStore().getDatasource()));
        } else {
            throw new IllegalArgumentException("pipeliner.store.type must be mysql or redis, got " + type);
        }
        store.init();
        return store;
    }

    @Bean
    @ConditionalOnMissingBean(DataPlane.class)
    public DataPlane pipelinerDataPlane(RedisClient client) {
        return new RedisDataPlane(client);
    }

    @Bean
    @ConditionalOnMissingBean(PipelinerMetrics.class)
    public PipelinerMetrics pipelinerMetrics(ObjectProvider<MeterRegistry> registry) {
        MeterRegistry reg = registry.getIfAvailable();
        return reg == null ? PipelinerMetrics.NOOP : new MicrometerPipelinerMetrics(reg);
    }

    @Bean
    @ConditionalOnMissingBean(RuntimeConfig.class)
    public RuntimeConfig pipelinerRuntimeConfig(PipelinerProperties props) {
        return props.toRuntimeConfig();
    }

    @Bean
    @ConditionalOnMissingBean(ComponentFactory.class)
    public ComponentFactory pipelinerComponentFactory(AutowireCapableBeanFactory beans) {
        return new SpringComponentFactory(beans);
    }

    @Bean
    @ConditionalOnMissingBean(PipelinerEngine.class)
    public PipelinerEngine pipelinerEngine(RuntimeConfig cfg, ControlPlaneStore store, DataPlane plane,
                                           DataSourceRegistry registry, PipelinerMetrics metrics,
                                           ComponentFactory factory, ObjectProvider<PipelineGraph> graphs) {
        DefaultPipelinerEngine engine = new DefaultPipelinerEngine(cfg, store, plane, registry, metrics, factory);
        graphs.orderedStream().forEach(engine::registerGraph);
        return engine;
    }

    @Bean
    @ConditionalOnMissingBean
    public PipelinerEngineLifecycle pipelinerEngineLifecycle(PipelinerEngine engine) {
        return new PipelinerEngineLifecycle(engine);
    }
}
