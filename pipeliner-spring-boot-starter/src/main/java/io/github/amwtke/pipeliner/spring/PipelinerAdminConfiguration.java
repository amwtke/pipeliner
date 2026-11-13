package io.github.amwtke.pipeliner.spring;

import io.github.amwtke.pipeliner.core.engine.PipelinerEngine;
import io.github.amwtke.pipeliner.core.engine.RuntimeConfig;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.autoconfigure.condition.ConditionalOnWebApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/** Registers the admin REST controller unless pipeliner.admin.enabled=false. */
@Configuration(proxyBeanMethods = false)
@ConditionalOnWebApplication
@ConditionalOnProperty(prefix = "pipeliner.admin", name = "enabled", havingValue = "true", matchIfMissing = true)
public class PipelinerAdminConfiguration {
    @Bean
    @ConditionalOnMissingBean
    public PipelinerAdminController pipelinerAdminController(PipelinerEngine engine, RuntimeConfig cfg,
            io.github.amwtke.pipeliner.core.store.ControlPlaneStore store, io.github.amwtke.pipeliner.core.dataplane.DataPlane plane) {
        return new PipelinerAdminController(engine, cfg, store, plane);
    }
}
