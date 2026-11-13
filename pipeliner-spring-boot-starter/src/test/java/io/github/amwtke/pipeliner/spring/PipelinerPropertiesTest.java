package io.github.amwtke.pipeliner.spring;

import static org.junit.jupiter.api.Assertions.*;

import io.github.amwtke.pipeliner.core.engine.RuntimeConfig;
import io.github.amwtke.pipeliner.core.graph.ExecMode;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Configuration;

/** Verifies pipeliner.* properties bind to RuntimeConfig and PlanOptions. */
class PipelinerPropertiesTest {
    @Configuration
    @EnableConfigurationProperties(PipelinerProperties.class)
    static class Cfg {}

    private final ApplicationContextRunner runner = new ApplicationContextRunner().withUserConfiguration(Cfg.class);

    @Test
    void bindsRuntimeAndPlan() {
        runner.withPropertyValues(
                "pipeliner.pod-id=pod-a", "pipeliner.master-enabled=false",
                "pipeliner.redis.uri=redis://h:1/2", "pipeliner.store.type=redis",
                "pipeliner.datasources.pgstore.url=jdbc:x", "pipeliner.datasources.pgstore.maximum-pool-size=3",
                "pipeliner.runtime.tick-millis=500", "pipeliner.runtime.claim-idle-millis=15000",
                "pipeliner.runtime.high-watermark=10", "pipeliner.runtime.autoscale=false",
                "pipeliner.plan.inline-max-rows=7", "pipeliner.plan.force-mode=LOCAL").run(ctx -> {
            PipelinerProperties p = ctx.getBean(PipelinerProperties.class);
            RuntimeConfig c = p.toRuntimeConfig();
            assertEquals("pod-a", c.getPodId());
            assertFalse(c.isMasterEnabled());
            assertTrue(c.isWorkerEnabled());
            assertEquals(500, c.getTickMillis());
            assertEquals(15000, c.getClaimIdleMillis());
            assertEquals(10, c.getHighWatermark());
            assertFalse(c.isAutoscale());
            assertEquals(32, c.getLowWatermark());
            assertEquals(7, c.getPlanOptions().getInlineMaxRows());
            assertEquals(ExecMode.LOCAL, c.getPlanOptions().getForceMode());
            assertEquals("redis", p.getStore().getType());
            assertEquals("redis://h:1/2", p.getRedis().getUri());
            assertEquals(3, p.getDatasources().get("pgstore").getMaximumPoolSize());
        });
    }

    @Test
    void blankPodIdGetsHostnameSuffix() {
        runner.withPropertyValues("pipeliner.pod-id=").run(ctx -> {
            String id = ctx.getBean(PipelinerProperties.class).toRuntimeConfig().getPodId();
            assertTrue(id.matches(".+-[0-9a-f]{4}"), id);
        });
    }
}
