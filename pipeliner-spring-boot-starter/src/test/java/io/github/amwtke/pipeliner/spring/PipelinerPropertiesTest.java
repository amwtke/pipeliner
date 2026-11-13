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
    void bindsMasterTickMode() {
        runner.withPropertyValues("pipeliner.runtime.master-tick-mode=external", "pipeliner.runtime.external-tick-interval-millis=1500").run(ctx -> {
            RuntimeConfig c = ctx.getBean(PipelinerProperties.class).toRuntimeConfig();
            assertEquals(io.github.amwtke.pipeliner.core.engine.MasterTickMode.EXTERNAL, c.getMasterTickMode());
            assertEquals(1500, c.getExternalTickIntervalMillis());
        });
        runner.run(ctx -> assertEquals(io.github.amwtke.pipeliner.core.engine.MasterTickMode.LOCAL,
                ctx.getBean(PipelinerProperties.class).toRuntimeConfig().getMasterTickMode()));
    }

    @Test
    void blankPodIdGetsHostnameSuffix() {
        runner.withPropertyValues("pipeliner.pod-id=").run(ctx -> {
            String id = ctx.getBean(PipelinerProperties.class).toRuntimeConfig().getPodId();
            assertTrue(id.matches(".+-[0-9a-f]{4}"), id);
        });
    }

    @Test
    void retentionDefaultsAndOverrides() {
        runner.run(ctx -> {
            RuntimeConfig c = ctx.getBean(PipelinerProperties.class).toRuntimeConfig();
            assertTrue(c.isRetentionEnabled());
            assertEquals(3L * 24 * 3600 * 1000, c.getChunkRetentionMillis());
            assertEquals(30L * 24 * 3600 * 1000, c.getRoundRetentionMillis());
            assertEquals(10L * 60 * 1000, c.getRetentionIntervalMillis());
            assertEquals(100, c.getRetentionBatch());
        });
        runner.withPropertyValues("pipeliner.retention.enabled=false", "pipeliner.retention.chunk-days=0", "pipeliner.retention.round-days=7",
                "pipeliner.retention.interval-minutes=0", "pipeliner.retention.batch=5").run(ctx -> {
            RuntimeConfig c = ctx.getBean(PipelinerProperties.class).toRuntimeConfig();
            assertFalse(c.isRetentionEnabled());
            assertEquals(0, c.getChunkRetentionMillis());
            assertEquals(7L * 24 * 3600 * 1000, c.getRoundRetentionMillis());
            assertEquals(0, c.getRetentionIntervalMillis());
            assertEquals(5, c.getRetentionBatch());
        });
    }
}
