package io.github.amwtke.pipeliner.spring;

import io.github.amwtke.pipeliner.core.engine.RuntimeConfig;
import io.github.amwtke.pipeliner.core.plan.PlanOptions;
import java.net.InetAddress;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

/** Binds the "pipeliner.*" properties onto RuntimeConfig, PlanOptions and wiring settings. */
@Data
@ConfigurationProperties(prefix = "pipeliner")
public class PipelinerProperties {
    /** Blank means hostname plus a random suffix. */
    private String podId = "";
    private boolean masterEnabled = true;
    private boolean workerEnabled = true;
    private Redis redis = new Redis();
    private Store store = new Store();
    private Map<String, Ds> datasources = new LinkedHashMap<>();
    /** Maps field by field onto RuntimeConfig (podId/master/worker flags come from the top level). */
    private RuntimeConfig runtime = new RuntimeConfig();
    private PlanOptions plan = new PlanOptions();
    private Admin admin = new Admin();
    private Retention retention = new Retention();

    /** Redis connection settings. */
    @Data
    public static class Redis {
        private String uri = "redis://127.0.0.1:6380/0";
    }

    /** Control plane store selection. */
    @Data
    public static class Store {
        /** redis (default) or mysql. */
        private String type = "redis";
        /** Name in pipeliner.datasources or a DataSource bean name (mysql type). */
        private String datasource = "pgstore";
    }

    /** One Hikari data source. */
    @Data
    public static class Ds {
        private String url;
        private String username;
        private String password;
        private String driverClassName;
        private int maximumPoolSize = 10;
    }

    /** Control-plane data retention (leader only). */
    @Data
    public static class Retention {
        private boolean enabled = true;
        /** Terminal rounds older than this lose their chunk records (per-pipeline summaries stay). */
        private long chunkDays = 3;
        /** Terminal rounds older than this are deleted completely. */
        private long roundDays = 30;
        /** Minimum time between two retention passes. */
        private long intervalMinutes = 10;
        /** Rounds compacted and purged per pass. */
        private int batch = 100;
    }

    /** Admin REST switch. */
    @Data
    public static class Admin {
        private boolean enabled = true;
        private Ui ui = new Ui();
    }

    /** Built-in web UI served at /pg-ui/ (404 when disabled). */
    @Data
    public static class Ui {
        private boolean enabled = true;
    }

    /** Builds the engine's RuntimeConfig from these properties. */
    public RuntimeConfig toRuntimeConfig() {
        rejectShadowedRuntimeKeys();
        RuntimeConfig cfg = runtime.toBuilder().build();
        cfg.setPodId(resolvePodId());
        cfg.setMasterEnabled(masterEnabled);
        cfg.setWorkerEnabled(workerEnabled);
        cfg.setPlanOptions(plan);
        cfg.setRetentionEnabled(retention.isEnabled());
        cfg.setChunkRetentionMillis(java.util.concurrent.TimeUnit.DAYS.toMillis(retention.getChunkDays()));
        cfg.setRoundRetentionMillis(java.util.concurrent.TimeUnit.DAYS.toMillis(retention.getRoundDays()));
        cfg.setRetentionIntervalMillis(java.util.concurrent.TimeUnit.MINUTES.toMillis(retention.getIntervalMinutes()));
        cfg.setRetentionBatch(retention.getBatch());
        return cfg;
    }

    /**
     * pipeliner.runtime.* binds the whole RuntimeConfig, but these fields are owned by dedicated properties and would be silently
     * overwritten. A runtime.* value that differs from the RuntimeConfig default fails fast instead, naming the property to use.
     */
    private void rejectShadowedRuntimeKeys() {
        RuntimeConfig d = new RuntimeConfig();
        java.util.List<String> bad = new java.util.ArrayList<>();
        if (!java.util.Objects.equals(runtime.getPodId(), d.getPodId())) bad.add("runtime.pod-id -> pipeliner.pod-id");
        if (runtime.isMasterEnabled() != d.isMasterEnabled()) bad.add("runtime.master-enabled -> pipeliner.master-enabled");
        if (runtime.isWorkerEnabled() != d.isWorkerEnabled()) bad.add("runtime.worker-enabled -> pipeliner.worker-enabled");
        if (runtime.getPlanOptions() != null) bad.add("runtime.plan-options.* -> pipeliner.plan.*");
        if (runtime.isRetentionEnabled() != d.isRetentionEnabled()) bad.add("runtime.retention-enabled -> pipeliner.retention.enabled");
        if (runtime.getChunkRetentionMillis() != d.getChunkRetentionMillis()) bad.add("runtime.chunk-retention-millis -> pipeliner.retention.chunk-days");
        if (runtime.getRoundRetentionMillis() != d.getRoundRetentionMillis()) bad.add("runtime.round-retention-millis -> pipeliner.retention.round-days");
        if (runtime.getRetentionIntervalMillis() != d.getRetentionIntervalMillis())
            bad.add("runtime.retention-interval-millis -> pipeliner.retention.interval-minutes");
        if (runtime.getRetentionBatch() != d.getRetentionBatch()) bad.add("runtime.retention-batch -> pipeliner.retention.batch");
        if (!bad.isEmpty())
            throw new IllegalStateException("pipeliner.runtime.* keys that are overridden by dedicated properties and would be ignored; use the dedicated property instead: "
                    + String.join("; ", bad));
    }

    String resolvePodId() {
        if (podId != null && !podId.trim().isEmpty()) return podId.trim();
        String host;
        try {
            host = InetAddress.getLocalHost().getHostName();
        } catch (Exception e) {
            host = "pod";
        }
        return host + "-" + String.format(java.util.Locale.ROOT, "%04x", ThreadLocalRandom.current().nextInt(0x10000));
    }
}
