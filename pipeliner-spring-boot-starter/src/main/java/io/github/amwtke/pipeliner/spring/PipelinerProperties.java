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

    String resolvePodId() {
        if (podId != null && !podId.trim().isEmpty()) return podId.trim();
        String host;
        try {
            host = InetAddress.getLocalHost().getHostName();
        } catch (Exception e) {
            host = "pod";
        }
        return host + "-" + String.format("%04x", ThreadLocalRandom.current().nextInt(0x10000));
    }
}
