package io.github.amwtke.pipeliner.core.engine;

/** Who drives the master pass (see {@link PipelinerEngine#tick()}). */
public enum MasterTickMode {
    /** The pod runs a private "pg-master" thread that calls tick() every tickMillis (default). */
    LOCAL,
    /** No master thread: an external scheduler (xxl-job, cron, ...) must call {@link PipelinerEngine#tick()} regularly. */
    EXTERNAL
}
