package io.github.amwtke.pipeliner.core.store;

/** Pipeline lifecycle. */
public enum PipelineStatus {
    PENDING, RUNNING, SUCCEEDED, FAILED;

    /** SUCCEEDED or FAILED: the pipeline has a finishedAt. */
    public boolean terminal() { return this == SUCCEEDED || this == FAILED; }
}
