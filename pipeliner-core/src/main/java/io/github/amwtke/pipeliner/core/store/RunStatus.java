package io.github.amwtke.pipeliner.core.store;

/** Run lifecycle. */
public enum RunStatus {
    PENDING, RUNNING, SUCCEEDED, FAILED;

    /** SUCCEEDED or FAILED: the run has a finishedAt. */
    public boolean terminal() { return this == SUCCEEDED || this == FAILED; }
}
