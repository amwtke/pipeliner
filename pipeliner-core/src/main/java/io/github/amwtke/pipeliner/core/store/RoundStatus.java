package io.github.amwtke.pipeliner.core.store;

/** Round lifecycle. */
public enum RoundStatus {
    CREATED, RUNNING, PAUSED, SUCCEEDED, FAILED, CANCELLED;

    public boolean terminal() { return this == SUCCEEDED || this == FAILED || this == CANCELLED; }
}
