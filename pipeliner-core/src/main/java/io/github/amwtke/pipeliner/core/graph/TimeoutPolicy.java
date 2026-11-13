package io.github.amwtke.pipeliner.core.graph;

/** What to do when a gate times out. */
public enum TimeoutPolicy { FAIL, OPEN_WITH_FALLBACK, ALERT_AND_WAIT }
