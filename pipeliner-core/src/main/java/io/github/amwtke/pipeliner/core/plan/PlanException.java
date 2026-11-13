package io.github.amwtke.pipeliner.core.plan;

import io.github.amwtke.pipeliner.api.PipelinerException;

/** Thrown when a graph cannot be planned. */
public class PlanException extends PipelinerException {
    private static final long serialVersionUID = 1L;
    public PlanException(String message) { super(message); }
    public PlanException(String message, Throwable cause) { super(message, cause); }
}
