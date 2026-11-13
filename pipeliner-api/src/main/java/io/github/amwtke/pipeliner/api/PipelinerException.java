package io.github.amwtke.pipeliner.api;

/** Base runtime exception of the framework. */
public class PipelinerException extends RuntimeException {
    private static final long serialVersionUID = 1L;
    public PipelinerException(String message) { super(message); }
    public PipelinerException(String message, Throwable cause) { super(message, cause); }
}
