package io.github.amwtke.pipeliner.core.store;

import io.github.amwtke.pipeliner.api.PipelinerException;

/** A master write was rejected because a newer master epoch holds the round (the writer has been deposed). */
public class StaleEpochException extends PipelinerException {
    private static final long serialVersionUID = 1L;
    public StaleEpochException(String message) { super(message); }
}
