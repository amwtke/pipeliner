package io.github.amwtke.pipeliner.core.store;

/** {@link ControlPlaneStore#createRound} refused a round id that already exists (for another biz key): the caller retries with a fresh id. */
public class DuplicateRoundIdException extends RuntimeException {
    public DuplicateRoundIdException(String roundId) { super("round id already exists: " + roundId); }
}
