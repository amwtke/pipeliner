package io.github.amwtke.pipeliner.runtime.process;

import io.github.amwtke.pipeliner.core.engine.PipelinerMetrics;
import io.github.amwtke.pipeliner.core.engine.RuntimeConfig;
import java.util.function.BooleanSupplier;
import java.util.function.Predicate;
import lombok.extern.slf4j.Slf4j;

/**
 * The one chunk retry policy shared by the cluster worker and the local executor: up to {@code maxAttempts} total attempts with a linear
 * backoff, any Throwable (including StackOverflowError and NoClassDefFoundError) counts as a failed attempt, OutOfMemoryError is
 * rethrown at once (the JVM is unreliable; the chunk is neither retried nor dead-lettered here), and a stopping worker aborts.
 */
@Slf4j
public final class ChunkRetryLoop {
    private ChunkRetryLoop() {}

    /** One attempt at a chunk; its return value is handed back on success. */
    public interface Body<T> { T run() throws Exception; }

    /** Result: a value, or the abandoned marker, or the last failure after the attempts were exhausted. */
    public static final class Outcome<T> {
        public final T value;
        public final Throwable failure;
        public final boolean abandoned;
        private Outcome(T value, Throwable failure, boolean abandoned) { this.value = value; this.failure = failure; this.abandoned = abandoned; }
        public boolean ok() { return failure == null && !abandoned; }
    }

    /**
     * @param stopping  true when the caller is being stopped/cancelled: the loop throws InterruptedException instead of counting a failure
     * @param abandon   true for failures after which there is nothing left to do (e.g. the round's streams are gone): the outcome is abandoned
     */
    public static <T> Outcome<T> run(RuntimeConfig cfg, PipelinerMetrics metrics, String roundId, String pipelineId, String what,
                                     BooleanSupplier stopping, Predicate<Throwable> abandon, Body<T> body) throws InterruptedException {
        int max = Math.max(1, cfg.getMaxAttempts());
        Throwable last = null;
        for (int attempt = 1; attempt <= max; attempt++) {
            try {
                return new Outcome<>(body.run(), null, false);
            } catch (InterruptedException ie) {
                throw ie;
            } catch (OutOfMemoryError oom) {
                throw oom;
            } catch (Throwable e) {
                if (stopping.getAsBoolean()) throw new InterruptedException("stopping");
                if (abandon.test(e)) return new Outcome<>(null, null, true);
                last = e;
                log.warn("chunk {} attempt {}/{} failed in {}: {}", what, attempt, max, pipelineId, Events.describe(e));
                if (attempt < max) {
                    metrics.chunkRetried(roundId, pipelineId);
                    Thread.sleep(cfg.getRetryBackoffMillis() * attempt);
                }
            }
        }
        return new Outcome<>(null, last, false);
    }
}
