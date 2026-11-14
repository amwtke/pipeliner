package io.github.amwtke.pipeliner.api;

/** Creates (and disposes) type-based runners on demand. */
public interface ComponentFactory {
    <T> T create(Class<T> type);
    /**
     * Called for every component this factory created, after Lifecycle.close() when the component opened successfully. A component
     * whose open() threw is destroyed without close() being called on it.
     */
    default void destroy(Object component) {}
}
