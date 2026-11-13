package io.github.amwtke.pipeliner.api;

/** Creates (and disposes) type-based runners on demand. */
public interface ComponentFactory {
    <T> T create(Class<T> type);
    /** Called after Lifecycle.close() for every component this factory created. */
    default void destroy(Object component) {}
}
