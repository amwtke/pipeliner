package io.github.amwtke.pipeliner.core.util;

import io.github.amwtke.pipeliner.api.ComponentFactory;
import io.github.amwtke.pipeliner.api.PipelinerException;
import java.lang.reflect.InvocationTargetException;

/** Instantiates components through their public no-arg constructor. */
public final class ReflectionComponentFactory implements ComponentFactory {
    public ReflectionComponentFactory() {}

    @Override public <T> T create(Class<T> type) {
        try {
            return type.getConstructor().newInstance();
        } catch (InvocationTargetException e) {
            throw new PipelinerException("cannot instantiate " + type.getName() + ": " + e.getCause(), e.getCause());
        } catch (ReflectiveOperationException e) {
            throw new PipelinerException("cannot instantiate " + type.getName() + " (needs a public no-arg constructor): " + e, e);
        }
    }
}
