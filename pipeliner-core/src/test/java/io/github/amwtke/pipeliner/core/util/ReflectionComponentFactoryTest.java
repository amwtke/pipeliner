package io.github.amwtke.pipeliner.core.util;

import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.amwtke.pipeliner.api.PipelinerException;
import org.junit.jupiter.api.Test;

public class ReflectionComponentFactoryTest {
    public static class Ok {}

    public static class NoDefault { public NoDefault(int x) {} }

    public static class Throws { public Throws() { throw new IllegalStateException("nope"); } }

    @Test void createsNewInstancesThroughPublicNoArgConstructor() {
        ReflectionComponentFactory f = new ReflectionComponentFactory();
        Ok a = f.create(Ok.class);
        assertTrue(a != null);
        assertNotSame(a, f.create(Ok.class));
        f.destroy(a);   // default no-op
    }

    @Test void clearErrors() {
        ReflectionComponentFactory f = new ReflectionComponentFactory();
        assertThrows(PipelinerException.class, () -> f.create(NoDefault.class));
        assertThrows(PipelinerException.class, () -> f.create(Throws.class));
    }
}
