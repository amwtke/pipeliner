package io.github.amwtke.pipeliner.runtime;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

class InterruptsTest {
    private static class RedisCommandInterruptedException extends RuntimeException {
        RedisCommandInterruptedException(Throwable cause) { super("Command interrupted", cause); }
    }

    @Test void recognisesOwnInterruptsDirectOrWrapped() {
        assertTrue(Interrupts.isInterrupt(new InterruptedException()));
        assertTrue(Interrupts.isInterrupt(new RuntimeException("x", new InterruptedException())));
        assertTrue(Interrupts.isInterrupt(new RedisCommandInterruptedException(null)));
        assertFalse(Interrupts.isInterrupt(new IllegalStateException("db down")));
        assertFalse(Interrupts.isInterrupt(null));
    }

    @Test void stoppingAlwaysCounts() {
        assertTrue(Interrupts.ownInterrupt(true, new IllegalStateException("anything while stopping")));
        assertFalse(Interrupts.ownInterrupt(false, new IllegalStateException("real failure")));
        assertTrue(Interrupts.ownInterrupt(false, new RedisCommandInterruptedException(null)));
    }
}
