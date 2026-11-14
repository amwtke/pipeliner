package io.github.amwtke.pipeliner.runtime;

/** Recognises the failure a thread gets when it is interrupted by our own shutdown (also wrapped by Lettuce as RedisCommandInterruptedException). */
public final class Interrupts {
    private Interrupts() {}

    /** True when the throwable or one of its causes is an InterruptedException or a Redis "command interrupted" exception. */
    public static boolean isInterrupt(Throwable t) {
        for (int i = 0; t != null && i < 10; i++, t = t.getCause()) {
            if (t instanceof InterruptedException || t instanceof java.io.InterruptedIOException || t.getClass().getSimpleName().equals("RedisCommandInterruptedException")) return true;
        }
        return false;
    }

    /**
     * Whether a loop failure is just our own interrupt: the loop is stopping, or the cause is an interrupt. The caller logs it at DEBUG and leaves the loop instead of reporting an ERROR.
     */
    public static boolean ownInterrupt(boolean stopping, Throwable t) {
        return stopping || isInterrupt(t);
    }
}
