package io.github.amwtke.pipeliner.core.store;

import java.util.ArrayList;
import java.util.List;

/** Round lifecycle. */
public enum RoundStatus {
    CREATED, RUNNING, PAUSED, SUCCEEDED, FAILED, CANCELLED;

    public boolean terminal() { return this == SUCCEEDED || this == FAILED || this == CANCELLED; }

    /** A terminal round that a rerun/retry flips back to RUNNING (a CANCELLED round stays cancelled). */
    public boolean revivable() { return this == SUCCEEDED || this == FAILED; }

    /** Names of the terminal statuses (the single source for SQL IN-lists and script arguments). */
    public static List<String> terminalNames() { return names(true, false); }

    /** Names of the non-terminal statuses (CREATED, RUNNING, PAUSED). */
    public static List<String> activeNames() { return names(false, false); }

    /** Names of the {@link #revivable()} statuses. */
    public static List<String> revivableNames() { return names(true, true); }

    private static List<String> names(boolean terminal, boolean revivableOnly) {
        List<String> out = new ArrayList<>();
        for (RoundStatus s : values()) if (s.terminal() == terminal && (!revivableOnly || s.revivable())) out.add(s.name());
        return out;
    }
}
