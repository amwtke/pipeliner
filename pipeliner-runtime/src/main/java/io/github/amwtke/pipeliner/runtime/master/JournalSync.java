package io.github.amwtke.pipeliner.runtime.master;

import io.github.amwtke.pipeliner.core.plan.ExecutionPlan;
import io.github.amwtke.pipeliner.runtime.Services;

/** Entry point for admin operations that need the store to reflect the whole journal (see {@code AdminOps.skipDeadLetters}). */
public final class JournalSync {
    private JournalSync() {}

    /** Drains the round's journal completely into the store, exactly like a master tick does (fenced by {@code epoch}). */
    public static void drain(Services s, long epoch, String roundId, ExecutionPlan plan) {
        MasterContext c = new MasterContext(s);
        c.epoch(epoch);
        new JournalDrainer(c).drain(roundId, plan);
    }
}
