package io.github.amwtke.pipeliner.runtime;

import io.github.amwtke.pipeliner.core.dataplane.Keys;
import io.github.amwtke.pipeliner.core.plan.ExecutionPlan;
import io.github.amwtke.pipeliner.core.plan.PlanChannel;
import io.github.amwtke.pipeliner.core.plan.PlanRun;
import java.util.ArrayList;
import java.util.List;

/** Data-plane keys owned by a run (channels, split queues and indexes, dead-letter queues). */
public final class RunKeys {
    private RunKeys() {}

    public static List<String> of(String roundId, ExecutionPlan plan, PlanRun pr) {
        List<String> keys = new ArrayList<>();
        for (PlanChannel ch : plan.getChannels()) if (ch.getRunId().equals(pr.getRunId())) keys.add(Keys.channel(roundId, ch.getEdgeId()));
        for (String p : pr.getPipelineIds()) {
            keys.add(Keys.splitQueue(roundId, p));
            keys.add(Keys.splitIndex(roundId, p));
            keys.add(Keys.dlq(roundId, p));
        }
        return keys;
    }

    /** Round-level control keys. */
    public static List<String> control(String roundId) {
        List<String> keys = new ArrayList<>();
        keys.add(Keys.desired(roundId));
        keys.add(Keys.assign(roundId));
        keys.add(Keys.paused(roundId));
        return keys;
    }
}
