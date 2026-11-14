package io.github.amwtke.pipeliner.runtime;

import io.github.amwtke.pipeliner.core.dataplane.Keys;
import io.github.amwtke.pipeliner.core.plan.ExecutionPlan;
import io.github.amwtke.pipeliner.core.plan.PlanChannel;
import io.github.amwtke.pipeliner.core.plan.PlanPipeline;
import io.github.amwtke.pipeliner.core.plan.PlanRun;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Data-plane keys owned by a run (channels, split queues and indexes, dead-letter queues). */
public final class RunKeys {
    private RunKeys() {}

    /** Keys of one attempt of the run. */
    public static List<String> of(String roundId, ExecutionPlan plan, PlanRun pr, int attempt) {
        List<String> keys = new ArrayList<>();
        for (PlanChannel ch : plan.getChannels()) if (ch.getRunId().equals(pr.getRunId())) keys.add(Keys.channel(roundId, ch.getEdgeId(), attempt));
        for (String p : pr.getPipelineIds()) {
            keys.add(Keys.splitQueue(roundId, p, attempt));
            keys.add(Keys.splitIndex(roundId, p, attempt));
            keys.add(Keys.dlq(roundId, p, attempt));
        }
        return keys;
    }

    /** Keys of every attempt in [0, toExclusive) of the run. */
    public static List<String> ofAttemptsBefore(String roundId, ExecutionPlan plan, PlanRun pr, int toExclusive) {
        List<String> keys = new ArrayList<>();
        for (int a = 0; a < toExclusive; a++) keys.addAll(of(roundId, plan, pr, a));
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

    /** Channels of run {@code runId}. The one definition of "channels of run r". */
    public static List<PlanChannel> channelsOf(ExecutionPlan plan, String runId) {
        List<PlanChannel> out = new ArrayList<>();
        for (PlanChannel ch : plan.getChannels()) if (ch.getRunId().equals(runId)) out.add(ch);
        return out;
    }

    /**
     * Input streams of pipeline {@code pp} in attempt {@code attempt}, mapped to their edge id (null for a source's split queue). The one
     * definition shared by the workers (what they read) and the master (what must be empty for the pipeline to be done).
     */
    public static Map<String, String> inputStreams(String roundId, PlanPipeline pp, int attempt) {
        Map<String, String> out = new LinkedHashMap<>();
        if (pp.isSource()) out.put(Keys.splitQueue(roundId, pp.getPipelineId(), attempt), null);
        else for (String e : pp.getInputEdgeIds()) out.put(Keys.channel(roundId, e, attempt), e);
        return out;
    }
}
