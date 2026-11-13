package io.github.amwtke.pipeliner.example.runner;

import io.github.amwtke.pipeliner.api.Row;
import io.github.amwtke.pipeliner.api.RunContext;
import java.util.Collections;
import java.util.List;

/**
 * Chaos fault-injection switch shared by all pods through the MySQL table ex_fault(pipeline, modv) (set via POST /demo/fault).
 * While a row (pipelineId, mod > 0) exists, a runner throws for every chunk whose chunk id hash % mod == 0 - deterministic per chunk,
 * identical on every pod and every attempt, so the chunk goes to the DLQ after max-attempts until the switch is turned off.
 */
public final class Fault {
    private Fault() {}

    public static void check(RunContext ctx) {
        List<Row> r = ctx.lookup("mysql", "select modv from ex_fault where pipeline = :p", Collections.singletonMap("p", ctx.pipelineId()));
        if (r.isEmpty()) return;
        int mod = r.get(0).getInt("modv");
        if (mod > 0 && Math.floorMod(ctx.chunkId().hashCode(), mod) == 0) {
            throw new IllegalStateException("injected fault: pipeline " + ctx.pipelineId() + " chunk " + ctx.chunkId() + " (mod " + mod + ")");
        }
    }
}
