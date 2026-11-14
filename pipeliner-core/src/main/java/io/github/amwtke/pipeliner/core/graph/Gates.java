package io.github.amwtke.pipeliner.core.graph;

import io.github.amwtke.pipeliner.api.GateCondition;
import io.github.amwtke.pipeliner.api.GateContext;
import io.github.amwtke.pipeliner.api.GateDecision;
import io.github.amwtke.pipeliner.core.util.ChunkIds;
import java.time.Duration;

/** Gate factories. Conditions are named classes so plan hashes are stable across JVMs. */
public final class Gates {
    private Gates() {}

    /** Opens when all `after` pipelines succeeded. */
    public static GateDef barrier() { return new GateDef(GateType.BARRIER, null); }

    /** Polled by the master once armed. */
    public static GateDef condition(GateCondition c) { return new GateDef(GateType.CONDITION, c); }

    /** Signals arrive via engine.signalGate. */
    public static GateDef external(GateCondition doneWhen) { return new GateDef(GateType.EXTERNAL, doneWhen); }

    /**
     * Opens when the number of DISTINCT chunks that signalled in the current attempt is &gt;= the number of chunks of the pipeline that called
     * the sink ({@code GateContext.sinkChunkCount}; chunks with no sink rows skip the sink and never signal). A signal key is the value of
     * {@code SinkContext.batchId(chunk)} ({@code <roundId>:<pipelineId>:<chunkId>@<attempt>}); keys without '@' count for any attempt,
     * keys of older attempts are ignored, and keys of the same chunk in different formats count once. Signals are stored per round and gate.
     */
    public static GateDef externalCountOf(String pipelineId) { return new GateDef(GateType.EXTERNAL, new CountOf(pipelineId)); }

    public static GateDef externalCount(int expected) { return new GateDef(GateType.EXTERNAL, new Count(expected)); }

    /** Opened only by engine.openGate. */
    public static GateDef manual() { return new GateDef(GateType.MANUAL, null); }

    public static GateDef timerAt(long epochMillis) { return new GateDef(GateType.TIMER, new TimerAt(epochMillis)); }

    /** Opens when now &gt;= armedAt + d. */
    public static GateDef delay(Duration d) { return new GateDef(GateType.TIMER, new Delay(d.toMillis())); }

    static final class CountOf implements GateCondition {
        private final String pipelineId;
        CountOf(String pipelineId) { this.pipelineId = pipelineId; }
        @Override public GateDecision evaluate(GateContext ctx) {
            String suffix = "@" + ctx.runAttemptOf(pipelineId);
            java.util.Set<String> chunks = new java.util.HashSet<>();
            for (String k : ctx.signals().keySet())
                if (k.indexOf('@') < 0 || k.endsWith(suffix)) chunks.add(ChunkIds.chunkOfSignalKey(k));   // signals of older attempts do not count
            return chunks.size() >= ctx.sinkChunkCount(pipelineId) ? GateDecision.OPEN : GateDecision.WAIT;
        }
    }

    static final class Count implements GateCondition {
        private final int expected;
        Count(int expected) { this.expected = expected; }
        @Override public GateDecision evaluate(GateContext ctx) {
            return ctx.signals().size() >= expected ? GateDecision.OPEN : GateDecision.WAIT;
        }
    }

    static final class TimerAt implements GateCondition {
        private final long epochMillis;
        TimerAt(long epochMillis) { this.epochMillis = epochMillis; }
        @Override public GateDecision evaluate(GateContext ctx) {
            return ctx.nowMillis() >= epochMillis ? GateDecision.OPEN : GateDecision.WAIT;
        }
    }

    static final class Delay implements GateCondition {
        private final long millis;
        Delay(long millis) { this.millis = millis; }
        @Override public GateDecision evaluate(GateContext ctx) {
            return ctx.nowMillis() >= ctx.armedAtMillis() + millis ? GateDecision.OPEN : GateDecision.WAIT;
        }
    }
}
