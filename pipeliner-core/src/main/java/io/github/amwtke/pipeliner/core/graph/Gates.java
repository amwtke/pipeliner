package io.github.amwtke.pipeliner.core.graph;

import io.github.amwtke.pipeliner.api.GateCondition;
import io.github.amwtke.pipeliner.api.GateContext;
import io.github.amwtke.pipeliner.api.GateDecision;
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

    /** Opens when signals().size() &gt;= doneChunkCount(pipelineId). */
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
            return ctx.signals().size() >= ctx.doneChunkCount(pipelineId) ? GateDecision.OPEN : GateDecision.WAIT;
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
