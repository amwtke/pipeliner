package io.github.amwtke.pipeliner.core.plan;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.amwtke.pipeliner.api.BatchRunner;
import io.github.amwtke.pipeliner.api.Chunk;
import io.github.amwtke.pipeliner.api.Row;
import io.github.amwtke.pipeliner.api.RowRunner;
import io.github.amwtke.pipeliner.api.Sink;
import io.github.amwtke.pipeliner.api.SinkContext;
import io.github.amwtke.pipeliner.api.Split;
import io.github.amwtke.pipeliner.api.SourceContext;
import io.github.amwtke.pipeliner.api.SplitSource;
import io.github.amwtke.pipeliner.core.graph.ExecMode;
import io.github.amwtke.pipeliner.core.graph.GateType;
import io.github.amwtke.pipeliner.core.graph.Gates;
import io.github.amwtke.pipeliner.core.graph.PipelineGraph;
import io.github.amwtke.pipeliner.core.graph.TimeoutPolicy;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.Test;

class PlannerTest {
    static class Src implements SplitSource {
        final String ds;
        Src(String ds) { this.ds = ds; }
        public String datasetName() { return ds; }
        public List<Split> enumerate(SourceContext ctx) { return Collections.emptyList(); }
        public List<Row> read(Split split, SourceContext ctx) { return Collections.emptyList(); }
    }

    static class MultiSrc extends Src implements ReadsDatasets {
        final List<String> more;
        MultiSrc(String ds, String... more) { super(ds); this.more = Arrays.asList(more); }
        public java.util.Collection<String> readDatasets() { return more; }
    }

    static class Snk implements Sink {
        final String ds;
        Snk(String ds) { this.ds = ds; }
        public String datasetName() { return ds; }
        public void write(Chunk chunk, SinkContext ctx) { }
    }

    static class Batch implements BatchRunner {
        public void process(List<Row> rows, io.github.amwtke.pipeliner.api.Collector out, io.github.amwtke.pipeliner.api.RunContext ctx) { }
    }

    static class RowR implements RowRunner {
        public void process(Row row, io.github.amwtke.pipeliner.api.Collector out, io.github.amwtke.pipeliner.api.RunContext ctx) { }
    }

    static PipelineGraph netDemand() {
        return netDemand(3, 10);
    }

    static PipelineGraph netDemand(int gdParallelism, int gateMinutes) {
        return PipelineGraph.builder("net-demand")
                .pipeline("algo-submit", p -> p.source(new Src("ex_material")).batch(new Batch()).sink(new Snk(null)).mode(ExecMode.INLINE))
                .pipeline("gd-fix", p -> p.source(new Src("algo_gd")).batch(new Batch()).ports("big", "fine").sink("fine", new Snk("fine_gd"))
                        .mode(ExecMode.CLUSTER).parallelism(gdParallelism).chunkKey("material_id").chunkRows(800))
                .pipeline("big-net", p -> p.batch(new Batch()).sink(new Snk("nd1_big")).chunkKey("material_id"))
                .pipeline("core-grade", p -> p.row(new RowR()).sink(new Snk("grade_result")))
                .pipeline("cap-alloc", p -> p.source(new Src("grade_result")).batch(new Batch()).ports("scrap")
                        .sink(new Snk("refurb_plan")).sink("scrap", new Snk("scrap")).mode(ExecMode.LOCAL))
                .pipeline("big-final", p -> p.source(new MultiSrc("nd1_big", "refurb_plan")).batch(new Batch()).sink(new Snk("nd_final")))
                .pipeline("fine-net", p -> p.source(new MultiSrc("fine_gd", "refurb_plan", "scrap")).batch(new Batch()).sink(new Snk("nd_final")))
                .pipeline("publish", p -> p.source(new Src("nd_final")).batch(new Batch()).sink(new Snk("publish_log")).mode(ExecMode.INLINE))
                .edge("gd-fix.big", "big-net")
                .edge("big-net", "core-grade")
                .gate("algo-callback", Gates.externalCountOf("algo-submit"),
                        g -> g.after("algo-submit").before("gd-fix").timeout(Duration.ofMinutes(gateMinutes), TimeoutPolicy.FAIL))
                .gate("conserve-check", Gates.condition(ctx -> null), g -> g.after("big-final", "fine-net").before("publish"))
                .gate("approve", Gates.manual(), g -> g.after("big-final", "fine-net").before("publish"))
                .build();
    }

    @Test void netDemandPlan() {
        ExecutionPlan plan = netDemand().plan();
        assertEquals(6, plan.getRuns().size());
        assertEquals(5, plan.getStageCount());
        assertRun(plan, "R1", 1, ExecMode.INLINE, "algo-submit");
        assertRun(plan, "R2", 2, ExecMode.CLUSTER, "gd-fix", "big-net", "core-grade");
        assertRun(plan, "R3", 3, ExecMode.LOCAL, "cap-alloc");
        assertRun(plan, "R4", 4, ExecMode.CLUSTER, "big-final");
        assertRun(plan, "R5", 4, ExecMode.CLUSTER, "fine-net");
        assertRun(plan, "R6", 5, ExecMode.INLINE, "publish");

        assertEquals(9, plan.getGates().size());
        int auto = 0;
        for (PlanGate g : plan.getGates()) if (g.isAuto()) auto++;
        assertEquals(6, auto);
        for (String ds : Arrays.asList("fine_gd", "grade_result", "nd1_big", "refurb_plan", "scrap", "nd_final"))
            assertTrue(plan.gate("barrier:" + ds).isAuto(), ds);

        PlanGate cb = plan.gate("algo-callback");
        assertEquals(GateType.EXTERNAL, cb.getType());
        assertEquals(Arrays.asList("R1"), cb.getAfterRuns());
        assertEquals(Arrays.asList("R2"), cb.getBeforeRuns());
        assertEquals(600_000L, cb.getTimeoutMillis());
        PlanGate nf = plan.gate("barrier:nd_final");
        assertEquals("nd_final", nf.getDataset());
        assertEquals(Arrays.asList("R4", "R5"), nf.getAfterRuns());
        assertEquals(Arrays.asList("R6"), nf.getBeforeRuns());
        assertEquals(Arrays.asList("big-final", "fine-net"), nf.getAfterPipelines());
        PlanGate fg = plan.gate("barrier:fine_gd");
        assertEquals(Arrays.asList("R2"), fg.getAfterRuns());
        assertEquals(Arrays.asList("R5"), fg.getBeforeRuns());
        assertEquals(Arrays.asList("R5"), plan.gate("barrier:scrap").getBeforeRuns());
        assertEquals(Arrays.asList("R3"), plan.gate("barrier:scrap").getAfterRuns());
        assertEquals(Arrays.asList("R3"), plan.gate("barrier:refurb_plan").getAfterRuns());
        assertEquals(Arrays.asList("R4", "R5"), plan.gate("barrier:refurb_plan").getBeforeRuns());

        assertEquals(Arrays.asList("algo-callback"), plan.run("R2").getInGateIds().subList(0, 1));
        assertEquals(Arrays.asList("R1"), plan.run("R2").getUpstreamRunIds());
        assertEquals(Arrays.asList("R3", "R4", "R5", "R6"), plan.downstreamRuns("R2"));
        assertEquals(2, plan.getChannels().size());
        PlanChannel e1 = plan.channel("e1"), e2 = plan.channel("e2");
        assertEquals("gd-fix", e1.getFromPipeline()); assertEquals("big", e1.getFromPort()); assertEquals("big-net", e1.getToPipeline());
        assertEquals("big-net", e2.getFromPipeline()); assertEquals("core-grade", e2.getToPipeline());
        assertEquals("R2", e1.getRunId());

        PlanPipeline gd = plan.pipeline("gd-fix");
        assertTrue(gd.isSource());
        assertEquals(3 * 0 + 2, plan.pipeline("big-net").getParallelism());
        assertEquals(Arrays.asList("material_id"), plan.pipeline("big-net").getChunkKey());
        assertEquals(800, gd.getChunkRows());
        assertEquals("e1", gd.getPortToEdge().get("big"));
        assertEquals(Arrays.asList("e1"), plan.pipeline("big-net").getInputEdgeIds());
        assertEquals(Arrays.asList("gd-fix"), plan.pipeline("big-net").getUpstreamPipelines());
        assertEquals("R2", plan.pipeline("core-grade").getRunId());
        assertEquals(32, plan.getHash().length());

        String ex = plan.explain();
        for (PlanRun r : plan.getRuns()) assertTrue(ex.contains(r.getRunId()), r.getRunId());
        for (PlanGate g : plan.getGates()) assertTrue(ex.contains(g.getGateId()), g.getGateId());
        assertTrue(ex.contains("gd-fix → big-net → core-grade"), ex);
        assertTrue(ex.startsWith("ExecutionPlan net-demand  hash=" + plan.getHash().substring(0, 8) + "...  6 runs / 9 gates / 8 pipelines"), ex);
        assertFalse(plan.toJson().isEmpty());
    }

    private static void assertRun(ExecutionPlan plan, String id, int stage, ExecMode mode, String... pipes) {
        PlanRun r = plan.run(id);
        assertEquals(stage, r.getStage(), id);
        assertEquals(mode, r.getMode(), id);
        assertEquals(Arrays.asList(pipes), r.getPipelineIds(), id);
    }

    // ------------------------------------------------------------- validation

    @Test void unknownPort() {
        PipelineGraph g = PipelineGraph.builder("g").pipeline("a", p -> p.source(new Src(null))).pipeline("b", p -> { })
                .edge("a.nope", "b").build();
        assertMsg(g, "nope");
    }

    @Test void twoEdgesOnePort() {
        PipelineGraph g = PipelineGraph.builder("g").pipeline("a", p -> p.source(new Src(null))).pipeline("b", p -> { }).pipeline("c", p -> { })
                .edge("a", "b").edge("a", "c").build();
        assertMsg(g, "more than one");
    }

    @Test void sourceWithIncomingEdge() {
        PipelineGraph g = PipelineGraph.builder("g").pipeline("a", p -> p.source(new Src(null))).pipeline("b", p -> p.source(new Src(null)))
                .edge("a", "b").build();
        assertMsg(g, "incoming");
    }

    @Test void streamPipelineWithoutInput() {
        PipelineGraph g = PipelineGraph.builder("g").pipeline("a", p -> { }).build();
        assertMsg(g, "no source");
    }

    @Test void bothRunners() {
        PipelineGraph g = PipelineGraph.builder("g").pipeline("a", p -> p.source(new Src(null)).row(new RowR()).batch(new Batch())).build();
        assertMsg(g, "both");
    }

    @Test void streamCycle() {
        PipelineGraph g = PipelineGraph.builder("g").pipeline("s", p -> p.source(new Src(null))).pipeline("b", p -> { }).pipeline("c", p -> { })
                .edge("s", "b").edge("b.out", "c").edge("c", "b").build();
        assertThrows(PlanException.class, g::plan);
    }

    @Test void cycleViaDatasets() {
        PipelineGraph g = PipelineGraph.builder("g")
                .pipeline("a", p -> p.source(new Src("x")).sink(new Snk("y")))
                .pipeline("b", p -> p.source(new Src("y")).sink(new Snk("x"))).build();
        PlanException e = assertThrows(PlanException.class, g::plan);
        assertTrue(e.getMessage().contains("cyclic"), e.getMessage());
    }

    @Test void cycleViaGates() {
        PipelineGraph g = PipelineGraph.builder("g")
                .pipeline("a", p -> p.source(new Src(null))).pipeline("b", p -> p.source(new Src(null)))
                .gate("g1", Gates.manual(), x -> x.after("a").before("b"))
                .gate("g2", Gates.manual(), x -> x.after("b").before("a")).build();
        assertMsg(g, "cyclic");
    }

    @Test void deadlock() {
        PipelineGraph g = PipelineGraph.builder("g").pipeline("A", p -> p.source(new Src(null))).pipeline("B", p -> { })
                .edge("A", "B").gate("X", Gates.manual(), x -> x.after("A").before("B")).build();
        PlanException e = assertThrows(PlanException.class, g::plan);
        assertTrue(e.getMessage().contains("A") && e.getMessage().contains("B") && e.getMessage().contains("X"), e.getMessage());
        assertTrue(e.getMessage().contains("materialized"), e.getMessage());
    }

    private static void assertMsg(PipelineGraph g, String frag) {
        PlanException e = assertThrows(PlanException.class, g::plan);
        assertTrue(e.getMessage().contains(frag), e.getMessage());
    }

    // ------------------------------------------------------------- regions / modes

    @Test void ungatedChainsMerge() {
        PipelineGraph g = PipelineGraph.builder("g")
                .pipeline("a", p -> p.source(new Src(null)).sink(new Snk(null)))
                .pipeline("b", p -> p.source(new Src(null)).sink(new Snk(null))).build();
        ExecutionPlan plan = g.plan();
        assertEquals(1, plan.getRuns().size());
        assertEquals("R1", plan.pipeline("a").getRunId());
        assertEquals("R1", plan.pipeline("b").getRunId());
        assertEquals(Arrays.asList("a", "b"), plan.run("R1").getPipelineIds());
    }

    private static PipelineGraph chain(long rows1, long rows2, ExecMode hint) {
        return PipelineGraph.builder("g")
                .pipeline("a", p -> { p.source(new Src(null)).estimatedRows(rows1); if (hint != null) p.mode(hint); })
                .pipeline("b", p -> p.estimatedRows(rows2)).edge("a", "b").build();
    }

    @Test void modeSelection() {
        assertEquals(ExecMode.INLINE, chain(100, 100, null).plan().getRuns().get(0).getMode());
        assertEquals(ExecMode.LOCAL, chain(40_000, 40_000, null).plan().getRuns().get(0).getMode());
        assertEquals(ExecMode.CLUSTER, chain(2_000_000, 1, null).plan().getRuns().get(0).getMode());
        assertEquals(ExecMode.CLUSTER, chain(100, -1, null).plan().getRuns().get(0).getMode());
        assertEquals(ExecMode.LOCAL, chain(100, 100, ExecMode.LOCAL).plan().getRuns().get(0).getMode());
        PlanOptions o = PlanOptions.defaults();
        o.setForceMode(ExecMode.CLUSTER);
        assertEquals(ExecMode.CLUSTER, chain(100, 100, null).plan(o).getRuns().get(0).getMode());
        assertEquals(ExecMode.CLUSTER, chain(100, 100, ExecMode.LOCAL).plan(o).getRuns().get(0).getMode()); // forceMode overrides hints
        // fan-out is not a linear chain: small => LOCAL
        PipelineGraph fan = PipelineGraph.builder("g").pipeline("a", p -> p.source(new Src(null)).ports("x").estimatedRows(1))
                .pipeline("b", p -> p.estimatedRows(1)).pipeline("c", p -> p.estimatedRows(1)).edge("a.out", "b").edge("a.x", "c").build();
        assertEquals(ExecMode.LOCAL, fan.plan().getRuns().get(0).getMode());
    }

    // ------------------------------------------------------------- hash

    @Test void hashStabilityAndSensitivity() {
        assertEquals(netDemand().plan().getHash(), netDemand().plan().getHash());
        String base = netDemand().plan().getHash();
        assertNotEquals(base, netDemand(4, 10).plan().getHash());
        assertNotEquals(base, netDemand(3, 11).plan().getHash());
        PipelineGraph ck = PipelineGraph.builder("g").pipeline("a", p -> p.source(new Src(null)).chunkKey("k")).build();
        PipelineGraph ck2 = PipelineGraph.builder("g").pipeline("a", p -> p.source(new Src(null)).chunkKey("j")).build();
        assertNotEquals(ck.plan().getHash(), ck2.plan().getHash());
    }

    // ------------------------------------------------------------- type-based (lazy) runners

    /** Hash of netDemand() computed before the lazy-runner change: instance-based graphs must keep hashing exactly the same. */
    static final String PRE_LAZY_NET_DEMAND_HASH = "0249bb16b5784b9a5aa7af3e8be98b1f";

    @Test void instanceBasedHashIsUnchanged() {
        assertEquals(PRE_LAZY_NET_DEMAND_HASH, netDemand().plan().getHash());
    }

    @Test void typeBasedRunnerHashesLikeItsInstanceForm() {
        String inst = PipelineGraph.builder("g").pipeline("a", p -> p.source(new Src(null)).batch(new Batch())).build().plan().getHash();
        String type = PipelineGraph.builder("g").pipeline("a", p -> p.source(new Src(null)).batch(Batch.class)).build().plan().getHash();
        String perThread = PipelineGraph.builder("g").pipeline("a", p -> p.source(new Src(null)).batch(Batch.class).runnerPerThread()).build().plan().getHash();
        String row = PipelineGraph.builder("g").pipeline("a", p -> p.source(new Src(null)).row(RowR.class)).build().plan().getHash();
        assertEquals(inst, type);
        assertNotEquals(type, perThread, "perThread is part of the hash");
        assertNotEquals(type, row, "runner class/kind is part of the hash");
    }

    @Test void builderKeepsRunnerTypeAndHelpers() {
        PipelineGraph g = PipelineGraph.builder("g").pipeline("a", p -> p.source(new Src(null)).batch(Batch.class).runnerPerThread())
                .pipeline("b", p -> p.source(new Src(null)).row(new RowR())).pipeline("c", p -> p.source(new Src(null))).build();
        io.github.amwtke.pipeliner.core.graph.PipelineSpec a = g.getPipelines().get("a"), b = g.getPipelines().get("b"), c = g.getPipelines().get("c");
        assertEquals(Batch.class, a.getBatchRunnerType());
        assertNull(a.getBatchRunner());
        assertTrue(a.isPerThread() && a.lazyRunner());
        assertEquals(Batch.class.getName(), a.runnerClassName());
        assertEquals("batch", a.runnerKind());
        assertEquals(RowR.class.getName(), b.runnerClassName());
        assertFalse(b.lazyRunner());
        assertNull(c.runnerClassName());
        assertFalse(c.hasRunner());
        g.plan();
    }

    @Test void atMostOneRunner() {
        assertMsg(PipelineGraph.builder("g").pipeline("a", p -> p.source(new Src(null)).batch(Batch.class).row(RowR.class)).build(), "more than one runner");
        assertMsg(PipelineGraph.builder("g").pipeline("a", p -> p.source(new Src(null)).batch(new Batch()).batch(Batch.class)).build(), "more than one runner");
        assertMsg(PipelineGraph.builder("g").pipeline("a", p -> p.source(new Src(null)).row(new RowR()).batch(new Batch())).build(), "more than one runner");
    }

    @Test void perThreadNeedsTypeBasedRunner() {
        assertMsg(PipelineGraph.builder("g").pipeline("a", p -> p.source(new Src(null)).batch(new Batch()).runnerPerThread()).build(), "runnerPerThread");
    }

    // ------------------------------------------------------------- review round 2

    static class DimRunner extends RowR implements ReadsDatasets {
        public java.util.Collection<String> readDatasets() { return Collections.singletonList("dim"); }
    }

    private static PipelineGraph runnerReads(java.util.function.Consumer<PipelineGraph.PipelineBuilder> reader) {
        return PipelineGraph.builder("g")
                .pipeline("w", p -> p.source(new Src(null)).sink(new Snk("dim")))
                .pipeline("r", reader).build();
    }

    @Test void runnerInstanceReadsDatasetsPlansABarrier() {
        ExecutionPlan plan = runnerReads(p -> p.source(new Src(null)).row(new DimRunner())).plan();
        assertEquals(2, plan.getRuns().size());
        assertTrue(plan.gate("barrier:dim").isAuto());
        assertEquals(Arrays.asList("w"), plan.gate("barrier:dim").getAfterPipelines());
        assertEquals(Arrays.asList("r"), plan.gate("barrier:dim").getBeforePipelines());
    }

    @Test void typeBasedRunnerReadingDatasetsMustBeDeclaredStaticallyOrFails() {
        // a type-based ReadsDatasets runner is never instantiated; without a static readDatasets() the plan fails loudly
        PlanException e = assertThrows(PlanException.class, () -> runnerReads(p -> p.source(new Src(null)).row(DimRunner.class)).plan());
        assertTrue(e.getMessage().contains("static"), e.getMessage());
    }

    /** Declares the datasets it reads statically (it never implements ReadsDatasets: a class cannot have both). */
    public static class StaticDimRunner extends RowR {
        public static java.util.Collection<String> readDatasets() { return Collections.singletonList("dim"); }
    }

    @Test void typeBasedRunnerWithAStaticReadDatasetsPlansABarrier() {
        ExecutionPlan plan = runnerReads(p -> p.source(new Src(null)).row(StaticDimRunner.class)).plan();
        assertEquals(2, plan.getRuns().size());
        assertTrue(plan.gate("barrier:dim").isAuto());
        assertEquals(Arrays.asList("w"), plan.gate("barrier:dim").getAfterPipelines());
        assertEquals(Arrays.asList("r"), plan.gate("barrier:dim").getBeforePipelines());
    }

    @Test void selfReadWriteWithSecondReaderIsNotOnBothSidesOfItsGate() {
        PipelineGraph g = PipelineGraph.builder("g")
                .pipeline("p1", p -> p.source(new Src("d")).sink(new Snk("d")))
                .pipeline("p2", p -> p.source(new Src("d")).sink(new Snk(null))).build();
        ExecutionPlan plan = g.plan();
        PlanGate b = plan.gate("barrier:d");
        assertEquals(Arrays.asList("p1"), b.getAfterPipelines());
        assertEquals(Arrays.asList("p2"), b.getBeforePipelines());
    }

    @Test void hintsApplyPerPipelineAndUnhintedMembersKeepTheirEstimate() {
        // R1 = [small(INLINE hint), big(no hint, 50M rows)]: big must not inherit INLINE
        PipelineGraph g = PipelineGraph.builder("g")
                .pipeline("small", p -> p.source(new Src(null)).mode(ExecMode.INLINE).estimatedRows(1))
                .pipeline("big", p -> p.source(new Src(null)).estimatedRows(50_000_000L)).build();
        assertEquals(ExecMode.CLUSTER, g.plan().getRuns().get(0).getMode());
        PipelineGraph h = PipelineGraph.builder("g")
                .pipeline("small", p -> p.source(new Src(null)).mode(ExecMode.INLINE).estimatedRows(1))
                .pipeline("tiny", p -> p.source(new Src(null)).estimatedRows(1)).build();
        assertEquals(ExecMode.INLINE, h.plan().getRuns().get(0).getMode());
        PipelineGraph k = PipelineGraph.builder("g")
                .pipeline("a", p -> p.source(new Src(null)).mode(ExecMode.LOCAL).estimatedRows(1))
                .pipeline("b", p -> p.source(new Src(null)).estimatedRows(1)).build();
        assertEquals(ExecMode.LOCAL, k.plan().getRuns().get(0).getMode());
    }

    @Test void proxyClassNamesAreNormalizedForTheHash() {
        assertEquals("a.b.Foo", Planner.stripLambda("a.b.Foo$$EnhancerBySpringCGLIB$$1a2b3c"));
        assertEquals("a.b.Foo", Planner.stripLambda("a.b.Foo$$FastClassBySpringCGLIB$$ff"));
        assertEquals("a.b.Foo", Planner.stripLambda("a.b.Foo$ByteBuddy$9xYz"));
        assertEquals("a.b.Foo$$Lambda", Planner.stripLambda("a.b.Foo$$Lambda$17/0x0000000800c03000"));
        assertEquals("a.b.Foo$Inner", Planner.stripLambda("a.b.Foo$Inner"));
    }

    @Test void parallelismMustBePositive() {
        assertThrows(IllegalArgumentException.class, () -> PipelineGraph.builder("g").pipeline("a", p -> p.source(new Src(null)).parallelism(0)).build());
        assertThrows(IllegalArgumentException.class, () -> PipelineGraph.builder("g").pipeline("a", p -> p.source(new Src(null)).parallelism(-3)).build());
    }
}
