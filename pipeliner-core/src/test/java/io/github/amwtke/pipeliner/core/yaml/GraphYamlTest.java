package io.github.amwtke.pipeliner.core.yaml;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.github.amwtke.pipeliner.api.BatchRunner;
import io.github.amwtke.pipeliner.api.Chunk;
import io.github.amwtke.pipeliner.api.Collector;
import io.github.amwtke.pipeliner.api.PipelinerException;
import io.github.amwtke.pipeliner.api.Row;
import io.github.amwtke.pipeliner.api.RunContext;
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
import io.github.amwtke.pipeliner.core.plan.ExecutionPlan;
import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class GraphYamlTest {
    static class Src implements SplitSource {
        public String datasetName() { return "ds_in"; }
        public List<Split> enumerate(SourceContext ctx) { return Collections.emptyList(); }
        public List<Row> read(Split s, SourceContext ctx) { return Collections.emptyList(); }
    }

    static class Snk implements Sink {
        public String datasetName() { return "ds_out"; }
        public void write(Chunk c, SinkContext ctx) { }
    }

    static class Run implements BatchRunner {
        public void process(List<Row> rows, Collector out, RunContext ctx) { }
    }

    static final Src SRC = new Src();
    static final Snk SNK = new Snk();
    static final Run RUN = new Run();

    static ComponentResolver resolver() {
        final Map<String, Object> m = new HashMap<>();
        m.put("src", SRC); m.put("snk", SNK); m.put("run", RUN);
        return m::get;
    }

    @Test void yamlEqualsJava() {
        String yaml = "graph: demo\n"
                + "pipelines:\n"
                + "  - id: a\n    source: src\n    runner: {type: batch, bean: run}\n    ports: [side]\n    parallelism: 3\n    mode: cluster\n    chunkKey: [k]\n"
                + "    sinks: {side: [snk]}\n"
                + "  - id: b\n    runner: {type: batch, bean: run}\n    sinks: {out: [snk]}\n"
                + "edges:\n  - {from: a.side, to: b}\n"
                + "gates:\n  - id: g\n    type: external\n    expectFrom: a\n    after: [a]\n    before: [b]\n    timeout: 2h\n    onTimeout: ALERT_AND_WAIT\n";
        // gate g is a deadlock-free? a and b are in the same run via stream edge -> use a separate pipeline instead
        yaml = yaml.replace("after: [a]\n    before: [b]", "after: [a]\n    before: [c]")
                + "";
        yaml = yaml.replace("edges:", "  - id: c\n    source: src\n    runner: {type: batch, bean: run}\nedges:");
        PipelineGraph fromYaml = GraphYaml.load(new ByteArrayInputStream(yaml.getBytes(StandardCharsets.UTF_8)), resolver());
        PipelineGraph java = PipelineGraph.builder("demo")
                .pipeline("a", p -> p.source(SRC).batch(RUN).ports("side").parallelism(3).mode(ExecMode.CLUSTER).chunkKey("k").sink("side", SNK))
                .pipeline("b", p -> p.batch(RUN).sink(SNK))
                .pipeline("c", p -> p.source(SRC).batch(RUN))
                .edge("a.side", "b")
                .gate("g", Gates.externalCountOf("a"), g -> g.after("a").before("c").timeout(Duration.ofHours(2), TimeoutPolicy.ALERT_AND_WAIT))
                .build();
        ExecutionPlan py = fromYaml.plan(), pj = java.plan();
        assertEquals(pj.getHash(), py.getHash());
        assertEquals(pj.explain(), py.explain());
        assertEquals(7_200_000L, fromYaml.getGates().get(0).getTimeoutMillis());
    }

    @Test void gateTypesAndDurations() {
        String yaml = "graph: g\npipelines:\n  - {id: a, source: src}\n  - {id: b, source: src}\n"
                + "gates:\n"
                + "  - {id: m, type: manual, after: [a], before: [b]}\n"
                + "  - {id: t1, type: timer, delay: 5m, after: [a], before: [b], timeout: 500ms, onTimeout: FAIL}\n"
                + "  - {id: t2, type: timer, at: 1700000000000, after: [a], before: [b], timeout: 30m, onTimeout: FAIL}\n"
                + "  - {id: bar, type: barrier, after: [a], before: [b], timeout: 10s}\n";
        PipelineGraph g = GraphYaml.load(new ByteArrayInputStream(yaml.getBytes(StandardCharsets.UTF_8)), resolver());
        assertEquals(GateType.MANUAL, g.getGates().get(0).getType());
        assertEquals(GateType.TIMER, g.getGates().get(1).getType());
        assertEquals(500L, g.getGates().get(1).getTimeoutMillis());
        assertEquals(1_800_000L, g.getGates().get(2).getTimeoutMillis());
        assertEquals(10_000L, g.getGates().get(3).getTimeoutMillis());
        assertEquals(Duration.ofHours(2), GraphYaml.duration("2h"));
    }

    @Test void unknownBean() {
        String yaml = "graph: g\npipelines:\n  - {id: a, source: nope}\n";
        assertThrows(PipelinerException.class, () -> GraphYaml.load(new ByteArrayInputStream(yaml.getBytes(StandardCharsets.UTF_8)), resolver()));
    }

    private static PipelineGraph load(String yaml) {
        return GraphYaml.load(new ByteArrayInputStream(yaml.getBytes(StandardCharsets.UTF_8)), resolver());
    }

    @Test void runnerClassIsLazy() {
        String cls = Run.class.getName();
        PipelineGraph g = load("graph: g\npipelines:\n"
                + "  - {id: a, source: src, runner: {type: batch, class: '" + cls + "'}}\n"
                + "  - {id: b, source: src, runner: {type: batch, class: '" + cls + "', perThread: true}}\n");
        assertEquals(Run.class, g.getPipelines().get("a").getBatchRunnerType());
        assertEquals(false, g.getPipelines().get("a").isPerThread());
        assertEquals(true, g.getPipelines().get("b").isPerThread());
        PipelineGraph java = PipelineGraph.builder("g").pipeline("a", p -> p.source(SRC).batch(Run.class))
                .pipeline("b", p -> p.source(SRC).batch(Run.class).runnerPerThread()).build();
        assertEquals(java.plan().getHash(), g.plan().getHash());
    }

    @Test void runnerClassErrors() {
        assertThrows(PipelinerException.class, () -> load("graph: g\npipelines:\n  - {id: a, source: src, runner: {type: batch, class: no.such.Clazz}}\n"));
        assertThrows(PipelinerException.class, () -> load("graph: g\npipelines:\n  - {id: a, source: src, runner: {type: row, class: '" + Run.class.getName() + "'}}\n"));
        assertThrows(PipelinerException.class, () -> load("graph: g\npipelines:\n  - {id: a, source: src, runner: {type: batch, bean: run, perThread: true}}\n"));
    }
}
