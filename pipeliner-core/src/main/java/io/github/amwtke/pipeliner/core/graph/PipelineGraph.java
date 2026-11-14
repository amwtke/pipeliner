package io.github.amwtke.pipeliner.core.graph;

import io.github.amwtke.pipeliner.api.BatchRunner;
import io.github.amwtke.pipeliner.api.PipelinerException;
import io.github.amwtke.pipeliner.api.RowRunner;
import io.github.amwtke.pipeliner.api.Sink;
import io.github.amwtke.pipeliner.api.SplitSource;
import io.github.amwtke.pipeliner.core.plan.ExecutionPlan;
import io.github.amwtke.pipeliner.core.plan.PlanOptions;
import io.github.amwtke.pipeliner.core.plan.Planner;
import io.github.amwtke.pipeliner.core.store.ControlPlaneStore;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

/** Immutable pipeline graph: pipelines, stream edges and gates. */
public final class PipelineGraph {
    private final String name;
    private final Map<String, PipelineSpec> pipelines;
    private final List<EdgeSpec> edges;
    private final List<GateSpec> gates;

    private PipelineGraph(String name, Map<String, PipelineSpec> pipelines, List<EdgeSpec> edges, List<GateSpec> gates) {
        this.name = name;
        this.pipelines = Collections.unmodifiableMap(pipelines);
        this.edges = Collections.unmodifiableList(edges);
        this.gates = Collections.unmodifiableList(gates);
    }

    public static Builder builder(String name) { return new Builder(name); }

    public String getName() { return name; }
    /** Declaration order. */
    public Map<String, PipelineSpec> getPipelines() { return pipelines; }
    public List<EdgeSpec> getEdges() { return edges; }
    public List<GateSpec> getGates() { return gates; }

    public ExecutionPlan plan() { return plan(PlanOptions.defaults()); }

    public ExecutionPlan plan(PlanOptions options) { return new Planner(options).plan(this); }

    /** Fluent graph builder. */
    public static final class Builder {
        private final String name;
        private final List<PipelineSpec> specs = new ArrayList<>();
        private final List<String[]> rawEdges = new ArrayList<>();
        private final List<GateSpec> gates = new ArrayList<>();

        private Builder(String name) { this.name = name; }

        public Builder pipeline(String id, Consumer<PipelineBuilder> cfg) {
            PipelineBuilder pb = new PipelineBuilder(id);
            if (cfg != null) cfg.accept(pb);
            specs.add(pb.build());
            return this;
        }

        /** from = "pipeline" or "pipeline.port"; to = pipeline id. */
        public Builder edge(String from, String to) {
            rawEdges.add(new String[] {from, to});
            return this;
        }

        public Builder gate(String id, GateDef def, Consumer<GateBuilder> cfg) {
            GateBuilder gb = new GateBuilder();
            if (cfg != null) cfg.accept(gb);
            gates.add(new GateSpec(id, def.getType(), def.getCondition(), Collections.unmodifiableList(gb.after),
                    Collections.unmodifiableList(gb.before), gb.timeoutMillis, gb.policy));
            return this;
        }

        /** Structural validation only (unique ids, existing references); planning validates the rest. */
        public PipelineGraph build() {
            // identifier limits: the control plane's columns / primary keys are sized for them (ControlPlaneStore.MAX_*)
            if (name == null || name.isEmpty() || name.length() > ControlPlaneStore.MAX_GRAPH_NAME_LENGTH)
                throw new PipelinerException("graph name must be 1.." + ControlPlaneStore.MAX_GRAPH_NAME_LENGTH + " characters: " + name);
            Map<String, PipelineSpec> map = new LinkedHashMap<>();
            for (PipelineSpec s : specs) {
                if (s.getId() == null || s.getId().isEmpty()) throw new PipelinerException("pipeline id must not be empty");
                if (s.getId().length() > ControlPlaneStore.MAX_PIPELINE_ID_LENGTH)
                    throw new PipelinerException("pipeline id must be at most " + ControlPlaneStore.MAX_PIPELINE_ID_LENGTH + " characters: " + s.getId());
                if (map.put(s.getId(), s) != null) throw new PipelinerException("duplicate pipeline id: " + s.getId());
                // ':' separates the parts of a batch id / signal key (ChunkIds.batchId); chunk ids embed port names
                if (s.getId().indexOf(':') >= 0) throw new PipelinerException("pipeline id must not contain ':': " + s.getId());
                for (String port : s.getPorts())
                    if (port == null || port.indexOf(':') >= 0) throw new PipelinerException("port name of pipeline " + s.getId() + " must not contain ':': " + port);
            }
            List<EdgeSpec> edges = new ArrayList<>();
            for (String[] e : rawEdges) {
                String from = e[0];
                String port = "out";
                if (!map.containsKey(from) && from != null && from.lastIndexOf('.') > 0) {
                    int i = from.lastIndexOf('.');
                    port = from.substring(i + 1);
                    from = from.substring(0, i);
                }
                if (!map.containsKey(from)) throw new PipelinerException("edge " + e[0] + " -> " + e[1] + ": unknown pipeline '" + from + "'");
                if (!map.containsKey(e[1])) throw new PipelinerException("edge " + e[0] + " -> " + e[1] + ": unknown pipeline '" + e[1] + "'");
                edges.add(new EdgeSpec(from, port, e[1]));
            }
            Set<String> gateIds = new HashSet<>();
            for (GateSpec g : gates) {
                if (g.getId() != null && g.getId().length() > ControlPlaneStore.MAX_GATE_ID_LENGTH)
                    throw new PipelinerException("gate id must be at most " + ControlPlaneStore.MAX_GATE_ID_LENGTH + " characters: " + g.getId());
                if (!gateIds.add(g.getId())) throw new PipelinerException("duplicate gate id: " + g.getId());
                for (String p : g.getAfter()) if (!map.containsKey(p)) throw new PipelinerException("gate " + g.getId() + ": unknown after pipeline '" + p + "'");
                for (String p : g.getBefore()) if (!map.containsKey(p)) throw new PipelinerException("gate " + g.getId() + ": unknown before pipeline '" + p + "'");
            }
            return new PipelineGraph(name, new LinkedHashMap<>(map), edges, new ArrayList<>(gates));
        }
    }

    /** Configures one pipeline. */
    public static final class PipelineBuilder {
        private final String id;
        private SplitSource source;
        private RowRunner rowRunner;
        private BatchRunner batchRunner;
        private Class<? extends RowRunner> rowRunnerType;
        private Class<? extends BatchRunner> batchRunnerType;
        private boolean perThread;
        private final Set<String> ports = new LinkedHashSet<>(Arrays.asList("out"));
        private final Map<String, List<Sink>> sinks = new LinkedHashMap<>();
        private Integer parallelism;
        private ExecMode mode;
        private long estimatedRows = -1;
        private int chunkRows = 5000;
        private int chunkBytes = 1 << 20;
        private final List<String> chunkKey = new ArrayList<>();

        private PipelineBuilder(String id) { this.id = id; }

        public PipelineBuilder source(SplitSource s) { this.source = s; return this; }
        public PipelineBuilder row(RowRunner r) { this.rowRunner = r; return this; }
        public PipelineBuilder batch(BatchRunner r) { this.batchRunner = r; return this; }
        /** Lazy runner: created through the ComponentFactory when the pipeline starts on a pod, released when it finishes. */
        public PipelineBuilder row(Class<? extends RowRunner> type) { this.rowRunnerType = type; return this; }
        public PipelineBuilder batch(Class<? extends BatchRunner> type) { this.batchRunnerType = type; return this; }
        /** Type-based runner only: each worker thread gets its own runner instance (no thread-safety needed). */
        public PipelineBuilder runnerPerThread() { this.perThread = true; return this; }
        public PipelineBuilder ports(String... p) { ports.addAll(Arrays.asList(p)); return this; }
        /** Sink on port "out". */
        public PipelineBuilder sink(Sink s) { return sink("out", s); }
        /** Multiple calls on the same port tee the output. */
        public PipelineBuilder sink(String port, Sink s) {
            ports.add(port);
            List<Sink> l = sinks.get(port);
            if (l == null) sinks.put(port, l = new ArrayList<>());
            l.add(s);
            return this;
        }
        public PipelineBuilder parallelism(int p) { this.parallelism = p; return this; }
        public PipelineBuilder mode(ExecMode m) { this.mode = m; return this; }
        public PipelineBuilder estimatedRows(long n) { this.estimatedRows = n; return this; }
        public PipelineBuilder chunkRows(int n) { this.chunkRows = n; return this; }
        public PipelineBuilder chunkBytes(int n) { this.chunkBytes = n; return this; }
        /** Chunk cuts are only allowed between rows whose key columns differ. */
        public PipelineBuilder chunkKey(String... columns) { chunkKey.addAll(Arrays.asList(columns)); return this; }

        private PipelineSpec build() {
            Map<String, List<Sink>> sk = new LinkedHashMap<>();
            for (Map.Entry<String, List<Sink>> e : sinks.entrySet())
                sk.put(e.getKey(), Collections.unmodifiableList(new ArrayList<>(e.getValue())));
            return new PipelineSpec(id, source, rowRunner, batchRunner, rowRunnerType, batchRunnerType, perThread, Collections.unmodifiableSet(new LinkedHashSet<>(ports)),
                    Collections.unmodifiableMap(sk), parallelism, mode, estimatedRows, chunkRows, chunkBytes,
                    Collections.unmodifiableList(new ArrayList<>(chunkKey)));
        }
    }

    /** Configures one gate. */
    public static final class GateBuilder {
        private final List<String> after = new ArrayList<>();
        private final List<String> before = new ArrayList<>();
        private long timeoutMillis;
        private TimeoutPolicy policy = TimeoutPolicy.FAIL;

        private GateBuilder() {}

        public GateBuilder after(String... ids) { after.addAll(Arrays.asList(ids)); return this; }
        public GateBuilder before(String... ids) { before.addAll(Arrays.asList(ids)); return this; }
        public GateBuilder timeout(Duration d, TimeoutPolicy policy) {
            this.timeoutMillis = d.toMillis();
            this.policy = policy;
            return this;
        }
    }
}
