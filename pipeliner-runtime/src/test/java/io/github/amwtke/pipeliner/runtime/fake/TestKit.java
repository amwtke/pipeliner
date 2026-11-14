package io.github.amwtke.pipeliner.runtime.fake;

import io.github.amwtke.pipeliner.api.*;
import io.github.amwtke.pipeliner.core.engine.RuntimeConfig;
import io.github.amwtke.pipeliner.core.graph.ExecMode;
import io.github.amwtke.pipeliner.core.graph.PipelineGraph;
import io.github.amwtke.pipeliner.core.plan.ExecutionPlan;
import io.github.amwtke.pipeliner.core.util.SimpleDataSourceRegistry;
import io.github.amwtke.pipeliner.runtime.DefaultPipelinerEngine;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;

/** Shared test helpers: sources, sinks, config, engine harness. */
public final class TestKit {
    private TestKit() {}

    /** N splits of M rows: {id, g}. */
    public static final class RangeSource implements SplitSource {
        private final int splits, rows;
        public RangeSource(int splits, int rows) { this.splits = splits; this.rows = rows; }
        @Override public String datasetName() { return null; }
        @Override public List<Split> enumerate(SourceContext ctx) {
            List<Split> l = new ArrayList<>();
            for (int i = 0; i < splits; i++) l.add(new Split(null, Collections.singletonMap("i", String.valueOf(i))));
            return l;
        }
        @Override public List<Row> read(Split split, SourceContext ctx) {
            long i = Long.parseLong(split.getSpec().get("i"));
            List<Row> l = new ArrayList<>();
            for (long j = 0; j < rows; j++) l.add(Row.of("id", i * rows + j, "g", i));
            return l;
        }
    }

    private static final Map<String, Map<String, List<Row>>> SINKS = new ConcurrentHashMap<>();
    private static final AtomicInteger SEQ = new AtomicInteger();

    public static String uniqueName(String prefix) { return prefix + SEQ.incrementAndGet(); }

    /** Idempotent collecting sink: sink chunk id -> rows. */
    public static Sink collect(final String name) {
        SINKS.put(name, new ConcurrentHashMap<String, List<Row>>());
        return new Sink() {
            @Override public String datasetName() { return null; }
            @Override public void write(Chunk chunk, SinkContext ctx) { SINKS.get(name).put(chunk.getChunkId(), new ArrayList<>(chunk.getRows())); }
        };
    }

    public static Map<String, List<Row>> sinkChunks(String name) { return new TreeMap<>(SINKS.get(name)); }

    /** All rows written to the sink, as sorted strings (duplicates preserved). */
    public static List<String> sinkRows(String name) {
        List<String> l = new ArrayList<>();
        for (List<Row> rows : SINKS.get(name).values()) for (Row r : rows) l.add(r.toString());
        Collections.sort(l);
        return l;
    }

    public static RuntimeConfig.RuntimeConfigBuilder cfg(String pod) {
        return RuntimeConfig.builder().podId(pod).tickMillis(50).leaderTtlMillis(600).heartbeatMillis(50).workerDeadMillis(600)
                .claimIdleMillis(500).renewIntervalMillis(100).autoClaimIntervalMillis(100).readBlockMillis(50)
                .maxAttempts(2).retryBackoffMillis(20).journalBatch(100).autoscale(false);
    }

    public static boolean waitFor(BooleanSupplier c, long timeoutMs) {
        long end = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < end) {
            if (c.getAsBoolean()) return true;
            try { Thread.sleep(10); } catch (InterruptedException e) { Thread.currentThread().interrupt(); return false; }
        }
        return c.getAsBoolean();
    }

    /** Shared store + plane with any number of engines ("pods"). */
    public static final class Harness implements AutoCloseable {
        public final InMemoryControlPlaneStore store = new InMemoryControlPlaneStore();
        public final InMemoryDataPlane plane = new InMemoryDataPlane();
        private final List<DefaultPipelinerEngine> engines = new ArrayList<>();
        private final ExecutionPlan plan;
        private final ComponentFactory factory;

        public Harness(PipelineGraph g, Map<String, ExecMode> modes) { this(g, modes, new io.github.amwtke.pipeliner.core.util.ReflectionComponentFactory()); }

        public Harness(PipelineGraph g, Map<String, ExecMode> modes, ComponentFactory factory) {
            this.plan = TestPlanner.plan(g, modes);
            this.factory = factory;
        }

        public ExecutionPlan plan() { return plan; }

        public DefaultPipelinerEngine pod(RuntimeConfig cfg, boolean start) { return pod(cfg, start, null); }

        public DefaultPipelinerEngine pod(RuntimeConfig cfg, boolean start, io.github.amwtke.pipeliner.core.engine.PipelinerMetrics metrics) {
            DefaultPipelinerEngine e = new DefaultPipelinerEngine(cfg, store, plane, new SimpleDataSourceRegistry(new HashMap<String, javax.sql.DataSource>()), metrics, factory);
            e.registerPlan(plan);
            engines.add(e);
            if (start) e.start();
            return e;
        }

        /** Master+worker pod. */
        public DefaultPipelinerEngine pod(String name) { return pod(cfg(name).build(), true); }

        @Override public void close() { for (DefaultPipelinerEngine e : engines) e.halt(); }
    }

    public static Map<String, ExecMode> mode(ExecMode m, String... pipelines) {
        Map<String, ExecMode> r = new HashMap<>();
        for (String p : pipelines) r.put(p, m);
        return r;
    }
}
