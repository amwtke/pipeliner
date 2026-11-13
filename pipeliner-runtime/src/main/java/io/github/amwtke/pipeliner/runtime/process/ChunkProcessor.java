package io.github.amwtke.pipeliner.runtime.process;

import io.github.amwtke.pipeliner.api.Chunk;
import io.github.amwtke.pipeliner.api.Collector;
import io.github.amwtke.pipeliner.api.PipelinerException;
import io.github.amwtke.pipeliner.api.Row;
import io.github.amwtke.pipeliner.api.Sink;
import io.github.amwtke.pipeliner.api.Split;
import io.github.amwtke.pipeliner.core.graph.PipelineSpec;
import io.github.amwtke.pipeliner.core.plan.ExecutionPlan;
import io.github.amwtke.pipeliner.core.plan.PlanPipeline;
import io.github.amwtke.pipeliner.core.util.ChunkIds;
import io.github.amwtke.pipeliner.runtime.Services;
import io.github.amwtke.pipeliner.runtime.context.RunContextImpl;
import io.github.amwtke.pipeliner.runtime.context.SinkContextImpl;
import io.github.amwtke.pipeliner.runtime.context.SourceContextImpl;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Runs one pipeline over one chunk: runner, idempotent sinks, and cutting of stream outputs. Used by all executors. */
public final class ChunkProcessor {
    private final Services s;
    private final ExecutionPlan plan;
    private final String roundId;
    private final Map<String, String> params;
    private final PlanPipeline pp;
    private final PipelineSpec spec;

    public ChunkProcessor(Services s, ExecutionPlan plan, String roundId, Map<String, String> params, String pipelineId) {
        this.s = s;
        this.plan = plan;
        this.roundId = roundId;
        this.params = params;
        this.pp = plan.pipeline(pipelineId);
        this.spec = plan.getGraph().getPipelines().get(pipelineId);
    }

    public PlanPipeline pipeline() { return pp; }

    /** Reads a source split into the input chunk (chunk id = split id). */
    public Chunk readSource(Split split) throws Exception {
        List<Row> rows = spec.getSource().read(split, new SourceContextImpl(roundId, params, s.dataSources));
        return new Chunk(split.getSplitId(), rows, headers(null));
    }

    /** Processes the chunk; safe to repeat (sinks are idempotent, ids are deterministic). */
    public ChunkResult process(Chunk in) throws Exception {
        long t0 = System.nanoTime();
        ChunkResult res = new ChunkResult();
        res.rowsIn = in.size();
        Map<String, List<Row>> byPort = run(in);

        for (Map.Entry<String, List<Sink>> e : spec.getSinks().entrySet()) {
            List<Row> rows = byPort.containsKey(e.getKey()) ? byPort.get(e.getKey()) : new ArrayList<Row>();
            res.rowsOut.put("sink:" + e.getKey(), (long) rows.size());
            if (rows.isEmpty()) continue;
            Chunk sinkChunk = new Chunk(ChunkIds.sinkChunk(in.getChunkId(), e.getKey()), rows, headers(null));
            SinkContextImpl ctx = new SinkContextImpl(roundId, pp.getRunId(), pp.getPipelineId(), params, s.dataSources);
            for (Sink sink : e.getValue()) sink.write(sinkChunk, ctx);
        }

        for (Map.Entry<String, String> pe : pp.getPortToEdge().entrySet()) {
            String edgeId = pe.getValue();
            List<Row> rows = byPort.containsKey(pe.getKey()) ? byPort.get(pe.getKey()) : new ArrayList<Row>();
            int ordinal = plan.channel(edgeId).getOrdinal();
            long n = 0;
            int seq = 0;
            for (List<Row> part : OutputCutter.cut(rows, pp.getChunkRows(), pp.getChunkBytes(), pp.getChunkKey())) {
                Chunk c = new Chunk(ChunkIds.child(in.getChunkId(), ordinal, seq++), part, headers(edgeId));
                res.outputs.add(new ChunkResult.EdgeChunk(plan.channel(edgeId), c));
                n += part.size();
            }
            res.rowsOut.put(edgeId, n);
        }
        res.costMillis = (System.nanoTime() - t0) / 1_000_000;
        return res;
    }

    private Map<String, List<Row>> run(Chunk in) throws Exception {
        Buffer out = new Buffer(spec.getPorts(), pp.getPipelineId());
        RunContextImpl ctx = new RunContextImpl(roundId, pp.getRunId(), pp.getPipelineId(), in.getChunkId(), params, s.dataSources, s.fallback);
        if (spec.getRowRunner() != null) {
            for (Row r : in.getRows()) spec.getRowRunner().process(r, out, ctx);
        } else if (spec.getBatchRunner() != null) {
            spec.getBatchRunner().process(in.getRows(), out, ctx);
        } else {
            for (Row r : in.getRows()) out.emit(r);
        }
        return out.byPort;
    }

    private Map<String, String> headers(String edgeId) {
        Map<String, String> h = new LinkedHashMap<>();
        h.put(Chunk.H_ROUND, roundId);
        h.put(Chunk.H_RUN, pp.getRunId());
        h.put(Chunk.H_PIPELINE, pp.getPipelineId());
        if (edgeId != null) h.put(Chunk.H_EDGE, edgeId);
        return h;
    }

    private static final class Buffer implements Collector {
        final Map<String, List<Row>> byPort = new LinkedHashMap<>();
        private final java.util.Set<String> ports;
        private final String pipelineId;

        Buffer(java.util.Set<String> ports, String pipelineId) { this.ports = ports; this.pipelineId = pipelineId; }

        @Override public void emit(Row row) { emit("out", row); }

        @Override public void emit(String port, Row row) {
            if (!ports.contains(port)) throw new PipelinerException("pipeline " + pipelineId + " emitted to undeclared port '" + port + "'");
            List<Row> l = byPort.get(port);
            if (l == null) { l = new ArrayList<>(); byPort.put(port, l); }
            l.add(row);
        }
    }
}
