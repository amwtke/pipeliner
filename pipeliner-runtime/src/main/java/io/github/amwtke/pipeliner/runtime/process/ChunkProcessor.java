package io.github.amwtke.pipeliner.runtime.process;

import io.github.amwtke.pipeliner.api.BatchRunner;
import io.github.amwtke.pipeliner.api.Chunk;
import io.github.amwtke.pipeliner.api.Collector;
import io.github.amwtke.pipeliner.api.PipelinerException;
import io.github.amwtke.pipeliner.api.Row;
import io.github.amwtke.pipeliner.api.RowRunner;
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
import io.github.amwtke.pipeliner.runtime.instance.PipelineInstance;
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
    private final PipelineInstance.Lease lease;
    private final int attempt;
    private final long writeVersion;

    /** writeVersion = roundSeq * VERSION_RADIX + attempt, so a run attempt must stay below VERSION_RADIX. */
    public static final int VERSION_RADIX = 1000;

    /**
     * The lease supplies the resolved source and runner (own runner when the pipeline is perThread). attempt: the run attempt this
     * processor works for (must be < {@link #VERSION_RADIX}); roundSeq: RoundRecord.seq.
     */
    public ChunkProcessor(Services s, ExecutionPlan plan, String roundId, Map<String, String> params, String pipelineId, PipelineInstance.Lease lease,
                          int attempt, long roundSeq) {
        if (attempt < 0 || attempt >= VERSION_RADIX)
            throw new PipelinerException("run attempt " + attempt + " of pipeline " + pipelineId + " in round " + roundId + " is out of range [0, "
                    + VERSION_RADIX + "): the write version (roundSeq * " + VERSION_RADIX + " + attempt) would collide with the next round's versions; "
                    + "cancel this round and submit a new one");
        this.attempt = attempt;
        this.writeVersion = roundSeq * VERSION_RADIX + attempt;
        this.s = s;
        this.plan = plan;
        this.roundId = roundId;
        this.params = params;
        this.pp = plan.pipeline(pipelineId);
        this.spec = plan.getGraph().getPipelines().get(pipelineId);
        this.lease = lease;
    }

    public PlanPipeline pipeline() { return pp; }

    /** Reads a source split into the input chunk (chunk id = split id). */
    public Chunk readSource(Split split) throws Exception {
        List<Row> rows = lease.instance().source().read(split, new SourceContextImpl(roundId, params, s.dataSources));
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
            SinkContextImpl ctx = new SinkContextImpl(roundId, pp.getRunId(), pp.getPipelineId(), params, s.dataSources, attempt, writeVersion);
            for (Sink sink : e.getValue()) sink.write(sinkChunk, ctx);
        }

        for (Map.Entry<String, String> pe : pp.getPortToEdge().entrySet()) {
            String edgeId = pe.getValue();
            List<Row> rows = byPort.containsKey(pe.getKey()) ? byPort.get(pe.getKey()) : new ArrayList<Row>();
            int ordinal = plan.channel(edgeId).getOrdinal();
            long n = 0;
            int seq = 0;
            List<List<Row>> parts;
            try {
                parts = OutputCutter.cut(rows, pp.getChunkRows(), pp.getChunkBytes(), pp.getChunkKey());
            } catch (PipelinerException ex) {
                throw new PipelinerException("output of port " + pe.getKey() + " of pipeline " + pp.getPipelineId() + ": " + ex.getMessage());
            }
            for (List<Row> part : parts) {
                Chunk c = new Chunk(ChunkIds.child(in.getChunkId(), ordinal, seq++), part, headers(edgeId));
                res.outputs.add(new ChunkResult.EdgeChunk(plan.channel(edgeId), c));
                n += part.size();
            }
            res.rowsOut.put(edgeId, n);
        }
        java.util.Set<String> used = new java.util.HashSet<>(spec.getSinks().keySet());
        used.addAll(pp.getPortToEdge().keySet());
        for (String port : used) { List<Row> l = byPort.get(port); if (l != null) res.rowsOutTotal += l.size(); }
        res.costMillis = (System.nanoTime() - t0) / 1_000_000;
        return res;
    }

    private Map<String, List<Row>> run(Chunk in) throws Exception {
        Buffer out = new Buffer(spec.getPorts(), pp.getPipelineId());
        RunContextImpl ctx = new RunContextImpl(roundId, pp.getRunId(), pp.getPipelineId(), in.getChunkId(), params, s.dataSources, s.fallback, attempt, writeVersion);
        RowRunner row = lease.rowRunner();
        BatchRunner batch = lease.batchRunner();
        if (row != null) {
            for (Row r : in.getRows()) row.process(r, out, ctx);
        } else if (batch != null) {
            batch.process(in.getRows(), out, ctx);
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
        private final java.util.Set<Row> seen = java.util.Collections.newSetFromMap(new java.util.IdentityHashMap<Row, Boolean>());
        private final java.util.Set<String> ports;
        private final String pipelineId;

        Buffer(java.util.Set<String> ports, String pipelineId) { this.ports = ports; this.pipelineId = pipelineId; }

        @Override public void emit(Row row) { emit("out", row); }

        @Override public void emit(String port, Row row) {
            if (!ports.contains(port)) throw new PipelinerException("pipeline " + pipelineId + " emitted to undeclared port '" + port + "'");
            List<Row> l = byPort.get(port);
            if (l == null) { l = new ArrayList<>(); byPort.put(port, l); }
            l.add(seen.add(row) ? row : row.copy());   // one row object per port entry, as after a cluster round trip
        }
    }
}
