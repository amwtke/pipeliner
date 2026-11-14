package io.github.amwtke.pipeliner.core.store;

import io.github.amwtke.pipeliner.core.codec.ChunkCodec;
import java.util.EnumMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Helpers shared by every {@link ControlPlaneStore} implementation (stores and the in-memory fake): summaries, row totals, JSON maps. */
public final class StoreViews {
    private StoreViews() {}

    /** Sum of all values of a chunk's rowsOut (null-safe). */
    public static long totalOf(Map<String, Long> m) {
        long t = 0;
        if (m != null) for (Long v : m.values()) if (v != null) t += v;
        return t;
    }

    /** Rows written to sinks: the {@code "sink:<port>"} entries of a chunk's rowsOut. */
    public static long sinkRowsOf(Map<String, Long> m) {
        long t = 0;
        if (m != null) for (Map.Entry<String, Long> e : m.entrySet()) if (e.getKey().startsWith("sink:") && e.getValue() != null) t += e.getValue();
        return t;
    }

    /** JSON of a value, null for null. */
    public static String json(Object o) { return o == null ? null : ChunkCodec.toJson(o); }

    /** Parses a JSON object of numbers (a pipeline's stored summary counts); null for null. */
    public static Map<String, Long> longMap(String js) {
        if (js == null) return null;
        Map<?, ?> raw = ChunkCodec.fromJson(js, Map.class);
        Map<String, Long> out = new LinkedHashMap<>();
        for (Map.Entry<?, ?> e : raw.entrySet()) out.put(String.valueOf(e.getKey()), ((Number) e.getValue()).longValue());
        return out;
    }

    /**
     * Adds the compaction summaries of pipelines whose chunk records are gone (summaryCounts set, no live counts) to the view maps.
     * Counts of 0 are dropped, and a pipeline whose summary is empty contributes nothing.
     */
    public static void mergeSummaries(List<PipelineRecord> pipelines, Map<String, Map<ChunkStatus, Long>> counts, Map<String, Long> rowsIn, Map<String, Long> rowsOut) {
        for (PipelineRecord p : pipelines) {
            if (p.getSummaryCounts() == null || counts.containsKey(p.getPipelineId())) continue;
            Map<ChunkStatus, Long> m = new EnumMap<>(ChunkStatus.class);
            for (Map.Entry<String, Long> e : p.getSummaryCounts().entrySet()) if (e.getValue() > 0) m.put(ChunkStatus.valueOf(e.getKey()), e.getValue());
            if (m.isEmpty()) continue;
            counts.put(p.getPipelineId(), m);
            if (p.getSummaryRowsIn() != null) rowsIn.put(p.getPipelineId(), p.getSummaryRowsIn());
            if (p.getSummaryRowsOut() != null) rowsOut.put(p.getPipelineId(), p.getSummaryRowsOut());
        }
    }
}
