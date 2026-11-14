package io.github.amwtke.pipeliner.core.util;

/** Deterministic chunk id scheme. */
public final class ChunkIds {
    private ChunkIds() {}

    public static String source(int splitIndex) { return String.format(java.util.Locale.ROOT, "S%04d", splitIndex); }

    public static String child(String parentChunkId, int edgeOrdinal, int seq) {
        return parentChunkId + ">" + edgeOrdinal + "." + seq;
    }

    /**
     * Globally unique idempotency id for external systems: {@code <roundId>:<pipelineId>:<chunkId>@<attempt>} (what
     * {@code SinkContext.batchId(Chunk)} returns). Chunk ids repeat in every round and in every source pipeline of a round, so both the
     * round and the pipeline are part of the id; the attempt suffix is what {@code Gates.externalCountOf} matches. Pipeline ids and port
     * names never contain ':' (the graph builder rejects them) and chunk ids are built only from {@code S0000}, '&gt;', '.', digits and
     * '#' plus a port name, so the last ':' always starts the chunk id.
     */
    public static String batchId(String roundId, String pipelineId, String chunkId, int attempt) {
        return roundId + ":" + pipelineId + ":" + chunkId + "@" + attempt;
    }

    /** The chunk id inside a signal key / batch id: strips a trailing {@code @<attempt>} and everything up to the last ':'. */
    public static String chunkOfSignalKey(String signalKey) {
        int at = signalKey.lastIndexOf('@');
        String k = at < 0 ? signalKey : signalKey.substring(0, at);
        return k.substring(k.lastIndexOf(':') + 1);
    }

    /** Chunk id passed to Sink.write. */
    public static String sinkChunk(String inputChunkId, String port) { return inputChunkId + "#" + port; }
}
