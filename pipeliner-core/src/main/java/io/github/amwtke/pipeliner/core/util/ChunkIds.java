package io.github.amwtke.pipeliner.core.util;

/** Deterministic chunk id scheme. */
public final class ChunkIds {
    private ChunkIds() {}

    public static String source(int splitIndex) { return String.format("S%04d", splitIndex); }

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

    /** The {@code <attempt>} of a signal key ending in {@code @<n>}, or -1 when it has none (or it is not a number). */
    public static int attemptOfSignalKey(String signalKey) {
        int at = signalKey.lastIndexOf('@');
        if (at < 0) return -1;
        try { return Integer.parseInt(signalKey.substring(at + 1)); } catch (NumberFormatException e) { return -1; }
    }

    /** The pipeline id inside a {@code <round>:<pipeline>:<chunk>[@n]} key (the segment before the chunk id), or null when the key has no such part. */
    public static String pipelineOfSignalKey(String signalKey) {
        int at = signalKey.lastIndexOf('@');
        String k = at < 0 ? signalKey : signalKey.substring(0, at);
        int last = k.lastIndexOf(':');
        if (last <= 0) return null;
        int prev = k.lastIndexOf(':', last - 1);
        if (prev < 0) return null;
        return k.substring(prev + 1, last);
    }

    /** Chunk id passed to Sink.write. */
    public static String sinkChunk(String inputChunkId, String port) { return inputChunkId + "#" + port; }
}
