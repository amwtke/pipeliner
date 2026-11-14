package io.github.amwtke.pipeliner.core.util;

/** Deterministic chunk id scheme. */
public final class ChunkIds {
    private ChunkIds() {}

    public static String source(int splitIndex) { return String.format("S%04d", splitIndex); }

    public static String child(String parentChunkId, int edgeOrdinal, int seq) {
        return parentChunkId + ">" + edgeOrdinal + "." + seq;
    }

    /**
     * Globally unique idempotency id for external systems: {@code <roundId>:<chunkId>@<attempt>}. Chunk ids are re-derived identically
     * in every round, so the round id is part of the id; the attempt suffix is what {@code Gates.externalCountOf} matches.
     */
    public static String batchId(String roundId, String chunkId, int attempt) { return roundId + ":" + chunkId + "@" + attempt; }

    /** @deprecated not unique across rounds (chunk ids repeat every round); use {@link #batchId(String, String, int)}. */
    @Deprecated
    public static String batchId(String chunkId, int attempt) { return chunkId + "@" + attempt; }

    /** Chunk id passed to Sink.write. */
    public static String sinkChunk(String inputChunkId, String port) { return inputChunkId + "#" + port; }
}
