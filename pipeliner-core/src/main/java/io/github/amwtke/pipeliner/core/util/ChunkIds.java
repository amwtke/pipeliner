package io.github.amwtke.pipeliner.core.util;

/** Deterministic chunk id scheme. */
public final class ChunkIds {
    private ChunkIds() {}

    public static String source(int splitIndex) { return String.format("S%04d", splitIndex); }

    public static String child(String parentChunkId, int edgeOrdinal, int seq) {
        return parentChunkId + ">" + edgeOrdinal + "." + seq;
    }

    /** Chunk id passed to Sink.write. */
    public static String sinkChunk(String inputChunkId, String port) { return inputChunkId + "#" + port; }
}
