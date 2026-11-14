package io.github.amwtke.pipeliner.core.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class ChunkIdsTest {
    @Test void ids() {
        assertEquals("S0012", ChunkIds.source(12));
        assertEquals("S0012>2.0", ChunkIds.child("S0012", 2, 0));
        assertEquals("S0012>2.0>5.1", ChunkIds.child(ChunkIds.child("S0012", 2, 0), 5, 1));
        assertEquals("S0001#out", ChunkIds.sinkChunk("S0001", "out"));
        assertEquals("r1:p:S0001@3", ChunkIds.batchId("r1", "p", "S0001", 3));
    }

    @Test void chunkOfSignalKeyHandlesEveryFormat() {
        assertEquals("S0003#out", ChunkIds.chunkOfSignalKey("S0003#out@0"));                 // oldest: chunk@attempt
        assertEquals("S0003#out", ChunkIds.chunkOfSignalKey("r-1:S0003#out@0"));             // round:chunk@attempt
        assertEquals("S0003#out", ChunkIds.chunkOfSignalKey("r-1:p:S0003#out@0"));           // round:pipeline:chunk@attempt
        assertEquals("S0003>1.2#out", ChunkIds.chunkOfSignalKey(ChunkIds.batchId("r:x", "p", "S0003>1.2#out", 12)));
        assertEquals("legacy-key", ChunkIds.chunkOfSignalKey("legacy-key"));
    }

    @Test void batchIdsDifferAcrossPipelinesOfTheSameRound() {
        // every source pipeline numbers its splits from S0000, sink chunk ids only append the port
        assertNotEquals(ChunkIds.batchId("r", "a", ChunkIds.sinkChunk("S0000", "out"), 0), ChunkIds.batchId("r", "b", ChunkIds.sinkChunk("S0000", "out"), 0));
    }

    @Test void batchIdsDifferAcrossRoundsForTheSameChunk() {
        String a = ChunkIds.batchId("net-demand-202610-aa", "p", ChunkIds.source(3), 0);
        String b = ChunkIds.batchId("net-demand-202611-bb", "p", ChunkIds.source(3), 0);
        assertNotEquals(a, b);
        assertTrue(a.endsWith("@0") && b.endsWith("@0"));
        assertNotEquals(ChunkIds.batchId("r", "p", "S0003", 0), ChunkIds.batchId("r", "p", "S0003", 1));
    }
}
