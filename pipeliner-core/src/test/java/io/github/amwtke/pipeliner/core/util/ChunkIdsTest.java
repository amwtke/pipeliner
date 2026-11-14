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
        assertEquals("S0001@3", ChunkIds.batchId("S0001", 3));
        assertEquals("r1:S0001@3", ChunkIds.batchId("r1", "S0001", 3));
    }

    @Test void batchIdsDifferAcrossRoundsForTheSameChunk() {
        String a = ChunkIds.batchId("net-demand-202610-aa", ChunkIds.source(3), 0);
        String b = ChunkIds.batchId("net-demand-202611-bb", ChunkIds.source(3), 0);
        assertNotEquals(a, b);
        assertTrue(a.endsWith("@0") && b.endsWith("@0"));
        assertNotEquals(ChunkIds.batchId("r", "S0003", 0), ChunkIds.batchId("r", "S0003", 1));
    }
}
