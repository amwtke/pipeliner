package io.github.amwtke.pipeliner.core.util;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

class ChunkIdsTest {
    @Test void ids() {
        assertEquals("S0012", ChunkIds.source(12));
        assertEquals("S0012>2.0", ChunkIds.child("S0012", 2, 0));
        assertEquals("S0012>2.0>5.1", ChunkIds.child(ChunkIds.child("S0012", 2, 0), 5, 1));
        assertEquals("S0001#out", ChunkIds.sinkChunk("S0001", "out"));
    }
}
