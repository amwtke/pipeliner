package io.github.amwtke.pipeliner.core.codec;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.amwtke.pipeliner.api.Chunk;
import io.github.amwtke.pipeliner.api.Row;
import io.github.amwtke.pipeliner.api.Split;
import io.github.amwtke.pipeliner.core.store.ChunkEvent;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class ChunkCodecTest {
    @Test void chunkRoundTrip() {
        List<Row> rows = new ArrayList<>();
        rows.add(Row.of("z", 1L, "a", new BigDecimal("12.3400"), "s", "txt", "n", null, "b", true));
        rows.add(Row.of("z", Long.MAX_VALUE, "a", new BigDecimal("0.1"), "s", "", "n", null, "b", false));
        Chunk c = new Chunk("S0001>1.0", rows, Collections.singletonMap(Chunk.H_ROUND, "r1"));
        Chunk d = ChunkCodec.decodeChunk(ChunkCodec.encodeChunk(c));
        assertEquals("S0001>1.0", d.getChunkId());
        assertEquals("r1", d.getHeaders().get(Chunk.H_ROUND));
        assertEquals(2, d.size());
        Row r = d.getRows().get(0);
        assertEquals(Arrays.asList("z", "a", "s", "n", "b"), new ArrayList<>(r.columns()));
        assertTrue(r.get("z") instanceof Long);
        assertEquals(new BigDecimal("12.3400"), r.get("a"));
        assertEquals(Long.MAX_VALUE, d.getRows().get(1).get("z"));
        assertEquals(rows, d.getRows());
    }

    @Test void splitAndEvent() {
        Map<String, String> spec = new HashMap<>();
        spec.put("mode", "hash");
        Split s = ChunkCodec.decodeSplit(ChunkCodec.encodeSplit(new Split("S0000", spec)));
        assertEquals("S0000", s.getSplitId());
        assertEquals("hash", s.getSpec().get("mode"));
        ChunkEvent e = ChunkEvent.builder().type("DONE").roundId("r").chunkId("c").rowsIn(5L)
                .rowsOut(Collections.singletonMap("e1", 3L)).attempt(1).ts(42).build();
        ChunkEvent f = ChunkCodec.decodeEvent(ChunkCodec.encodeEvent(e));
        assertEquals(e, f);
        assertEquals("1", ChunkCodec.toStringMap("{\"a\":\"1\"}").get("a"));
        assertTrue(ChunkCodec.estimateBytes(Row.of("a", "xyz")) > 3);
    }
}
