package io.github.amwtke.pipeliner.runtime;

import static org.junit.jupiter.api.Assertions.*;

import io.github.amwtke.pipeliner.api.PipelinerException;
import io.github.amwtke.pipeliner.api.Row;
import io.github.amwtke.pipeliner.runtime.process.OutputCutter;
import java.util.*;
import org.junit.jupiter.api.Test;

class OutputCutterTest {
    private static List<Row> rows(int... keys) {
        List<Row> l = new ArrayList<>();
        for (int i = 0; i < keys.length; i++) l.add(Row.of("k", (long) keys[i], "n", (long) i));
        return l;
    }

    private static List<Integer> sizes(List<List<Row>> parts) {
        List<Integer> s = new ArrayList<>();
        for (List<Row> p : parts) s.add(p.size());
        return s;
    }

    @Test void ungroupedOutputIsRejected() {
        // key 1 reappears after key 2 closed its group: cutting it silently would split the group over two chunks
        PipelinerException ex = assertThrows(PipelinerException.class,
                () -> OutputCutter.cut(rows(1, 1, 2, 1), 100, 1 << 20, Arrays.asList("k")));
        assertTrue(ex.getMessage().contains("not grouped by chunkKey"), ex.getMessage());
        // grouped output and unkeyed output are fine, even with repeated values
        assertEquals(Arrays.asList(4), sizes(OutputCutter.cut(rows(1, 1, 2, 2), 100, 1 << 20, Arrays.asList("k"))));
        assertEquals(Arrays.asList(4), sizes(OutputCutter.cut(rows(1, 2, 1, 2), 100, 1 << 20, Collections.<String>emptyList())));
    }

    @Test void cutsByRowCount() {
        assertEquals(Arrays.asList(3, 3, 1), sizes(OutputCutter.cut(rows(1, 2, 3, 4, 5, 6, 7), 3, 1 << 20, Collections.<String>emptyList())));
    }

    @Test void neverEmptyChunks() {
        assertTrue(OutputCutter.cut(new ArrayList<Row>(), 3, 100, Collections.<String>emptyList()).isEmpty());
        for (List<Row> p : OutputCutter.cut(rows(1, 1, 2, 2, 3), 1, 1, Arrays.asList("k"))) assertFalse(p.isEmpty());
    }

    @Test void neverSplitsAKeyGroup() {
        // groups: 1,1,1 | 2,2 | 3 ; chunkRows=2 would cut inside group 1 without chunkKey
        List<List<Row>> parts = OutputCutter.cut(rows(1, 1, 1, 2, 2, 3), 2, 1 << 20, Arrays.asList("k"));
        assertEquals(Arrays.asList(3, 2, 1), sizes(parts));
        Set<Object> seen = new HashSet<>();
        for (List<Row> p : parts) {
            Set<Object> keysInPart = new HashSet<>();
            for (Row r : p) keysInPart.add(r.get("k"));
            for (Object k : keysInPart) assertTrue(seen.add(k), "key " + k + " appears in two chunks");
        }
    }

    @Test void oversizedGroupStaysWhole() {
        List<List<Row>> parts = OutputCutter.cut(rows(7, 7, 7, 7, 7, 8), 2, 1 << 20, Arrays.asList("k"));
        assertEquals(Arrays.asList(5, 1), sizes(parts));
    }

    @Test void cutsByBytesAndRespectsKey() {
        List<Row> rs = rows(1, 2, 3, 4, 5, 6);
        int one = io.github.amwtke.pipeliner.core.codec.ChunkCodec.estimateBytes(rs.get(0));
        assertEquals(Arrays.asList(2, 2, 2), sizes(OutputCutter.cut(rs, 100, one * 2, Collections.<String>emptyList())));
        List<List<Row>> keyed = OutputCutter.cut(rows(1, 1, 1, 1, 2), 100, one * 2, Arrays.asList("k"));
        assertEquals(Arrays.asList(4, 1), sizes(keyed));
    }

    @Test void deterministic() {
        List<Row> in = rows(1, 1, 2, 3, 3, 3, 4);
        assertEquals(OutputCutter.cut(in, 2, 50, Arrays.asList("k")), OutputCutter.cut(in, 2, 50, Arrays.asList("k")));
    }
}
