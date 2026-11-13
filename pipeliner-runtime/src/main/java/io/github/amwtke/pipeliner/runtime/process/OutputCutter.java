package io.github.amwtke.pipeliner.runtime.process;

import io.github.amwtke.pipeliner.api.Row;
import io.github.amwtke.pipeliner.core.codec.ChunkCodec;
import java.util.ArrayList;
import java.util.List;

/** Cuts the rows of one output port into chunks by row count and byte size, never inside a run of equal chunkKey values. */
public final class OutputCutter {
    private OutputCutter() {}

    /** Deterministic; returns no empty chunks. An oversized key group stays whole. */
    public static List<List<Row>> cut(List<Row> rows, int chunkRows, int chunkBytes, List<String> chunkKey) {
        int maxRows = chunkRows <= 0 ? Integer.MAX_VALUE : chunkRows;
        long maxBytes = chunkBytes <= 0 ? Long.MAX_VALUE : chunkBytes;
        boolean keyed = chunkKey != null && !chunkKey.isEmpty();
        List<List<Row>> out = new ArrayList<>();
        List<Row> cur = new ArrayList<>();
        long bytes = 0;
        List<String> prevKey = null;
        for (Row row : rows) {
            List<String> key = keyed ? keyOf(row, chunkKey) : null;
            long rb = ChunkCodec.estimateBytes(row);
            boolean full = cur.size() >= maxRows || bytes + rb > maxBytes;
            boolean boundary = !keyed || !key.equals(prevKey);
            if (!cur.isEmpty() && full && boundary) {
                out.add(cur);
                cur = new ArrayList<>();
                bytes = 0;
            }
            cur.add(row);
            bytes += rb;
            prevKey = key;
        }
        if (!cur.isEmpty()) out.add(cur);
        return out;
    }

    private static List<String> keyOf(Row row, List<String> cols) {
        List<String> k = new ArrayList<>(cols.size());
        for (String c : cols) k.add(String.valueOf(row.get(c)));
        return k;
    }
}
