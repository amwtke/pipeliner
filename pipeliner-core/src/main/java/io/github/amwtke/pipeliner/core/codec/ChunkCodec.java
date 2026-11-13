package io.github.amwtke.pipeliner.core.codec;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.core.JsonGenerator;
import io.github.amwtke.pipeliner.api.Chunk;
import io.github.amwtke.pipeliner.api.PipelinerException;
import io.github.amwtke.pipeliner.api.Row;
import io.github.amwtke.pipeliner.api.Split;
import io.github.amwtke.pipeliner.core.store.ChunkEvent;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** JSON codec for chunks, splits and events. Thread-safe. */
public final class ChunkCodec {
    private static final ObjectMapper M = new ObjectMapper()
            .configure(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS, true)
            .configure(DeserializationFeature.USE_LONG_FOR_INTS, true)
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
            .configure(JsonGenerator.Feature.WRITE_BIGDECIMAL_AS_PLAIN, true)
            .setDefaultPropertyInclusion(JsonInclude.Value.construct(JsonInclude.Include.NON_NULL, JsonInclude.Include.ALWAYS));

    private ChunkCodec() {}

    static final class ChunkDto {
        public String id;
        public Map<String, String> h;
        public List<LinkedHashMap<String, Object>> rows;
    }

    static final class SplitDto {
        public String id;
        public Map<String, String> spec;
    }

    public static String encodeChunk(Chunk c) {
        ChunkDto d = new ChunkDto();
        d.id = c.getChunkId();
        d.h = c.getHeaders();
        d.rows = new ArrayList<>(c.size());
        for (Row r : c.getRows()) d.rows.add(new LinkedHashMap<>(r.asMap()));
        return toJson(d);
    }

    public static Chunk decodeChunk(String json) {
        ChunkDto d = fromJson(json, ChunkDto.class);
        List<Row> rows = new ArrayList<>();
        if (d.rows != null) for (Map<String, Object> m : d.rows) rows.add(new Row(m));
        return new Chunk(d.id, rows, d.h);
    }

    public static String encodeSplit(Split s) {
        SplitDto d = new SplitDto();
        d.id = s.getSplitId();
        d.spec = s.getSpec();
        return toJson(d);
    }

    public static Split decodeSplit(String json) {
        SplitDto d = fromJson(json, SplitDto.class);
        return new Split(d.id, d.spec);
    }

    public static String encodeEvent(ChunkEvent e) { return toJson(e); }

    public static ChunkEvent decodeEvent(String json) { return fromJson(json, ChunkEvent.class); }

    public static String toJson(Object o) {
        try {
            return M.writeValueAsString(o);
        } catch (JsonProcessingException e) {
            throw new PipelinerException("json encode failed", e);
        }
    }

    public static <T> T fromJson(String json, Class<T> type) {
        try {
            return M.readValue(json, type);
        } catch (JsonProcessingException e) {
            throw new PipelinerException("json decode failed: " + e.getOriginalMessage(), e);
        }
    }

    public static Map<String, String> toStringMap(String json) {
        if (json == null || json.isEmpty()) return new LinkedHashMap<>();
        try {
            return M.readValue(json, new TypeReference<LinkedHashMap<String, String>>() {});
        } catch (JsonProcessingException e) {
            throw new PipelinerException("json decode failed: " + e.getOriginalMessage(), e);
        }
    }

    /** Cheap size estimate of a row, used for chunkBytes splitting. */
    public static int estimateBytes(Row row) {
        int n = 2;
        for (Map.Entry<String, Object> e : row.asMap().entrySet()) {
            n += e.getKey().length() + 4;
            Object v = e.getValue();
            if (v == null) n += 4;
            else if (v instanceof String) n += ((String) v).length() + 2;
            else if (v instanceof Boolean) n += 5;
            else if (v instanceof Integer || v instanceof Long) n += 8;
            else n += v.toString().length();
        }
        return n;
    }
}
