package io.github.amwtke.pipeliner.api;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** One unit of work. Immutable. */
public final class Chunk {
    public static final String H_ROUND = "roundId", H_RUN = "runId", H_PIPELINE = "pipelineId", H_EDGE = "edgeId";
    private final String chunkId;
    private final List<Row> rows;
    private final Map<String, String> headers;

    public Chunk(String chunkId, List<Row> rows, Map<String, String> headers) {
        this.chunkId = chunkId;
        this.rows = Collections.unmodifiableList(rows == null ? new ArrayList<Row>() : new ArrayList<>(rows));
        this.headers = Collections.unmodifiableMap(headers == null ? new LinkedHashMap<String, String>() : new LinkedHashMap<>(headers));
    }

    public String getChunkId() { return chunkId; }
    public List<Row> getRows() { return rows; }
    public Map<String, String> getHeaders() { return headers; }
    public int size() { return rows.size(); }

    @Override public String toString() { return "Chunk[" + chunkId + ", rows=" + rows.size() + "]"; }
}
