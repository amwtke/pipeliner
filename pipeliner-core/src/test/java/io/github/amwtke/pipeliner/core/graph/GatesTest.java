package io.github.amwtke.pipeliner.core.graph;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.github.amwtke.pipeliner.api.DataSourceRegistry;
import io.github.amwtke.pipeliner.api.GateContext;
import io.github.amwtke.pipeliner.api.GateDecision;
import java.util.LinkedHashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

class GatesTest {
    private static GateContext ctx(Map<String, String> signals, long done, int attempt) { return ctx(signals, done, done, attempt); }

    /** done: DONE chunks; sink: DONE chunks that called the sink. */
    private static GateContext ctx(Map<String, String> signals, long done, long sink, int attempt) {
        return new GateContext() {
            @Override public long sinkChunkCount(String pipelineId) { return sink; }
            public String roundId() { return "r"; }
            public String gateId() { return "g"; }
            public Map<String, String> params() { return null; }
            public Map<String, String> signals() { return signals; }
            public long armedAtMillis() { return 0; }
            public long nowMillis() { return 0; }
            public long doneChunkCount(String pipelineId) { return done; }
            public DataSourceRegistry dataSources() { return null; }
            @Override public int runAttemptOf(String pipelineId) { return attempt; }
        };
    }

    @Test void externalCountOfIgnoresSignalsOfOlderAttempts() throws Exception {
        Map<String, String> sig = new LinkedHashMap<>();
        sig.put("S0000@1", "x");
        sig.put("S0001@1", "x");
        sig.put("S0002@1", "x");
        // after a rerun the run is at attempt 2: three old signals must not open a gate that waits for three chunks
        assertEquals(GateDecision.WAIT, Gates.externalCountOf("p").getCondition().evaluate(ctx(sig, 3, 2)));
        sig.put("S0000@2", "x");
        sig.put("S0001@2", "x");
        assertEquals(GateDecision.WAIT, Gates.externalCountOf("p").getCondition().evaluate(ctx(sig, 3, 2)));
        sig.put("S0002@2", "x");
        assertEquals(GateDecision.OPEN, Gates.externalCountOf("p").getCondition().evaluate(ctx(sig, 3, 2)));
        // suffix-safe: "@12" must not count for attempt 2
        Map<String, String> tricky = new LinkedHashMap<>();
        tricky.put("S0000@12", "x");
        assertEquals(GateDecision.WAIT, Gates.externalCountOf("p").getCondition().evaluate(ctx(tricky, 1, 2)));
    }

    @Test void roundScopedBatchIdsCount() throws Exception {
        Map<String, String> sig = new LinkedHashMap<>();
        sig.put(io.github.amwtke.pipeliner.core.util.ChunkIds.batchId("r", "p", "S0000", 2), "x");
        sig.put(io.github.amwtke.pipeliner.core.util.ChunkIds.batchId("r", "p", "S0001", 2), "x");
        sig.put(io.github.amwtke.pipeliner.core.util.ChunkIds.batchId("r", "p", "S0002", 1), "x");   // older attempt
        assertEquals(GateDecision.WAIT, Gates.externalCountOf("p").getCondition().evaluate(ctx(sig, 3, 2)));
        sig.put(io.github.amwtke.pipeliner.core.util.ChunkIds.batchId("r", "p", "S0002", 2), "x");
        assertEquals(GateDecision.OPEN, Gates.externalCountOf("p").getCondition().evaluate(ctx(sig, 3, 2)));
    }

    @Test void unscopedSignalKeysStillCount() throws Exception {
        Map<String, String> sig = new LinkedHashMap<>();
        sig.put("legacy-key", "x");
        assertEquals(GateDecision.OPEN, Gates.externalCountOf("p").getCondition().evaluate(ctx(sig, 1, 5)));
    }

    @Test void countsDistinctChunksNotRawKeys() throws Exception {
        Map<String, String> sig = new LinkedHashMap<>();
        // a rolling deploy: the old pod signalled "S0003#out@0", the new pod re-ran the chunk and signalled the new format: one chunk, two keys
        sig.put("S0003#out@0", "x");
        sig.put(io.github.amwtke.pipeliner.core.util.ChunkIds.batchId("r", "p", "S0003#out", 0), "x");
        sig.put("r:S0003#out@0", "x");
        sig.put(io.github.amwtke.pipeliner.core.util.ChunkIds.batchId("r", "p", "S0004#out", 0), "x");
        assertEquals(GateDecision.WAIT, Gates.externalCountOf("p").getCondition().evaluate(ctx(sig, 3, 0)), "two distinct chunks of three");
        sig.put(io.github.amwtke.pipeliner.core.util.ChunkIds.batchId("r", "p", "S0005#out", 0), "x");
        assertEquals(GateDecision.OPEN, Gates.externalCountOf("p").getCondition().evaluate(ctx(sig, 3, 0)));
    }

    @Test void expectsOnlyChunksThatCalledTheSink() throws Exception {
        Map<String, String> sig = new LinkedHashMap<>();
        sig.put(io.github.amwtke.pipeliner.core.util.ChunkIds.batchId("r", "p", "S0001#out", 0), "x");
        sig.put(io.github.amwtke.pipeliner.core.util.ChunkIds.batchId("r", "p", "S0003#out", 0), "x");
        // 8 DONE chunks, but 4 of them were empty and never called the sink: 2 of 4 signals is not enough, 4 of 4 is
        assertEquals(GateDecision.WAIT, Gates.externalCountOf("p").getCondition().evaluate(ctx(sig, 8, 4, 0)));
        sig.put(io.github.amwtke.pipeliner.core.util.ChunkIds.batchId("r", "p", "S0005#out", 0), "x");
        sig.put(io.github.amwtke.pipeliner.core.util.ChunkIds.batchId("r", "p", "S0007#out", 0), "x");
        assertEquals(GateDecision.OPEN, Gates.externalCountOf("p").getCondition().evaluate(ctx(sig, 8, 4, 0)));
        assertEquals(GateDecision.OPEN, Gates.externalCountOf("p").getCondition().evaluate(ctx(new LinkedHashMap<String, String>(), 8, 0, 0)), "all chunks empty: nothing to wait for");
    }
}
