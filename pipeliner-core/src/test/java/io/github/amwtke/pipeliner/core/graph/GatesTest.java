package io.github.amwtke.pipeliner.core.graph;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.github.amwtke.pipeliner.api.DataSourceRegistry;
import io.github.amwtke.pipeliner.api.GateContext;
import io.github.amwtke.pipeliner.api.GateDecision;
import java.util.LinkedHashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

class GatesTest {
    private static GateContext ctx(Map<String, String> signals, long done, int attempt) {
        return new GateContext() {
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
        sig.put(io.github.amwtke.pipeliner.core.util.ChunkIds.batchId("r", "S0000", 2), "x");
        sig.put(io.github.amwtke.pipeliner.core.util.ChunkIds.batchId("r", "S0001", 2), "x");
        sig.put(io.github.amwtke.pipeliner.core.util.ChunkIds.batchId("r", "S0002", 1), "x");   // older attempt
        assertEquals(GateDecision.WAIT, Gates.externalCountOf("p").getCondition().evaluate(ctx(sig, 3, 2)));
        sig.put(io.github.amwtke.pipeliner.core.util.ChunkIds.batchId("r", "S0002", 2), "x");
        assertEquals(GateDecision.OPEN, Gates.externalCountOf("p").getCondition().evaluate(ctx(sig, 3, 2)));
    }

    @Test void unscopedSignalKeysStillCount() throws Exception {
        Map<String, String> sig = new LinkedHashMap<>();
        sig.put("legacy-key", "x");
        assertEquals(GateDecision.OPEN, Gates.externalCountOf("p").getCondition().evaluate(ctx(sig, 1, 5)));
    }
}
