package io.github.amwtke.pipeliner.runtime.process;

import io.github.amwtke.pipeliner.core.store.ChunkEvent;

/** Factory for journal events. */
public final class Events {
    private Events() {}

    public static ChunkEvent done(String roundId, String runId, String pipelineId, String chunkId, String inputEdge,
                                  String owner, ChunkResult r, int attempt) {
        return ChunkEvent.builder().type("DONE").roundId(roundId).runId(runId).pipelineId(pipelineId).chunkId(chunkId)
                .inputEdge(inputEdge).owner(owner).rowsIn(r.rowsIn).rowsOut(new java.util.LinkedHashMap<>(r.rowsOut))
                .costMs((int) r.costMillis).attempt(attempt).ts(System.currentTimeMillis()).build();
    }

    public static ChunkEvent dead(String roundId, String runId, String pipelineId, String chunkId, String inputEdge,
                                  String owner, String error) {
        return ChunkEvent.builder().type("DEAD").roundId(roundId).runId(runId).pipelineId(pipelineId).chunkId(chunkId)
                .inputEdge(inputEdge).owner(owner).error(error).ts(System.currentTimeMillis()).build();
    }

    public static ChunkEvent localRun(boolean ok, String roundId, String runId, String pipelineId, String owner, int attempt, String error) {
        return ChunkEvent.builder().type(ok ? "LOCAL_RUN_DONE" : "LOCAL_RUN_FAILED").roundId(roundId).runId(runId)
                .pipelineId(pipelineId).owner(owner).attempt(attempt).error(error).ts(System.currentTimeMillis()).build();
    }

    /** Short single-line error text. */
    public static String describe(Throwable t) {
        String s = t.getClass().getSimpleName() + ": " + t.getMessage();
        return s.length() > 500 ? s.substring(0, 500) : s;
    }
}
