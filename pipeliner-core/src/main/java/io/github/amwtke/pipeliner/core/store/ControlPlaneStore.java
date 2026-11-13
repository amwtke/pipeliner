package io.github.amwtke.pipeliner.core.store;

import java.util.List;
import java.util.Map;

/**
 * Persistent control plane. Every master write (all methods taking an epoch) is fenced by the ROUND's masterEpoch: it is applied
 * only if round.masterEpoch &lt;= epoch (see {@link #fenceRound}); CAS methods additionally require status == expect and return
 * whether they updated. applyChunkEvents throws {@link StaleEpochException} when fenced.
 */
public interface ControlPlaneStore {
    /** Creates schema/indices if absent (idempotent). */
    void init();

    /** If (graphName, bizKey) exists, returns the existing round unchanged. */
    RoundRecord createRound(RoundRecord round, List<RunRecord> runs, List<GateRecord> gates, List<PipelineRecord> pipelines);

    RoundRecord findRoundByBizKey(String graphName, String bizKey);

    /** Null if absent. */
    RoundRecord loadRoundRecord(String roundId);

    /** Null if absent. */
    RoundView loadRound(String roundId);

    /** Round, runs, gates and pipelines only (chunk stats empty): cheap enough for every master tick. Null if absent. */
    RoundView loadRoundState(String roundId);

    /** Sets round.masterEpoch = epoch iff stored &lt;= epoch. False if a higher epoch holds the round (or it is missing). */
    boolean fenceRound(String roundId, long epoch);

    /** Status in CREATED, RUNNING, PAUSED. */
    List<String> findActiveRounds();

    /** Sets startedAt on -&gt;RUNNING (if null), finishedAt on terminal. */
    boolean casRoundStatus(String roundId, RoundStatus expect, RoundStatus target, String error, long epoch);

    void setRoundIncomplete(String roundId);

    /** startedAt/finishedAt likewise. */
    boolean casRunStatus(String roundId, String runId, RunStatus expect, RunStatus target, String error, long epoch);

    boolean updateRunAssignment(String roundId, String runId, String assignee, int attempt, long epoch);

    boolean casPipelineStatus(String roundId, String pipelineId, PipelineStatus expect, PipelineStatus target, String error, long epoch);

    void setPipelineEnumDone(String roundId, String pipelineId, boolean done, long epoch);

    void setPipelineParallelism(String roundId, String pipelineId, int parallelism, long epoch);

    void armGate(String roundId, String gateId, long armedAt, Long deadlineAt, long epoch);

    boolean casGateStatus(String roundId, String gateId, GateStatus expect, GateStatus target, String openedBy, Map<String, String> context, long epoch);

    void markGateAlerted(String roundId, String gateId);

    /** To CLOSED; clears armed/deadline/opened/context/alerted (signals kept). */
    void resetGate(String roundId, String gateId, long epoch);

    /** Idempotent per key. */
    void saveGateSignal(String roundId, String gateId, String signalKey, String payload);

    Map<String, String> loadGateSignals(String roundId, String gateId);

    /** Insert if absent; never overwrites an existing record. */
    void savePendingChunks(String roundId, List<ChunkRecord> chunks);

    /**
     * Applies journal events idempotently and persists the new journal offset (atomically where supported).
     * DONE: upsert status DONE (overwrite fields). DEAD: upsert DEAD unless existing is DONE. Other types ignored (offset still saved).
     * @throws StaleEpochException when the round is fenced by a higher epoch (nothing is applied)
     */
    void applyChunkEvents(String roundId, List<ChunkEvent> events, String newJournalOffset, long epoch);

    /** DEAD to SKIPPED for the run. */
    void markChunksSkipped(String roundId, String runId, long epoch);

    /** Run to PENDING attempt+1 assignee null; its pipelines to PENDING enumDone=false; deletes its chunk records. */
    void resetRun(String roundId, String runId, long epoch);

    /** Ordered by pipelineId, chunkId. */
    List<ChunkRecord> queryChunks(ChunkQuery q);

    long countChunks(String roundId, String pipelineId, ChunkStatus status);

    /** Over DONE chunks: rowsOut keys starting with "e" summed. */
    Map<String, Long> sumRowsOutByEdge(String roundId, String runId);

    /** Over DONE chunks grouped by inputEdge (excluding "split"). */
    Map<String, Long> sumRowsInByEdge(String roundId, String runId);
}
