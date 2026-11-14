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

    /**
     * Highest master epoch ever stored on any round (0 if none). Used as the floor for a new master epoch, so epochs stay above the
     * stored round epochs even after the data plane's epoch counter was lost (Redis wiped).
     */
    long maxMasterEpoch();

    /** Status in CREATED, RUNNING, PAUSED. */
    List<String> findActiveRounds();

    /**
     * Up to {@code limit} rounds for the admin UI: active ones (CREATED/RUNNING/PAUSED) first, then the rest by createdAt descending
     * (ties by roundId descending). {@code graphName} null/blank means all graphs. Records may omit {@code planJson}.
     */
    List<RoundRecord> listRecentRounds(String graphName, int limit);

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

    /** To CLOSED; clears armed/deadline/opened/context/alerted (signals kept; resetRuns also deletes them). */
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

    /**
     * Atomic rerun reset (one transaction / one script), fenced by the round epoch: every run to PENDING with attempt+1,
     * assignee/error null; its pipelines to PENDING enumDone=false; its chunk records and counters deleted; every gate to CLOSED with
     * armed/deadline/opened/context/alerted cleared AND its signals deleted; a FAILED/SUCCEEDED round back to RUNNING (finishedAt/error cleared).
     * @throws StaleEpochException when the round is fenced by a higher epoch (nothing is applied)
     */
    void resetRuns(String roundId, List<String> runIds, List<String> gateIds, long epoch);

    /** Compatibility: resets one run (see {@link #resetRuns}); a fenced call is a no-op. */
    default void resetRun(String roundId, String runId, long epoch) {
        try {
            resetRuns(roundId, java.util.Collections.singletonList(runId), java.util.Collections.<String>emptyList(), epoch);
        } catch (StaleEpochException e) {
            // fenced: nothing applied
        }
    }

    /** Ordered by pipelineId, chunkId. */
    List<ChunkRecord> queryChunks(ChunkQuery q);

    long countChunks(String roundId, String pipelineId, ChunkStatus status);

    /** Over DONE chunks: rowsOut keys starting with "e" summed. */
    Map<String, Long> sumRowsOutByEdge(String roundId, String runId);

    /** Over DONE chunks grouped by inputEdge (excluding "split"). */
    Map<String, Long> sumRowsInByEdge(String roundId, String runId);

    // ---------------------------------------------------------------- retention

    /**
     * Compaction of ONE terminal (SUCCEEDED/FAILED/CANCELLED), not yet compacted round with finishedAt &lt; finishedBeforeMillis, atomically
     * (status check and write in one transaction / script): persists per pipeline a summary (chunk counts by status, rowsIn, rowsOut) on the
     * pipeline record, deletes every chunk record and counter of the round and marks the round compacted. {@link #loadRound} keeps returning
     * chunkCounts/rowsIn/rowsOut for it (from the summaries). Returns the number of chunk records removed (0 when not eligible).
     * A later {@link #resetRuns} drops the summaries of the reset pipelines and the compacted mark.
     */
    int compactRound(String roundId, long finishedBeforeMillis);

    /**
     * Deletes everything of up to {@code max} terminal rounds with finishedAt &lt; finishedBeforeMillis (round, runs, gates, signals, pipelines,
     * chunks, counters, biz-key index entry, sorted indexes). Per round, the status/finishedAt check and the delete are ONE atomic step (MySQL:
     * transaction with SELECT ... FOR UPDATE on the round row; Redis: one script), so a concurrent rerun/retry that flipped the round back to
     * RUNNING can never lose data. Returns the purged round ids (oldest first).
     */
    List<String> purgeRounds(long finishedBeforeMillis, int max);

    /** Ids (oldest finished first, at most {@code max}) of terminal, not yet compacted rounds with finishedAt &lt; finishedBeforeMillis. */
    List<String> findTerminalRounds(long finishedBeforeMillis, int max);
}
