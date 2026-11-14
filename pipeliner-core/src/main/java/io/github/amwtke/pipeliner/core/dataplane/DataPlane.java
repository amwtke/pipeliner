package io.github.amwtke.pipeliner.core.dataplane;

import io.github.amwtke.pipeliner.api.Split;
import io.github.amwtke.pipeliner.core.store.ChunkEvent;
import io.github.amwtke.pipeliner.core.store.StaleEpochException;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Streaming data plane (channels, split queues, DLQ, journal, membership). */
public interface DataPlane extends AutoCloseable {
    // membership and leadership
    void heartbeat(String podId, Map<String, String> info);
    /** Info map of the pod's last heartbeat (empty if unknown/expired). */
    Map<String, String> workerInfo(String podId);
    /** podId to last heartbeat millis (only fresh ones). */
    Map<String, Long> liveWorkers(long maxAgeMillis);
    /** SET NX PX, or PEXPIRE if already ours. */
    boolean tryAcquireOrRenewLeadership(String podId, long ttlMillis);
    /** Deletes only if ours. */
    void releaseLeadership(String podId);
    String currentLeader();
    /** Increments the epoch counter and returns it (new master term). */
    long nextEpoch();
    /**
     * Atomically raises the epoch counter to at least {@code floor}, then increments it: the result is always &gt; floor.
     * Default ignores the floor (implementations that predate it).
     */
    default long nextEpoch(long floor) { return nextEpoch(); }
    long currentEpoch();

    // rounds and desired state
    /**
     * Fencing: every MASTER-side mutation below takes the master epoch and is executed atomically after checking the round's fence
     * ({@code pg:{r}:fence}) is &lt;= epoch, otherwise it throws {@link StaleEpochException} and changes nothing. A master takes the
     * fence at the start of each round tick, which deposes every older master (zombie) for this round's data plane.
     * Worker-side {@link #commit}/{@link #deadLetter} stay unfenced (the XACK guard handles them).
     */
    /** Sets the fence to epoch if stored &lt;= epoch; false if a higher epoch already holds it. */
    boolean fenceRound(String roundId, long epoch);
    /** Global, monotonic submit sequence (INCR pg:roundseq). */
    long nextRoundSeq();
    /**
     * Like {@link #nextRoundSeq()} but the result is always &gt; {@code floor} (the counter is raised to the floor first, atomically), so
     * sequences stay above the stored rounds' even after the counter was lost. Default ignores the floor.
     */
    default long nextRoundSeq(long floor) { return nextRoundSeq(); }
    void addActiveRound(String roundId);
    void removeActiveRound(String roundId, long epoch);
    Set<String> activeRounds();
    /** Stores "parallelism|attempt" under the pipeline id; parallelism &lt;= 0 removes the entry. Fenced. */
    void setDesiredParallelism(String roundId, String pipelineId, int parallelism, int attempt, long epoch);
    Map<String, DesiredState> desiredState(String roundId);
    /** Parallelism only (see {@link #desiredState}). */
    default Map<String, Integer> desiredParallelism(String roundId) {
        Map<String, Integer> out = new LinkedHashMap<>();
        for (Map.Entry<String, DesiredState> e : desiredState(roundId).entrySet()) out.put(e.getKey(), e.getValue().getParallelism());
        return out;
    }
    /** Stores "pod|attempt" under runId. Fenced. */
    void assignLocalRun(String roundId, String runId, String podId, int attempt, long epoch);
    Map<String, String> localRunAssignments(String roundId);
    /** Fenced. */
    void clearLocalRun(String roundId, String runId, long epoch);
    /** Fenced. */
    void setPaused(String roundId, boolean paused, long epoch);
    boolean isPaused(String roundId);
    /** On: SET with a TTL (0 = none; refresh it while still over the limit). Off: DEL. Global, not round fenced. */
    void setGlobalThrottle(boolean on, long ttlMillis);
    boolean globalThrottle();

    // admin command queue (executed by the leader)
    /** RPUSH onto the command list. */
    void pushCommand(String commandJson);
    /** LPOP up to max commands (oldest first). */
    List<String> popCommands(int max);
    /** SET with a 10 minute expiry. */
    void putCommandResult(String commandId, String resultJson);
    /** Null while the command has not finished. */
    String getCommandResult(String commandId);

    // streams
    /** XGROUP CREATE key pg 0 MKSTREAM, ignoring BUSYGROUP. Fenced. */
    void ensureGroup(String roundId, String streamKey, long epoch);
    /** Adds the split to the attempt's split index and, if new, to its split queue. Fenced. */
    boolean enqueueSplit(String roundId, String pipelineId, int attempt, Split split, long epoch);
    long streamLength(String streamKey);
    /** XREADGROUP with ">"; blocking reads use a per-thread dedicated connection. */
    List<StreamEntry> read(String consumer, List<String> streamKeys, int count, long blockMillis);
    /** XAUTOCLAIM from 0-0. */
    List<StreamEntry> autoClaim(String consumer, String streamKey, long minIdleMillis, int count);
    /** XCLAIM with min-idle 0 to reset the idle clock. Returns false when the entry is no longer pending (acked or deleted). */
    boolean renew(String consumer, String streamKey, String entryId);
    CommitResult commit(CommitRequest req);
    void deadLetter(DeadLetterRequest req);
    /** Moves every entry of the attempt's DLQ back to its source stream; returns the count. Fenced. */
    int requeueDeadLetters(String roundId, String pipelineId, int attempt, long epoch);
    /** Fenced. */
    int clearDeadLetters(String roundId, String pipelineId, int attempt, long epoch);
    void appendJournal(String roundId, ChunkEvent event);
    /** Entries after afterId (exclusive), up to count; sets eventId. */
    List<ChunkEvent> readJournal(String roundId, String afterId, int count);
    /** Removes entries with id &lt; upToId. Fenced. */
    void trimJournal(String roundId, String upToId, long epoch);
    /** DEL of keys that all belong to the round. Fenced. */
    void deleteKeys(String roundId, Collection<String> keys, long epoch);
    long usedMemoryBytes();
    /** Removes worker heartbeat entries older than maxAgeMillis; returns how many. */
    int pruneWorkers(long maxAgeMillis);
    /** XPENDING summary plus the oldest pending entry of the group on the stream; {@link PendingSummary#none()} when empty, missing stream or group. */
    PendingSummary pendingSummary(String streamKey);
    /** Deletes the remaining data plane keys (channels, split queues, DLQs, ...) of rounds that no longer exist in the control plane; returns the key count. */
    int purgeRoundData(Collection<String> roundIds);
    /** Closes this thread's dedicated connection, if any. */
    void releaseThreadResources();
    @Override void close();
}
