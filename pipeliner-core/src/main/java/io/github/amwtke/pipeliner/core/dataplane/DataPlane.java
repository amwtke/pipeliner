package io.github.amwtke.pipeliner.core.dataplane;

import io.github.amwtke.pipeliner.api.Split;
import io.github.amwtke.pipeliner.core.store.ChunkEvent;
import java.util.Collection;
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
    long nextEpoch();
    long currentEpoch();

    // rounds and desired state
    void addActiveRound(String roundId);
    void removeActiveRound(String roundId);
    Set<String> activeRounds();
    /** 0 removes the entry. */
    void setDesiredParallelism(String roundId, String pipelineId, int parallelism);
    Map<String, Integer> desiredParallelism(String roundId);
    /** Stores "pod|attempt" under runId. */
    void assignLocalRun(String roundId, String runId, String podId, int attempt);
    Map<String, String> localRunAssignments(String roundId);
    void clearLocalRun(String roundId, String runId);
    void setPaused(String roundId, boolean paused);
    boolean isPaused(String roundId);
    void setGlobalThrottle(boolean on);
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
    /** XGROUP CREATE key pg 0 MKSTREAM, ignoring BUSYGROUP. */
    void ensureGroup(String streamKey);
    /** Adds the split to the split index and, if new, to the split queue. */
    boolean enqueueSplit(String roundId, String pipelineId, Split split);
    long streamLength(String streamKey);
    /** XREADGROUP with ">"; blocking reads use a per-thread dedicated connection. */
    List<StreamEntry> read(String consumer, List<String> streamKeys, int count, long blockMillis);
    /** XAUTOCLAIM from 0-0. */
    List<StreamEntry> autoClaim(String consumer, String streamKey, long minIdleMillis, int count);
    /** XCLAIM with min-idle 0 to reset the idle clock. */
    void renew(String consumer, String streamKey, String entryId);
    CommitResult commit(CommitRequest req);
    void deadLetter(DeadLetterRequest req);
    /** Moves every DLQ entry back to its source stream; returns the count. */
    int requeueDeadLetters(String roundId, String pipelineId);
    int clearDeadLetters(String roundId, String pipelineId);
    void appendJournal(String roundId, ChunkEvent event);
    /** Entries after afterId (exclusive), up to count; sets eventId. */
    List<ChunkEvent> readJournal(String roundId, String afterId, int count);
    /** Removes entries with id &lt; upToId. */
    void trimJournal(String roundId, String upToId);
    void deleteKeys(Collection<String> keys);
    long usedMemoryBytes();
    /** Closes this thread's dedicated connection, if any. */
    void releaseThreadResources();
    @Override void close();
}
