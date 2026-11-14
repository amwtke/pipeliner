package io.github.amwtke.pipeliner.runtime.fake;

import io.github.amwtke.pipeliner.core.engine.PipelinerMetrics;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/** Records the hooks the retention / alerting tests care about. */
public class RecordingMetrics implements PipelinerMetrics {
    public final AtomicInteger chunks = new AtomicInteger();
    public final AtomicLong rowsOutTotal = new AtomicLong();
    public final AtomicLong rowsInTotal = new AtomicLong();
    public final AtomicInteger deadLetters = new AtomicInteger();
    public final AtomicInteger leaderChanges = new AtomicInteger();
    public final AtomicInteger leaderLosses = new AtomicInteger();
    public final List<String> roundsFinished = new CopyOnWriteArrayList<>();   // graph|round|status
    public final List<String> gateTimeouts = new CopyOnWriteArrayList<>();     // graph|gate|policy
    public final List<String> stalled = new CopyOnWriteArrayList<>();          // graph|pipeline
    public final AtomicLong maxOldestPendingMillis = new AtomicLong();
    public final AtomicInteger retentionPasses = new AtomicInteger();
    public final AtomicInteger purged = new AtomicInteger();
    public final AtomicInteger compacted = new AtomicInteger();
    public final AtomicInteger prunedWorkers = new AtomicInteger();
    public volatile int activeRoundsSeen = -1;
    public volatile int liveInstancesSeen = -1;
    public volatile long redisMemory = -1;
    public volatile int highWatermark = -1;

    @Override public void chunkProcessed(String round, String run, String pipeline, long rowsIn, long rowsOut, long costMs) { chunks.incrementAndGet(); rowsInTotal.addAndGet(rowsIn); rowsOutTotal.addAndGet(rowsOut); }
    @Override public void deadLetter(String round, String pipeline) { deadLetters.incrementAndGet(); }
    @Override public void leaderChanged(String podId, long epoch) { leaderChanges.incrementAndGet(); }
    @Override public void leaderLost(String podId) { leaderLosses.incrementAndGet(); }
    @Override public void roundFinished(String graph, String round, String status) { roundsFinished.add(graph + "|" + round + "|" + status); }
    @Override public void gateTimeout(String graph, String gate, String policy) { gateTimeouts.add(graph + "|" + gate + "|" + policy); }
    @Override public void stalledChunk(String graph, String pipeline) { stalled.add(graph + "|" + pipeline); }
    @Override public void oldestPending(String round, String pipeline, long idleMillis) { maxOldestPendingMillis.accumulateAndGet(idleMillis, Math::max); }
    @Override public void activeRounds(int count) { activeRoundsSeen = count; }
    @Override public void liveInstances(String podId, int count) { liveInstancesSeen = count; }
    @Override public void redisUsedMemory(long bytes) { redisMemory = bytes; }
    @Override public void highWatermark(int value) { highWatermark = value; }
    @Override public void retentionPass(int c, int p, int w) { retentionPasses.incrementAndGet(); compacted.addAndGet(c); purged.addAndGet(p); prunedWorkers.addAndGet(w); }
}
