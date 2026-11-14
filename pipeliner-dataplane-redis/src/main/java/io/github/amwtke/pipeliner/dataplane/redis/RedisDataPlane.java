package io.github.amwtke.pipeliner.dataplane.redis;

import io.github.amwtke.pipeliner.api.PipelinerException;
import io.github.amwtke.pipeliner.api.Split;
import io.github.amwtke.pipeliner.core.codec.ChunkCodec;
import io.github.amwtke.pipeliner.core.dataplane.*;
import io.github.amwtke.pipeliner.core.store.ChunkEvent;
import io.github.amwtke.pipeliner.core.store.StaleEpochException;
import io.lettuce.core.*;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.codec.StringCodec;
import io.lettuce.core.models.stream.PendingMessage;
import io.lettuce.core.models.stream.PendingMessages;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import lombok.extern.slf4j.Slf4j;

/** Redis Streams data plane built on Lettuce, Lua scripts and per-thread blocking connections. */
@Slf4j
public class RedisDataPlane implements DataPlane {
    private static final long WORKER_INFO_TTL_MS = 60_000L;
    private static final int REQUEUE_BATCH = 500;
    private static final int TRIM_BATCH = 1000;
    private static final int PURGE_BATCH = 200;
    /** Rounds purged without a key registry (created before pg:{r}:keys existed) whose keys are still to be found by an incremental SCAN. */
    static final String LEGACY_PENDING = "pg:purge:legacy";
    /** round id to the persisted SCAN cursor of its legacy purge, so the scan resumes where the last call stopped (any pod, after a restart). */
    static final String LEGACY_CURSOR = "pg:purge:legacycur";
    private volatile long legacyScanBudgetMillis = 50;
    private volatile int legacyScanCount = 500;

    /** Time budget (per purgeRoundData call) and SCAN COUNT of the legacy pre-registry purge. A call always does at least one SCAN step. */
    public void setLegacyScan(long budgetMillis, int count) { this.legacyScanBudgetMillis = Math.max(0, budgetMillis); this.legacyScanCount = Math.max(1, count); }

    private final RedisClient client;
    private final StatefulRedisConnection<String, String> shared;
    private final RedisCommands<String, String> cmd;
    private final ThreadLocal<StatefulRedisConnection<String, String>> blocking = new ThreadLocal<>();
    private final Set<StatefulRedisConnection<String, String>> blockingAll = ConcurrentHashMap.newKeySet();
    private volatile boolean closed;

    private final Script commitS = new Script("commit");
    private final Script enqueueS = new Script("enqueue_split");
    private final Script deadS = new Script("dead_letter");
    private final Script requeueS = new Script("requeue_dlq");
    private final Script fenceS = new Script("fence");
    private final Script epochS = new Script("next_epoch");
    private final Script seqS = new Script("next_round_seq");
    private final Script fencedCmdS = new Script("fenced_cmd");
    private final Script ensureGroupS = new Script("ensure_group");
    private final Script clearDlqS = new Script("clear_dlq");
    private final Script leaderS = new Script("leader");
    private final Script releaseS = new Script("release_leader");
    private final Script renewS = new Script("renew");
    private final Script workerInfoS = new Script("worker_info");
    private final Script claimIdleS = new Script("claim_idle");
    private final Script trimJournalS = new Script("trim_journal");
    /** Per-stream resume cursor of the idle-claim scan (only streams mid-scan are kept). */
    private final Map<String, String> claimCursors = new ConcurrentHashMap<>();

    public RedisDataPlane(RedisClient client) {
        this.client = client;
        this.shared = client.connect(StringCodec.UTF8);
        this.cmd = shared.sync();
        for (Script s : Arrays.asList(commitS, enqueueS, deadS, requeueS, fenceS, epochS, seqS, fencedCmdS, ensureGroupS, clearDlqS, leaderS, releaseS, renewS, workerInfoS, claimIdleS, trimJournalS)) s.load();
    }

    /** A Lua script loaded via SCRIPT LOAD, run with EVALSHA and falling back to EVAL on NOSCRIPT. */
    private final class Script {
        private final String text;
        private volatile String sha;

        Script(String name) {
            try (InputStream in = RedisDataPlane.class.getResourceAsStream("/lua/" + name + ".lua")) {
                if (in == null) throw new PipelinerException("missing lua script " + name);
                ByteArrayOutputStream bo = new ByteArrayOutputStream();
                byte[] buf = new byte[4096];
                int n;
                while ((n = in.read(buf)) > 0) bo.write(buf, 0, n);
                text = new String(bo.toByteArray(), StandardCharsets.UTF_8);
            } catch (IOException e) {
                throw new PipelinerException("cannot read lua script " + name, e);
            }
        }

        void load() { sha = cmd.scriptLoad(text); }

        long run(String[] keys, String... args) {
            return ((Number) eval(ScriptOutputType.INTEGER, keys, args)).longValue();
        }

        Object eval(ScriptOutputType type, String[] keys, String... args) {
            try {
                String h = sha;   // read once: a concurrent NOSCRIPT fallback must not null it between check and use
                if (h == null) {
                    h = cmd.scriptLoad(text);
                    sha = h;
                }
                return cmd.evalsha(h, type, keys, args);
            } catch (RedisNoScriptException e) {
                return cmd.eval(text, type, keys, args);   // EVAL also caches the script; sha stays valid
            } catch (RedisCommandExecutionException e) {
                if (e.getMessage() == null || !e.getMessage().startsWith("NOSCRIPT")) throw e;
                return cmd.eval(text, type, keys, args);
            }
        }
    }

    /** Runs a fenced script: KEYS[1]=fence key and ARGV[1]=epoch are prepended; -2 means a newer master holds the round. */
    private long fenced(Script sc, String roundId, long epoch, String[] keys, String... args) {
        String[] k = new String[keys.length + 1];
        k[0] = Keys.fence(roundId);
        System.arraycopy(keys, 0, k, 1, keys.length);
        String[] a = new String[args.length + 1];
        a[0] = String.valueOf(epoch);
        System.arraycopy(args, 0, a, 1, args.length);
        long r = sc.run(k, a);
        if (r == -2) throw new StaleEpochException("round " + roundId + " data plane is fenced by a newer master epoch (epoch " + epoch + ")");
        return r;
    }

    private long fencedCmd(String roundId, long epoch, String command, String[] keys, String... args) {
        String[] a = new String[args.length + 1];
        a[0] = command;
        System.arraycopy(args, 0, a, 1, args.length);
        return fenced(fencedCmdS, roundId, epoch, keys, a);
    }

    // membership and leadership

    @Override public void heartbeat(String podId, Map<String, String> info) {
        cmd.zadd(Keys.workers(), (double) System.currentTimeMillis(), podId);
        if (info != null && !info.isEmpty()) {   // replaces the hash atomically (DEL + HSET + PEXPIRE): stale fields disappear
            String[] args = new String[1 + 2 * info.size()];
            args[0] = String.valueOf(WORKER_INFO_TTL_MS);
            int i = 1;
            for (Map.Entry<String, String> e : info.entrySet()) {
                args[i++] = e.getKey();
                args[i++] = e.getValue();
            }
            workerInfoS.run(new String[] {Keys.workerInfo(podId)}, args);
        }
    }

    @Override public Map<String, String> workerInfo(String podId) { return new LinkedHashMap<>(cmd.hgetall(Keys.workerInfo(podId))); }

    @Override public Map<String, Long> liveWorkers(long maxAgeMillis) {
        double min = (double) (System.currentTimeMillis() - maxAgeMillis);
        Range<Double> range = Range.from(Range.Boundary.including(min), Range.Boundary.unbounded());
        Map<String, Long> out = new LinkedHashMap<>();
        for (ScoredValue<String> sv : cmd.zrangebyscoreWithScores(Keys.workers(), range)) {
            out.put(sv.getValue(), (long) sv.getScore());
        }
        return out;
    }

    @Override public boolean tryAcquireOrRenewLeadership(String podId, long ttlMillis) {
        return leaderS.run(new String[] {Keys.leader()}, podId, String.valueOf(ttlMillis)) == 1;
    }

    @Override public void releaseLeadership(String podId) {
        releaseS.run(new String[] {Keys.leader()}, podId);
    }

    @Override public String currentLeader() { return cmd.get(Keys.leader()); }

    @Override public long nextEpoch() { return nextEpoch(0); }

    @Override public long nextEpoch(long floor) { return epochS.run(new String[] {Keys.epoch()}, String.valueOf(floor)); }

    @Override public long currentEpoch() {
        String v = cmd.get(Keys.epoch());
        return v == null ? 0L : Long.parseLong(v);
    }

    // rounds and desired state

    @Override public boolean fenceRound(String roundId, long epoch) {
        return fenceS.run(new String[] {Keys.fence(roundId)}, String.valueOf(epoch)) == 1;
    }

    @Override public long nextRoundSeq() { return nextRoundSeq(0); }

    @Override public long nextRoundSeq(long floor) { return seqS.run(new String[] {Keys.roundSeq()}, String.valueOf(floor)); }

    @Override public void addActiveRound(String roundId) { cmd.sadd(Keys.activeRounds(), roundId); }

    @Override public void removeActiveRound(String roundId, long epoch) {
        fencedCmd(roundId, epoch, "SREM", new String[] {Keys.activeRounds()}, roundId);
    }

    @Override public Set<String> activeRounds() { return new LinkedHashSet<>(cmd.smembers(Keys.activeRounds())); }

    @Override public void setDesiredParallelism(String roundId, String pipelineId, int parallelism, int attempt, long epoch) {
        if (parallelism <= 0) fencedCmd(roundId, epoch, "HDEL", new String[] {Keys.desired(roundId)}, pipelineId);
        else fencedCmd(roundId, epoch, "HSET", new String[] {Keys.desired(roundId)}, pipelineId, parallelism + "|" + attempt);
    }

    @Override public Map<String, DesiredState> desiredState(String roundId) {
        Map<String, DesiredState> out = new LinkedHashMap<>();
        for (Map.Entry<String, String> e : cmd.hgetall(Keys.desired(roundId)).entrySet()) {
            String v = e.getValue();
            int bar = v.indexOf('|');
            out.put(e.getKey(), bar < 0 ? new DesiredState(Integer.parseInt(v), 0)
                    : new DesiredState(Integer.parseInt(v.substring(0, bar)), Integer.parseInt(v.substring(bar + 1))));
        }
        return out;
    }

    @Override public void assignLocalRun(String roundId, String runId, String podId, int attempt, long epoch) {
        fencedCmd(roundId, epoch, "HSET", new String[] {Keys.assign(roundId)}, runId, io.github.amwtke.pipeliner.core.dataplane.LocalRunAssignment.format(podId, attempt));
    }

    @Override public Map<String, String> localRunAssignments(String roundId) {
        return new LinkedHashMap<>(cmd.hgetall(Keys.assign(roundId)));
    }

    @Override public void clearLocalRun(String roundId, String runId, long epoch) {
        fencedCmd(roundId, epoch, "HDEL", new String[] {Keys.assign(roundId)}, runId);
    }

    @Override public void setPaused(String roundId, boolean paused, long epoch) {
        if (paused) fencedCmd(roundId, epoch, "SET", new String[] {Keys.paused(roundId)}, "1");
        else fencedCmd(roundId, epoch, "DEL", new String[] {Keys.paused(roundId)});
    }

    @Override public boolean isPaused(String roundId) { return cmd.exists(Keys.paused(roundId)) > 0; }

    @Override public void setGlobalThrottle(boolean on, long ttlMillis) {
        if (!on) cmd.del(Keys.throttle());
        else if (ttlMillis > 0) cmd.set(Keys.throttle(), "1", SetArgs.Builder.px(ttlMillis));
        else cmd.set(Keys.throttle(), "1");
    }

    @Override public boolean globalThrottle() { return cmd.exists(Keys.throttle()) > 0; }

    // admin command queue

    @Override public void pushCommand(String commandJson) { cmd.rpush(Keys.commands(), commandJson); }

    @Override public List<String> popCommands(int max) {
        List<String> out = new ArrayList<>();
        for (int i = 0; i < max; i++) {
            String c = cmd.lpop(Keys.commands());
            if (c == null) break;
            out.add(c);
        }
        return out;
    }

    @Override public boolean withdrawCommand(String commandJson) { return cmd.lrem(Keys.commands(), 1, commandJson) > 0; }

    @Override public void putCommandResult(String commandId, String resultJson) {
        cmd.set(Keys.commandResult(commandId), resultJson, SetArgs.Builder.ex(600));
    }

    @Override public String getCommandResult(String commandId) { return cmd.get(Keys.commandResult(commandId)); }

    // streams

    @Override public void ensureGroup(String roundId, String streamKey, long epoch) {
        fenced(ensureGroupS, roundId, epoch, new String[] {streamKey}, Keys.GROUP);
    }

    @Override public boolean enqueueSplit(String roundId, String pipelineId, int attempt, Split split, long epoch) {
        return fenced(enqueueS, roundId, epoch, new String[] {Keys.splitIndex(roundId, pipelineId, attempt), Keys.splitQueue(roundId, pipelineId, attempt)},
                split.getSplitId(), ChunkCodec.encodeSplit(split)) == 1;
    }

    @Override public long streamLength(String streamKey) {
        Long n = cmd.xlen(streamKey);
        return n == null ? 0L : n;
    }

    private RedisCommands<String, String> readConnection(long blockMillis) {
        if (blockMillis <= 0) return cmd;
        StatefulRedisConnection<String, String> c = blocking.get();
        // isOpen() is false while Lettuce auto-reconnects: keep that connection (it recovers by itself) instead of orphaning it
        if (closed) throw new PipelinerException("data plane closed");
        if (c == null) {
            c = client.connect(StringCodec.UTF8);
            c.setTimeout(Duration.ofSeconds(60));
            blocking.set(c);
            blockingAll.add(c);
        }
        return c.sync();
    }

    @Override public List<StreamEntry> read(String consumer, List<String> streamKeys, int count, long blockMillis) {
        if (streamKeys.isEmpty()) return new ArrayList<>();
        XReadArgs.StreamOffset<String>[] offsets = streamOffsets(streamKeys);
        XReadArgs args = XReadArgs.Builder.count(count);
        if (blockMillis > 0) args.block(blockMillis);
        RedisCommands<String, String> c = readConnection(blockMillis);
        List<StreamMessage<String, String>> msgs = c.xreadgroup(Consumer.from(Keys.GROUP, consumer), args, offsets);
        return toEntries(msgs, null);
    }

    @SuppressWarnings("unchecked")
    private static XReadArgs.StreamOffset<String>[] streamOffsets(List<String> keys) {
        XReadArgs.StreamOffset<String>[] a = new XReadArgs.StreamOffset[keys.size()];
        for (int i = 0; i < a.length; i++) a[i] = XReadArgs.StreamOffset.lastConsumed(keys.get(i));
        return a;
    }

    private static List<StreamEntry> toEntries(List<StreamMessage<String, String>> msgs, String defaultKey) {
        List<StreamEntry> out = new ArrayList<>(msgs.size());
        for (StreamMessage<String, String> m : msgs) {
            Map<String, String> f = m.getBody() == null ? new LinkedHashMap<String, String>() : new LinkedHashMap<>(m.getBody());
            out.add(new StreamEntry(m.getStream() != null ? m.getStream() : defaultKey, m.getId(), f));
        }
        return out;
    }

    /** Max claim_idle round trips per call when a window held nothing claimable (each inspects COUNT*10 PEL entries). */
    private static final int CLAIM_SCAN_ROUNDS = 20;

    @Override public List<StreamEntry> autoClaim(String consumer, String streamKey, long minIdleMillis, int count) {
        int n = Math.max(1, count);
        List<StreamEntry> out = new ArrayList<>();
        try {
            for (int round = 0; round < CLAIM_SCAN_ROUNDS && out.isEmpty(); round++) {
                String start = claimCursors.get(streamKey);
                // XPENDING + XCLAIM in one script instead of XAUTOCLAIM, which needs Redis 6.2
                List<?> r = (List<?>) claimIdleS.eval(ScriptOutputType.MULTI, new String[] {streamKey},
                        Keys.GROUP, consumer, String.valueOf(minIdleMillis), start == null ? "0-0" : start, String.valueOf(n));
                String next = String.valueOf(r.get(0));
                if ("0-0".equals(next)) claimCursors.remove(streamKey);   // scanned to the end: restart next call
                else claimCursors.put(streamKey, next);
                for (int i = 1; i < r.size(); i++) {   // {entry id, delivery count, {field, value, ...}}
                    List<?> c = (List<?>) r.get(i);
                    List<?> kv = (List<?>) c.get(2);
                    Map<String, String> f = new LinkedHashMap<>();
                    for (int j = 0; j + 1 < kv.size(); j += 2) f.put(String.valueOf(kv.get(j)), String.valueOf(kv.get(j + 1)));
                    StreamEntry e = new StreamEntry(streamKey, String.valueOf(c.get(0)), f);
                    e.setDeliveryCount(((Number) c.get(1)).longValue());
                    out.add(e);
                }
                if ("0-0".equals(next)) break;
            }
        } catch (RuntimeException e) {
            claimCursors.remove(streamKey);
            throw e;
        }
        return out;
    }

    @Override public boolean renew(String consumer, String streamKey, String entryId) {
        return renewS.run(new String[] {streamKey}, Keys.GROUP, consumer, entryId) > 0;
    }

    @Override public CommitResult commit(CommitRequest req) {
        List<String> keys = new ArrayList<>();
        keys.add(req.getInputStreamKey());
        keys.add(Keys.journal(req.getRoundId()));
        List<String> args = new ArrayList<>();
        args.add(Keys.GROUP);
        args.add(req.getInputEntryId());
        args.add(String.valueOf(req.getHighWatermark()));
        args.add(req.getEvent() == null ? "" : ChunkCodec.encodeEvent(req.getEvent()));
        args.add(req.getConsumer() == null ? "" : req.getConsumer());
        if (req.getOutputs() != null) {
            for (OutputChunk o : req.getOutputs()) {
                int idx = keys.subList(2, keys.size()).indexOf(o.getStreamKey());
                if (idx < 0) {
                    keys.add(o.getStreamKey());
                    idx = keys.size() - 3;
                }
                idx += 2;
                args.add(String.valueOf(idx + 1));
                args.add(o.getPayload());
            }
        }
        long r = commitS.run(keys.toArray(new String[0]), args.toArray(new String[0]));
        return r == 1 ? CommitResult.OK : r == -1 ? CommitResult.BUSY : CommitResult.CONFLICT;
    }

    @Override public boolean deadLetter(DeadLetterRequest req) {
        String[] keys = {req.getInputStreamKey(), Keys.dlq(req.getRoundId(), req.getPipelineId(), req.getAttempt()), Keys.journal(req.getRoundId())};
        List<String> args = new ArrayList<>(Arrays.asList(Keys.GROUP, req.getInputEntryId(),
                req.getError() == null ? "" : req.getError(),
                req.getEvent() == null ? "" : ChunkCodec.encodeEvent(req.getEvent()),
                req.getConsumer() == null ? "" : req.getConsumer()));
        if (req.getInputFields() != null) {
            for (Map.Entry<String, String> e : req.getInputFields().entrySet()) {
                args.add(e.getKey());
                args.add(e.getValue());
            }
        }
        return deadS.run(keys, args.toArray(new String[0])) == 1;
    }

    @Override public int requeueDeadLetters(String roundId, String pipelineId, int attempt, long epoch) {
        String[] keys = {Keys.dlq(roundId, pipelineId, attempt)};
        int total = 0;
        long n;
        do {
            n = fenced(requeueS, roundId, epoch, keys, String.valueOf(REQUEUE_BATCH));
            total += n;
        } while (n >= REQUEUE_BATCH);
        return total;
    }

    @Override public int clearDeadLetters(String roundId, String pipelineId, int attempt, long epoch) {
        return (int) fenced(clearDlqS, roundId, epoch, new String[] {Keys.dlq(roundId, pipelineId, attempt)});
    }

    @Override public void appendJournal(String roundId, ChunkEvent event) {
        cmd.xadd(Keys.journal(roundId), Collections.singletonMap("e", ChunkCodec.encodeEvent(event)));
    }

    @Override public List<ChunkEvent> readJournal(String roundId, String afterId, int count) {
        Range<String> range = (afterId == null || afterId.isEmpty() || "0-0".equals(afterId) || "0".equals(afterId))
                ? Range.unbounded()
                : Range.from(Range.Boundary.including(nextStreamId(afterId)), Range.Boundary.unbounded());   // "(id" needs Redis 6.2
        List<ChunkEvent> out = new ArrayList<>();
        for (StreamMessage<String, String> m : cmd.xrange(Keys.journal(roundId), range, Limit.from(count))) {
            String json = m.getBody() == null ? null : m.getBody().get("e");
            if (json == null) continue;
            ChunkEvent e = ChunkCodec.decodeEvent(json);
            e.setEventId(m.getId());
            out.add(e);
        }
        return out;
    }

    /** The smallest stream id above {@code id} ("ms-seq"; a bare "ms" means "ms-0"). */
    static String nextStreamId(String id) {
        int d = id.indexOf('-');
        long ms = Long.parseUnsignedLong(d < 0 ? id : id.substring(0, d));
        long seq = d < 0 ? 0 : Long.parseUnsignedLong(id.substring(d + 1));
        if (seq == -1L) return Long.toUnsignedString(ms + 1) + "-0";   // seq is 2^64-1: roll over to the next millisecond
        return Long.toUnsignedString(ms) + "-" + Long.toUnsignedString(seq + 1);
    }

    @Override public void trimJournal(String roundId, String upToId, long epoch) {
        String[] keys = {Keys.journal(roundId)};
        // XRANGE + XDEL in a script instead of XTRIM MINID, which needs Redis 6.2
        while (fenced(trimJournalS, roundId, epoch, keys, upToId, String.valueOf(TRIM_BATCH)) >= TRIM_BATCH) { }
    }

    @Override public void deleteKeys(String roundId, Collection<String> keys, long epoch) {
        if (keys == null || keys.isEmpty()) return;
        fencedCmd(roundId, epoch, "DEL", keys.toArray(new String[0]));
    }

    @Override public boolean groupExists(String streamKey) {
        try {
            for (Object g : cmd.xinfoGroups(streamKey)) {
                if (!(g instanceof List)) continue;
                List<?> kv = (List<?>) g;
                for (int i = 0; i + 1 < kv.size(); i += 2)
                    if ("name".equals(String.valueOf(kv.get(i))) && Keys.GROUP.equals(String.valueOf(kv.get(i + 1)))) return true;
            }
            return false;
        } catch (io.lettuce.core.RedisCommandExecutionException e) {
            String m = e.getMessage();
            if (m != null && (m.startsWith("NOGROUP") || m.contains("no such key"))) return false;   // missing stream
            throw e;
        }
    }

    @Override public long usedMemoryBytes() {
        for (String line : cmd.info("memory").split("\r?\n")) {
            if (line.startsWith("used_memory:")) return Long.parseLong(line.substring("used_memory:".length()).trim());
        }
        return 0L;
    }

    @Override public int pruneWorkers(long maxAgeMillis) {
        double max = (double) (System.currentTimeMillis() - maxAgeMillis);
        Long n = cmd.zremrangebyscore(Keys.workers(), Range.from(Range.Boundary.unbounded(), Range.Boundary.excluding(max)));
        return n == null ? 0 : n.intValue();
    }

    @Override public PendingSummary pendingSummary(String streamKey) {
        try {
            PendingMessages pm = cmd.xpending(streamKey, Keys.GROUP);
            if (pm == null || pm.getCount() == 0) return PendingSummary.none();
            List<PendingMessage> oldest = cmd.xpending(streamKey, Keys.GROUP, Range.create("-", "+"), Limit.from(1));
            if (oldest.isEmpty()) return PendingSummary.none();
            PendingMessage m = oldest.get(0);
            return new PendingSummary(pm.getCount(), m.getId(), m.getMsSinceLastDelivery(), m.getConsumer(), m.getRedeliveryCount());
        } catch (io.lettuce.core.RedisCommandExecutionException e) {
            if (e.getMessage() != null && e.getMessage().startsWith("NOGROUP")) return PendingSummary.none();   // stream/group not created (yet)
            throw e;
        }
    }

    /** Deletes by the round's key registry (pg:{r}:keys, filled by the Lua scripts as keys are created) plus the fixed control keys; never scans the keyspace. */
    @Override public int purgeRoundData(Collection<String> roundIds) {
        int deleted = 0;
        if (roundIds == null) roundIds = Collections.emptyList();
        for (String r : new LinkedHashSet<>(roundIds)) {
            String reg = Keys.registry(r);
            if (cmd.exists(reg) == 0) cmd.sadd(LEGACY_PENDING, r);   // no registry: a round of an older version (or one that never created keys), finish by SCAN below
            List<String> batch = new ArrayList<>(PURGE_BATCH + 8);
            batch.addAll(Arrays.asList(Keys.journal(r), Keys.desired(r), Keys.assign(r), Keys.paused(r), Keys.fence(r)));
            ScanArgs args = ScanArgs.Builder.limit(PURGE_BATCH);
            ValueScanCursor<String> cur = cmd.sscan(reg, args);
            while (true) {
                batch.addAll(cur.getValues());
                if (batch.size() >= PURGE_BATCH) {
                    deleted += unlink(batch);
                    batch.clear();
                }
                if (cur.isFinished()) break;
                cur = cmd.sscan(reg, cur, args);
            }
            batch.add(reg);
            deleted += unlink(batch);
        }
        return deleted + purgeLegacy();
    }

    /**
     * Bounded, resumable migration for rounds without a key registry: for each pending round an incremental {@code SCAN cursor MATCH pg:{round}:*}
     * whose cursor is persisted in {@link #LEGACY_CURSOR}. One call spends at most {@code legacyScanBudgetMillis} (plus one SCAN step of
     * COUNT keys), then stops and leaves the rest for the next call; rounds with a stored cursor are continued first.
     */
    private int purgeLegacy() {
        List<String> pending = cmd.srandmember(LEGACY_PENDING, 20);
        if (pending == null || pending.isEmpty()) return 0;
        long deadline = System.nanoTime() + legacyScanBudgetMillis * 1_000_000L;
        List<String> order = new ArrayList<>(cmd.hkeys(LEGACY_CURSOR));
        for (String r : pending) if (!order.contains(r)) order.add(r);
        int deleted = 0;
        for (String r : order) {
            String cursor = cmd.hget(LEGACY_CURSOR, r);
            ScanArgs args = ScanArgs.Builder.matches(globEscape("pg:{" + r + "}:") + "*").limit(legacyScanCount);
            boolean finished = false;
            do {
                KeyScanCursor<String> kc = cmd.scan(ScanCursor.of(cursor == null ? "0" : cursor), args);
                deleted += unlink(new ArrayList<>(kc.getKeys()));
                cursor = kc.getCursor();
                finished = kc.isFinished();
            } while (!finished && System.nanoTime() < deadline);
            if (finished) {
                cmd.srem(LEGACY_PENDING, r);
                cmd.hdel(LEGACY_CURSOR, r);
            } else {
                cmd.hset(LEGACY_CURSOR, r, cursor);   // resume here next call
                break;                                // budget spent
            }
            if (System.nanoTime() >= deadline) break;
        }
        return deleted;
    }

    private static String globEscape(String s) {
        StringBuilder b = new StringBuilder();
        for (char ch : s.toCharArray()) {
            if (ch == '\\' || ch == '*' || ch == '?' || ch == '[' || ch == ']') b.append('\\');
            b.append(ch);
        }
        return b.toString();
    }

    private int unlink(List<String> keys) {
        if (keys.isEmpty()) return 0;
        Long n = cmd.unlink(keys.toArray(new String[0]));
        return n == null ? 0 : n.intValue();
    }

    @Override public void releaseThreadResources() {
        StatefulRedisConnection<String, String> c = blocking.get();
        if (c != null) {
            blocking.remove();
            blockingAll.remove(c);
            closeQuietly(c);
        }
    }

    @Override public void close() {
        closed = true;
        for (StatefulRedisConnection<String, String> c : blockingAll) closeQuietly(c);
        blockingAll.clear();
        closeQuietly(shared);
    }

    private static void closeQuietly(StatefulRedisConnection<String, String> c) {
        try {
            c.close();
        } catch (RuntimeException e) {
            log.debug("close failed", e);
        }
    }
}
