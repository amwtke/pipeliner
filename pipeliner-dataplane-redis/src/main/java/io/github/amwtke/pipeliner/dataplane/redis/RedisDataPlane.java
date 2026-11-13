package io.github.amwtke.pipeliner.dataplane.redis;

import io.github.amwtke.pipeliner.api.PipelinerException;
import io.github.amwtke.pipeliner.api.Split;
import io.github.amwtke.pipeliner.core.codec.ChunkCodec;
import io.github.amwtke.pipeliner.core.dataplane.*;
import io.github.amwtke.pipeliner.core.store.ChunkEvent;
import io.lettuce.core.*;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.codec.StringCodec;
import io.lettuce.core.models.stream.ClaimedMessages;
import io.lettuce.core.models.stream.PendingMessage;
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
    private final Script leaderS = new Script("leader");
    private final Script releaseS = new Script("release_leader");

    public RedisDataPlane(RedisClient client) {
        this.client = client;
        this.shared = client.connect(StringCodec.UTF8);
        this.cmd = shared.sync();
        for (Script s : Arrays.asList(commitS, enqueueS, deadS, requeueS, leaderS, releaseS)) s.load();
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
            Object r;
            try {
                if (sha == null) load();
                r = cmd.evalsha(sha, ScriptOutputType.INTEGER, keys, args);
            } catch (RedisNoScriptException e) {
                r = cmd.eval(text, ScriptOutputType.INTEGER, keys, args);
                sha = null;
            } catch (RedisCommandExecutionException e) {
                if (e.getMessage() == null || !e.getMessage().startsWith("NOSCRIPT")) throw e;
                r = cmd.eval(text, ScriptOutputType.INTEGER, keys, args);
                sha = null;
            }
            return ((Number) r).longValue();
        }
    }

    // membership and leadership

    @Override public void heartbeat(String podId, Map<String, String> info) {
        cmd.zadd(Keys.workers(), (double) System.currentTimeMillis(), podId);
        if (info != null && !info.isEmpty()) {
            cmd.hset(Keys.workerInfo(podId), info);
            cmd.pexpire(Keys.workerInfo(podId), WORKER_INFO_TTL_MS);
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

    @Override public long nextEpoch() { return cmd.incr(Keys.epoch()); }

    @Override public long currentEpoch() {
        String v = cmd.get(Keys.epoch());
        return v == null ? 0L : Long.parseLong(v);
    }

    // rounds and desired state

    @Override public void addActiveRound(String roundId) { cmd.sadd(Keys.activeRounds(), roundId); }

    @Override public void removeActiveRound(String roundId) { cmd.srem(Keys.activeRounds(), roundId); }

    @Override public Set<String> activeRounds() { return new LinkedHashSet<>(cmd.smembers(Keys.activeRounds())); }

    @Override public void setDesiredParallelism(String roundId, String pipelineId, int parallelism) {
        if (parallelism <= 0) cmd.hdel(Keys.desired(roundId), pipelineId);
        else cmd.hset(Keys.desired(roundId), pipelineId, String.valueOf(parallelism));
    }

    @Override public Map<String, Integer> desiredParallelism(String roundId) {
        Map<String, Integer> out = new LinkedHashMap<>();
        for (Map.Entry<String, String> e : cmd.hgetall(Keys.desired(roundId)).entrySet()) {
            out.put(e.getKey(), Integer.valueOf(e.getValue()));
        }
        return out;
    }

    @Override public void assignLocalRun(String roundId, String runId, String podId, int attempt) {
        cmd.hset(Keys.assign(roundId), runId, podId + "|" + attempt);
    }

    @Override public Map<String, String> localRunAssignments(String roundId) {
        return new LinkedHashMap<>(cmd.hgetall(Keys.assign(roundId)));
    }

    @Override public void clearLocalRun(String roundId, String runId) { cmd.hdel(Keys.assign(roundId), runId); }

    @Override public void setPaused(String roundId, boolean paused) {
        if (paused) cmd.set(Keys.paused(roundId), "1");
        else cmd.del(Keys.paused(roundId));
    }

    @Override public boolean isPaused(String roundId) { return cmd.exists(Keys.paused(roundId)) > 0; }

    @Override public void setGlobalThrottle(boolean on) {
        if (on) cmd.set(Keys.throttle(), "1");
        else cmd.del(Keys.throttle());
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

    @Override public void putCommandResult(String commandId, String resultJson) {
        cmd.set(Keys.commandResult(commandId), resultJson, SetArgs.Builder.ex(600));
    }

    @Override public String getCommandResult(String commandId) { return cmd.get(Keys.commandResult(commandId)); }

    // streams

    @Override public void ensureGroup(String streamKey) {
        try {
            cmd.xgroupCreate(XReadArgs.StreamOffset.from(streamKey, "0"), Keys.GROUP, XGroupCreateArgs.Builder.mkstream());
        } catch (RedisBusyException e) {
            // group already exists
        } catch (RedisCommandExecutionException e) {
            if (e.getMessage() == null || !e.getMessage().startsWith("BUSYGROUP")) throw e;
        }
    }

    @Override public boolean enqueueSplit(String roundId, String pipelineId, Split split) {
        return enqueueS.run(new String[] {Keys.splitIndex(roundId, pipelineId), Keys.splitQueue(roundId, pipelineId)},
                split.getSplitId(), ChunkCodec.encodeSplit(split)) == 1;
    }

    @Override public long streamLength(String streamKey) {
        Long n = cmd.xlen(streamKey);
        return n == null ? 0L : n;
    }

    private RedisCommands<String, String> readConnection(long blockMillis) {
        if (blockMillis <= 0) return cmd;
        StatefulRedisConnection<String, String> c = blocking.get();
        if (c == null || !c.isOpen()) {
            if (closed) throw new PipelinerException("data plane closed");
            if (c != null) blockingAll.remove(c);
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

    @Override public List<StreamEntry> autoClaim(String consumer, String streamKey, long minIdleMillis, int count) {
        XAutoClaimArgs<String> args = new XAutoClaimArgs<String>()
                .consumer(Consumer.from(Keys.GROUP, consumer))
                .minIdleTime(minIdleMillis).startId("0-0").count(count);
        ClaimedMessages<String, String> r = cmd.xautoclaim(streamKey, args);
        List<StreamEntry> out = toEntries(r.getMessages(), streamKey);
        for (StreamEntry e : out) {   // XAUTOCLAIM has just counted this delivery
            List<PendingMessage> pm = cmd.xpending(streamKey, Consumer.from(Keys.GROUP, consumer),
                    Range.create(e.getEntryId(), e.getEntryId()), Limit.from(1));
            if (!pm.isEmpty()) e.setDeliveryCount(pm.get(0).getRedeliveryCount());
        }
        return out;
    }

    @Override public void renew(String consumer, String streamKey, String entryId) {
        cmd.xclaim(streamKey, Consumer.from(Keys.GROUP, consumer), new XClaimArgs().minIdleTime(0).justid(), entryId);
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

    @Override public void deadLetter(DeadLetterRequest req) {
        String[] keys = {req.getInputStreamKey(), Keys.dlq(req.getRoundId(), req.getPipelineId()), Keys.journal(req.getRoundId())};
        List<String> args = new ArrayList<>(Arrays.asList(Keys.GROUP, req.getInputEntryId(),
                req.getError() == null ? "" : req.getError(),
                req.getEvent() == null ? "" : ChunkCodec.encodeEvent(req.getEvent())));
        if (req.getInputFields() != null) {
            for (Map.Entry<String, String> e : req.getInputFields().entrySet()) {
                args.add(e.getKey());
                args.add(e.getValue());
            }
        }
        deadS.run(keys, args.toArray(new String[0]));
    }

    @Override public int requeueDeadLetters(String roundId, String pipelineId) {
        String[] keys = {Keys.dlq(roundId, pipelineId)};
        int total = 0;
        long n;
        do {
            n = requeueS.run(keys, String.valueOf(REQUEUE_BATCH));
            total += n;
        } while (n >= REQUEUE_BATCH);
        return total;
    }

    @Override public int clearDeadLetters(String roundId, String pipelineId) {
        String key = Keys.dlq(roundId, pipelineId);
        Long n = cmd.xlen(key);
        cmd.del(key);
        return n == null ? 0 : n.intValue();
    }

    @Override public void appendJournal(String roundId, ChunkEvent event) {
        cmd.xadd(Keys.journal(roundId), Collections.singletonMap("e", ChunkCodec.encodeEvent(event)));
    }

    @Override public List<ChunkEvent> readJournal(String roundId, String afterId, int count) {
        Range<String> range = (afterId == null || afterId.isEmpty() || "0-0".equals(afterId) || "0".equals(afterId))
                ? Range.unbounded()
                : Range.from(Range.Boundary.excluding(afterId), Range.Boundary.unbounded());
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

    @Override public void trimJournal(String roundId, String upToId) {
        cmd.xtrim(Keys.journal(roundId), new XTrimArgs().minId(upToId).exactTrimming());
    }

    @Override public void deleteKeys(Collection<String> keys) {
        if (keys == null || keys.isEmpty()) return;
        cmd.del(keys.toArray(new String[0]));
    }

    @Override public long usedMemoryBytes() {
        for (String line : cmd.info("memory").split("\r?\n")) {
            if (line.startsWith("used_memory:")) return Long.parseLong(line.substring("used_memory:".length()).trim());
        }
        return 0L;
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
