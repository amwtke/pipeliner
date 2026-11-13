package io.github.amwtke.pipeliner.runtime.fake;

import io.github.amwtke.pipeliner.api.Split;
import io.github.amwtke.pipeliner.core.codec.ChunkCodec;
import io.github.amwtke.pipeliner.core.dataplane.*;
import io.github.amwtke.pipeliner.core.store.ChunkEvent;
import io.github.amwtke.pipeliner.core.store.StaleEpochException;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

/** Small faithful in-memory DataPlane: streams with a single consumer group, pending entries, commit/BUSY/CONFLICT, journal. */
public class InMemoryDataPlane implements DataPlane {
    private static final class Pending { String consumer; long since; long deliveries = 1; }

    private static final class Stream {
        final TreeMap<Long, Map<String, String>> entries = new TreeMap<>();
        boolean group;
        long lastDelivered = 0;
        final Map<Long, Pending> pending = new HashMap<>();
        int maxLen;
    }

    private final AtomicLong seq = new AtomicLong(1000);
    private final Map<String, Stream> streams = new HashMap<>();
    private final Map<String, Map<String, DesiredState>> desired = new HashMap<>();
    private final Map<String, Long> fences = new HashMap<>();
    private long roundSeq;
    private final Map<String, Map<String, String>> assign = new HashMap<>();
    private final Set<String> paused = new HashSet<>();
    private final Set<String> active = new LinkedHashSet<>();
    private final Map<String, Long> workers = new HashMap<>();
    private final Map<String, Map<String, String>> workerInfos = new HashMap<>();
    private final LinkedList<String> commands = new LinkedList<>();
    private final Map<String, String> commandResults = new HashMap<>();
    private String leader;
    private long leaderExpiry;
    private long epoch;
    private boolean throttle;
    private long throttleUntil;
    public final AtomicLong busyCount = new AtomicLong();
    public final AtomicLong conflictCount = new AtomicLong();

    private Stream stream(String key) {
        Stream s = streams.get(key);
        if (s == null) { s = new Stream(); streams.put(key, s); }
        return s;
    }

    private static long num(String id) { return Long.parseLong(id.substring(0, id.indexOf('-'))); }

    private long add(String key, Map<String, String> fields) {
        Stream s = stream(key);
        long id = seq.incrementAndGet();
        s.entries.put(id, fields);
        s.maxLen = Math.max(s.maxLen, s.entries.size());
        return id;
    }

    private static String idStr(long id) { return id + "-0"; }

    /** Highest XLEN ever observed on the stream. */
    public synchronized int maxLen(String key) { Stream s = streams.get(key); return s == null ? 0 : s.maxLen; }

    public synchronized Set<String> keys() { return new TreeSet<>(streams.keySet()); }

    // membership
    @Override public synchronized void heartbeat(String podId, Map<String, String> info) {
        workers.put(podId, System.currentTimeMillis());
        if (info != null) workerInfos.put(podId, new HashMap<>(info));
    }
    @Override public synchronized Map<String, String> workerInfo(String podId) {
        return workerInfos.containsKey(podId) ? new HashMap<>(workerInfos.get(podId)) : new HashMap<String, String>();
    }
    @Override public synchronized void pushCommand(String json) { commands.add(json); }
    @Override public synchronized List<String> popCommands(int max) {
        List<String> out = new ArrayList<>();
        while (out.size() < max && !commands.isEmpty()) out.add(commands.removeFirst());
        return out;
    }
    @Override public synchronized void putCommandResult(String id, String json) { commandResults.put(id, json); }
    @Override public synchronized String getCommandResult(String id) { return commandResults.get(id); }
    @Override public synchronized Map<String, Long> liveWorkers(long maxAge) {
        Map<String, Long> m = new HashMap<>();
        long now = System.currentTimeMillis();
        for (Map.Entry<String, Long> e : workers.entrySet()) if (now - e.getValue() <= maxAge) m.put(e.getKey(), e.getValue());
        return m;
    }
    @Override public synchronized boolean tryAcquireOrRenewLeadership(String podId, long ttl) {
        long now = System.currentTimeMillis();
        if (leader == null || now > leaderExpiry || leader.equals(podId)) { leader = podId; leaderExpiry = now + ttl; return true; }
        return false;
    }
    @Override public synchronized void releaseLeadership(String podId) { if (podId.equals(leader)) leader = null; }
    @Override public synchronized String currentLeader() { return leader != null && System.currentTimeMillis() <= leaderExpiry ? leader : null; }
    @Override public synchronized long nextEpoch() { return ++epoch; }
    @Override public synchronized long currentEpoch() { return epoch; }

    // rounds
    @Override public synchronized void addActiveRound(String r) { active.add(r); }
    /** Every master mutation: rejected when a newer master epoch fenced the round (nothing changes). */
    private void fenced(String r, long epoch) {
        Long f = fences.get(r);
        if (f != null && f > epoch) throw new StaleEpochException("round " + r + " data plane fenced by epoch " + f + " > " + epoch);
    }
    @Override public synchronized boolean fenceRound(String r, long epoch) {
        Long f = fences.get(r);
        if (f != null && f > epoch) return false;
        fences.put(r, epoch);
        return true;
    }
    @Override public synchronized long nextRoundSeq() { return ++roundSeq; }
    @Override public synchronized void removeActiveRound(String r, long epoch) { fenced(r, epoch); active.remove(r); }
    @Override public synchronized Set<String> activeRounds() { return new LinkedHashSet<>(active); }
    @Override public synchronized void setDesiredParallelism(String r, String p, int n, int attempt, long epoch) {
        fenced(r, epoch);
        Map<String, DesiredState> m = desired.get(r);
        if (m == null) { m = new LinkedHashMap<>(); desired.put(r, m); }
        if (n <= 0) m.remove(p); else m.put(p, new DesiredState(n, attempt));
    }
    @Override public synchronized Map<String, DesiredState> desiredState(String r) {
        return desired.get(r) == null ? new HashMap<String, DesiredState>() : new LinkedHashMap<>(desired.get(r));
    }
    @Override public synchronized void assignLocalRun(String r, String run, String pod, int attempt, long epoch) {
        fenced(r, epoch);
        Map<String, String> m = assign.get(r);
        if (m == null) { m = new HashMap<>(); assign.put(r, m); }
        m.put(run, pod + "|" + attempt);
    }
    @Override public synchronized Map<String, String> localRunAssignments(String r) {
        return assign.get(r) == null ? new HashMap<String, String>() : new HashMap<>(assign.get(r));
    }
    @Override public synchronized void clearLocalRun(String r, String run, long epoch) { fenced(r, epoch); if (assign.get(r) != null) assign.get(r).remove(run); }
    @Override public synchronized void setPaused(String r, boolean p, long epoch) { fenced(r, epoch); if (p) paused.add(r); else paused.remove(r); }
    @Override public synchronized boolean isPaused(String r) { return paused.contains(r); }
    public final AtomicLong throttleSets = new AtomicLong();
    public volatile long lastThrottleTtl;
    /** Test knob for usedMemoryBytes(). */
    public volatile long usedMemory;
    @Override public synchronized void setGlobalThrottle(boolean on, long ttlMillis) {
        if (on) { throttleSets.incrementAndGet(); lastThrottleTtl = ttlMillis; }
        throttle = on;
        throttleUntil = on && ttlMillis > 0 ? System.currentTimeMillis() + ttlMillis : 0;
    }
    @Override public synchronized boolean globalThrottle() { return throttle && (throttleUntil == 0 || System.currentTimeMillis() < throttleUntil); }

    // streams
    @Override public synchronized void ensureGroup(String r, String key, long epoch) { fenced(r, epoch); stream(key).group = true; }

    @Override public synchronized boolean enqueueSplit(String r, String p, int attempt, Split split, long epoch) {
        fenced(r, epoch);
        String idx = Keys.splitIndex(r, p, attempt);
        Stream i = stream(idx);
        long marker = Long.parseLong(split.getSplitId().substring(1)) + 1;
        if (i.entries.containsKey(marker)) return false;
        i.entries.put(marker, new HashMap<String, String>());
        Map<String, String> f = new HashMap<>();
        f.put("s", ChunkCodec.encodeSplit(split));
        add(Keys.splitQueue(r, p, attempt), f);
        return true;
    }

    @Override public synchronized long streamLength(String key) { Stream s = streams.get(key); return s == null ? 0 : s.entries.size(); }

    @Override public List<StreamEntry> read(String consumer, List<String> keys, int count, long block) {
        long deadline = System.currentTimeMillis() + block;
        while (true) {
            synchronized (this) {
                List<StreamEntry> out = new ArrayList<>();
                for (String key : keys) {
                    Stream s = streams.get(key);
                    if (s == null || !s.group) throw new IllegalStateException("NOGROUP " + key);
                    for (Map.Entry<Long, Map<String, String>> e : s.entries.tailMap(s.lastDelivered, false).entrySet()) {
                        if (out.size() >= count) break;
                        Pending p = new Pending();
                        p.consumer = consumer;
                        p.since = System.currentTimeMillis();
                        s.pending.put(e.getKey(), p);
                        s.lastDelivered = e.getKey();
                        out.add(new StreamEntry(key, idStr(e.getKey()), new HashMap<>(e.getValue())));
                    }
                    if (!out.isEmpty()) return out;
                }
            }
            if (System.currentTimeMillis() >= deadline) return new ArrayList<>();
            try { Thread.sleep(5); } catch (InterruptedException ie) { Thread.currentThread().interrupt(); throw new RuntimeException(ie); }
        }
    }

    @Override public synchronized List<StreamEntry> autoClaim(String consumer, String key, long minIdle, int count) {
        List<StreamEntry> out = new ArrayList<>();
        Stream s = streams.get(key);
        if (s == null) return out;
        long now = System.currentTimeMillis();
        List<Long> ids = new ArrayList<>(s.pending.keySet());
        Collections.sort(ids);
        for (Long id : ids) {
            if (out.size() >= count) break;
            Pending p = s.pending.get(id);
            if (now - p.since < minIdle || !s.entries.containsKey(id)) continue;
            p.consumer = consumer;
            p.since = now;
            p.deliveries++;
            out.add(new StreamEntry(key, idStr(id), new HashMap<>(s.entries.get(id)), p.deliveries));
        }
        return out;
    }

    @Override public synchronized boolean renew(String consumer, String key, String entryId) {
        Stream s = streams.get(key);
        if (s == null || !s.group) throw new IllegalStateException("NOGROUP " + key);
        Pending p = s.pending.get(num(entryId));
        if (p != null) { p.consumer = consumer; p.since = System.currentTimeMillis(); }
        return p != null;
    }

    @Override public synchronized CommitResult commit(CommitRequest req) {
        Set<String> outKeys = new LinkedHashSet<>();
        for (OutputChunk o : req.getOutputs()) outKeys.add(o.getStreamKey());
        for (String k : outKeys) if (streamLength(k) >= req.getHighWatermark()) { busyCount.incrementAndGet(); return CommitResult.BUSY; }
        Stream in = streams.get(req.getInputStreamKey());   // like XACK, never creates the key
        long id = num(req.getInputEntryId());
        if (in == null || in.pending.remove(id) == null) { conflictCount.incrementAndGet(); return CommitResult.CONFLICT; }
        in.entries.remove(id);
        for (OutputChunk o : req.getOutputs()) {
            Map<String, String> f = new HashMap<>();
            f.put("c", o.getPayload());
            add(o.getStreamKey(), f);
        }
        Map<String, String> je = new HashMap<>();
        je.put("e", ChunkCodec.encodeEvent(req.getEvent()));
        add(Keys.journal(req.getRoundId()), je);
        return CommitResult.OK;
    }

    @Override public synchronized void deadLetter(DeadLetterRequest req) {
        Stream in = streams.get(req.getInputStreamKey());
        long id = num(req.getInputEntryId());
        if (in == null || in.pending.remove(id) == null) return;   // not pending any more: already committed or dead-lettered
        in.entries.remove(id);
        Map<String, String> f = new HashMap<>(req.getInputFields());
        f.put("src", req.getInputStreamKey());
        f.put("err", req.getError());
        add(Keys.dlq(req.getRoundId(), req.getPipelineId(), req.getAttempt()), f);
        Map<String, String> je = new HashMap<>();
        je.put("e", ChunkCodec.encodeEvent(req.getEvent()));
        add(Keys.journal(req.getRoundId()), je);
    }

    @Override public synchronized int requeueDeadLetters(String r, String p, int attempt, long epoch) {
        fenced(r, epoch);
        Stream d = streams.get(Keys.dlq(r, p, attempt));
        if (d == null) return 0;
        int n = 0;
        for (Map<String, String> f : new ArrayList<>(d.entries.values())) {
            Map<String, String> copy = new HashMap<>(f);
            String src = copy.remove("src");
            copy.remove("err");
            add(src, copy);
            n++;
        }
        d.entries.clear();
        return n;
    }

    @Override public synchronized int clearDeadLetters(String r, String p, int attempt, long epoch) {
        fenced(r, epoch);
        Stream d = streams.get(Keys.dlq(r, p, attempt));
        if (d == null) return 0;
        int n = d.entries.size();
        d.entries.clear();
        return n;
    }

    @Override public synchronized void appendJournal(String r, ChunkEvent e) {
        Map<String, String> je = new HashMap<>();
        je.put("e", ChunkCodec.encodeEvent(e));
        add(Keys.journal(r), je);
    }

    @Override public synchronized List<ChunkEvent> readJournal(String r, String afterId, int count) {
        List<ChunkEvent> out = new ArrayList<>();
        Stream s = streams.get(Keys.journal(r));
        if (s == null) return out;
        for (Map.Entry<Long, Map<String, String>> e : s.entries.tailMap(num(afterId), false).entrySet()) {
            if (out.size() >= count) break;
            ChunkEvent ev = ChunkCodec.decodeEvent(e.getValue().get("e"));
            ev.setEventId(idStr(e.getKey()));
            out.add(ev);
        }
        return out;
    }

    @Override public synchronized void trimJournal(String r, String upTo, long epoch) {
        fenced(r, epoch);
        Stream s = streams.get(Keys.journal(r));
        if (s != null) s.entries.headMap(num(upTo), false).clear();
    }

    @Override public synchronized void deleteKeys(String round, Collection<String> keys, long epoch) {
        fenced(round, epoch);
        for (String k : keys) {
            streams.remove(k);
            String r = roundOf(k);
            if (k.equals(Keys.desired(r))) desired.remove(r);
            if (k.equals(Keys.assign(r))) assign.remove(r);
            if (k.equals(Keys.paused(r))) paused.remove(r);
        }
    }

    private static String roundOf(String key) {
        int a = key.indexOf('{'), b = key.indexOf('}');
        return a < 0 ? "" : key.substring(a + 1, b);
    }

    @Override public long usedMemoryBytes() { return usedMemory; }

    /** Test knob: workers pruned so far. */
    public final AtomicLong pruned = new AtomicLong();
    public final List<String> purgedRoundData = new java.util.concurrent.CopyOnWriteArrayList<>();

    @Override public synchronized int pruneWorkers(long maxAgeMillis) {
        long now = System.currentTimeMillis();
        int n = 0;
        for (Iterator<Map.Entry<String, Long>> it = workers.entrySet().iterator(); it.hasNext(); )
            if (now - it.next().getValue() > maxAgeMillis) { it.remove(); n++; }
        pruned.addAndGet(n);
        return n;
    }

    @Override public synchronized PendingSummary pendingSummary(String key) {
        Stream s = streams.get(key);
        if (s == null || s.pending.isEmpty()) return PendingSummary.none();
        long id = Collections.min(s.pending.keySet());
        Pending p = s.pending.get(id);
        return new PendingSummary(s.pending.size(), idStr(id), System.currentTimeMillis() - p.since, p.consumer, p.deliveries);
    }

    /** Test helper: makes a pending entry look idle for the given time. */
    public synchronized void ageAllPending(String key, long millis) {
        Stream s = streams.get(key);
        if (s != null) for (Pending p : s.pending.values()) p.since -= millis;
    }

    @Override public synchronized int purgeRoundData(Collection<String> roundIds) {
        int n = 0;
        for (String r : roundIds) {
            purgedRoundData.add(r);
            for (Iterator<String> it = streams.keySet().iterator(); it.hasNext(); ) if (roundOf(it.next()).equals(r)) { it.remove(); n++; }
        }
        return n;
    }

    /** Test helper: backdates a worker heartbeat. */
    public synchronized void putWorker(String pod, long lastBeat) { workers.put(pod, lastBeat); }
    @Override public void releaseThreadResources() {}
    @Override public void close() {}
}
