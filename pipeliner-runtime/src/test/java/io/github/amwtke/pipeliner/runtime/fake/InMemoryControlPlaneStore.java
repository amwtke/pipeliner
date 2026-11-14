package io.github.amwtke.pipeliner.runtime.fake;

import io.github.amwtke.pipeliner.core.codec.ChunkCodec;
import io.github.amwtke.pipeliner.core.store.*;
import java.util.*;

/** Small faithful in-memory ControlPlaneStore (epoch fencing, copy-on-read) for tests. */
public class InMemoryControlPlaneStore implements ControlPlaneStore {
    private final Map<String, RoundRecord> rounds = new LinkedHashMap<>();
    private final Map<String, Map<String, RunRecord>> runs = new HashMap<>();
    private final Map<String, Map<String, GateRecord>> gates = new HashMap<>();
    private final Map<String, Map<String, PipelineRecord>> pipes = new HashMap<>();
    private final Map<String, TreeMap<String, ChunkRecord>> chunks = new HashMap<>();
    private final Map<String, Map<String, String>> signals = new HashMap<>();

    private static <T> T copy(T o) { return o == null ? null : (T) ChunkCodec.fromJson(ChunkCodec.toJson(o), o.getClass()); }

    private static String ck(String pipelineId, String chunkId) { return pipelineId + "\u0000" + chunkId; }

    @Override public void init() {}

    @Override public synchronized RoundRecord createRound(RoundRecord round, List<RunRecord> rs, List<GateRecord> gs, List<PipelineRecord> ps) {
        RoundRecord ex = findRoundByBizKey(round.getGraphName(), round.getBizKey());
        if (ex != null) return ex;
        String id = round.getRoundId();
        if (rounds.containsKey(id)) throw new DuplicateRoundIdException(id);
        maxSeq = Math.max(maxSeq, round.getSeq());
        rounds.put(id, copy(round));
        Map<String, RunRecord> rm = new LinkedHashMap<>();
        for (RunRecord r : rs) rm.put(r.getRunId(), copy(r));
        runs.put(id, rm);
        Map<String, GateRecord> gm = new LinkedHashMap<>();
        for (GateRecord g : gs) gm.put(g.getGateId(), copy(g));
        gates.put(id, gm);
        Map<String, PipelineRecord> pm = new LinkedHashMap<>();
        for (PipelineRecord p : ps) pm.put(p.getPipelineId(), copy(p));
        pipes.put(id, pm);
        chunks.put(id, new TreeMap<String, ChunkRecord>());
        return copy(round);
    }

    @Override public synchronized RoundRecord findRoundByBizKey(String graphName, String bizKey) {
        for (RoundRecord r : rounds.values()) if (r.getGraphName().equals(graphName) && Objects.equals(r.getBizKey(), bizKey)) return copy(r);
        return null;
    }

    @Override public synchronized RoundRecord loadRoundRecord(String roundId) { return copy(rounds.get(roundId)); }

    @Override public synchronized RoundView loadRound(String roundId) {
        RoundRecord rr = rounds.get(roundId);
        if (rr == null) return null;
        Map<String, Map<ChunkStatus, Long>> counts = new LinkedHashMap<>();
        Map<String, Long> in = new LinkedHashMap<>(), out = new LinkedHashMap<>();
        for (ChunkRecord c : chunks.get(roundId).values()) {
            Map<ChunkStatus, Long> m = counts.get(c.getPipelineId());
            if (m == null) { m = new EnumMap<>(ChunkStatus.class); counts.put(c.getPipelineId(), m); }
            m.merge(c.getStatus(), 1L, Long::sum);
            if (c.getStatus() == ChunkStatus.DONE) {
                if (c.getRowsIn() != null) in.merge(c.getPipelineId(), c.getRowsIn(), Long::sum);
                if (c.getRowsOut() != null) for (Long v : c.getRowsOut().values()) out.merge(c.getPipelineId(), v, Long::sum);
            }
        }
        StoreViews.mergeSummaries(new ArrayList<>(pipes.get(roundId).values()), counts, in, out);   // compacted pipelines keep their summary
        return RoundView.builder().round(copy(rr)).runs(copyAll(runs.get(roundId).values())).gates(copyAll(gates.get(roundId).values()))
                .pipelines(copyAll(pipes.get(roundId).values())).chunkCounts(counts).rowsIn(in).rowsOut(out).build();
    }

    private static <T> List<T> copyAll(Collection<T> c) {
        List<T> l = new ArrayList<>();
        for (T t : c) l.add(copy(t));
        return l;
    }

    @Override public synchronized RoundView loadRoundState(String roundId) {
        RoundRecord rr = rounds.get(roundId);
        if (rr == null) return null;
        return RoundView.builder().round(copy(rr)).runs(copyAll(runs.get(roundId).values())).gates(copyAll(gates.get(roundId).values()))
                .pipelines(copyAll(pipes.get(roundId).values())).chunkCounts(new LinkedHashMap<String, Map<ChunkStatus, Long>>())
                .rowsIn(new LinkedHashMap<String, Long>()).rowsOut(new LinkedHashMap<String, Long>()).build();
    }

    @Override public synchronized boolean fenceRound(String roundId, long epoch) {
        RoundRecord r = rounds.get(roundId);
        if (r == null || r.getMasterEpoch() > epoch) return false;
        r.setMasterEpoch(epoch);
        return true;
    }

    private long reservedEpoch;
    private long maxSeq;
    /** Test hooks: maxMasterEpoch throws it (RuntimeException or Error), reserveMasterEpoch always refuses. */
    public volatile Throwable failMaxMasterEpoch;
    public volatile boolean refuseReservations;

    @Override public synchronized long maxMasterEpoch() {
        Throwable f = failMaxMasterEpoch;
        if (f instanceof Error) throw (Error) f;
        if (f instanceof RuntimeException) throw (RuntimeException) f;
        long m = reservedEpoch;
        for (RoundRecord r : rounds.values()) m = Math.max(m, r.getMasterEpoch());
        return m;
    }

    @Override public synchronized boolean reserveMasterEpoch(long epoch) {
        if (refuseReservations) return false;
        if (epoch <= maxMasterEpoch()) return false;
        reservedEpoch = epoch;
        return true;
    }

    @Override public synchronized long maxRoundSeq() { return maxSeq; }

    /** Round-level fence: a write is allowed only if round.masterEpoch &lt;= epoch. */
    private boolean fenced(String roundId, long epoch) { return rounds.get(roundId).getMasterEpoch() > epoch; }

    @Override public synchronized List<String> findActiveRounds() {
        List<String> l = new ArrayList<>();
        for (RoundRecord r : rounds.values()) if (!r.getStatus().terminal()) l.add(r.getRoundId());
        return l;
    }

    @Override public synchronized List<RoundRecord> listRecentRounds(String graphName, int limit) {
        List<RoundRecord> all = new ArrayList<>();
        for (RoundRecord r : rounds.values()) if (graphName == null || graphName.isEmpty() || graphName.equals(r.getGraphName())) all.add(r);
        all.sort((a, b) -> {
            if (a.getStatus().terminal() != b.getStatus().terminal()) return a.getStatus().terminal() ? 1 : -1;
            long x = a.getCreatedAt() == null ? 0 : a.getCreatedAt(), y = b.getCreatedAt() == null ? 0 : b.getCreatedAt();
            return x != y ? Long.compare(y, x) : b.getRoundId().compareTo(a.getRoundId());
        });
        return new ArrayList<>(all.subList(0, Math.min(Math.max(limit, 0), all.size())));
    }

    @Override public synchronized boolean casRoundStatus(String roundId, RoundStatus expect, RoundStatus target, String error, long epoch) {
        RoundRecord r = rounds.get(roundId);
        if (r == null || r.getStatus() != expect || r.getMasterEpoch() > epoch) return false;
        r.setStatus(target);
        r.setMasterEpoch(epoch);
        long now = System.currentTimeMillis();
        if (expect == RoundStatus.FAILED && target == RoundStatus.RUNNING) { r.setError(null); r.setFinishedAt(null); }   // revive (contract C1)
        if (error != null) r.setError(error);
        if (target == RoundStatus.RUNNING && r.getStartedAt() == null) r.setStartedAt(now);
        if (target.terminal()) r.setFinishedAt(now);
        return true;
    }

    @Override public synchronized void setRoundIncomplete(String roundId) { rounds.get(roundId).setIncomplete(true); }

    @Override public synchronized boolean casRunStatus(String roundId, String runId, RunStatus expect, RunStatus target, String error, long epoch) {
        RunRecord r = runs.get(roundId).get(runId);
        if (r == null || r.getStatus() != expect || r.getMasterEpoch() > epoch || fenced(roundId, epoch)) return false;
        r.setStatus(target);
        r.setMasterEpoch(epoch);
        long now = System.currentTimeMillis();
        if (expect == RunStatus.FAILED && target == RunStatus.RUNNING) { r.setError(null); r.setFinishedAt(null); }
        if (error != null) r.setError(error);
        if (target == RunStatus.RUNNING && r.getStartedAt() == null) r.setStartedAt(now);
        if (target.terminal()) r.setFinishedAt(now);
        return true;
    }

    @Override public synchronized boolean updateRunAssignment(String roundId, String runId, String assignee, int attempt, long epoch) {
        RunRecord r = runs.get(roundId).get(runId);
        if (r == null || r.getMasterEpoch() > epoch || fenced(roundId, epoch)) return false;
        r.setAssignee(assignee);
        r.setAttempt(attempt);
        r.setMasterEpoch(epoch);
        return true;
    }

    @Override public synchronized boolean casPipelineStatus(String roundId, String pipelineId, PipelineStatus expect, PipelineStatus target, String error, long epoch) {
        PipelineRecord p = pipes.get(roundId).get(pipelineId);
        if (p == null || p.getStatus() != expect || p.getMasterEpoch() > epoch || fenced(roundId, epoch)) return false;
        p.setStatus(target);
        p.setMasterEpoch(epoch);
        long now = System.currentTimeMillis();
        if (expect == PipelineStatus.FAILED && target == PipelineStatus.RUNNING) { p.setError(null); p.setFinishedAt(null); }
        if (error != null) p.setError(error);
        if (target == PipelineStatus.RUNNING && p.getStartedAt() == null) p.setStartedAt(now);
        if (target.terminal()) p.setFinishedAt(now);
        return true;
    }

    @Override public synchronized void setPipelineEnumDone(String roundId, String pipelineId, boolean done, long epoch) {
        PipelineRecord p = pipes.get(roundId).get(pipelineId);
        if (p.getMasterEpoch() > epoch || fenced(roundId, epoch)) return;
        p.setEnumDone(done);
        p.setMasterEpoch(epoch);
    }

    @Override public synchronized void setPipelineParallelism(String roundId, String pipelineId, int parallelism, long epoch) {
        PipelineRecord p = pipes.get(roundId).get(pipelineId);
        if (p.getMasterEpoch() > epoch || fenced(roundId, epoch)) return;
        p.setParallelism(parallelism);
        p.setMasterEpoch(epoch);
    }

    @Override public synchronized void armGate(String roundId, String gateId, long armedAt, Long deadlineAt, long epoch) {
        GateRecord g = gates.get(roundId).get(gateId);
        if (g.getMasterEpoch() > epoch || fenced(roundId, epoch)) return;
        g.setArmedAt(armedAt);
        g.setDeadlineAt(deadlineAt);
        g.setMasterEpoch(epoch);
    }

    @Override public synchronized boolean casGateStatus(String roundId, String gateId, GateStatus expect, GateStatus target, String openedBy, Map<String, String> context, long epoch) {
        GateRecord g = gates.get(roundId).get(gateId);
        if (g == null || g.getStatus() != expect || g.getMasterEpoch() > epoch || fenced(roundId, epoch)) return false;
        g.setStatus(target);
        if (openedBy != null) g.setOpenedBy(openedBy);
        if (target != GateStatus.CLOSED) g.setOpenedAt(System.currentTimeMillis());
        if (context != null) g.setContext(new HashMap<>(context));
        g.setMasterEpoch(epoch);
        return true;
    }

    @Override public synchronized void markGateAlerted(String roundId, String gateId) { gates.get(roundId).get(gateId).setAlerted(true); }

    @Override public synchronized void saveGateSignal(String roundId, String gateId, String key, String payload) {
        if (key == null || key.isEmpty() || key.length() > MAX_SIGNAL_KEY_LENGTH) throw new IllegalArgumentException("signal key must be 1.." + MAX_SIGNAL_KEY_LENGTH + " characters");
        Map<String, String> m = signals.get(roundId + "|" + gateId);
        if (m == null) { m = new LinkedHashMap<>(); signals.put(roundId + "|" + gateId, m); }
        m.putIfAbsent(key, payload);   // idempotent per key: the first payload is kept (HSETNX / INSERT IGNORE)
    }

    @Override public synchronized Map<String, String> loadGateSignals(String roundId, String gateId) {
        Map<String, String> m = signals.get(roundId + "|" + gateId);
        return m == null ? new LinkedHashMap<String, String>() : new LinkedHashMap<>(m);
    }

    @Override public synchronized void savePendingChunks(String roundId, List<ChunkRecord> list, long epoch) {
        if (rounds.get(roundId) == null || fenced(roundId, epoch)) return;
        for (ChunkRecord c : list) chunks.get(roundId).putIfAbsent(ck(c.getPipelineId(), c.getChunkId()), copy(c));
    }

    /** Test hook: the N-th applyChunkEvents call (1-based) throws once; 0 = never. */
    public volatile int failApplyOnCall;
    private int applyCalls;

    @Override public synchronized void applyChunkEvents(String roundId, List<ChunkEvent> events, String newOffset, long epoch) {
        if (++applyCalls == failApplyOnCall) throw new IllegalStateException("injected store failure");
        RoundRecord rr = rounds.get(roundId);
        if (rr == null) throw new StaleEpochException("round " + roundId + " missing");
        if (rr.getMasterEpoch() > epoch) throw new StaleEpochException("fenced: round " + roundId + " epoch " + rr.getMasterEpoch() + " > " + epoch);
        for (ChunkEvent e : events) {
            if (!"DONE".equals(e.getType()) && !"DEAD".equals(e.getType())) continue;
            String key = ck(e.getPipelineId(), e.getChunkId());
            ChunkRecord c = chunks.get(roundId).get(key);
            if (c == null) {
                c = ChunkRecord.builder().roundId(roundId).pipelineId(e.getPipelineId()).chunkId(e.getChunkId()).build();
                chunks.get(roundId).put(key, c);
            }
            if ("DEAD".equals(e.getType())) {
                if (c.getStatus() == ChunkStatus.DONE) continue;
                c.setStatus(ChunkStatus.DEAD);
                c.setError(e.getError());
            } else {
                c.setStatus(ChunkStatus.DONE);
                c.setError(null);
                c.setRowsIn(e.getRowsIn());
                c.setRowsOut(e.getRowsOut() == null ? null : new HashMap<>(e.getRowsOut()));
                c.setCostMs(e.getCostMs());
            }
            c.setRunId(e.getRunId());
            c.setInputEdge(e.getInputEdge() != null ? e.getInputEdge() : c.getInputEdge());
            c.setOwner(e.getOwner());
            c.setUpdatedAt(e.getTs());
        }
        rr.setJournalOffset(newOffset);
        rr.setMasterEpoch(epoch);
    }

    @Override public synchronized void markChunksSkipped(String roundId, String runId, long epoch) {
        if (rounds.get(roundId) == null || fenced(roundId, epoch)) return;
        for (ChunkRecord c : chunks.get(roundId).values())
            if (runId.equals(c.getRunId()) && c.getStatus() == ChunkStatus.DEAD) c.setStatus(ChunkStatus.SKIPPED);
    }

    @Override public synchronized void resetRuns(String roundId, List<String> runIds, List<String> gateIds, long epoch) {
        RoundRecord rr = rounds.get(roundId);
        if (rr == null || rr.getMasterEpoch() > epoch) throw new StaleEpochException("resetRuns fenced: round " + roundId + " epoch " + epoch);
        for (String runId : runIds) {
            RunRecord r = runs.get(roundId).get(runId);
            if (r != null && r.getMasterEpoch() > epoch) throw new StaleEpochException("resetRuns fenced: run " + runId + " epoch " + r.getMasterEpoch());
        }
        rr.setMasterEpoch(epoch);
        rr.setIncomplete(false);
        for (String runId : runIds) {
            RunRecord r = runs.get(roundId).get(runId);
            if (r == null) continue;
            r.setStatus(RunStatus.PENDING);
            r.setAttempt(r.getAttempt() + 1);
            r.setAssignee(null);
            r.setStartedAt(null);
            r.setFinishedAt(null);
            r.setError(null);
            r.setMasterEpoch(Math.max(r.getMasterEpoch(), epoch));
            for (PipelineRecord p : pipes.get(roundId).values()) {
                if (!p.getRunId().equals(runId)) continue;
                p.setStatus(PipelineStatus.PENDING);
                p.setEnumDone(false);
                p.setStartedAt(null);
                p.setFinishedAt(null);
                p.setError(null);
                p.setSummaryCounts(null);
                p.setSummaryRowsIn(null);
                p.setSummaryRowsOut(null);
                p.setMasterEpoch(Math.max(p.getMasterEpoch(), epoch));
            }
            chunks.get(roundId).values().removeIf(c -> runId.equals(c.getRunId()));
        }
        for (String gateId : gateIds) {
            GateRecord g = gates.get(roundId).get(gateId);
            if (g != null) {
                g.setStatus(GateStatus.CLOSED);
                g.setArmedAt(null);
                g.setDeadlineAt(null);
                g.setOpenedBy(null);
                g.setOpenedAt(null);
                g.setContext(new HashMap<String, String>());
                g.setAlerted(false);
                g.setMasterEpoch(Math.max(g.getMasterEpoch(), epoch));
            }
            signals.remove(roundId + "|" + gateId);
        }
        if (rr.getStatus().revivable()) {
            rr.setStatus(RoundStatus.RUNNING);
            rr.setFinishedAt(null);
            rr.setError(null);
            rr.setCompacted(false);
        }
    }

    @Override public synchronized List<ChunkRecord> queryChunks(ChunkQuery q) {
        List<ChunkRecord> l = new ArrayList<>();
        TreeMap<String, ChunkRecord> m = chunks.get(q.getRoundId());
        if (m == null) return l;
        int skipped = 0;
        for (ChunkRecord c : m.values()) {
            if (q.getPipelineId() != null && !q.getPipelineId().equals(c.getPipelineId())) continue;
            if (q.getRunId() != null && !q.getRunId().equals(c.getRunId())) continue;
            if (q.getStatus() != null && q.getStatus() != c.getStatus()) continue;
            if (skipped++ < q.getOffset()) continue;
            if (l.size() >= q.getLimit()) break;
            l.add(copy(c));
        }
        return l;
    }

    private Collection<ChunkRecord> chunksOf(String roundId) {
        TreeMap<String, ChunkRecord> m = chunks.get(roundId);
        return m == null ? Collections.<ChunkRecord>emptyList() : m.values();
    }

    @Override public synchronized long countChunks(String roundId, String pipelineId, ChunkStatus status) {
        long n = 0;   // a null pipelineId / status means all
        for (ChunkRecord c : chunksOf(roundId)) if ((pipelineId == null || c.getPipelineId().equals(pipelineId)) && (status == null || c.getStatus() == status)) n++;
        return n;
    }

    @Override public synchronized long countSinkChunks(String roundId, String pipelineId) {
        long n = 0;
        for (ChunkRecord c : chunksOf(roundId)) {
            if (!c.getPipelineId().equals(pipelineId) || c.getStatus() != ChunkStatus.DONE) continue;
            if (StoreViews.sinkRowsOf(c.getRowsOut()) > 0) n++;
        }
        return n;
    }

    @Override public synchronized Map<String, Long> sumRowsOutByEdge(String roundId, String runId) {
        Map<String, Long> m = new HashMap<>();
        for (ChunkRecord c : chunksOf(roundId)) {
            if (!runId.equals(c.getRunId()) || c.getStatus() != ChunkStatus.DONE || c.getRowsOut() == null) continue;
            for (Map.Entry<String, Long> e : c.getRowsOut().entrySet()) if (e.getKey().startsWith("e")) m.merge(e.getKey(), e.getValue(), Long::sum);
        }
        return m;
    }

    @Override public synchronized Map<String, Long> sumRowsInByEdge(String roundId, String runId) {
        Map<String, Long> m = new HashMap<>();
        for (ChunkRecord c : chunksOf(roundId)) {
            if (!runId.equals(c.getRunId()) || c.getStatus() != ChunkStatus.DONE || c.getInputEdge() == null || "split".equals(c.getInputEdge())) continue;
            m.merge(c.getInputEdge(), c.getRowsIn() == null ? 0 : c.getRowsIn(), Long::sum);
        }
        return m;
    }

    // retention (simple, synchronized: atomic like the real stores)
    /** Test knob: number of calls per method, to assert bounded work. */
    public final java.util.concurrent.atomic.AtomicInteger purgeCalls = new java.util.concurrent.atomic.AtomicInteger();
    public final java.util.concurrent.atomic.AtomicInteger compactCalls = new java.util.concurrent.atomic.AtomicInteger();
    public final java.util.concurrent.atomic.AtomicInteger findCalls = new java.util.concurrent.atomic.AtomicInteger();

    private boolean eligible(RoundRecord r, long cutoff) { return r != null && r.getStatus().terminal() && r.getFinishedAt() != null && r.getFinishedAt() < cutoff; }

    @Override public synchronized int compactRound(String roundId, long cutoff) {
        compactCalls.incrementAndGet();
        RoundRecord r = rounds.get(roundId);
        if (!eligible(r, cutoff) || r.isCompacted()) return 0;
        RoundView v = loadRound(roundId);
        for (PipelineRecord p : pipes.get(roundId).values()) {
            Map<ChunkStatus, Long> m = v.getChunkCounts().get(p.getPipelineId());
            if (m == null) continue;
            Map<String, Long> sc = new LinkedHashMap<>();
            for (Map.Entry<ChunkStatus, Long> e : m.entrySet()) sc.put(e.getKey().name(), e.getValue());
            p.setSummaryCounts(sc);
            p.setSummaryRowsIn(v.getRowsIn().get(p.getPipelineId()));
            p.setSummaryRowsOut(v.getRowsOut().get(p.getPipelineId()));
        }
        int n = chunks.get(roundId).size();
        chunks.get(roundId).clear();
        r.setCompacted(true);
        return n;
    }

    @Override public synchronized List<String> purgeRounds(long cutoff, int max) {
        purgeCalls.incrementAndGet();
        List<String> out = new ArrayList<>();
        for (RoundRecord r : new ArrayList<>(rounds.values())) {
            if (out.size() >= max) break;
            if (!eligible(r, cutoff)) continue;
            String id = r.getRoundId();
            rounds.remove(id); runs.remove(id); gates.remove(id); pipes.remove(id); chunks.remove(id);
            signals.keySet().removeIf(sk -> sk.startsWith(id + "|"));   // keyed roundId|gateId
            out.add(id);
        }
        return out;
    }

    @Override public synchronized List<String> findTerminalRounds(long cutoff, int max) {
        findCalls.incrementAndGet();
        List<String> out = new ArrayList<>();
        for (RoundRecord r : rounds.values()) if (out.size() < max && eligible(r, cutoff) && !r.isCompacted()) out.add(r.getRoundId());
        return out;
    }

    /** Test helper: number of rounds held. */
    public synchronized int roundCount() { return rounds.size(); }
}
