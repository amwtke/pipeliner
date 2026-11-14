package io.github.amwtke.pipeliner.runtime.worker;

import io.github.amwtke.pipeliner.runtime.Interrupts;
import io.github.amwtke.pipeliner.core.codec.ChunkCodec;
import io.github.amwtke.pipeliner.core.dataplane.DesiredState;
import io.github.amwtke.pipeliner.core.plan.ExecutionPlan;
import io.github.amwtke.pipeliner.core.store.RoundRecord;
import io.github.amwtke.pipeliner.runtime.Services;
import io.github.amwtke.pipeliner.runtime.local.LocalRunExecutor;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;

/** Per-pod daemon: heartbeats, and reconciles cluster worker threads and assigned local runs with the desired state. */
@Slf4j
public final class WorkerAgent implements Runnable {
    private static final class LocalHandle {
        final int attempt;
        final LocalRunExecutor executor;
        LocalHandle(int attempt, LocalRunExecutor executor) { this.attempt = attempt; this.executor = executor; }
    }

    private final Services s;
    private final ClaimRenewer renewer;
    private final Map<String, Group> workers = new HashMap<>();
    /** Threads of superseded attempts that were asked to stop (killed with the agent). */
    private final List<ClusterPipelineWorker> retiring = new ArrayList<>();
    private final Map<String, LocalHandle> locals = new HashMap<>();
    private final Map<String, RoundRecord> roundCache = new HashMap<>();
    private final Set<String> warned = new HashSet<>();
    private volatile boolean stopped;
    private long lastHeartbeat;
    private Thread thread;

    public WorkerAgent(Services s) {
        this.s = s;
        this.renewer = new ClaimRenewer(s.plane, s.cfg.getRenewIntervalMillis());
    }

    public synchronized void start() {
        renewer.start();
        thread = new Thread(this, "pg-worker-agent");
        thread.setDaemon(true);
        thread.start();
    }

    /** Stops everything abruptly (in-flight chunks stay pending in the streams). */
    public void stop() {
        stopped = true;
        Thread t;
        synchronized (this) { t = thread; }
        if (t != null) {
            t.interrupt();
            try { t.join(2000); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
        }
        List<ClusterPipelineWorker> ws = new ArrayList<>();
        List<LocalRunExecutor> exs = new ArrayList<>();
        synchronized (this) {
            for (Group g : workers.values()) ws.addAll(g.list);
            ws.addAll(retiring);
            for (LocalHandle h : locals.values()) exs.add(h.executor);
            retiring.clear();
            workers.clear();
            locals.clear();
        }
        for (ClusterPipelineWorker w : ws) w.kill();
        for (LocalRunExecutor e : exs) e.cancel();
        // give the threads a bounded time to leave user code and release their instances, so the caller does not close pools under them
        long deadline = System.currentTimeMillis() + s.cfg.getStopJoinMillis();
        int left = 0;
        for (ClusterPipelineWorker w : ws) if (!w.join(deadline - System.currentTimeMillis())) left++;
        for (LocalRunExecutor e : exs) if (!e.join(deadline - System.currentTimeMillis())) left++;
        if (left > 0) log.warn("worker agent stop: {} worker/local-run thread(s) still running after {} ms", left, s.cfg.getStopJoinMillis());
        renewer.stop();
    }

    @Override public void run() {
        while (!stopped) {
            try {
                tick();
                Thread.sleep(s.cfg.getTickMillis());
            } catch (InterruptedException e) {
                return;
            } catch (Throwable e) {   // incl. Errors (e.g. AbstractMethodError from an old custom plane): the worker agent thread must never die
                if (Interrupts.ownInterrupt(stopped, e)) { log.debug("worker agent stopping ({})", e.toString()); return; }
                log.warn("worker agent tick failed: {}", e.toString());
                try { Thread.sleep(s.cfg.getTickMillis()); } catch (InterruptedException ie) { return; }
            }
        }
    }

    /** Heartbeat field holding this pod's live pipeline instances: {@code [[round,pipeline,attempt,threads],...]} (capped). */
    public static final String LIVE_FIELD = "live";
    private static final int MAX_LIVE = 200;

    /** Heartbeat field prefix: {@code graph.<name>} holds the plan hash this pod can execute. */
    public static final String GRAPH_PREFIX = "graph.";

    static String liveJson(List<io.github.amwtke.pipeliner.core.engine.LiveInstance> live) {
        List<List<Object>> rows = new ArrayList<>();
        for (io.github.amwtke.pipeliner.core.engine.LiveInstance li : live) {
            if (rows.size() >= MAX_LIVE) break;
            List<Object> row = new ArrayList<>(4);
            row.add(li.getRoundId() == null ? "" : li.getRoundId());
            row.add(li.getPipelineId() == null ? "" : li.getPipelineId());
            row.add(li.getAttempt());
            row.add(li.getThreads());
            rows.add(row);
        }
        return ChunkCodec.toJson(rows);
    }

    private synchronized void tick() {
        long now = System.currentTimeMillis();
        if (now - lastHeartbeat >= s.cfg.getHeartbeatMillis()) {
            Map<String, String> info = new HashMap<>();
            info.put("startedAt", String.valueOf(now));
            info.put(LIVE_FIELD, liveJson(s.instances.snapshot()));
            for (String g : s.registry.names()) info.put(GRAPH_PREFIX + g, s.registry.require(g).getHash());   // plans this pod can execute
            s.plane.heartbeat(s.podId(), info);
            lastHeartbeat = now;
        }
        Set<String> active = s.plane.activeRounds();
        s.metrics.liveInstances(s.podId(), s.instances.snapshot().size());
        for (String roundId : active) {
            try { reconcileRound(roundId); }
            catch (RuntimeException e) { log.warn("reconcile of round {} failed: {}", roundId, e.toString()); }
        }
        // stop everything belonging to rounds that are no longer active
        for (Iterator<Map.Entry<String, Group>> it = workers.entrySet().iterator(); it.hasNext(); ) {
            Map.Entry<String, Group> e = it.next();
            if (!active.contains(roundOf(e.getKey()))) { for (ClusterPipelineWorker w : e.getValue().list) w.stop(); it.remove(); }
        }
        for (Iterator<Map.Entry<String, LocalHandle>> it = locals.entrySet().iterator(); it.hasNext(); ) {
            Map.Entry<String, LocalHandle> e = it.next();
            if (!active.contains(roundOf(e.getKey()))) { e.getValue().executor.cancel(); it.remove(); }
        }
        roundCache.keySet().retainAll(active);
        warned.retainAll(active);
    }

    private void reconcileRound(String roundId) {
        RoundRecord rr = roundCache.get(roundId);
        if (rr == null) {
            rr = s.store.loadRoundRecord(roundId);
            if (rr == null) return;
            roundCache.put(roundId, rr);
        }
        ExecutionPlan plan = s.registry.find(rr.getGraphName());
        if (plan == null || !plan.getHash().equals(rr.getPlanHash())) {
            if (warned.add(roundId))
                log.warn("round {}: graph '{}' not registered here or plan hash differs; this pod will not work on it", roundId, rr.getGraphName());
            return;
        }
        reconcileWorkers(roundId, rr, plan);
        reconcileLocalRuns(roundId, rr, plan);
    }

    /** Worker threads of one pipeline, all working on the same run attempt. */
    private static final class Group {
        final int attempt;
        final List<ClusterPipelineWorker> list = new ArrayList<>();
        Group(int attempt) { this.attempt = attempt; }
    }

    private void reconcileWorkers(String roundId, RoundRecord rr, ExecutionPlan plan) {
        Map<String, DesiredState> desired = s.plane.desiredState(roundId);
        for (Map.Entry<String, DesiredState> e : desired.entrySet()) {
            String pipeline = e.getKey();
            if (plan.pipeline(pipeline) == null) continue;
            String key = roundId + "|" + pipeline;
            DesiredState ds = e.getValue();
            Group g = workers.get(key);
            if (g != null && g.attempt != ds.getAttempt()) {   // rerun: the old attempt's threads finish their chunk and exit; fresh instance for the new one
                for (ClusterPipelineWorker w : g.list) { w.stop(); retiring.add(w); }
                g = null;
            }
            if (g == null) { g = new Group(ds.getAttempt()); workers.put(key, g); }
            List<ClusterPipelineWorker> list = g.list;
            list.removeIf(w -> w.stopRequested() || !w.alive());
            int n = ds.getParallelism();
            while (list.size() > n) list.remove(list.size() - 1).stop();
            while (list.size() < n) {
                int idx = list.size();
                ClusterPipelineWorker w = new ClusterPipelineWorker(s, plan, rr, pipeline, ds.getAttempt(), s.podId() + "#" + idx, renewer);
                list.add(w);
                w.start("pg-w-" + roundId + "-" + pipeline + "-" + ds.getAttempt() + "-" + idx);
            }
        }
        String prefix = roundId + "|";
        for (Map.Entry<String, Group> e : workers.entrySet()) {
            if (!e.getKey().startsWith(prefix) || desired.containsKey(e.getKey().substring(prefix.length()))) continue;
            for (ClusterPipelineWorker w : e.getValue().list) w.stop();
            e.getValue().list.clear();
        }
        retiring.removeIf(w -> !w.alive());
    }

    private void reconcileLocalRuns(String roundId, RoundRecord rr, ExecutionPlan plan) {
        Map<String, String> assign = s.plane.localRunAssignments(roundId);
        String prefix = roundId + "|";
        for (Iterator<Map.Entry<String, LocalHandle>> it = locals.entrySet().iterator(); it.hasNext(); ) {
            Map.Entry<String, LocalHandle> e = it.next();
            if (!e.getKey().startsWith(prefix)) continue;
            String a = assign.get(e.getKey().substring(prefix.length()));
            if (a == null || !isMine(a) || attemptOf(a) != e.getValue().attempt) { e.getValue().executor.cancel(); it.remove(); }
        }
        for (Map.Entry<String, String> e : assign.entrySet()) {
            if (!isMine(e.getValue())) continue;
            String key = prefix + e.getKey();
            LocalHandle h = locals.get(key);
            if (h != null) {
                // still running, or finished and reported (the master has not drained it yet): leave it. A finished executor that never
                // reported (it died) must not block the run: drop the handle so the run is driven again.
                if (h.executor.alive() || h.executor.reported()) continue;
                log.warn("local run {} ended without reporting; driving it again", key);
                locals.remove(key);
            }
            int attempt = attemptOf(e.getValue());
            LocalRunExecutor ex = new LocalRunExecutor(s, plan, rr, e.getKey(), attempt);
            locals.put(key, new LocalHandle(attempt, ex));
            ex.start();
        }
    }

    private boolean isMine(String assignment) { return s.podId().equals(io.github.amwtke.pipeliner.core.dataplane.LocalRunAssignment.podOf(assignment)); }

    /** Only called for assignments that passed {@link #isMine} (well formed); -1 for a malformed value. */
    private static int attemptOf(String assignment) {
        io.github.amwtke.pipeliner.core.dataplane.LocalRunAssignment a = io.github.amwtke.pipeliner.core.dataplane.LocalRunAssignment.parse(assignment);
        return a == null ? -1 : a.attempt;
    }

    private static String roundOf(String key) { return key.substring(0, key.indexOf('|')); }
}
