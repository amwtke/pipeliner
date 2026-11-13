package io.github.amwtke.pipeliner.runtime.master;

import io.github.amwtke.pipeliner.api.PipelinerException;
import io.github.amwtke.pipeliner.core.codec.ChunkCodec;
import io.github.amwtke.pipeliner.core.store.StaleEpochException;
import io.github.amwtke.pipeliner.runtime.AdminOps;
import io.github.amwtke.pipeliner.runtime.Services;
import java.util.LinkedHashMap;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;

/** Leader election plus the per-tick loop over all active rounds. */
@Slf4j
public final class MasterLoop implements Runnable {
    private final Services s;
    private final MasterContext ctx;
    private final RoundMaster master;
    private volatile boolean stopped;
    private volatile boolean leader;
    private volatile long lastRenew;
    private long lastActiveSync;
    private Thread thread;
    private static final long ACTIVE_SYNC_MILLIS = 30000;
    private static final int COMMANDS_PER_TICK = 20;

    public MasterLoop(Services s) {
        this.s = s;
        this.ctx = new MasterContext(s);
        this.master = new RoundMaster(ctx);
    }

    public synchronized void start() {
        thread = new Thread(this, "pg-master");
        thread.setDaemon(true);
        thread.start();
    }

    public boolean isLeader() { return leader; }

    /** Stops the loop; releases leadership only when {@code release} (false simulates a crash). */
    public void stop(boolean release) {
        stopped = true;
        Thread t;
        synchronized (this) { t = thread; }
        if (t != null) {
            t.interrupt();
            try { t.join(5000); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
        }
        if (leader && release) {
            try { s.plane.releaseLeadership(s.podId()); } catch (RuntimeException e) { log.warn("release leadership failed: {}", e.toString()); }
        }
        leader = false;
    }

    @Override public void run() {
        while (!stopped) {
            try {
                if (renew()) tickAll();
                Thread.sleep(s.cfg.getTickMillis());
            } catch (InterruptedException e) {
                return;
            } catch (StaleEpochException e) {
                stepDown("fenced: " + e.getMessage());
                try { Thread.sleep(s.cfg.getTickMillis()); } catch (InterruptedException ie) { return; }
            } catch (RuntimeException e) {
                log.warn("master tick failed: {}", e.toString());
                try { Thread.sleep(s.cfg.getTickMillis()); } catch (InterruptedException ie) { return; }
            }
        }
    }

    /**
     * Acquires or renews the lease. A new epoch is taken on every (re)gain of leadership, or when the global epoch moved on
     * (another master was elected in between). Returns whether this pod is the leader now.
     */
    private boolean renew() {
        if (!s.plane.tryAcquireOrRenewLeadership(s.podId(), s.cfg.getLeaderTtlMillis())) {
            if (leader) stepDown("lost leadership");
            return false;
        }
        lastRenew = System.currentTimeMillis();
        if (!leader || s.plane.currentEpoch() != ctx.epoch()) {
            long epoch = s.plane.nextEpoch();
            ctx.epoch(epoch);
            leader = true;
            lastActiveSync = 0;   // re-register every active round (the Redis active set may have been lost)
            s.metrics.leaderChanged(s.podId(), epoch);
            log.info("{} became leader (epoch {})", s.podId(), epoch);
        }
        return true;
    }

    private void stepDown(String why) {
        if (leader) log.warn("{} steps down: {}", s.podId(), why);
        leader = false;
    }

    /** Makes sure the tick keeps the lease: renews, and aborts (false) if deposed. */
    private boolean keepLease() {
        return !stopped && renew();
    }

    private void tickAll() {
        syncActiveRounds();
        executeCommands();
        for (String roundId : s.plane.activeRounds()) {
            if (stopped || !leader || !keepLease()) return;
            try {
                master.tick(roundId);
            } catch (StaleEpochException e) {
                throw e;
            } catch (RuntimeException e) {
                log.error("master tick for round {} failed", roundId, e);
            }
            if (System.currentTimeMillis() - lastRenew > s.cfg.getLeaderTtlMillis() / 2) {
                log.warn("master tick is slow (lease renewed {} ms ago); aborting it", System.currentTimeMillis() - lastRenew);
                return;
            }
        }
        master.memoryThrottle();
    }

    /** The active set lives in Redis only: re-add every non-terminal round of the store on gaining leadership and every 30 s. */
    private void syncActiveRounds() {
        long now = System.currentTimeMillis();
        if (now - lastActiveSync < ACTIVE_SYNC_MILLIS) return;
        for (String id : s.store.findActiveRounds()) s.plane.addActiveRound(id);
        lastActiveSync = now;
    }

    private void executeCommands() {
        for (String json : s.plane.popCommands(COMMANDS_PER_TICK)) {
            Map<String, String> cmd;
            String id = null;
            try {
                cmd = ChunkCodec.toStringMap(json);
                id = cmd.get("id");
            } catch (RuntimeException e) {
                log.warn("dropping malformed admin command: {}", json);
                continue;
            }
            Map<String, String> result = new LinkedHashMap<>();
            boolean deposed = false;
            try {
                if (!keepLease()) throw new PipelinerException("this pod is no longer the leader; retry");
                new AdminOps(s, ctx.epoch()).execute(cmd);
                result.put("ok", "true");
            } catch (StaleEpochException e) {
                deposed = true;
                result.put("ok", "false");
                result.put("error", "leader deposed while executing " + cmd.get("op") + "; retry");
            } catch (RuntimeException e) {
                log.warn("admin command {} failed: {}", cmd.get("op"), e.toString());
                result.put("ok", "false");
                result.put("error", e.getMessage() == null ? e.toString() : e.getMessage());
            }
            if (id != null) s.plane.putCommandResult(id, ChunkCodec.toJson(result));
            if (deposed) throw new StaleEpochException("deposed by admin command");
        }
    }
}
