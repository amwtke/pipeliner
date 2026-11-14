package io.github.amwtke.pipeliner.runtime.master;

import io.github.amwtke.pipeliner.runtime.Interrupts;
import io.github.amwtke.pipeliner.api.PipelinerException;
import io.github.amwtke.pipeliner.core.codec.ChunkCodec;
import io.github.amwtke.pipeliner.core.store.StaleEpochException;
import io.github.amwtke.pipeliner.runtime.AdminOps;
import io.github.amwtke.pipeliner.runtime.Services;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import io.github.amwtke.pipeliner.core.engine.MasterTickMode;
import io.github.amwtke.pipeliner.core.engine.TickResult;
import lombok.extern.slf4j.Slf4j;

/**
 * Leader election plus the per-tick pass over all active rounds. One pass is {@link #tick()}; in LOCAL mode the private "pg-master" thread
 * calls it every tickMillis, in EXTERNAL mode no thread exists and the caller (engine.tick(), e.g. an xxl-job handler) drives it.
 * The worker agent is NOT affected by the mode: pod liveness/heartbeats must not depend on an external scheduler.
 */
@Slf4j
public final class MasterLoop implements Runnable {
    private final Services s;
    private final MasterContext ctx;
    private final RoundMaster master;
    private final RetentionJanitor janitor;
    private volatile boolean stopped;
    private volatile boolean leader;
    private volatile long lastRenew;
    private long lastActiveSync;
    private volatile long lastExternalTick;
    private volatile long lastGapWarn;
    private static final long GAP_WARN_EVERY_MILLIS = 60_000;
    private Thread thread;
    private Thread leaseKeeper;
    private int resumeIndex;   // fair rotation: where the next tick starts after a slow-tick abort
    private final ReentrantLock tickLock = new ReentrantLock();
    private int roundsTicked;
    private int commandsExecuted;
    private static final long ACTIVE_SYNC_MILLIS = 30000;
    private static final int COMMANDS_PER_TICK = 20;

    public MasterLoop(Services s) {
        this.s = s;
        this.ctx = new MasterContext(s);
        this.master = new RoundMaster(ctx);
        this.janitor = new RetentionJanitor(s, () -> leader);
    }

    public synchronized void start() {
        if (s.cfg.getMasterTickMode() == MasterTickMode.EXTERNAL) {
            long ttl = s.cfg.getLeaderTtlMillis(), iv = s.cfg.getExternalTickIntervalMillis();
            if (ttl < 3 * iv)
                log.warn("master-tick-mode=external but leaderTtlMillis ({}) < 3 x externalTickIntervalMillis ({}): the lease may expire between "
                        + "triggers and leadership will flap; use leaderTtlMillis >= {}", ttl, iv, 3 * iv);
            log.info("{} master is driven externally (no pg-master thread); call engine.tick() / POST /pg/master/tick every ~{} ms; "
                    + "a local lease keeper renews the lease (and the throttle flag) of a leading pod every ~{} ms", s.podId(), iv, leaseKeeperPeriod());
            leaseKeeper = new Thread(this::leaseKeeperLoop, "pg-lease-keeper");
            leaseKeeper.setDaemon(true);
            leaseKeeper.start();
            return;
        }
        thread = new Thread(this, "pg-master");
        thread.setDaemon(true);
        thread.start();
    }

    public boolean isLeader() { return leader; }

    /** Stops the loop; releases leadership only when {@code release} (false simulates a crash). */
    public void stop(boolean release) {
        stopped = true;
        Thread t, k;
        synchronized (this) { t = thread; k = leaseKeeper; }
        for (Thread x : new Thread[] {t, k}) {
            if (x == null) continue;
            x.interrupt();
            try { x.join(5000); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
        }
        boolean locked = false;
        try { locked = tickLock.tryLock(5, TimeUnit.SECONDS); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }   // wait out an external tick
        try {
            if (release) {   // not only when leader: a pod that took the lease but failed before becoming leader holds it too (release deletes only our own)
                try { s.plane.releaseLeadership(s.podId()); } catch (RuntimeException e) { log.warn("release leadership failed: {}", e.toString()); }
            }
            if (leader) s.metrics.leaderLost(s.podId());
            leader = false;
        } finally {
            if (locked) tickLock.unlock();
        }
    }

    /**
     * One master pass; idempotent and safe to call from any thread. Overlapping calls on this pod return skipped "tick in progress". A pod that
     * does not hold the lease after the acquire attempt returns skipped "not leader". Runtime failures propagate (the LOCAL loop logs them).
     */
    public TickResult tick() {
        long t0 = System.currentTimeMillis();
        if (stopped) return TickResult.skipped("master stopped", false, ctx.epoch(), 0);
        if (s.cfg.getMasterTickMode() == MasterTickMode.EXTERNAL) checkExternalGap(t0);
        if (!tickLock.tryLock()) return TickResult.skipped(TickResult.IN_PROGRESS, leader, ctx.epoch(), 0);
        try {
            roundsTicked = 0;
            commandsExecuted = 0;
            try {
                if (!renew()) return TickResult.skipped(TickResult.NOT_LEADER, false, ctx.epoch(), System.currentTimeMillis() - t0);
                tickAll();
            } catch (StaleEpochException e) {
                stepDown("fenced: " + e.getMessage());
                return TickResult.skipped("deposed", false, ctx.epoch(), System.currentTimeMillis() - t0);
            } catch (RuntimeException e) {
                throw e;
            } catch (Error e) {
                // never keep claiming leadership while the pass is broken: give the lease away so a healthy pod can lead
                log.error("{}: master tick died with {}; stepping down and releasing the lease", s.podId(), e.toString(), e);
                if (leader) stepDown("tick failed with " + e);
                releaseLease();
                throw e;
            }
            long took = System.currentTimeMillis() - t0;
            if (leader) s.metrics.masterTick(took);
            return new TickResult(leader, ctx.epoch(), roundsTicked, commandsExecuted, took, false, null);
        } finally {
            tickLock.unlock();
        }
    }

    /**
     * EXTERNAL mode: the lease is only renewed by ticks, so a gap above leaderTtl/2 between two ticks is one bad delay away from an expired lease
     * (and a leadership flap). Warns at most once a minute. Rule of thumb: trigger interval &lt; leaderTtl / 3.
     */
    private void checkExternalGap(long now) {
        long prev = lastExternalTick;
        lastExternalTick = now;
        long limit = s.cfg.getLeaderTtlMillis() / 2;
        if (prev == 0 || now - prev <= limit || now - lastGapWarn < GAP_WARN_EVERY_MILLIS) return;
        lastGapWarn = now;
        log.warn("{}: {} ms passed between two master ticks, more than half of leader-ttl-millis ({}). The lease may expire between triggers and "
                + "leadership will flap: raise pipeliner.runtime.leader-ttl-millis or shorten the trigger interval (keep it below leader-ttl-millis / 3)",
                s.podId(), now - prev, s.cfg.getLeaderTtlMillis());
    }

    @Override public void run() {
        while (!stopped) {
            try {
                tick();
                Thread.sleep(s.cfg.getTickMillis());
            } catch (InterruptedException e) {
                return;
            } catch (Throwable e) {   // incl. Errors (e.g. AbstractMethodError from an old custom store): the master thread must never die
                if (Interrupts.ownInterrupt(stopped, e)) { log.debug("master loop stopping ({})", e.toString()); return; }
                log.error("master tick failed: {}", e.toString(), e);
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
            long epoch;
            boolean reserved;
            try {
                // floor: epochs already reserved / stored survive a wiped Redis epoch counter; the reservation makes the epoch durable BEFORE
                // it is used, so a master that stalls before its first write cannot have its epoch handed out again
                epoch = s.plane.nextEpoch(s.store.maxMasterEpoch());
                reserved = s.store.reserveMasterEpoch(epoch);
            } catch (RuntimeException | Error e) {
                if (!leader) releaseLease();   // do not sit on the lease (renewed every tick) while unable to become leader
                throw e;
            }
            if (!reserved) {
                log.warn("{} could not reserve master epoch {} (another master holds it or a higher one); retrying next tick", s.podId(), epoch);
                if (leader) stepDown("epoch " + epoch + " already reserved by another master");
                return false;
            }
            ctx.epoch(epoch);
            leader = true;
            lastActiveSync = 0;   // re-register every active round (the Redis active set may have been lost)
            s.metrics.leaderChanged(s.podId(), epoch);
            log.info("{} became leader (epoch {})", s.podId(), epoch);
        }
        return true;
    }

    private void releaseLease() {
        try { s.plane.releaseLeadership(s.podId()); } catch (RuntimeException e) { log.warn("release leadership failed: {}", e.toString()); }
    }

    private void stepDown(String why) {
        if (leader) log.warn("{} steps down: {}", s.podId(), why);
        leader = false;
        s.metrics.leaderLost(s.podId());
    }

    /** Makes sure the tick keeps the lease: renews, and aborts (false) if deposed. */
    private boolean keepLease() {
        if (stopped) return false;
        // time-gated: every master write is epoch-fenced, so a renewal only has to happen well inside the TTL
        if (leader && System.currentTimeMillis() - lastRenew <= s.cfg.getLeaderTtlMillis() / 4) return true;
        return renew();
    }

    private long leaseKeeperPeriod() { return Math.max(50, s.cfg.getLeaderTtlMillis() / 3); }

    /**
     * EXTERNAL mode only: while this pod leads, renews just the leader lease and the global throttle flag every ~ttl/3, so neither depends on
     * how often (or on which pod) the scheduler fires. It never acquires leadership (external ticks do) and never ticks rounds or runs commands.
     */
    private void leaseKeeperLoop() {
        long period = leaseKeeperPeriod();
        while (!stopped) {
            try {
                Thread.sleep(period);
                keepAlive();
            } catch (InterruptedException e) {
                return;
            } catch (Throwable e) {
                if (Interrupts.ownInterrupt(stopped, e)) { log.debug("lease keeper stopping ({})", e.toString()); return; }
                log.warn("lease keeper failed: {}", e.toString());
            }
        }
    }

    private void keepAlive() {
        if (!leader || stopped) return;
        if (!s.podId().equals(s.plane.currentLeader())) return;   // lost it (the next tick steps down); never re-acquire from here
        if (stopped) return;
        if (s.plane.tryAcquireOrRenewLeadership(s.podId(), s.cfg.getLeaderTtlMillis())) lastRenew = System.currentTimeMillis();
        master.refreshThrottleFlag();
    }

    private void tickAll() {
        syncActiveRounds();
        executeCommands();
        java.util.Set<String> activeSet = s.plane.activeRounds();
        java.util.List<String> active = new java.util.ArrayList<>(activeSet);
        java.util.Collections.sort(active);
        s.metrics.activeRounds(active.size());
        master.retain(activeSet);
        master.memoryThrottle();   // every tick, before the rounds: a slow round must not let the throttle flag lapse
        int n = active.size(), start = n == 0 ? 0 : resumeIndex % n;
        boolean aborted = false;
        for (int k = 0; k < n; k++) {
            String roundId = active.get((start + k) % n);
            if (stopped || !leader || !keepLease()) return;
            try {
                master.tick(roundId);
                roundsTicked++;
            } catch (StaleEpochException e) {
                throw e;
            } catch (OutOfMemoryError e) {
                throw e;
            } catch (RuntimeException | Error e) {   // an Error from user code (gate condition, enumerate) must not take the master down or starve the other rounds
                log.error("master tick for round {} failed", roundId, e);
            }
            if (System.currentTimeMillis() - lastRenew > s.cfg.getLeaderTtlMillis() / 2) {
                log.warn("master tick is slow (lease renewed {} ms ago); aborting it, the next tick resumes after round {}",
                        System.currentTimeMillis() - lastRenew, roundId);
                resumeIndex = (start + k + 1) % n;   // fairness: the rounds behind a slow one get their turn next tick
                aborted = true;
                break;
            }
        }
        if (!aborted) resumeIndex = 0;
        if (leader && keepLease()) janitor.runIfDue(System.currentTimeMillis());   // within its own small budget, also after a slow-tick abort
    }

    /** The active set lives in Redis only: re-add every non-terminal round of the store on gaining leadership and every 30 s. */
    private void syncActiveRounds() {
        long now = System.currentTimeMillis();
        if (now - lastActiveSync < ACTIVE_SYNC_MILLIS) return;
        for (String id : s.store.findActiveRounds()) s.plane.addActiveRound(id);
        lastActiveSync = now;
    }

    /**
     * Pops ONE command at a time, so a deposition (or a lost lease) in the middle never drops commands that were popped but not run: the rest
     * simply stays queued for the next leader. A command past its deadline (client timeout) is answered "expired" and not executed.
     */
    private void executeCommands() {
        for (int n = 0; n < COMMANDS_PER_TICK; n++) {
            if (stopped || !leader || !keepLease()) return;   // checked BEFORE popping: a popped command is always answered
            java.util.List<String> popped = s.plane.popCommands(1);
            if (popped.isEmpty()) return;
            String json = popped.get(0);
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
            long deadline = parseLong(cmd.get("deadline"));
            if (deadline > 0 && System.currentTimeMillis() > deadline) {
                log.warn("admin command {} ({}) expired before execution; skipped", id, cmd.get("op"));
                result.put("ok", "false");
                result.put("expired", "true");
                result.put("error", "command expired before the leader executed it (not executed)");
                if (id != null) s.plane.putCommandResult(id, ChunkCodec.toJson(result));
                continue;
            }
            try {
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
            commandsExecuted++;
            if (id != null) s.plane.putCommandResult(id, ChunkCodec.toJson(result));
            if (deposed) throw new StaleEpochException("deposed by admin command");
        }
    }

    private static long parseLong(String v) {
        try { return v == null ? 0 : Long.parseLong(v); } catch (NumberFormatException e) { return 0; }
    }
}
