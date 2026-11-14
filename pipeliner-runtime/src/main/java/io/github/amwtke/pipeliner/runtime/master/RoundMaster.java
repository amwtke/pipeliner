package io.github.amwtke.pipeliner.runtime.master;

import io.github.amwtke.pipeliner.core.plan.ExecutionPlan;
import io.github.amwtke.pipeliner.core.store.RoundRecord;
import io.github.amwtke.pipeliner.core.store.RoundStatus;
import io.github.amwtke.pipeliner.core.store.StaleEpochException;
import java.util.HashSet;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;

/** One master tick over one round (design 8.2). Stateless: everything is read from the store and data plane. */
@Slf4j
final class RoundMaster {
    private final MasterContext c;
    private final JournalDrainer drainer;
    private final GateEvaluator gates;
    private final RunStarter starter;
    private final CompletionChecker completion;
    private final BackpressureMonitor backpressure;
    private final StalledChunkMonitor stalled;
    private final Set<String> warned = new HashSet<>();

    RoundMaster(MasterContext c) {
        this.c = c;
        this.drainer = new JournalDrainer(c);
        this.gates = new GateEvaluator(c);
        this.starter = new RunStarter(c);
        this.completion = new CompletionChecker(c, drainer);
        this.backpressure = new BackpressureMonitor(c);
        this.stalled = new StalledChunkMonitor(c);
    }

    void memoryThrottle() { backpressure.memoryThrottle(); }

    /** Lease keeper (EXTERNAL mode): re-arms the global throttle flag's TTL. */
    void refreshThrottleFlag() { backpressure.refreshThrottleFlag(); }

    /** Prunes the per-round state of every round that left the active set (finished, admin-cancelled, purged). */
    void retain(Set<String> active) {
        warned.retainAll(active);
        backpressure.retain(active);
        stalled.retain(active);
    }

    private void forget(String roundId) {
        stalled.forget(roundId);
        warned.remove(roundId);
        c.s.fallback.evict(roundId);
    }

    void tick(String roundId) {
        RoundRecord rr = c.s.store.loadRoundRecord(roundId);
        if (rr == null || rr.getStatus().terminal()) { c.s.plane.removeActiveRound(roundId, c.epoch()); forget(roundId); return; }
        ExecutionPlan plan = c.s.registry.find(rr.getGraphName());
        if (plan == null || !plan.getHash().equals(rr.getPlanHash())) {
            if (warned.add(roundId)) log.warn("round {}: graph '{}' not registered here or plan hash differs; master skips it", roundId, rr.getGraphName());
            return;
        }
        // take ownership of the round: from now on a deposed master's writes are rejected
        if (!c.s.store.fenceRound(roundId, c.epoch())) throw new StaleEpochException("round " + roundId + " is held by a newer master epoch");
        if (!c.s.plane.fenceRound(roundId, c.epoch())) throw new StaleEpochException("round " + roundId + " data plane is held by a newer master epoch");
        // the paused flag is derived from the round status every tick: a zombie's late setPaused is corrected here
        boolean paused = rr.getStatus() == RoundStatus.PAUSED;
        if (c.s.plane.isPaused(roundId) != paused) c.s.plane.setPaused(roundId, paused, c.epoch());
        if (paused) return;
        boolean started = false;
        if (rr.getStatus() == RoundStatus.CREATED) {
            if (!c.s.store.casRoundStatus(roundId, RoundStatus.CREATED, RoundStatus.RUNNING, null, c.epoch())) return;
            started = true;
        }

        // the drain reuses the record loaded above and hands back the RoundData it loaded (unless local-run events made it stale)
        JournalDrainer.Drained dr = drainer.drain(roundId, plan, rr);
        RoundData d = dr.data != null && !started ? dr.data : RoundData.load(c, roundId, plan);
        if (d == null) return;
        gates.evaluate(d);
        d = refresh(d);
        starter.startRuns(d);
        d = refresh(d);
        starter.enumerate(d);
        d = refresh(d);
        completion.checkPipelines(d);
        d = refresh(d);
        completion.finishRuns(d);
        d = refresh(d);
        starter.watchdog(d);
        d = refresh(d);
        if (completion.finishRound(d)) forget(roundId);
        else { backpressure.monitor(d); stalled.sample(d); }
    }

    /** Reloads the state only if the previous phase wrote something. */
    private RoundData refresh(RoundData d) {
        if (!d.dirty()) return d;
        RoundData n = RoundData.load(c, d.roundId(), d.plan);
        if (n == null) throw new IllegalStateException("round vanished: " + d.roundId());
        return n;
    }
}
