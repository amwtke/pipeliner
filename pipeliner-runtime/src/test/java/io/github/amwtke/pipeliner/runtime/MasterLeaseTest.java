package io.github.amwtke.pipeliner.runtime;

import static io.github.amwtke.pipeliner.runtime.fake.TestKit.*;
import static org.junit.jupiter.api.Assertions.*;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import io.github.amwtke.pipeliner.core.engine.MasterTickMode;
import io.github.amwtke.pipeliner.core.engine.RuntimeConfig;
import io.github.amwtke.pipeliner.core.engine.TickResult;
import io.github.amwtke.pipeliner.core.graph.ExecMode;
import io.github.amwtke.pipeliner.core.store.ControlPlaneStore;
import io.github.amwtke.pipeliner.runtime.master.MasterLoop;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

/** Leader election edge cases: durable epoch reservation, lease release on failure and shutdown, a master thread that never dies, tick gap warning, seq floor. */
class MasterLeaseTest {
    private static RuntimeConfig ext(String pod) { return cfg(pod).masterTickMode(MasterTickMode.EXTERNAL).externalTickIntervalMillis(100).build(); }

    private static Harness harness() { return new Harness(MasterTickTest.graph(uniqueName("s")), Collections.<String, ExecMode>emptyMap()); }

    @Test void epochReservedByAStalledMasterIsNotHandedOutAgain() throws Exception {
        try (Harness h = harness()) {
            DefaultPipelinerEngine a = h.pod(ext("a"), true), b = h.pod(ext("b"), true);
            TickResult ta = a.tick();
            assertTrue(ta.isLeader());
            long ea = ta.getEpoch();
            assertEquals(ea, h.store.maxMasterEpoch(), "the epoch is durable in the store before the master wrote anything to a round");
            // A stalls (GC pause) before its first round write; Redis loses pg:epoch and the lease. The store holds no round epoch at all.
            h.plane.resetEpoch();
            h.plane.releaseLeadership("a");
            TickResult tb = b.tick();
            assertTrue(tb.isLeader());
            assertTrue(tb.getEpoch() > ea, "B must not get A's epoch " + ea + " again, got " + tb.getEpoch());
        }
    }

    @Test void masterThatWasHandedAnAlreadyReservedEpochDoesNotBecomeLeader() throws Exception {
        try (Harness h = harness()) {
            DefaultPipelinerEngine b = h.pod(ext("b"), true);
            assertTrue(h.store.reserveMasterEpoch(1));   // another master owns epoch 1 ...
            h.plane.forcedEpoch = 1L;                     // ... and the plane hands out 1 again (both masters read the floor before either reserved)
            TickResult first = b.tick();
            assertFalse(first.isLeader(), "reservation refused: not leader this tick");
            assertTrue(first.isSkipped());
            assertFalse(b.isLeader());
            h.plane.forcedEpoch = null;
            TickResult second = b.tick();   // the next tick re-checks the lease and draws a fresh epoch
            assertTrue(second.isLeader());
            assertTrue(second.getEpoch() > 1);
        }
    }

    @Test void storeFailureAfterTakingTheLeaseReleasesIt() throws Exception {
        try (Harness h = harness()) {
            DefaultPipelinerEngine e = h.pod(ext("x1"), true);
            h.store.failMaxMasterEpoch = new IllegalStateException("db down");
            assertThrows(IllegalStateException.class, e::tick);
            assertNull(h.plane.currentLeader(), "the lease must not stay with a pod that cannot become leader");
            assertFalse(e.isLeader());
            h.store.failMaxMasterEpoch = new AbstractMethodError("old custom store");
            assertThrows(AbstractMethodError.class, e::tick);
            assertNull(h.plane.currentLeader());
            h.store.failMaxMasterEpoch = null;
            assertTrue(e.tick().isLeader());
            assertEquals("x1", h.plane.currentLeader());
        }
    }

    @Test void gracefulStopReleasesALeaseHeldWithoutLeadership() throws Exception {
        try (Harness h = harness()) {
            DefaultPipelinerEngine e = h.pod(ext("x1"), true);
            h.store.refuseReservations = true;
            assertFalse(e.tick().isLeader());
            assertEquals("x1", h.plane.currentLeader(), "lease taken, leadership refused");
            assertFalse(e.isLeader());
            e.stop();
            assertNull(h.plane.currentLeader(), "stop() frees the lease although leader == false");
        }
    }

    @Test void crashStopKeepsTheLease() throws Exception {
        try (Harness h = harness()) {
            DefaultPipelinerEngine e = h.pod(ext("x1"), true);
            assertTrue(e.tick().isLeader());
            e.halt();
            assertEquals("x1", h.plane.currentLeader(), "a crash leaves the lease to expire by TTL");
        }
    }

    @Test void masterThreadSurvivesErrorsFromTheStore() throws Exception {
        try (Harness h = harness()) {
            h.store.failMaxMasterEpoch = new AbstractMethodError("old custom store");
            DefaultPipelinerEngine e = h.pod("pod1");   // LOCAL master thread
            Thread.sleep(400);
            assertTrue(MasterTickTest.masterThreadAlive(), "an Error in a tick must not kill pg-master");
            assertFalse(e.isLeader());
            h.store.failMaxMasterEpoch = null;
            assertTrue(waitFor(e::isLeader, 5000), "the master recovers once the store works");
        }
    }

    @Test void warnsWhenExternalTicksAreFurtherApartThanHalfTheLeaseTtl() throws Exception {
        Logger log = (Logger) LoggerFactory.getLogger(MasterLoop.class);
        ListAppender<ILoggingEvent> app = new ListAppender<>();
        app.start();
        log.addAppender(app);
        Level old = log.getLevel();
        log.setLevel(Level.WARN);
        try (Harness h = harness()) {
            DefaultPipelinerEngine e = h.pod(ext("x1"), true);   // leaderTtl 600 ms: limit 300 ms
            e.tick();
            Thread.sleep(100);
            e.tick();
            assertTrue(gapWarnings(app).isEmpty(), "ticks well inside TTL/2 do not warn");
            Thread.sleep(450);
            e.tick();
            List<String> w = gapWarnings(app);
            assertEquals(1, w.size(), w.toString());
            assertTrue(w.get(0).contains("leader-ttl-millis"), w.get(0));
            Thread.sleep(450);
            e.tick();
            assertEquals(1, gapWarnings(app).size(), "rate limited: one warning per minute");
        } finally {
            log.detachAppender(app);
            log.setLevel(old);
        }
    }

    private static List<String> gapWarnings(ListAppender<ILoggingEvent> app) {
        List<String> out = new ArrayList<>();
        for (ILoggingEvent ev : new ArrayList<>(app.list))
            if (ev.getLevel() == Level.WARN && ev.getFormattedMessage().contains("between two master ticks")) out.add(ev.getFormattedMessage());
        return out;
    }

    @Test void roundSeqStaysAboveStoredRoundsAfterTheCounterWasLost() throws Exception {
        try (Harness h = harness()) {
            DefaultPipelinerEngine e = h.pod(ext("x1"), true);
            String r1 = e.submit("pipe", "k1", null);
            String r2 = e.submit("pipe", "k2", null);
            long old = h.store.loadRoundRecord(r2).getSeq();
            assertEquals(old, h.store.maxRoundSeq());
            h.plane.resetRoundSeq();   // Redis wiped: INCR would restart at 1, below the versions already written to the sink tables
            String r3 = e.submit("pipe", "k3", null);
            assertTrue(h.store.loadRoundRecord(r3).getSeq() > old, "seq " + h.store.loadRoundRecord(r3).getSeq() + " must exceed " + old);
            assertNotEquals(r1, r3);
        }
    }

    @Test void signalKeysAboveTheStoreLimitAreRejected() throws Exception {
        try (Harness h = harness()) {
            DefaultPipelinerEngine e = h.pod(ext("x1"), true);
            String round = e.submit("pipe", "k", null);
            StringBuilder ok = new StringBuilder();
            while (ok.length() < ControlPlaneStore.MAX_SIGNAL_KEY_LENGTH) ok.append('k');
            e.signalGate(round, "any", ok.toString(), "x");
            assertThrows(IllegalArgumentException.class, () -> e.signalGate(round, "any", ok + "k", "x"));
            assertThrows(IllegalArgumentException.class, () -> e.signalGate(round, "any", null, "x"));
        }
    }
}
