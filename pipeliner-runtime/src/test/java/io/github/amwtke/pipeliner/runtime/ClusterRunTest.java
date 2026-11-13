package io.github.amwtke.pipeliner.runtime;

import static io.github.amwtke.pipeliner.runtime.fake.TestKit.*;
import static org.junit.jupiter.api.Assertions.*;

import io.github.amwtke.pipeliner.api.Row;
import io.github.amwtke.pipeliner.core.dataplane.Keys;
import io.github.amwtke.pipeliner.core.engine.RuntimeConfig;
import io.github.amwtke.pipeliner.core.graph.ExecMode;
import io.github.amwtke.pipeliner.core.graph.PipelineGraph;
import io.github.amwtke.pipeliner.core.store.*;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;

class ClusterRunTest {
    interface Hook { void before(Row row) throws Exception; }

    /** a (source, batch: tag, chunkRows 3) -> b (row: v = 2*id) -> sink. */
    static PipelineGraph graph(String sinkName, int splits, int rows, Hook hook) {
        return PipelineGraph.builder("pipe")
                .pipeline("a", p -> p.source(new RangeSource(splits, rows)).chunkRows(3)
                        .batch((rs, out, ctx) -> { for (Row r : rs) out.emit(r.copy().set("a", 1L)); }))
                .pipeline("b", p -> p.row((row, out, ctx) -> {
                    if (hook != null) hook.before(row);
                    out.emit(row.copy().set("v", row.getLong("id") * 2));
                }).sink(collect(sinkName)))
                .edge("a", "b").build();
    }

    static List<String> expected(int total) {
        List<String> l = new ArrayList<>();
        for (long id = 0; id < total; id++) l.add(Row.of("id", id, "g", id / 4, "a", 1L, "v", id * 2).toString());
        Collections.sort(l);
        return l;
    }

    @Test void singlePodRoundSucceeds() throws Exception {
        String sink = uniqueName("s");
        try (Harness h = new Harness(graph(sink, 6, 4, null), Collections.<String, ExecMode>emptyMap())) {
            io.github.amwtke.pipeliner.runtime.DefaultPipelinerEngine e = h.pod("pod1");
            String round = e.submit("pipe", "k1", new HashMap<String, String>());
            assertEquals(round, e.submit("pipe", "k1", null), "submit is idempotent per bizKey");
            assertEquals(RoundStatus.SUCCEEDED, e.awaitRound(round, 15000));
            assertEquals(expected(24), sinkRows(sink));
            RoundView v = e.view(round);
            for (RunRecord r : v.getRuns()) assertEquals(RunStatus.SUCCEEDED, r.getStatus());
            assertEquals(6, h.store.countChunks(round, "a", ChunkStatus.DONE));
            assertTrue(h.plane.keys().stream().noneMatch(k -> k.contains(round + "}:ch") || k.contains("journal") && h.plane.streamLength(k) > 0),
                    "channels cleaned up");
        }
    }

    @Test void killedWorkerChunkIsAutoclaimedByAnotherPod() throws Exception {
        String sink = uniqueName("s"), ref = uniqueName("r");
        // reference: single pod, no failures
        try (Harness h = new Harness(graph(ref, 8, 4, null), Collections.<String, ExecMode>emptyMap())) {
            io.github.amwtke.pipeliner.runtime.DefaultPipelinerEngine e = h.pod("solo");
            assertEquals(RoundStatus.SUCCEEDED, e.awaitRound(e.submit("pipe", "ref", null), 15000));
        }
        AtomicBoolean hung = new AtomicBoolean();
        CountDownLatch hungLatch = new CountDownLatch(1);
        Hook hook = row -> {
            if (row.getLong("id") == 13 && hung.compareAndSet(false, true)) { hungLatch.countDown(); Thread.sleep(Long.MAX_VALUE); }
        };
        try (Harness h = new Harness(graph(sink, 8, 4, hook), Collections.<String, ExecMode>emptyMap())) {
            h.pod(cfg("master").workerEnabled(false).build(), true);
            io.github.amwtke.pipeliner.runtime.DefaultPipelinerEngine a = h.pod(cfg("podA").masterEnabled(false).build(), true);
            String round = a.submit("pipe", "k", null);
            assertTrue(hungLatch.await(10, TimeUnit.SECONDS), "a worker got stuck mid-chunk");
            a.halt(); // kill pod A: its in-flight claim is no longer renewed
            h.pod(cfg("podB").masterEnabled(false).build(), true);
            assertEquals(RoundStatus.SUCCEEDED, a.awaitRound(round, 20000));
            assertEquals(sinkRows(ref), sinkRows(sink), "no row lost or duplicated");
            assertEquals(sinkChunks(ref).keySet(), sinkChunks(sink).keySet(), "same stable sink chunk ids");
        }
    }

    @Test void busyBackpressureBoundsDownstreamChannel() throws Exception {
        String sink = uniqueName("s");
        Hook slow = row -> Thread.sleep(5);
        RuntimeConfig cfg = cfg("pod1").highWatermark(4).lowWatermark(2).build();
        try (Harness h = new Harness(graph(sink, 30, 8, slow), Collections.<String, ExecMode>emptyMap())) {
            io.github.amwtke.pipeliner.runtime.DefaultPipelinerEngine e = h.pod(cfg, true);
            String round = e.submit("pipe", "bp", null);
            assertEquals(RoundStatus.SUCCEEDED, e.awaitRound(round, 60000));
            assertEquals(expected(240).size(), sinkRows(sink).size());
            assertTrue(h.plane.busyCount.get() > 0, "upstream must have been told BUSY");
            // a commit may add up to ceil(8 rows / 3) = 3 chunks after seeing XLEN < highWatermark
            assertTrue(h.plane.maxLen(Keys.channel(round, "e1")) <= 4 - 1 + 3, "max e1 length " + h.plane.maxLen(Keys.channel(round, "e1")));
        }
    }

    @Test void inlineLocalAndClusterProduceIdenticalSinkOutput() throws Exception {
        Map<ExecMode, Map<String, List<Row>>> chunks = new EnumMap<>(ExecMode.class);
        Map<ExecMode, List<String>> rows = new EnumMap<>(ExecMode.class);
        for (ExecMode m : ExecMode.values()) {
            String sink = uniqueName("s");
            try (Harness h = new Harness(graph(sink, 7, 4, null), mode(m, "a"))) {
                io.github.amwtke.pipeliner.runtime.DefaultPipelinerEngine e = h.pod("pod-" + m);
                String round = e.submit("pipe", "m", null);
                assertEquals(RoundStatus.SUCCEEDED, e.awaitRound(round, 20000), m.toString());
                chunks.put(m, sinkChunks(sink));
                rows.put(m, sinkRows(sink));
                assertEquals(7, h.store.countChunks(round, "a", ChunkStatus.DONE), m + " source chunks journaled");
            }
        }
        assertEquals(expected(28), rows.get(ExecMode.CLUSTER));
        assertEquals(rows.get(ExecMode.CLUSTER), rows.get(ExecMode.INLINE));
        assertEquals(rows.get(ExecMode.CLUSTER), rows.get(ExecMode.LOCAL));
        assertEquals(chunks.get(ExecMode.CLUSTER), chunks.get(ExecMode.INLINE));
        assertEquals(chunks.get(ExecMode.CLUSTER), chunks.get(ExecMode.LOCAL));
    }

    @Test void localRunIsReassignedWhenAssigneeDies() throws Exception {
        String sink = uniqueName("s");
        AtomicBoolean hung = new AtomicBoolean();
        CountDownLatch hungLatch = new CountDownLatch(1);
        Hook hook = row -> { if (hung.compareAndSet(false, true)) { hungLatch.countDown(); Thread.sleep(Long.MAX_VALUE); } };
        try (Harness h = new Harness(graph(sink, 5, 4, hook), mode(ExecMode.LOCAL, "a"))) {
            h.pod(cfg("master").workerEnabled(false).build(), true);
            io.github.amwtke.pipeliner.runtime.DefaultPipelinerEngine a = h.pod(cfg("podA").masterEnabled(false).build(), true);
            String round = a.submit("pipe", "k", null);
            assertTrue(hungLatch.await(10, TimeUnit.SECONDS));
            a.halt();
            h.pod(cfg("podB").masterEnabled(false).build(), true);
            assertEquals(RoundStatus.SUCCEEDED, a.awaitRound(round, 20000));
            assertEquals(expected(20), sinkRows(sink));
            RunRecord run = a.view(round).getRuns().get(0);
            assertTrue(run.getAttempt() >= 2, "run was reassigned, attempt=" + run.getAttempt());
        }
    }
}
