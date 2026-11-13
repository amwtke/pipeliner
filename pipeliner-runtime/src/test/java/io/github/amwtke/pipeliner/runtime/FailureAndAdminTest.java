package io.github.amwtke.pipeliner.runtime;

import static io.github.amwtke.pipeliner.runtime.fake.TestKit.*;
import static org.junit.jupiter.api.Assertions.*;

import io.github.amwtke.pipeliner.api.Row;
import io.github.amwtke.pipeliner.core.graph.ExecMode;
import io.github.amwtke.pipeliner.core.graph.Gates;
import io.github.amwtke.pipeliner.core.graph.PipelineGraph;
import io.github.amwtke.pipeliner.core.store.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class FailureAndAdminTest {
    private static List<String> rowsOf(int total, long perSplit) {
        List<String> l = new ArrayList<>();
        for (long id = 0; id < total; id++) l.add(Row.of("id", id, "g", id / perSplit).toString());
        Collections.sort(l);
        return l;
    }

    /** Two independent runs: p1 (poisoned while {@code poison} is set) and p2. */
    private static PipelineGraph twoRuns(String s1, String s2, AtomicBoolean poison) {
        return PipelineGraph.builder("two")
                .pipeline("p1", p -> p.source(new RangeSource(4, 3)).row((row, out, ctx) -> {
                    if (poison.get() && row.getLong("id") == 5) throw new IllegalStateException("poison row");
                    out.emit(row);
                }).sink(collect(s1)))
                .pipeline("p2", p -> p.source(new RangeSource(3, 3)).sink(collect(s2)))
                .build();
    }

    @Test void deadLetterFailsOnlyItsRunThenRetrySucceeds() throws Exception {
        String s1 = uniqueName("s"), s2 = uniqueName("s");
        AtomicBoolean poison = new AtomicBoolean(true);
        try (Harness h = new Harness(twoRuns(s1, s2, poison), Collections.<String, ExecMode>emptyMap())) {
            DefaultPipelinerEngine e = h.pod("pod1");
            String round = e.submit("two", "k", null);
            assertEquals(RoundStatus.FAILED, e.awaitRound(round, 15000));
            RoundView v = e.view(round);
            Map<String, RunStatus> runs = new HashMap<>();
            for (RunRecord r : v.getRuns()) runs.put(r.getRunId(), r.getStatus());
            assertEquals(RunStatus.FAILED, runs.get("R1"));
            assertEquals(RunStatus.SUCCEEDED, runs.get("R2"), "independent run keeps running");
            assertEquals(rowsOf(9, 3), sinkRows(s2));
            assertEquals(1, h.store.countChunks(round, "p1", ChunkStatus.DEAD));
            assertEquals(1L, (long) e.channelLengths(round).get("dlq:p1"));
            assertEquals(3, sinkChunks(s1).size(), "other chunks of the failed pipeline were still processed");

            poison.set(false);
            e.retryRun(round, "R1");
            assertEquals(RoundStatus.SUCCEEDED, e.awaitRound(round, 15000));
            assertEquals(rowsOf(12, 3), sinkRows(s1));
            assertEquals(0, h.store.countChunks(round, "p1", ChunkStatus.DEAD));
        }
    }

    @Test void skipDeadLettersCompletesIncomplete() throws Exception {
        String s1 = uniqueName("s"), s2 = uniqueName("s");
        try (Harness h = new Harness(twoRuns(s1, s2, new AtomicBoolean(true)), Collections.<String, ExecMode>emptyMap())) {
            DefaultPipelinerEngine e = h.pod("pod1");
            String round = e.submit("two", "k", null);
            assertEquals(RoundStatus.FAILED, e.awaitRound(round, 15000));
            e.skipDeadLetters(round, "R1");
            assertEquals(RoundStatus.SUCCEEDED, e.awaitRound(round, 15000));
            assertTrue(e.view(round).getRound().isIncomplete());
            assertEquals(1, h.store.countChunks(round, "p1", ChunkStatus.SKIPPED));
            assertEquals(9, sinkRows(s1).size());
        }
    }

    @Test void pauseStopsProgressResumeContinues() throws Exception {
        String sink = uniqueName("s");
        PipelineGraph g = PipelineGraph.builder("slow")
                .pipeline("p", p -> p.source(new RangeSource(20, 1)).row((row, out, ctx) -> { Thread.sleep(40); out.emit(row); }).sink(collect(sink)))
                .build();
        try (Harness h = new Harness(g, Collections.<String, ExecMode>emptyMap())) {
            DefaultPipelinerEngine e = h.pod("pod1");
            String round = e.submit("slow", "k", null);
            assertTrue(waitFor(() -> sinkChunks(sink).size() >= 3, 10000));
            e.pause(round);
            Thread.sleep(300);
            int n = sinkChunks(sink).size();
            Thread.sleep(400);
            assertEquals(n, sinkChunks(sink).size(), "no progress while paused");
            assertEquals(RoundStatus.PAUSED, e.view(round).getRound().getStatus());
            e.resume(round);
            assertEquals(RoundStatus.SUCCEEDED, e.awaitRound(round, 20000));
            assertEquals(20, sinkRows(sink).size());
        }
    }

    @Test void cancelEndsRound() throws Exception {
        PipelineGraph g = PipelineGraph.builder("slow")
                .pipeline("p", p -> p.source(new RangeSource(50, 1)).row((row, out, ctx) -> { Thread.sleep(50); out.emit(row); }).sink(collect(uniqueName("s"))))
                .build();
        try (Harness h = new Harness(g, Collections.<String, ExecMode>emptyMap())) {
            DefaultPipelinerEngine e = h.pod("pod1");
            String round = e.submit("slow", "k", null);
            Thread.sleep(300);
            e.cancel(round);
            assertEquals(RoundStatus.CANCELLED, e.awaitRound(round, 5000));
            assertTrue(h.plane.activeRounds().isEmpty());
        }
    }

    @Test void rerunFromResetsRunAndDownstream() throws Exception {
        String s1 = uniqueName("s"), s2 = uniqueName("s");
        AtomicInteger c1 = new AtomicInteger(), c2 = new AtomicInteger();
        PipelineGraph g = PipelineGraph.builder("rr")
                .pipeline("p1", p -> p.source(new RangeSource(3, 2)).row((r, o, c) -> { c1.incrementAndGet(); o.emit(r); }).sink(collect(s1)))
                .pipeline("p2", p -> p.source(new RangeSource(2, 2)).row((r, o, c) -> { c2.incrementAndGet(); o.emit(r); }).sink(collect(s2)))
                .gate("G", Gates.barrier(), gb -> gb.after("p1").before("p2"))
                .build();
        try (Harness h = new Harness(g, Collections.<String, ExecMode>emptyMap())) {
            DefaultPipelinerEngine e = h.pod("pod1");
            String round = e.submit("rr", "k", null);
            assertEquals(RoundStatus.SUCCEEDED, e.awaitRound(round, 15000));
            assertEquals(6, c1.get());
            assertEquals(4, c2.get());
            e.rerunFrom(round, "R1");
            assertEquals(RoundStatus.SUCCEEDED, e.awaitRound(round, 15000));
            assertEquals(12, c1.get());
            assertEquals(8, c2.get());
            assertEquals(rowsOf(6, 2), sinkRows(s1));
            assertEquals(rowsOf(4, 2), sinkRows(s2));
            for (RunRecord r : e.view(round).getRuns()) assertEquals(RunStatus.SUCCEEDED, r.getStatus());
        }
    }

    @Test void masterFailoverContinuesRound() throws Exception {
        String sink = uniqueName("s"), ref = uniqueName("r");
        for (String name : new String[] {ref, sink}) {
            PipelineGraph g = PipelineGraph.builder("fo")
                    .pipeline("a", p -> p.source(new RangeSource(30, 2)).chunkRows(1))
                    .pipeline("b", p -> p.row((row, out, ctx) -> { Thread.sleep(15); out.emit(row.copy().set("v", row.getLong("id") + 1)); }).sink(collect(name)))
                    .edge("a", "b").build();
            try (Harness h = new Harness(g, Collections.<String, ExecMode>emptyMap())) {
                if (name.equals(ref)) {
                    DefaultPipelinerEngine e = h.pod("solo");
                    assertEquals(RoundStatus.SUCCEEDED, e.awaitRound(e.submit("fo", "k", null), 30000));
                    continue;
                }
                DefaultPipelinerEngine m1 = h.pod(cfg("m1").workerEnabled(false).build(), true);
                h.pod(cfg("w").masterEnabled(false).build(), true);
                String round = m1.submit("fo", "k", null);
                assertTrue(waitFor(() -> h.store.countChunks(round, "b", ChunkStatus.DONE) >= 6, 10000));
                m1.halt(); // crash: no leadership release
                assertTrue(h.store.countChunks(round, "b", ChunkStatus.DONE) < 60);
                DefaultPipelinerEngine m2 = h.pod(cfg("m2").workerEnabled(false).build(), true);
                assertEquals(RoundStatus.SUCCEEDED, m2.awaitRound(round, 30000));
                assertTrue(h.plane.currentEpoch() >= 2, "new leader got a new epoch");
                assertEquals(sinkRows(ref), sinkRows(sink));
                assertEquals(60, sinkRows(sink).size());
            }
        }
    }
}
