package io.github.amwtke.pipeliner.runtime;

import static io.github.amwtke.pipeliner.runtime.fake.TestKit.*;
import static org.junit.jupiter.api.Assertions.*;

import io.github.amwtke.pipeliner.api.Split;
import io.github.amwtke.pipeliner.core.dataplane.*;
import io.github.amwtke.pipeliner.core.graph.ExecMode;
import io.github.amwtke.pipeliner.core.graph.PipelineGraph;
import io.github.amwtke.pipeliner.core.store.ChunkEvent;
import io.github.amwtke.pipeliner.core.store.RoundStatus;
import io.github.amwtke.pipeliner.runtime.fake.InMemoryDataPlane;
import java.util.*;
import org.junit.jupiter.api.Test;

class FanInTest {
    @Test void fanInRoundSucceedsThroughTheMultiEntryWorkerPath() throws Exception {
        String sink = uniqueName("s");
        PipelineGraph g = PipelineGraph.builder("fanin")
                .pipeline("a", p -> p.source(new RangeSource(4, 2)))
                .pipeline("b", p -> p.source(new RangeSource(4, 2)))
                .pipeline("c", p -> p.row((row, out, ctx) -> { Thread.sleep(2); out.emit(row); }).sink(collect(sink)))
                .edge("a", "c").edge("b", "c").build();
        try (Harness h = new Harness(g, Collections.<String, ExecMode>emptyMap())) {
            DefaultPipelinerEngine e = h.pod("pod1");
            String round = e.submit("fanin", "k", null);
            assertEquals(RoundStatus.SUCCEEDED, e.awaitRound(round, 20000));
            assertEquals(16, sinkRows(sink).size(), "both inputs fully delivered");
        }
    }

    @Test void fakeReadAppliesCountPerStreamAndReturnsEveryInput() {
        InMemoryDataPlane dp = new InMemoryDataPlane();
        dp.fenceRound("r", 1);
        String q1 = Keys.splitQueue("r", "p1", 0), q2 = Keys.splitQueue("r", "p2", 0);
        dp.ensureGroup("r", q1, 1);
        dp.ensureGroup("r", q2, 1);
        for (int i = 0; i < 3; i++) {
            dp.enqueueSplit("r", "p1", 0, new Split("s" + i, Collections.singletonMap("i", "" + i)), 1);
            dp.enqueueSplit("r", "p2", 0, new Split("s" + i, Collections.singletonMap("i", "" + i)), 1);
        }
        List<StreamEntry> got = dp.read("A", Arrays.asList(q1, q2), 1, 0);
        assertEquals(2, got.size(), "COUNT 1 per stream, both streams served");
        assertEquals(new HashSet<>(Arrays.asList(q1, q2)), new HashSet<>(Arrays.asList(got.get(0).getStreamKey(), got.get(1).getStreamKey())));
    }

    @Test void fakeDeadLetterAndCommitHonourOwnership() {
        InMemoryDataPlane dp = new InMemoryDataPlane();
        String in = Keys.channel("r", "e1", 0);
        dp.ensureGroup("r", in, 1);
        dp.fenceRound("r", 1);
        Split sp = new Split("s0", Collections.singletonMap("i", "0"));
        // use a split queue as a convenient stream with entries
        String q = Keys.splitQueue("r", "p", 0);
        dp.ensureGroup("r", q, 1);
        dp.enqueueSplit("r", "p", 0, sp, 1);
        List<StreamEntry> got = dp.read("A", Collections.singletonList(q), 1, 0);
        assertEquals(1, got.size());
        dp.ageAllPending(q, 10_000);
        assertEquals(1, dp.autoClaim("B", q, 100, 1).size());
        assertFalse(dp.renew("A", q, got.get(0).getEntryId()), "A lost the entry");
        assertTrue(dp.renew("B", q, got.get(0).getEntryId()));
        DeadLetterRequest dl = DeadLetterRequest.builder().roundId("r").pipelineId("p").attempt(0).inputStreamKey(q)
                .inputEntryId(got.get(0).getEntryId()).inputFields(got.get(0).getFields()).error("x").consumer("A")
                .event(ChunkEvent.builder().type("DEAD").roundId("r").chunkId("c").ts(1L).build()).build();
        assertFalse(dp.deadLetter(dl), "A no longer owns it");
        dl.setConsumer("B");
        assertTrue(dp.deadLetter(dl));
        assertFalse(dp.deadLetter(dl), "already moved");
    }
}
