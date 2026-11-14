package io.github.amwtke.pipeliner.runtime;

import static io.github.amwtke.pipeliner.runtime.fake.TestKit.*;
import static org.junit.jupiter.api.Assertions.*;

import io.github.amwtke.pipeliner.api.Chunk;
import io.github.amwtke.pipeliner.api.Sink;
import io.github.amwtke.pipeliner.api.SinkContext;
import io.github.amwtke.pipeliner.core.graph.ExecMode;
import io.github.amwtke.pipeliner.core.graph.Gates;
import io.github.amwtke.pipeliner.core.graph.PipelineGraph;
import io.github.amwtke.pipeliner.core.store.ChunkStatus;
import io.github.amwtke.pipeliner.core.store.GateRecord;
import io.github.amwtke.pipeliner.core.store.RoundStatus;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

/** externalCountOf must not wait for chunks that never called the sink (empty hash splits, filtered output). */
class EmptyChunkGateTest {
    private static final AtomicReference<DefaultPipelinerEngine> ENGINE = new AtomicReference<>();
    private static final List<String> BATCH_IDS = new CopyOnWriteArrayList<>();

    /** Stands in for an algorithm service: signals the gate with the framework-issued batch id, synchronously. */
    public static final class SignallingSink implements Sink {
        @Override public String datasetName() { return null; }
        @Override public void write(Chunk chunk, SinkContext ctx) {
            String id = ctx.batchId(chunk);
            BATCH_IDS.add(id);
            ENGINE.get().signalGate(ctx.roundId(), "G", id, "rows=" + chunk.getRows().size());
        }
    }

    private static PipelineGraph graph() {
        return PipelineGraph.builder("eg")
                .pipeline("p1", p -> p.source(new RangeSource(4, 1)).row((row, out, ctx) -> { if (row.getLong("g") % 2 == 0) out.emit(row); }).sink(new SignallingSink()))
                .pipeline("p2", p -> p.source(new RangeSource(1, 1)).sink(collect(uniqueName("b"))))
                .gate("G", Gates.externalCountOf("p1"), gb -> gb.after("p1").before("p2"))
                .build();
    }

    @Test void gateOpensAlthoughHalfOfTheSplitsAreEmpty() throws Exception {
        BATCH_IDS.clear();
        try (Harness h = new Harness(graph(), Collections.<String, ExecMode>emptyMap())) {
            DefaultPipelinerEngine e = h.pod("pod");
            ENGINE.set(e);
            String round = e.submit("eg", "k", null);
            assertEquals(RoundStatus.SUCCEEDED, e.awaitRound(round, 15000), "the gate must open: only 2 of the 4 DONE chunks called the sink");
            assertEquals(4, h.store.countChunks(round, "p1", ChunkStatus.DONE));
            assertEquals(2, h.store.countSinkChunks(round, "p1"));
            assertEquals(2, BATCH_IDS.size());
            for (String id : BATCH_IDS) assertTrue(id.startsWith(round + ":p1:S000") && id.endsWith("#out@0"), id);
            for (GateRecord g : e.view(round).getGates()) assertEquals("G", g.getGateId());
        }
    }
}
