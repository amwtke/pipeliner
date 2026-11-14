package io.github.amwtke.pipeliner.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.amwtke.pipeliner.api.PipelinerException;
import io.github.amwtke.pipeliner.core.engine.RuntimeConfig;
import io.github.amwtke.pipeliner.core.graph.GateType;
import io.github.amwtke.pipeliner.core.graph.Gates;
import io.github.amwtke.pipeliner.core.graph.PipelineGraph;
import io.github.amwtke.pipeliner.core.store.ChunkQuery;
import io.github.amwtke.pipeliner.core.store.RoundRecord;
import org.junit.jupiter.api.Test;

class RuntimeConfigAndGraphTest {
    @Test void defaults() {
        assertEquals(64, new RuntimeConfig().getHighWatermark());
        assertEquals(64, RuntimeConfig.builder().podId("p").build().getHighWatermark());
        assertEquals(1000, new ChunkQuery().getLimit());
        assertEquals("0-0", new RoundRecord().getJournalOffset());
    }

    @Test void builder() {
        PipelineGraph g = PipelineGraph.builder("g")
                .pipeline("a", p -> p.chunkKey("k").sink("side", null))
                .pipeline("b", p -> { })
                .edge("a", "b").edge("a.side", "b")
                .gate("x", Gates.barrier(), gb -> gb.after("a").before("b"))
                .build();
        assertEquals("out", g.getEdges().get(0).getFromPort());
        assertEquals("side", g.getEdges().get(1).getFromPort());
        assertTrue(g.getPipelines().get("a").getPorts().contains("side"));
        assertTrue(g.getPipelines().get("a").getPorts().contains("out"));
        assertEquals(1, g.getPipelines().get("a").getChunkKey().size());
        assertEquals(GateType.BARRIER, g.getGates().get(0).getType());
        assertThrows(PipelinerException.class, () -> PipelineGraph.builder("g").pipeline("a", p -> { }).edge("a", "zz").build());
        assertThrows(PipelinerException.class, () -> PipelineGraph.builder("g").pipeline("a", p -> { }).pipeline("a", p -> { }).build());
        assertThrows(io.github.amwtke.pipeliner.core.plan.PlanException.class, g::plan);
    }

    @Test void colonIsReservedForBatchIds() {
        // ':' separates round, pipeline and chunk in batch ids / signal keys; chunk ids embed port names
        assertThrows(PipelinerException.class, () -> PipelineGraph.builder("g").pipeline("a:b", p -> { }).build());
        assertThrows(PipelinerException.class, () -> PipelineGraph.builder("g").pipeline("a", p -> p.ports("x:y")).build());
        PipelineGraph.builder("g").pipeline("a", p -> p.ports("side-1")).build();
    }
}
