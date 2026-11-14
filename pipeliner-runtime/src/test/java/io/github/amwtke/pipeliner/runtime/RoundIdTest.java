package io.github.amwtke.pipeliner.runtime;

import static io.github.amwtke.pipeliner.runtime.fake.TestKit.*;
import static org.junit.jupiter.api.Assertions.*;

import io.github.amwtke.pipeliner.core.graph.ExecMode;
import io.github.amwtke.pipeliner.core.graph.PipelineGraph;
import io.github.amwtke.pipeliner.core.store.ControlPlaneStore;
import io.github.amwtke.pipeliner.core.store.DuplicateRoundIdException;
import io.github.amwtke.pipeliner.core.store.GateRecord;
import io.github.amwtke.pipeliner.core.store.PipelineRecord;
import io.github.amwtke.pipeliner.core.store.RoundRecord;
import io.github.amwtke.pipeliner.core.store.RunRecord;
import io.github.amwtke.pipeliner.core.util.ReflectionComponentFactory;
import io.github.amwtke.pipeliner.core.util.SimpleDataSourceRegistry;
import io.github.amwtke.pipeliner.runtime.fake.InMemoryControlPlaneStore;
import io.github.amwtke.pipeliner.runtime.fake.InMemoryDataPlane;
import io.github.amwtke.pipeliner.runtime.fake.TestPlanner;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

/** Round id format / uniqueness (contract C5). */
class RoundIdTest {
    @Test
    void idsCarry48RandomBitsAndFitTheDeclaredLimit() {
        Set<String> ids = new HashSet<>();
        String graph = "g";
        for (int i = 0; i < 2000; i++) ids.add(DefaultPipelinerEngine.newRoundId(graph));
        assertEquals(2000, ids.size(), "no collision within the same second");
        for (String id : ids) assertTrue(id.matches("g-\\d{8}-\\d{6}-[0-9a-f]{12}"), id);
        StringBuilder longName = new StringBuilder();
        while (longName.length() < ControlPlaneStore.MAX_GRAPH_NAME_LENGTH) longName.append('x');
        assertEquals(ControlPlaneStore.MAX_ROUND_ID_LENGTH, DefaultPipelinerEngine.newRoundId(longName.toString()).length());
    }

    @Test
    void submitRetriesWithAFreshIdOnACollision() {
        final AtomicInteger calls = new AtomicInteger();
        final java.util.List<String> tried = new java.util.ArrayList<>();
        InMemoryControlPlaneStore store = new InMemoryControlPlaneStore() {
            @Override public synchronized RoundRecord createRound(RoundRecord r, List<RunRecord> runs, List<GateRecord> gates, List<PipelineRecord> pls) {
                tried.add(r.getRoundId());
                if (calls.incrementAndGet() == 1) throw new DuplicateRoundIdException(r.getRoundId());
                return super.createRound(r, runs, gates, pls);
            }
        };
        PipelineGraph g = PipelineGraph.builder("rid").pipeline("p", p -> p.source(new RangeSource(1, 1)).sink(collect(uniqueName("s")))).build();
        DefaultPipelinerEngine e = new DefaultPipelinerEngine(cfg("rid-pod").build(), store, new InMemoryDataPlane(),
                new SimpleDataSourceRegistry(new HashMap<String, javax.sql.DataSource>()), null, new ReflectionComponentFactory());
        e.registerPlan(TestPlanner.plan(g, mode(ExecMode.CLUSTER, "p")));
        String id = e.submit("rid", "biz-1", null);
        assertEquals(2, calls.get());
        assertNotEquals(tried.get(0), tried.get(1), "the retry used a fresh id");
        assertEquals(tried.get(1), id);
        assertNotNull(store.loadRoundRecord(id));
        assertEquals(id, e.submit("rid", "biz-1", null), "the biz key is idempotent");
    }
}
