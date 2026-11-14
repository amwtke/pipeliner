package io.github.amwtke.pipeliner.core.graph;

import static org.junit.jupiter.api.Assertions.*;

import io.github.amwtke.pipeliner.api.PipelinerException;
import io.github.amwtke.pipeliner.core.store.ControlPlaneStore;
import org.junit.jupiter.api.Test;

/** Identifier limits the control plane's columns are sized for. */
class PipelineGraphLimitsTest {
    private static String len(int n) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < n; i++) sb.append('a');
        return sb.toString();
    }

    @Test
    void graphNameAtTheLimitBuildsAndLongerIsRejected() {
        PipelineGraph.builder(len(ControlPlaneStore.MAX_GRAPH_NAME_LENGTH)).pipeline("p", null).build();
        assertThrows(PipelinerException.class, () -> PipelineGraph.builder(len(ControlPlaneStore.MAX_GRAPH_NAME_LENGTH + 1)).pipeline("p", null).build());
        assertThrows(PipelinerException.class, () -> PipelineGraph.builder("").pipeline("p", null).build());
    }

    @Test
    void pipelineAndGateIdsAreLimited() {
        PipelineGraph.builder("g").pipeline(len(ControlPlaneStore.MAX_PIPELINE_ID_LENGTH), null).build();
        assertThrows(PipelinerException.class, () -> PipelineGraph.builder("g").pipeline(len(ControlPlaneStore.MAX_PIPELINE_ID_LENGTH + 1), null).build());
        assertThrows(PipelinerException.class, () -> PipelineGraph.builder("g").pipeline("p", null)
                .gate(len(ControlPlaneStore.MAX_GATE_ID_LENGTH + 1), Gates.manual(), null).build());
    }

    @Test
    void aMaximumRoundIdFitsTheDeclaredLimit() {
        String roundId = len(ControlPlaneStore.MAX_GRAPH_NAME_LENGTH) + "-20260101-120000-0123456789ab";
        assertEquals(ControlPlaneStore.MAX_ROUND_ID_LENGTH, roundId.length());
    }
}
