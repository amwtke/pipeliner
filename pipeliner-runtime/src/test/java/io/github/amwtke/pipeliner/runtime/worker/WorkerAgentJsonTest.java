package io.github.amwtke.pipeliner.runtime.worker;

import static org.junit.jupiter.api.Assertions.*;

import io.github.amwtke.pipeliner.core.codec.ChunkCodec;
import io.github.amwtke.pipeliner.core.engine.LiveInstance;
import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.Test;

class WorkerAgentJsonTest {
    @Test void heartbeatJsonStaysValidForIdsWithControlCharacters() {
        String weird = "r\"1\\\n\u0001\t";
        String json = WorkerAgent.liveJson(Collections.singletonList(new LiveInstance(weird, "p\n", 3, false, null, 2)));
        List<?> rows = ChunkCodec.fromJson(json, List.class);
        List<?> row = (List<?>) rows.get(0);
        assertEquals(weird, row.get(0));
        assertEquals("p\n", row.get(1));
        assertEquals(2L, ((Number) row.get(2)).longValue());
        assertEquals(3L, ((Number) row.get(3)).longValue());
    }
}
