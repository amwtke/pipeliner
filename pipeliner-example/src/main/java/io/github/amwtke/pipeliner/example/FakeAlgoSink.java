package io.github.amwtke.pipeliner.example;

import io.github.amwtke.pipeliner.api.Chunk;
import io.github.amwtke.pipeliner.api.Sink;
import io.github.amwtke.pipeliner.api.SinkContext;
import io.github.amwtke.pipeliner.core.util.ChunkIds;
import org.springframework.stereotype.Component;

/** Sink that hands each chunk of materials to the fake algorithm service; batchId is globally unique: round id, replay-stable chunk id and run attempt. */
@Component
public class FakeAlgoSink implements Sink {
    private final FakeAlgoService service;

    public FakeAlgoSink(FakeAlgoService service) {
        this.service = service;
    }

    @Override public String datasetName() { return null; }

    @Override
    public void write(Chunk chunk, SinkContext ctx) {
        // <roundId>:<chunkId>@<attempt>: unique across rounds; the attempt suffix keeps signals of a superseded attempt from opening the gate
        service.submit(ctx.roundId(), ctx.params().get("planVersion"), ChunkIds.batchId(ctx.roundId(), chunk.getChunkId(), ctx.attempt()), chunk.getRows());
    }
}
