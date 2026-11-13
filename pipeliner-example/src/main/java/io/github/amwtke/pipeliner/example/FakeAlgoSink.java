package io.github.amwtke.pipeliner.example;

import io.github.amwtke.pipeliner.api.Chunk;
import io.github.amwtke.pipeliner.api.Sink;
import io.github.amwtke.pipeliner.api.SinkContext;
import org.springframework.stereotype.Component;

/** Sink that hands each chunk of materials to the fake algorithm service; batchId is the (replay-stable) chunk id. */
@Component
public class FakeAlgoSink implements Sink {
    private final FakeAlgoService service;

    public FakeAlgoSink(FakeAlgoService service) {
        this.service = service;
    }

    @Override public String datasetName() { return null; }

    @Override
    public void write(Chunk chunk, SinkContext ctx) {
        service.submit(ctx.roundId(), ctx.params().get("planVersion"), chunk.getChunkId(), chunk.getRows());
    }
}
