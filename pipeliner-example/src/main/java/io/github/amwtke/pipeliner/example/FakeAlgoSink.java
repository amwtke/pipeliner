package io.github.amwtke.pipeliner.example;

import io.github.amwtke.pipeliner.api.Chunk;
import io.github.amwtke.pipeliner.api.Sink;
import io.github.amwtke.pipeliner.api.SinkContext;
import org.springframework.stereotype.Component;

/** Sink that hands each chunk of materials to the fake algorithm service; the batch id is issued by the framework (SinkContext.batchId): globally unique and replay-stable. */
@Component
public class FakeAlgoSink implements Sink {
    private final FakeAlgoService service;

    public FakeAlgoSink(FakeAlgoService service) {
        this.service = service;
    }

    @Override public String datasetName() { return null; }

    @Override
    public void write(Chunk chunk, SinkContext ctx) {
        // <roundId>:<pipelineId>:<chunkId>@<attempt>: unique across rounds and pipelines; the attempt suffix keeps signals of a superseded attempt from opening the gate
        service.submit(ctx.roundId(), ctx.params().get("planVersion"), ctx.batchId(chunk), chunk.getRows());
    }
}
