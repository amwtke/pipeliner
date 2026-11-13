package io.github.amwtke.pipeliner.core.dataplane;

import lombok.AllArgsConstructor;
import lombok.Data;

/** Chunk to append to an output stream; payload = ChunkCodec.encodeChunk. */
@Data
@AllArgsConstructor
public class OutputChunk {
    private String streamKey;
    private String payload;
}
