package io.github.amwtke.pipeliner.core.dataplane;

import lombok.AllArgsConstructor;
import lombok.Data;

/** Desired worker state of one pipeline: thread count and the run attempt the threads must work on. */
@Data
@AllArgsConstructor
public class DesiredState {
    private final int parallelism;
    private final int attempt;
}
