package io.github.amwtke.pipeliner.example.runner;

import io.github.amwtke.pipeliner.api.BatchRunner;
import io.github.amwtke.pipeliner.api.Collector;
import io.github.amwtke.pipeliner.api.Row;
import io.github.amwtke.pipeliner.api.RunContext;
import java.util.List;

/** Passes the materials of a split through to the algorithm sink. */
public final class AlgoSubmitRunner implements BatchRunner {
    @Override
    public void process(List<Row> rows, Collector out, RunContext ctx) {
        for (Row r : rows) out.emit(r);
    }
}
