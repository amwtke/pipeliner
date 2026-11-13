package io.github.amwtke.pipeliner.example.runner;

import io.github.amwtke.pipeliner.api.BatchRunner;
import io.github.amwtke.pipeliner.api.Collector;
import io.github.amwtke.pipeliner.api.Row;
import io.github.amwtke.pipeliner.api.RunContext;
import java.util.List;

/** Tags the per-kind summary rows with the plan version for the publish log. */
public final class PublishRunner implements BatchRunner {
    @Override
    public void process(List<Row> rows, Collector out, RunContext ctx) {
        for (Row r : rows) {
            out.emit(Row.of("plan_version", ctx.param("planVersion"), "kind", r.getString("kind"),
                    "materials", r.getLong("materials"), "total_qty", r.getDecimal("total_qty")));
        }
    }
}
