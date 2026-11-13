package io.github.amwtke.pipeliner.example.runner;

import static io.github.amwtke.pipeliner.example.Demo.*;

import io.github.amwtke.pipeliner.api.Collector;
import io.github.amwtke.pipeliner.api.Row;
import io.github.amwtke.pipeliner.api.RowRunner;
import io.github.amwtke.pipeliner.api.RunContext;
import java.math.BigDecimal;

/** Grades recoverable cores: core = rho*gd, refurbishable = g*core, scrap = c*core. */
public final class CoreGradeRunner implements RowRunner {
    private final long delayMs;

    public CoreGradeRunner(long delayMs) {
        this.delayMs = delayMs;
    }

    @Override
    public void process(Row row, Collector out, RunContext ctx) throws Exception {
        if (delayMs > 0) Thread.sleep(delayMs);
        String node = row.getString("node");
        BigDecimal gd = row.getDecimal("gd");
        BigDecimal core = q(RHO.multiply(gd));
        out.emit(Row.of("plan_version", row.getString("plan_version"), "material_id", row.getString("material_id"),
                "site", siteOf(node), "node", node, "month", row.getInt("month"), "gd", q(gd), "nd1", q(row.getDecimal("nd1")),
                "core", core, "refurbishable", q(G.multiply(core)), "scrap", q(C.multiply(core))));
    }
}
