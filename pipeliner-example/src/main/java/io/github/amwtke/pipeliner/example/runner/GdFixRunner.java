package io.github.amwtke.pipeliner.example.runner;

import static io.github.amwtke.pipeliner.example.Demo.max0;
import static io.github.amwtke.pipeliner.example.Demo.q;

import io.github.amwtke.pipeliner.api.BatchRunner;
import io.github.amwtke.pipeliner.api.Collector;
import io.github.amwtke.pipeliner.api.Row;
import io.github.amwtke.pipeliner.api.RunContext;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/** Cleans gross demand: negatives to zero, zero after end-of-life, route BIG/FINE to separate ports. */
public final class GdFixRunner implements BatchRunner {
    @Override
    public void process(List<Row> rows, Collector out, RunContext ctx) {
        if (rows.isEmpty()) return;
        List<Row> eols = ctx.lookup("mysql", "select material_id, eol_month from ex_material where material_id in (:ids)",
                Collections.singletonMap("ids", Lk.distinct(rows, "material_id")));
        Map<String, java.math.BigDecimal> eol = Lk.decimals(eols, "eol_month", "material_id");
        for (Row r : rows) {
            String mat = r.getString("material_id");
            java.math.BigDecimal qty = max0(r.getDecimal("qty"));
            if (eol.containsKey(mat) && r.getInt("month") > eol.get(mat).intValue()) qty = java.math.BigDecimal.ZERO;
            qty = q(qty);
            if ("BIG".equals(r.getString("kind"))) {
                out.emit("big", Row.of("plan_version", r.getString("plan_version"), "material_id", mat, "node", r.getString("node"),
                        "month", r.getInt("month"), "gd", qty));
            } else {
                out.emit("fine", Row.of("plan_version", r.getString("plan_version"), "material_id", mat, "node", r.getString("node"),
                        "month", r.getInt("month"), "qty", qty));
            }
        }
    }
}
