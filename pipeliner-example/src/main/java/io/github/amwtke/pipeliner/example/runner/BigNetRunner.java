package io.github.amwtke.pipeliner.example.runner;

import static io.github.amwtke.pipeliner.example.Demo.q;

import io.github.amwtke.pipeliner.api.BatchRunner;
import io.github.amwtke.pipeliner.api.Collector;
import io.github.amwtke.pipeliner.api.Row;
import io.github.amwtke.pipeliner.api.RunContext;
import java.math.BigDecimal;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/** Rolling netting for BIG materials: emits (plan_version, material_id, node, month, gd, nd1). */
public final class BigNetRunner implements BatchRunner {
    @Override
    public void process(List<Row> rows, Collector out, RunContext ctx) {
        if (rows.isEmpty()) return;
        Map<String, Object> p = Collections.singletonMap("ids", Lk.distinct(rows, "material_id"));
        Map<String, BigDecimal> inv = Lk.decimals(ctx.lookup("mysql", "select material_id, node, qty from ex_inventory where material_id in (:ids)", p), "qty", "material_id", "node");
        Map<String, BigDecimal> ss = Lk.decimals(ctx.lookup("mysql", "select material_id, node, qty from ex_safety_stock where material_id in (:ids)", p), "qty", "material_id", "node");
        Map<String, Map<Integer, BigDecimal>> sr = Lk.transit(ctx.lookup("mysql", "select material_id, node, month, qty from ex_in_transit where material_id in (:ids)", p));
        String planVersion = rows.get(0).getString("plan_version");
        for (Map.Entry<String, Map<Integer, BigDecimal>> e : Lk.series(rows, "gd").entrySet()) {
            String[] k = e.getKey().split("\\|");
            Map<Integer, BigDecimal> nd1 = Netting.nd1(Lk.get(inv, e.getKey()), Lk.get(ss, e.getKey()), e.getValue(),
                    sr.getOrDefault(e.getKey(), Collections.<Integer, BigDecimal>emptyMap()));
            for (Map.Entry<Integer, BigDecimal> m : e.getValue().entrySet()) {
                out.emit(Row.of("plan_version", planVersion, "material_id", k[0], "node", k[1], "month", m.getKey(),
                        "gd", q(m.getValue()), "nd1", nd1.get(m.getKey())));
            }
        }
    }
}
