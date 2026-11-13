package io.github.amwtke.pipeliner.example.runner;

import static io.github.amwtke.pipeliner.example.Demo.*;

import io.github.amwtke.pipeliner.api.BatchRunner;
import io.github.amwtke.pipeliner.api.Collector;
import io.github.amwtke.pipeliner.api.Row;
import io.github.amwtke.pipeliner.api.RunContext;
import java.math.BigDecimal;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/** Final BIG demand: nd1 minus refurbished supply (with carry-over of unused supply). */
public final class BigFinalRunner implements BatchRunner {
    @Override
    public void process(List<Row> rows, Collector out, RunContext ctx) {
        if (rows.isEmpty()) return;
        Map<String, Object> p = new HashMap<>();
        p.put("ids", Lk.distinct(rows, "material_id"));
        p.put("pv", ctx.param("planVersion"));
        List<Row> rf = ctx.lookup("ch", "select material_id, node, out_month, sum(refurb_out) as rout from " + DB + ".ex_refurb_plan FINAL "
                + "where plan_version = :pv and material_id in (:ids) and out_month <= " + MONTHS + " group by material_id, node, out_month", p);
        Map<String, Map<Integer, BigDecimal>> supply = new HashMap<>();
        for (Row r : rf) {
            supply.computeIfAbsent(Lk.key(r.getString("material_id"), r.getString("node")), k -> new TreeMap<>())
                    .put(r.getInt("out_month"), r.getDecimal("rout"));
        }
        String planVersion = rows.get(0).getString("plan_version");
        for (Map.Entry<String, Map<Integer, BigDecimal>> e : Lk.series(rows, "nd1").entrySet()) {
            String[] k = e.getKey().split("\\|");
            Map<Integer, BigDecimal> nd2 = Netting.nd2(e.getValue(), supply.getOrDefault(e.getKey(), new TreeMap<Integer, BigDecimal>()));
            for (Map.Entry<Integer, BigDecimal> m : nd2.entrySet()) {
                out.emit(Row.of("plan_version", planVersion, "material_id", k[0], "node", k[1], "month", m.getKey(), "kind", "BIG", "qty", m.getValue()));
            }
        }
    }
}
