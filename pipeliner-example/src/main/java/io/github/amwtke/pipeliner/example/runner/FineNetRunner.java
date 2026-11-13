package io.github.amwtke.pipeliner.example.runner;

import static io.github.amwtke.pipeliner.example.Demo.*;

import io.github.amwtke.pipeliner.api.BatchRunner;
import io.github.amwtke.pipeliner.api.Collector;
import io.github.amwtke.pipeliner.api.Row;
import io.github.amwtke.pipeliner.api.RunContext;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/** FINE materials: add refurb demand of parent BIGs to gross demand, net it, then offset by harvested scrap. */
public final class FineNetRunner implements BatchRunner {
    @Override
    public void process(List<Row> rows, Collector out, RunContext ctx) {
        if (rows.isEmpty()) return;
        List<String> fineIds = Lk.distinct(rows, "material_id");
        Map<String, Object> p = Collections.singletonMap("ids", fineIds);
        Map<String, BigDecimal> inv = Lk.decimals(ctx.lookup("mysql", "select material_id, node, qty from ex_inventory where material_id in (:ids)", p), "qty", "material_id", "node");
        Map<String, BigDecimal> ss = Lk.decimals(ctx.lookup("mysql", "select material_id, node, qty from ex_safety_stock where material_id in (:ids)", p), "qty", "material_id", "node");
        Map<String, Map<Integer, BigDecimal>> sr = Lk.transit(ctx.lookup("mysql", "select material_id, node, month, qty from ex_in_transit where material_id in (:ids)", p));
        List<Row> bom = new ArrayList<>(ctx.lookup("mysql", "select big_id, fine_id, usage_qty, recovery_rate from ex_bom where fine_id in (:ids)", p));
        bom.sort(Comparator.comparing((Row r) -> r.getString("fine_id")).thenComparing(r -> r.getString("big_id")));
        Map<String, Object> bp = new HashMap<>();
        bp.put("ids", Lk.distinct(bom, "big_id"));
        bp.put("pv", ctx.param("planVersion"));
        Map<String, BigDecimal> refurbIn = new HashMap<>();
        Map<String, BigDecimal> scrap = new HashMap<>();
        if (!bom.isEmpty()) {
            refurbIn = Lk.decimals(ctx.lookup("ch", "select material_id, node, month, sum(refurb_in) as rin from " + DB + ".ex_refurb_plan FINAL "
                    + "where plan_version = :pv and material_id in (:ids) group by material_id, node, month", bp), "rin", "material_id", "node", "month");
            scrap = Lk.decimals(ctx.lookup("ch", "select material_id, node, month, sum(scrap) as sc from " + DB + ".ex_scrap FINAL "
                    + "where plan_version = :pv and material_id in (:ids) group by material_id, node, month", bp), "sc", "material_id", "node", "month");
        }
        Map<String, List<Row>> parents = new HashMap<>();
        for (Row b : bom) parents.computeIfAbsent(b.getString("fine_id"), k -> new ArrayList<>()).add(b);

        String planVersion = rows.get(0).getString("plan_version");
        for (Map.Entry<String, Map<Integer, BigDecimal>> e : Lk.series(rows, "qty").entrySet()) {
            String[] k = e.getKey().split("\\|");
            List<Row> ps = parents.getOrDefault(k[0], Collections.<Row>emptyList());
            Map<Integer, BigDecimal> gd = new TreeMap<>();
            Map<Integer, BigDecimal> harvest = new TreeMap<>();
            for (Map.Entry<Integer, BigDecimal> m : e.getValue().entrySet()) {
                int t = m.getKey();
                BigDecimal demand = BigDecimal.ZERO;
                BigDecimal scrapSum = BigDecimal.ZERO;
                for (Row b : ps) {
                    BigDecimal usage = b.getDecimal("usage_qty");
                    demand = demand.add(Lk.get(refurbIn, Lk.key(b.getString("big_id"), k[1], t)).multiply(usage));
                    scrapSum = scrapSum.add(usage.multiply(b.getDecimal("recovery_rate"))
                            .multiply(Lk.get(scrap, Lk.key(b.getString("big_id"), k[1], t - L_DIS))));
                }
                gd.put(t, q(m.getValue().add(demand)));
                harvest.put(t, q(K.multiply(scrapSum)));
            }
            Map<Integer, BigDecimal> nd1 = Netting.nd1(Lk.get(inv, e.getKey()), Lk.get(ss, e.getKey()), gd,
                    sr.getOrDefault(e.getKey(), Collections.<Integer, BigDecimal>emptyMap()));
            Map<Integer, BigDecimal> nd2 = Netting.nd2(nd1, harvest);
            for (Map.Entry<Integer, BigDecimal> m : nd2.entrySet()) {
                out.emit(Row.of("plan_version", planVersion, "material_id", k[0], "node", k[1], "month", m.getKey(), "kind", "FINE", "qty", m.getValue()));
            }
        }
    }
}
