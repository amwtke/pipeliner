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

/** Allocates the shared monthly refurbishing capacity of a site in (month, material, node) order. */
public final class CapAllocRunner implements BatchRunner {
    @Override
    public void process(List<Row> input, Collector out, RunContext ctx) {
        if (input.isEmpty()) return;
        List<Row> rows = new ArrayList<>(input);
        rows.sort(Comparator.comparing((Row r) -> r.getInt("month")).thenComparing(r -> r.getString("material_id"))
                .thenComparing(r -> r.getString("node")));
        Map<String, BigDecimal> remaining = new HashMap<>(Lk.decimals(
                ctx.lookup("mysql", "select site, month, capacity from ex_site_capacity where site in (:sites)",
                        Collections.singletonMap("sites", Lk.distinct(rows, "site"))), "capacity", "site", "month"));
        for (Row r : rows) {
            String site = r.getString("site");
            int month = r.getInt("month");
            String capKey = Lk.key(site, month);
            BigDecimal cap = Lk.get(remaining, capKey);
            BigDecimal in = max0(r.getDecimal("nd1").min(r.getDecimal("refurbishable")).min(cap));
            remaining.put(capKey, cap.subtract(in));
            BigDecimal refurbOut = q(Y.multiply(in));
            out.emit(Row.of("plan_version", r.getString("plan_version"), "material_id", r.getString("material_id"), "site", site,
                    "node", r.getString("node"), "month", month, "refurb_in", q(in), "out_month", month + L_RF, "refurb_out", refurbOut));
            out.emit("scrap", Row.of("plan_version", r.getString("plan_version"), "material_id", r.getString("material_id"),
                    "node", r.getString("node"), "month", month,
                    "scrap", q(r.getDecimal("scrap").add(BigDecimal.ONE.subtract(Y).multiply(in)))));
        }
    }
}
