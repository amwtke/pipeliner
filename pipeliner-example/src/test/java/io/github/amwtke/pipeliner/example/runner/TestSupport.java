package io.github.amwtke.pipeliner.example.runner;

import io.github.amwtke.pipeliner.api.Collector;
import io.github.amwtke.pipeliner.api.Row;
import io.github.amwtke.pipeliner.api.RunContext;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Fake RunContext with canned lookups keyed by a sql fragment, and a port-recording collector. */
final class TestSupport {
    private TestSupport() {}

    static final class Ctx implements RunContext {
        final Map<String, List<Row>> canned = new LinkedHashMap<>();
        final Map<String, String> params = new LinkedHashMap<>();

        Ctx() {
            params.put("planVersion", "V1");
            on("from ex_fault");   // chaos fault switch (Fault.check): empty table = no injected fault
        }

        Ctx on(String sqlFragment, Row... rows) {
            canned.put(sqlFragment, java.util.Arrays.asList(rows));
            return this;
        }
        @Override public String roundId() { return "r"; }
        @Override public String runId() { return "R1"; }
        @Override public String pipelineId() { return "p"; }
        @Override public String chunkId() { return "S0000"; }
        @Override public Map<String, String> params() { return params; }
        @Override public String param(String n) { return params.get(n); }
        @Override public List<Row> lookup(String ds, String sql, Map<String, ?> p) {
            for (Map.Entry<String, List<Row>> e : canned.entrySet()) if (sql.contains(e.getKey())) return e.getValue();
            throw new IllegalStateException("no canned rows for: " + sql);
        }
        @Override public boolean gateFallback(String g) { return false; }
    }

    static final class Out implements Collector {
        final Map<String, List<Row>> ports = new LinkedHashMap<>();
        @Override public void emit(Row row) { emit("out", row); }
        @Override public void emit(String port, Row row) { ports.computeIfAbsent(port, k -> new ArrayList<>()).add(row); }
        List<Row> port(String p) { return ports.getOrDefault(p, new ArrayList<Row>()); }
    }

    static BigDecimal d(String s) { return new BigDecimal(s); }

    static Row gd(String mat, String node, int month, String gd) {
        return Row.of("plan_version", "V1", "material_id", mat, "node", node, "month", month, "gd", d(gd));
    }
}
