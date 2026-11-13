package io.github.amwtke.pipeliner.example.runner;

import io.github.amwtke.pipeliner.api.GateCondition;
import io.github.amwtke.pipeliner.api.GateContext;
import io.github.amwtke.pipeliner.api.GateDecision;
import io.github.amwtke.pipeliner.core.util.NamedSql;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import javax.sql.DataSource;

/** Rejects negative final quantities; opens once ex_nd_final has materials x 5 nodes x 16 months rows. */
public final class ConserveCheckGate implements GateCondition {
    @Override
    public GateDecision evaluate(GateContext ctx) throws Exception {
        DataSource ds = ctx.dataSources().get("mysql");
        Map<String, Object> p = new HashMap<>();
        p.put("pv", ctx.params().get("planVersion"));
        if (count(ds, "select count(*) c from ex_nd_final where plan_version = :pv and qty < 0", p) > 0) return GateDecision.REJECT;
        long total = count(ds, "select count(*) c from ex_nd_final where plan_version = :pv", p);
        long expected = count(ds, "select count(*) c from ex_material", Collections.<String, Object>emptyMap()) * 5 * 16;
        return total == expected ? GateDecision.OPEN : GateDecision.WAIT;
    }

    private static long count(DataSource ds, String sql, Map<String, ?> p) {
        return NamedSql.query(ds, sql, p).get(0).getLong("c");
    }
}
