package io.github.amwtke.pipeliner.example.runner;

import static io.github.amwtke.pipeliner.example.Demo.max0;
import static io.github.amwtke.pipeliner.example.Demo.q;

import java.math.BigDecimal;
import java.util.Map;
import java.util.TreeMap;

/** Rolling netting formulas shared by big-net, big-final and fine-net. */
public final class Netting {
    private Netting() {}

    /**
     * ND1_t = max(0, GD_t + SS - PAB_{t-1} - SR_t); PAB_t = PAB_{t-1} + SR_t + ND1_t - GD_t. Months come from gd (ascending).
     */
    public static Map<Integer, BigDecimal> nd1(BigDecimal pab0, BigDecimal ss, Map<Integer, BigDecimal> gd, Map<Integer, BigDecimal> sr) {
        Map<Integer, BigDecimal> out = new TreeMap<>();
        BigDecimal pab = pab0;
        for (Map.Entry<Integer, BigDecimal> e : new TreeMap<>(gd).entrySet()) {
            BigDecimal g = e.getValue();
            BigDecimal r = sr.getOrDefault(e.getKey(), BigDecimal.ZERO);
            BigDecimal nd = max0(g.add(ss).subtract(pab).subtract(r));
            pab = pab.add(r).add(nd).subtract(g);
            out.put(e.getKey(), q(nd));
        }
        return out;
    }

    /** avail_t = supply_t + C_{t-1}; ND2_t = max(0, ND1_t - avail_t); C_t = max(0, avail_t - ND1_t). */
    public static Map<Integer, BigDecimal> nd2(Map<Integer, BigDecimal> nd1, Map<Integer, BigDecimal> supply) {
        Map<Integer, BigDecimal> out = new TreeMap<>();
        BigDecimal carry = BigDecimal.ZERO;
        for (Map.Entry<Integer, BigDecimal> e : new TreeMap<>(nd1).entrySet()) {
            BigDecimal avail = supply.getOrDefault(e.getKey(), BigDecimal.ZERO).add(carry);
            out.put(e.getKey(), q(max0(e.getValue().subtract(avail))));
            carry = max0(avail.subtract(e.getValue()));
        }
        return out;
    }
}
