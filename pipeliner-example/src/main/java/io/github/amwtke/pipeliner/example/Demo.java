package io.github.amwtke.pipeliner.example;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Arrays;
import java.util.List;

/** Shared constants and numeric helpers of the demo business model. */
public final class Demo {
    public static final String GRAPH = "net-demand";
    public static final List<String> NODES = Arrays.asList("CN-N", "CN-S", "EU", "US", "HQ");
    public static final List<String> SITES = Arrays.asList("SITE-CN", "SITE-EU", "SITE-US");
    public static final int MONTHS = 16;
    /** ClickHouse database; override with -Ddemo.ch-db=... to isolate a test run. */
    public static final String DB = System.getProperty("demo.ch-db", "pipeliner_demo");

    /** Recovery rate rho. */
    public static final BigDecimal RHO = new BigDecimal("0.7");
    /** Refurbishable share g. */
    public static final BigDecimal G = new BigDecimal("0.6");
    /** Scrap share c. */
    public static final BigDecimal C = new BigDecimal("0.25");
    /** Refurb yield y. */
    public static final BigDecimal Y = new BigDecimal("0.9");
    /** Harvest inspection pass rate k. */
    public static final BigDecimal K = new BigDecimal("0.95");
    /** Refurb lead time L_rf in months. */
    public static final int L_RF = 1;
    /** Disassembly lead time L_dis in months. */
    public static final int L_DIS = 1;

    private Demo() {}

    /** Scale 4, HALF_UP. */
    public static BigDecimal q(BigDecimal v) {
        return v.setScale(4, RoundingMode.HALF_UP);
    }

    public static BigDecimal zero() {
        return q(BigDecimal.ZERO);
    }

    public static BigDecimal max0(BigDecimal v) {
        return v.signum() < 0 ? BigDecimal.ZERO : v;
    }

    public static String siteOf(String node) {
        switch (node) {
            case "EU": return "SITE-EU";
            case "US": return "SITE-US";
            default: return "SITE-CN";
        }
    }
}
