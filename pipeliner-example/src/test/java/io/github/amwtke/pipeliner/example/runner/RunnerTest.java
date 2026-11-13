package io.github.amwtke.pipeliner.example.runner;

import static io.github.amwtke.pipeliner.example.runner.TestSupport.*;
import static org.junit.jupiter.api.Assertions.*;

import io.github.amwtke.pipeliner.api.Row;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.Test;

/** Unit tests of the pure demo runners against hand-computed examples. */
class RunnerTest {
    private static void assertQty(String expected, Row r, String col) {
        assertEquals(0, d(expected).compareTo(r.getDecimal(col)), col + " expected " + expected + " got " + r.getDecimal(col));
    }

    @Test
    void bigNetRollingNetting() {
        // pab0=10, ss=5, sr m2=20; gd 8,12,30 -> nd1 3,0,22
        Ctx ctx = new Ctx()
                .on("ex_inventory", Row.of("material_id", "A", "node", "EU", "qty", d("10")))
                .on("ex_safety_stock", Row.of("material_id", "A", "node", "EU", "qty", d("5")))
                .on("ex_in_transit", Row.of("material_id", "A", "node", "EU", "month", 2, "qty", d("20")));
        Out out = new Out();
        new BigNetRunner().process(Arrays.asList(gd("A", "EU", 3, "30"), gd("A", "EU", 1, "8"), gd("A", "EU", 2, "12")), out, ctx);
        List<Row> r = out.port("out");
        assertEquals(3, r.size());
        assertQty("3", r.get(0), "nd1");
        assertQty("0", r.get(1), "nd1");
        assertQty("22", r.get(2), "nd1");
        assertEquals(3, r.get(2).getInt("month"));
        assertQty("30", r.get(2), "gd");
    }

    @Test
    void nettingNd2CarryOver() {
        java.util.Map<Integer, java.math.BigDecimal> nd1 = new java.util.TreeMap<>();
        nd1.put(1, d("5")); nd1.put(2, d("5")); nd1.put(3, d("5"));
        java.util.Map<Integer, java.math.BigDecimal> sup = new java.util.TreeMap<>();
        sup.put(1, d("12"));
        java.util.Map<Integer, java.math.BigDecimal> nd2 = Netting.nd2(nd1, sup);
        // avail1=12 -> nd2=0, C=7; avail2=7 -> 0, C=2; avail3=2 -> 3
        assertEquals(0, nd2.get(1).signum());
        assertEquals(0, nd2.get(2).signum());
        assertEquals(0, d("3").compareTo(nd2.get(3)));
    }

    @Test
    void gdFixCutsEolNegativesAndRoutes() {
        Ctx ctx = new Ctx().on("ex_material", Row.of("material_id", "B1", "eol_month", 2), Row.of("material_id", "F1", "eol_month", 99));
        Out out = new Out();
        List<Row> in = Arrays.asList(
                Row.of("plan_version", "V1", "material_id", "B1", "group_id", "G0", "kind", "BIG", "node", "EU", "month", 1, "qty", d("-3")),
                Row.of("plan_version", "V1", "material_id", "B1", "group_id", "G0", "kind", "BIG", "node", "EU", "month", 2, "qty", d("7")),
                Row.of("plan_version", "V1", "material_id", "B1", "group_id", "G0", "kind", "BIG", "node", "EU", "month", 3, "qty", d("9")),
                Row.of("plan_version", "V1", "material_id", "F1", "group_id", "G0", "kind", "FINE", "node", "US", "month", 3, "qty", d("9")));
        new GdFixRunner().process(in, out, ctx);
        List<Row> big = out.port("big");
        assertEquals(3, big.size());
        assertQty("0", big.get(0), "gd");
        assertQty("7", big.get(1), "gd");
        assertQty("0", big.get(2), "gd");
        assertEquals(1, out.port("fine").size());
        assertQty("9", out.port("fine").get(0), "qty");
        assertTrue(out.port("out").isEmpty());
    }

    @Test
    void coreGrade() throws Exception {
        Out out = new Out();
        new CoreGradeRunner(0).process(Row.of("plan_version", "V1", "material_id", "A", "node", "HQ", "month", 4, "gd", d("10"), "nd1", d("6")), out, new Ctx());
        Row r = out.port("out").get(0);
        assertEquals("SITE-CN", r.getString("site"));
        assertQty("7", r, "core");
        assertQty("4.2", r, "refurbishable");
        assertQty("1.75", r, "scrap");
    }

    @Test
    void capAllocRespectsCapacityOrder() throws Exception {
        Ctx ctx = new Ctx().on("ex_site_capacity", Row.of("site", "SITE-EU", "month", 1, "capacity", d("10")),
                Row.of("site", "SITE-EU", "month", 2, "capacity", d("100")));
        Out out = new Out();
        List<Row> in = Arrays.asList(
                grade("M2", 1, "8", "8", "1"), grade("M1", 1, "9", "6", "1"), grade("M1", 2, "50", "4", "2"));
        new CapAllocRunner().process(in, out, ctx);
        List<Row> plan = out.port("out");
        // month1 order: M1 first: min(9,6,10)=6, then M2: min(8,8,4)=4; month2: M1 min(50,4,100)=4
        assertEquals("M1", plan.get(0).getString("material_id"));
        assertQty("6", plan.get(0), "refurb_in");
        assertQty("5.4", plan.get(0), "refurb_out");
        assertEquals(2, plan.get(0).getInt("out_month"));
        assertQty("4", plan.get(1), "refurb_in");
        assertEquals("M2", plan.get(1).getString("material_id"));
        assertQty("4", plan.get(2), "refurb_in");
        List<Row> scrap = out.port("scrap");
        assertQty("1.4", scrap.get(1), "scrap");   // M2: 1 + 0.1*4
    }

    private static Row grade(String mat, int month, String nd1, String refurbishable, String scrap) {
        return Row.of("plan_version", "V1", "material_id", mat, "site", "SITE-EU", "node", "EU", "month", month,
                "nd1", d(nd1), "refurbishable", d(refurbishable), "scrap", d(scrap));
    }

    @Test
    void bigFinalCarryOver() {
        Ctx ctx = new Ctx().on("ex_refurb_plan", Row.of("material_id", "A", "node", "EU", "out_month", 2, "rout", d("12")));
        Out out = new Out();
        List<Row> in = Arrays.asList(nd1("A", 1, "5"), nd1("A", 2, "5"), nd1("A", 3, "5"), nd1("A", 4, "5"));
        new BigFinalRunner().process(in, out, ctx);
        List<Row> r = out.port("out");
        // m1: 5; m2: avail 12 -> 0 C=7; m3: avail 7 -> 0 C=2; m4: avail 2 -> 3
        assertQty("5", r.get(0), "qty");
        assertQty("0", r.get(1), "qty");
        assertQty("0", r.get(2), "qty");
        assertQty("3", r.get(3), "qty");
        assertEquals("BIG", r.get(0).getString("kind"));
    }

    private static Row nd1(String mat, int month, String nd1) {
        return Row.of("plan_version", "V1", "material_id", mat, "node", "EU", "month", month, "gd", d("0"), "nd1", d(nd1));
    }

    @Test
    void fineNetRefurbDemandAndHarvest() {
        // fine F has parent B (usage 2, rate 0.5). refurb_in(B,EU,1)=10 -> demand 20 at m1; scrap(B,EU,1)=40 -> harvest at m2 = 0.95*2*0.5*40 = 38
        Ctx ctx = new Ctx()
                .on("ex_inventory", Row.of("material_id", "F", "node", "EU", "qty", d("0")))
                .on("ex_safety_stock", Row.of("material_id", "F", "node", "EU", "qty", d("0")))
                .on("ex_in_transit")
                .on("ex_bom", Row.of("big_id", "B", "fine_id", "F", "usage_qty", d("2"), "recovery_rate", d("0.5")))
                .on("ex_refurb_plan", Row.of("material_id", "B", "node", "EU", "month", 1, "rin", d("10")))
                .on("ex_scrap", Row.of("material_id", "B", "node", "EU", "month", 1, "sc", d("40")));
        Out out = new Out();
        List<Row> in = Arrays.asList(fine("F", 1, "3"), fine("F", 2, "30"), fine("F", 3, "1"));
        new FineNetRunner().process(in, out, ctx);
        List<Row> r = out.port("out");
        // gd' = 23, 30, 1; nd1 (pab0=0, ss=0): m1=23 pab=0; m2=30 pab=0; m3=1
        // nd2: m1: avail 0 -> 23; m2: avail 38 -> 0, C=8; m3: avail 8 -> 0
        assertQty("23", r.get(0), "qty");
        assertQty("0", r.get(1), "qty");
        assertQty("0", r.get(2), "qty");
        assertEquals("FINE", r.get(0).getString("kind"));
    }

    private static Row fine(String mat, int month, String qty) {
        return Row.of("plan_version", "V1", "material_id", mat, "node", "EU", "month", month, "qty", d(qty));
    }

    @Test
    void publishAddsPlanVersion() {
        Out out = new Out();
        new PublishRunner().process(Arrays.asList(Row.of("kind", "BIG", "materials", 3L, "total_qty", d("9.5"))), out, new Ctx());
        assertEquals("V1", out.port("out").get(0).getString("plan_version"));
        assertEquals(3L, out.port("out").get(0).getLong("materials"));
    }
}
