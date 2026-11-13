package io.github.amwtke.pipeliner.example;

import io.github.amwtke.pipeliner.connectors.jdbc.JdbcSinks;
import io.github.amwtke.pipeliner.connectors.jdbc.JdbcSources;
import io.github.amwtke.pipeliner.core.graph.ExecMode;
import io.github.amwtke.pipeliner.core.graph.Gates;
import io.github.amwtke.pipeliner.core.graph.PipelineGraph;
import io.github.amwtke.pipeliner.core.graph.TimeoutPolicy;
import io.github.amwtke.pipeliner.example.runner.*;
import java.time.Duration;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/** Declares the net-demand PipelineGraph. */
@Configuration
public class NetDemandGraphConfig {
    private static final String CH = Demo.DB + ".";

    @Bean
    public PipelineGraph netDemandGraph(FakeAlgoSink algoSink, @Value("${demo.core-grade-delay-ms:0}") long coreGradeDelayMs) {
        return PipelineGraph.builder(Demo.GRAPH)
                .pipeline("algo-submit", p -> p
                        .source(JdbcSources.mysql("mysql").table("ex_material").columns("material_id", "group_id", "kind")
                                .hashSplit("group_id", 8).orderBy("material_id").build())
                        .batch(new AlgoSubmitRunner()).mode(ExecMode.INLINE).sink(algoSink))
                .pipeline("gd-fix", p -> p
                        .source(JdbcSources.clickhouse("ch").table(CH + "ex_algo_gd").dataset("algo_gd").useFinal()
                                .where("plan_version = :planVersion").hashSplit("group_id", 16).orderBy("material_id, node, month").build())
                        .batch(new GdFixRunner()).ports("big", "fine")
                        .sink("fine", JdbcSinks.clickhouse("ch").table(CH + "ex_fine_gd").dataset("fine_gd").build())
                        .mode(ExecMode.CLUSTER).parallelism(3).chunkKey("material_id").chunkRows(800))
                .pipeline("big-net", p -> p
                        .batch(new BigNetRunner())
                        .sink(JdbcSinks.mysql("mysql").table("ex_nd1_big").upsertKeys("plan_version", "material_id", "node", "month")
                                .dataset("nd1_big").build())
                        .chunkKey("material_id"))
                .pipeline("core-grade", p -> p
                        .row(new CoreGradeRunner(coreGradeDelayMs))
                        .sink(JdbcSinks.clickhouse("ch").table(CH + "ex_grade_result").dataset("grade_result").build()))
                .pipeline("cap-alloc", p -> p
                        .source(JdbcSources.clickhouse("ch").table(CH + "ex_grade_result").dataset("grade_result").useFinal()
                                .where("plan_version = :planVersion").distinctSplit("site").orderBy("month, material_id, node").build())
                        .batch(new CapAllocRunner()).ports("scrap")
                        .sink(JdbcSinks.clickhouse("ch").table(CH + "ex_refurb_plan").dataset("refurb_plan").build())
                        .sink("scrap", JdbcSinks.clickhouse("ch").table(CH + "ex_scrap").dataset("scrap").build())
                        .mode(ExecMode.LOCAL))
                .pipeline("big-final", p -> p
                        .source(JdbcSources.mysql("mysql").table("ex_nd1_big").dataset("nd1_big")
                                .where("plan_version = :planVersion").hashSplit("material_id", 8).orderBy("material_id, node, month").build())
                        .batch(new BigFinalRunner())
                        .sink(JdbcSinks.mysql("mysql").table("ex_nd_final").upsertKeys("plan_version", "material_id", "node", "month")
                                .dataset("nd_final").build())
                        .mode(ExecMode.CLUSTER).estimatedRows(3_000_000))
                .pipeline("fine-net", p -> p
                        .source(JdbcSources.clickhouse("ch").table(CH + "ex_fine_gd").dataset("fine_gd").useFinal()
                                .where("plan_version = :planVersion").hashSplit("material_id", 8).orderBy("material_id, node, month").build())
                        .batch(new FineNetRunner())
                        .sink(JdbcSinks.mysql("mysql").table("ex_nd_final").upsertKeys("plan_version", "material_id", "node", "month")
                                .dataset("nd_final").build())
                        .mode(ExecMode.CLUSTER).estimatedRows(3_000_000))
                .pipeline("publish", p -> p
                        .source(JdbcSources.mysql("mysql").dataset("nd_final")
                                .query("select kind, count(distinct material_id) materials, sum(qty) total_qty from ex_nd_final "
                                        + "where plan_version = :planVersion group by kind").noSplit().build())
                        .batch(new PublishRunner())
                        .sink(JdbcSinks.mysql("mysql").table("ex_publish_log").upsertKeys("plan_version", "kind").build())
                        .mode(ExecMode.INLINE))
                .edge("gd-fix.big", "big-net")
                .edge("big-net", "core-grade")
                .gate("algo-callback", Gates.externalCountOf("algo-submit"),
                        g -> g.after("algo-submit").before("gd-fix").timeout(Duration.ofMinutes(10), TimeoutPolicy.FAIL))
                .gate("refurb-ready", Gates.barrier(), g -> g.after("cap-alloc").before("big-final", "fine-net"))
                .gate("conserve-check", Gates.condition(new ConserveCheckGate()), g -> g.after("big-final", "fine-net").before("publish"))
                .gate("approve", Gates.manual(), g -> g.after("big-final", "fine-net").before("publish"))
                .build();
    }
}
