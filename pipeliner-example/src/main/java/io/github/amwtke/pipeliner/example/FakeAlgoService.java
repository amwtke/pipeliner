package io.github.amwtke.pipeliner.example;

import io.github.amwtke.pipeliner.api.DataSourceRegistry;
import io.github.amwtke.pipeliner.api.Row;
import io.github.amwtke.pipeliner.core.engine.PipelinerEngine;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import javax.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/** In-process stand-in for the external gross-demand algorithm: async, writes ClickHouse, then signals the gate. */
@Slf4j
@Component
public class FakeAlgoService {
    private final DataSourceRegistry dataSources;
    private final ObjectProvider<PipelinerEngine> engine;
    private final long delayMs;
    private final ExecutorService executor = Executors.newFixedThreadPool(2, r -> {
        Thread t = new Thread(r, "fake-algo");
        t.setDaemon(true);
        return t;
    });

    public FakeAlgoService(DataSourceRegistry dataSources, ObjectProvider<PipelinerEngine> engine,
                           @Value("${demo.algo-delay-ms:200}") long delayMs) {
        this.dataSources = dataSources;
        this.engine = engine;
        this.delayMs = delayMs;
    }

    /** Deterministic gross demand in [-3, 36] for one material, node, month. */
    public static BigDecimal grossDemand(String material, String node, int month) {
        int h = material.hashCode() * 31 + node.hashCode();
        h = h * 31 + month;
        h ^= h >>> 16; h *= 0x85ebca6b; h ^= h >>> 13; h *= 0xc2b2ae35; h ^= h >>> 16;
        return Demo.q(BigDecimal.valueOf(Math.floorMod(h, 40) - 3));
    }

    /** Asynchronously computes demand for the given material rows and signals gate algo-callback with batchId ({@code <roundId>:<pipelineId>:<chunkId>@<attempt>} from {@code SinkContext.batchId}, globally unique; a real service should dedupe on it). */
    public void submit(String roundId, String planVersion, String batchId, List<Row> materials) {
        executor.submit(() -> {
            try {
                Thread.sleep(delayMs);
                long n = insert(planVersion, materials);
                engine.getObject().signalGate(roundId, "algo-callback", batchId, "rows=" + n);
            } catch (Exception e) {
                log.error("fake algo failed for batch {}", batchId, e);
            }
        });
    }

    private long insert(String planVersion, List<Row> materials) throws Exception {
        String sql = "INSERT INTO " + Demo.DB + ".ex_algo_gd (plan_version, material_id, group_id, kind, node, month, qty) VALUES (?,?,?,?,?,?,?)";
        long n = 0;
        try (Connection c = dataSources.get("ch").getConnection(); PreparedStatement ps = c.prepareStatement(sql)) {
            for (Row m : materials) {
                for (String node : Demo.NODES) {
                    for (int month = 1; month <= Demo.MONTHS; month++) {
                        ps.setString(1, planVersion);
                        ps.setString(2, m.getString("material_id"));
                        ps.setString(3, m.getString("group_id"));
                        ps.setString(4, m.getString("kind"));
                        ps.setString(5, node);
                        ps.setInt(6, month);
                        ps.setBigDecimal(7, grossDemand(m.getString("material_id"), node, month));
                        ps.addBatch();
                        n++;
                    }
                }
            }
            ps.executeBatch();
        }
        return n;
    }

    @PreDestroy
    void shutdown() {
        executor.shutdownNow();
    }
}
