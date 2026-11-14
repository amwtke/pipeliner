package io.github.amwtke.pipeliner.example;

import io.github.amwtke.pipeliner.api.DataSourceRegistry;
import io.github.amwtke.pipeliner.api.Row;
import io.github.amwtke.pipeliner.core.engine.PipelinerEngine;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
 * In-process stand-in for the external gross-demand algorithm. The request is recorded durably (MySQL {@code ex_algo_request}, batch id as
 * primary key, INSERT IGNORE) before {@link #submit} returns, so a pod kill cannot lose it. Every pod runs background pollers that claim
 * pending requests (atomic UPDATE; a claim older than {@code demo.algo-claim-timeout-ms} is considered abandoned), write the results to
 * ClickHouse (ReplacingMergeTree: replays collapse) and signal gate algo-callback (idempotent per batch id), then mark the request done.
 */
@Slf4j
@Component
public class FakeAlgoService {
    /** DDL of the durable request table (also recreated by {@link Seeder}). */
    static final String REQUEST_DDL = "create table if not exists ex_algo_request(batch_id varchar(512) primary key, round_id varchar(255) not null, "
            + "plan_version varchar(64) not null, payload mediumtext not null, status tinyint not null default 0, claimed_by varchar(64), "
            + "claimed_at bigint not null default 0, created_at bigint not null, key idx_algo_status(status, created_at))";
    private static final int PENDING = 0, CLAIMED = 1, DONE = 2;

    private final DataSourceRegistry dataSources;
    private final ObjectProvider<PipelinerEngine> engine;
    private final long delayMs;
    private final long claimTimeoutMs;
    private final String owner = "algo-" + UUID.randomUUID().toString().substring(0, 8);
    private final ScheduledExecutorService pollers = Executors.newScheduledThreadPool(2, r -> {
        Thread t = new Thread(r, "fake-algo");
        t.setDaemon(true);
        return t;
    });

    public FakeAlgoService(DataSourceRegistry dataSources, ObjectProvider<PipelinerEngine> engine,
                           @Value("${demo.algo-delay-ms:200}") long delayMs,
                           @Value("${demo.algo-claim-timeout-ms:10000}") long claimTimeoutMs) {
        this.dataSources = dataSources;
        this.engine = engine;
        this.delayMs = delayMs;
        this.claimTimeoutMs = claimTimeoutMs;
    }

    @PostConstruct
    void start() {
        for (int i = 0; i < 2; i++) pollers.scheduleWithFixedDelay(this::pollSafely, 300 + 100L * i, 200, TimeUnit.MILLISECONDS);
    }

    /** Deterministic gross demand in [-3, 36] for one material, node, month. */
    public static BigDecimal grossDemand(String material, String node, int month) {
        int h = material.hashCode() * 31 + node.hashCode();
        h = h * 31 + month;
        h ^= h >>> 16; h *= 0x85ebca6b; h ^= h >>> 13; h *= 0xc2b2ae35; h ^= h >>> 16;
        return Demo.q(BigDecimal.valueOf(Math.floorMod(h, 40) - 3));
    }

    /**
     * Durably records the request, then returns; the result is computed asynchronously by whichever pod claims it and gate algo-callback is
     * signalled with batchId ({@code <roundId>:<pipelineId>:<chunkId>@<attempt>} from {@code SinkContext.batchId}, globally unique). Idempotent per batch id.
     * Throws when the request cannot be recorded, so the chunk fails and is retried instead of losing the callback.
     */
    public void submit(String roundId, String planVersion, String batchId, List<Row> materials) {
        StringBuilder payload = new StringBuilder();
        for (Row m : materials) payload.append(m.getString("material_id")).append(',').append(m.getString("group_id")).append(',')
                .append(m.getString("kind")).append('\n');
        String sql = "INSERT IGNORE INTO ex_algo_request (batch_id, round_id, plan_version, payload, status, created_at) VALUES (?,?,?,?,0,?)";
        try {
            try {
                insertRequest(sql, roundId, planVersion, batchId, payload.toString());
            } catch (SQLException e) {
                if (!isMissingTable(e)) throw e;
                ensureTable();
                insertRequest(sql, roundId, planVersion, batchId, payload.toString());
            }
        } catch (SQLException e) {
            throw new IllegalStateException("cannot record algo request " + batchId, e);
        }
    }

    private void insertRequest(String sql, String roundId, String planVersion, String batchId, String payload) throws SQLException {
        try (Connection c = dataSources.get("mysql").getConnection(); PreparedStatement ps = c.prepareStatement(sql)) {
            ps.setString(1, batchId);
            ps.setString(2, roundId);
            ps.setString(3, planVersion);
            ps.setString(4, payload);
            ps.setLong(5, System.currentTimeMillis());
            ps.executeUpdate();
        }
    }

    private static boolean isMissingTable(SQLException e) { return e.getErrorCode() == 1146 || "42S02".equals(e.getSQLState()); }

    private void ensureTable() throws SQLException {
        try (Connection c = dataSources.get("mysql").getConnection(); Statement st = c.createStatement()) {
            st.execute(REQUEST_DDL);
        }
    }

    private void pollSafely() {
        try {
            poll();
        } catch (SQLException e) {
            if (isMissingTable(e)) {
                try { ensureTable(); } catch (SQLException ex) { log.debug("fake algo: cannot create request table: {}", ex.toString()); }
            } else {
                log.warn("fake algo poll failed: {}", e.toString());
            }
        } catch (Throwable t) {
            log.warn("fake algo poll failed", t);
        }
    }

    /** Claims and completes due requests (pending, or claimed longer than the claim timeout ago by a pod that died). */
    void poll() throws SQLException {
        long now = System.currentTimeMillis();
        List<String> due = new ArrayList<>();
        try (Connection c = dataSources.get("mysql").getConnection();
             PreparedStatement ps = c.prepareStatement("SELECT batch_id FROM ex_algo_request WHERE (status=? AND created_at<=?) OR (status=? AND claimed_at<?) "
                     + "ORDER BY created_at LIMIT 8")) {
            ps.setInt(1, PENDING); ps.setLong(2, now - delayMs); ps.setInt(3, CLAIMED); ps.setLong(4, now - claimTimeoutMs);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) due.add(rs.getString(1));
            }
        }
        for (String id : due) {
            String[] req = claim(id, now);
            if (req == null) continue;   // another pod won the claim
            try {
                long n = insert(req[1], req[2]);
                engine.getObject().signalGate(req[0], "algo-callback", id, "rows=" + n);
                update("UPDATE ex_algo_request SET status=? WHERE batch_id=?", DONE, id);
            } catch (Exception e) {
                log.warn("fake algo failed for batch {} (released for retry): {}", id, e.toString());
                try {
                    update("UPDATE ex_algo_request SET status=?, claimed_at=0 WHERE batch_id=? AND claimed_by=?", PENDING, id, owner);
                } catch (SQLException ex) {
                    log.warn("fake algo could not release batch {}: {}", id, ex.toString());
                }
            }
        }
    }

    /** Atomically takes a request; returns {roundId, planVersion, payload} or null when someone else holds it. */
    private String[] claim(String id, long now) throws SQLException {
        try (Connection c = dataSources.get("mysql").getConnection()) {
            try (PreparedStatement ps = c.prepareStatement("UPDATE ex_algo_request SET status=?, claimed_by=?, claimed_at=? "
                    + "WHERE batch_id=? AND (status=? OR (status=? AND claimed_at<?))")) {
                ps.setInt(1, CLAIMED); ps.setString(2, owner); ps.setLong(3, now); ps.setString(4, id);
                ps.setInt(5, PENDING); ps.setInt(6, CLAIMED); ps.setLong(7, now - claimTimeoutMs);
                if (ps.executeUpdate() != 1) return null;
            }
            try (PreparedStatement ps = c.prepareStatement("SELECT round_id, plan_version, payload FROM ex_algo_request WHERE batch_id=?")) {
                ps.setString(1, id);
                try (ResultSet rs = ps.executeQuery()) {
                    return rs.next() ? new String[] {rs.getString(1), rs.getString(2), rs.getString(3)} : null;
                }
            }
        }
    }

    private void update(String sql, Object... args) throws SQLException {
        try (Connection c = dataSources.get("mysql").getConnection(); PreparedStatement ps = c.prepareStatement(sql)) {
            for (int i = 0; i < args.length; i++) ps.setObject(i + 1, args[i]);
            ps.executeUpdate();
        }
    }

    private long insert(String planVersion, String payload) throws Exception {
        String sql = "INSERT INTO " + Demo.DB + ".ex_algo_gd (plan_version, material_id, group_id, kind, node, month, qty) VALUES (?,?,?,?,?,?,?)";
        long n = 0;
        try (Connection c = dataSources.get("ch").getConnection(); PreparedStatement ps = c.prepareStatement(sql)) {
            for (String line : payload.split("\n")) {
                if (line.isEmpty()) continue;
                String[] m = line.split(",");
                for (String node : Demo.NODES) {
                    for (int month = 1; month <= Demo.MONTHS; month++) {
                        ps.setString(1, planVersion);
                        ps.setString(2, m[0]);
                        ps.setString(3, m[1]);
                        ps.setString(4, m[2]);
                        ps.setString(5, node);
                        ps.setInt(6, month);
                        ps.setBigDecimal(7, grossDemand(m[0], node, month));
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
        pollers.shutdownNow();   // pending requests stay in MySQL; any pod's poller (including this one after restart) completes them
    }
}
