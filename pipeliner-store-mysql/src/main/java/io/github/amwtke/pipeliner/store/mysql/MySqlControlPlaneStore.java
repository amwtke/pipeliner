package io.github.amwtke.pipeliner.store.mysql;

import io.github.amwtke.pipeliner.core.codec.ChunkCodec;
import io.github.amwtke.pipeliner.core.graph.ExecMode;
import io.github.amwtke.pipeliner.core.graph.GateType;
import io.github.amwtke.pipeliner.core.store.*;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLIntegrityConstraintViolationException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import javax.sql.DataSource;
import lombok.extern.slf4j.Slf4j;

/** MySQL control plane store using plain JDBC; times are BIGINT epoch millis. */
@Slf4j
public class MySqlControlPlaneStore implements ControlPlaneStore {
    private static final String TBL = " ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin";
    private static final String R_COLS = "round_id,graph_name,biz_key,plan_hash,plan_json,params_json,status,master_epoch,journal_offset,"
            + "created_at,started_at,finished_at,error,incomplete,seq,compacted";

    private final DataSource ds;

    public MySqlControlPlaneStore(DataSource ds) { this.ds = ds; }

    // ---------------------------------------------------------------- helpers

    @FunctionalInterface private interface Work<T> { T run(Connection c) throws SQLException; }

    /** Runs the work in one transaction; rolls back on ANY Throwable (an Error must not be committed by the autocommit restore). */
    private <T> T tx(Work<T> w) {
        try (Connection c = ds.getConnection()) {
            boolean ac = c.getAutoCommit();
            c.setAutoCommit(false);
            try {
                T r = w.run(c);
                c.commit();
                return r;
            } catch (Throwable e) {
                try { c.rollback(); } catch (SQLException ignore) { /* best effort */ }
                throw e;
            } finally {
                try { c.setAutoCommit(ac); } catch (SQLException ignore) { /* best effort */ }
            }
        } catch (SQLException e) {
            throw new IllegalStateException("store failure: " + e.getMessage(), e);
        }
    }

    /**
     * Single-statement work on a pooled connection that may have autocommit off (a shared application DataSource): commits explicitly in
     * that case, so a write is never silently rolled back when the connection goes back to the pool, and a read never holds a stale snapshot.
     */
    private <T> T withConn(Work<T> w) {
        try (Connection c = ds.getConnection()) {
            boolean ac = c.getAutoCommit();
            try {
                T r = w.run(c);
                if (!ac) c.commit();
                return r;
            } catch (Throwable e) {
                if (!ac) { try { c.rollback(); } catch (SQLException ignore) { /* best effort */ } }
                throw e;
            }
        } catch (SQLException e) {
            throw new IllegalStateException("store failure: " + e.getMessage(), e);
        }
    }

    private static void bind(PreparedStatement ps, Object... args) throws SQLException {
        for (int i = 0; i < args.length; i++) ps.setObject(i + 1, args[i]);
    }

    private static int exec(Connection c, String sql, Object... args) throws SQLException {
        try (PreparedStatement ps = c.prepareStatement(sql)) {
            bind(ps, args);
            return ps.executeUpdate();
        }
    }

    private int update(String sql, Object... args) { return withConn(c -> exec(c, sql, args)); }

    private static boolean exists(Connection c, String sql, Object... args) throws SQLException {
        try (PreparedStatement ps = c.prepareStatement(sql)) {
            bind(ps, args);
            try (ResultSet rs = ps.executeQuery()) { return rs.next(); }
        }
    }

    @FunctionalInterface private interface RowMap<T> { T map(ResultSet rs) throws SQLException; }

    private <T> List<T> query(String sql, RowMap<T> m, Object... args) {
        return withConn(c -> {
            try (PreparedStatement ps = c.prepareStatement(sql)) {
                bind(ps, args);
                try (ResultSet rs = ps.executeQuery()) {
                    List<T> out = new ArrayList<>();
                    while (rs.next()) out.add(m.map(rs));
                    return out;
                }
            }
        });
    }

    private static Long lng(ResultSet rs, String col) throws SQLException {
        long v = rs.getLong(col);
        return rs.wasNull() ? null : v;
    }

    private static String json(Object o) { return StoreViews.json(o); }

    private static Map<String, String> strMap(String j) { return j == null ? null : ChunkCodec.toStringMap(j); }

    private static Map<String, Long> longMap(String j) { return StoreViews.longMap(j); }

    @SuppressWarnings("unchecked")
    private static List<String> strList(String j) { return j == null ? null : (List<String>) ChunkCodec.fromJson(j, List.class); }

    private static long sinkRowsOf(Map<String, Long> m) { return StoreViews.sinkRowsOf(m); }

    private static long totalOf(Map<String, Long> m) { return StoreViews.totalOf(m); }

    /** SQL IN-lists generated from the status enums (single source of truth). */
    private static String in(List<String> names) {
        StringBuilder sb = new StringBuilder("(");
        for (String n : names) sb.append(sb.length() > 1 ? "," : "").append('\'').append(n).append('\'');
        return sb.append(')').toString();
    }

    private static final String ACTIVE_IN = in(RoundStatus.activeNames());
    private static final String TERMINAL_IN = in(RoundStatus.terminalNames());
    private static final String REVIVABLE_IN = in(RoundStatus.revivableNames());

    // ---------------------------------------------------------------- schema

    @Override
    public void init() {
        String[] ddl = {
            "CREATE TABLE IF NOT EXISTS pg_round (round_id VARCHAR(96) NOT NULL, graph_name VARCHAR(128) NOT NULL, biz_key VARCHAR(190) NOT NULL,"
                + " plan_hash VARCHAR(64), plan_json MEDIUMTEXT, params_json TEXT, status VARCHAR(16) NOT NULL, master_epoch BIGINT NOT NULL DEFAULT 0,"
                + " journal_offset VARCHAR(64) NOT NULL DEFAULT '0-0', created_at BIGINT, started_at BIGINT, finished_at BIGINT, error TEXT,"
                + " incomplete TINYINT(1) NOT NULL DEFAULT 0, seq BIGINT NOT NULL DEFAULT 0, PRIMARY KEY (round_id), UNIQUE KEY uk_biz (graph_name, biz_key), KEY idx_status (status))" + TBL,
            "CREATE TABLE IF NOT EXISTS pg_run (round_id VARCHAR(96) NOT NULL, run_id VARCHAR(64) NOT NULL, stage INT NOT NULL DEFAULT 0,"
                + " mode VARCHAR(16), status VARCHAR(16) NOT NULL, attempt INT NOT NULL DEFAULT 0, assignee VARCHAR(128), master_epoch BIGINT NOT NULL DEFAULT 0,"
                + " started_at BIGINT, finished_at BIGINT, error TEXT, PRIMARY KEY (round_id, run_id))" + TBL,
            "CREATE TABLE IF NOT EXISTS pg_gate (round_id VARCHAR(96) NOT NULL, gate_id VARCHAR(128) NOT NULL, type VARCHAR(16), before_runs TEXT,"
                + " status VARCHAR(16) NOT NULL, armed_at BIGINT, deadline_at BIGINT, opened_by VARCHAR(128), opened_at BIGINT, context_json TEXT,"
                + " master_epoch BIGINT NOT NULL DEFAULT 0, alerted TINYINT(1) NOT NULL DEFAULT 0, PRIMARY KEY (round_id, gate_id))" + TBL,
            "CREATE TABLE IF NOT EXISTS pg_gate_signal (round_id VARCHAR(96) NOT NULL, gate_id VARCHAR(128) NOT NULL, signal_key VARCHAR(512) NOT NULL,"
                + " payload MEDIUMTEXT, created_at BIGINT, PRIMARY KEY (round_id, gate_id, signal_key))" + TBL,
            "CREATE TABLE IF NOT EXISTS pg_pipeline (round_id VARCHAR(96) NOT NULL, pipeline_id VARCHAR(128) NOT NULL, run_id VARCHAR(64) NOT NULL,"
                + " status VARCHAR(16) NOT NULL, parallelism INT NOT NULL DEFAULT 1, enum_done TINYINT(1) NOT NULL DEFAULT 0, master_epoch BIGINT NOT NULL DEFAULT 0,"
                + " started_at BIGINT, finished_at BIGINT, error TEXT, PRIMARY KEY (round_id, pipeline_id), KEY idx_run (round_id, run_id))" + TBL,
            "CREATE TABLE IF NOT EXISTS pg_chunk (round_id VARCHAR(96) NOT NULL, pipeline_id VARCHAR(128) NOT NULL, chunk_id VARCHAR(255) NOT NULL,"
                + " run_id VARCHAR(64) NOT NULL, status VARCHAR(16) NOT NULL, attempt INT NOT NULL DEFAULT 0, owner VARCHAR(128), input_edge VARCHAR(64),"
                + " split_spec TEXT, rows_in BIGINT, rows_out_json TEXT, rows_out_total BIGINT, sink_rows BIGINT NOT NULL DEFAULT 0, cost_ms INT, error TEXT, updated_at BIGINT,"
                + " PRIMARY KEY (round_id, pipeline_id, chunk_id), KEY idx_run_status (round_id, run_id, status))" + TBL,
            // single row (id=1): the highest master epoch ever reserved (reserveMasterEpoch)
            "CREATE TABLE IF NOT EXISTS pg_master_epoch (id TINYINT NOT NULL, max_epoch BIGINT NOT NULL DEFAULT 0, PRIMARY KEY (id))" + TBL
        };
        try (Connection c = ds.getConnection(); Statement st = c.createStatement()) {
            for (String s : ddl) st.execute(s);
            // tables created before a column/index existed; 1060 duplicate column, 1061 duplicate key name
            alter(st, "ALTER TABLE pg_round ADD COLUMN seq BIGINT NOT NULL DEFAULT 0");
            alter(st, "ALTER TABLE pg_round ADD COLUMN compacted TINYINT(1) NOT NULL DEFAULT 0");
            alter(st, "ALTER TABLE pg_round ADD KEY idx_status_finished (status, finished_at)");
            alter(st, "ALTER TABLE pg_round ADD KEY idx_graph_created (graph_name, created_at)");
            alter(st, "ALTER TABLE pg_round ADD KEY idx_created (created_at)");
            alter(st, "ALTER TABLE pg_round ADD KEY idx_master_epoch (master_epoch)");   // MAX(master_epoch) = index lookup
            alter(st, "ALTER TABLE pg_round ADD KEY idx_seq (seq)");   // MAX(seq) = index lookup
            alter(st, "ALTER TABLE pg_chunk ADD COLUMN sink_rows BIGINT NOT NULL DEFAULT 0");   // rows written to sinks (0 for chunks stored before the column existed)
            st.execute("INSERT IGNORE INTO pg_master_epoch (id, max_epoch) VALUES (1, 0)");
            widenColumns(c);
            alter(st, "ALTER TABLE pg_pipeline ADD COLUMN sum_counts TEXT");
            alter(st, "ALTER TABLE pg_pipeline ADD COLUMN sum_rows_in BIGINT");
            alter(st, "ALTER TABLE pg_pipeline ADD COLUMN sum_rows_out BIGINT");
        } catch (SQLException e) {
            throw new IllegalStateException("init failed: " + e.getMessage(), e);
        }
    }

    /**
     * Column widths (characters) every table must have; tables created by older versions are widened in place on init. Sized for
     * {@link ControlPlaneStore#MAX_ROUND_ID_LENGTH} (graph name + round id suffix), {@link ControlPlaneStore#MAX_GATE_ID_LENGTH} and
     * {@link ControlPlaneStore#MAX_PIPELINE_ID_LENGTH}. Primary keys under utf8mb4 (4 bytes per character), InnoDB limit 3072 bytes:
     * pg_gate_signal = round_id 96 + gate_id 128 + signal_key 512 = 736 characters = 2944 bytes; pg_chunk = 96 + 128 + 255 = 479 = 1916 bytes.
     */
    private static final Object[][] WIDTHS = {
        {"pg_round", "round_id", 96}, {"pg_run", "round_id", 96}, {"pg_gate", "round_id", 96}, {"pg_gate_signal", "round_id", 96},
        {"pg_pipeline", "round_id", 96}, {"pg_chunk", "round_id", 96},
        {"pg_gate", "gate_id", 128}, {"pg_gate_signal", "gate_id", 128},
        {"pg_pipeline", "pipeline_id", 128}, {"pg_chunk", "pipeline_id", 128},
        {"pg_gate_signal", "signal_key", MAX_SIGNAL_KEY_LENGTH},
    };

    private static void widenColumns(Connection c) throws SQLException {
        for (Object[] w : WIDTHS) {
            String table = (String) w[0], col = (String) w[1];
            int want = (Integer) w[2];
            int len = 0;
            try (PreparedStatement ps = c.prepareStatement("SELECT CHARACTER_MAXIMUM_LENGTH FROM information_schema.COLUMNS "
                    + "WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME=? AND COLUMN_NAME=?")) {
                ps.setString(1, table);
                ps.setString(2, col);
                try (ResultSet rs = ps.executeQuery()) { if (rs.next()) len = rs.getInt(1); }
            }
            if (len > 0 && len < want) {
                try (Statement st = c.createStatement()) { st.execute("ALTER TABLE " + table + " MODIFY " + col + " VARCHAR(" + want + ") NOT NULL"); }
            }
        }
    }

    private static void alter(Statement st, String sql) throws SQLException {
        try {
            st.execute(sql);
        } catch (SQLException e) {
            if (e.getErrorCode() != 1060 && e.getErrorCode() != 1061) throw e;
        }
    }

    // ---------------------------------------------------------------- rounds

    private static RoundRecord mapRound(ResultSet rs) throws SQLException {
        return RoundRecord.builder().roundId(rs.getString("round_id")).graphName(rs.getString("graph_name")).bizKey(rs.getString("biz_key"))
                .planHash(rs.getString("plan_hash")).planJson(rs.getString("plan_json")).params(strMap(rs.getString("params_json")))
                .status(RoundStatus.valueOf(rs.getString("status"))).masterEpoch(rs.getLong("master_epoch"))
                .journalOffset(rs.getString("journal_offset")).createdAt(lng(rs, "created_at")).startedAt(lng(rs, "started_at"))
                .finishedAt(lng(rs, "finished_at")).error(rs.getString("error")).incomplete(rs.getBoolean("incomplete")).seq(rs.getLong("seq")).compacted(rs.getBoolean("compacted")).build();
    }

    private static RunRecord mapRun(ResultSet rs) throws SQLException {
        String mode = rs.getString("mode");
        return RunRecord.builder().roundId(rs.getString("round_id")).runId(rs.getString("run_id")).stage(rs.getInt("stage"))
                .mode(mode == null ? null : ExecMode.valueOf(mode)).status(RunStatus.valueOf(rs.getString("status"))).attempt(rs.getInt("attempt"))
                .assignee(rs.getString("assignee")).masterEpoch(rs.getLong("master_epoch")).startedAt(lng(rs, "started_at"))
                .finishedAt(lng(rs, "finished_at")).error(rs.getString("error")).build();
    }

    private static GateRecord mapGate(ResultSet rs) throws SQLException {
        String type = rs.getString("type");
        return GateRecord.builder().roundId(rs.getString("round_id")).gateId(rs.getString("gate_id")).type(type == null ? null : GateType.valueOf(type))
                .beforeRuns(strList(rs.getString("before_runs"))).status(GateStatus.valueOf(rs.getString("status"))).armedAt(lng(rs, "armed_at"))
                .deadlineAt(lng(rs, "deadline_at")).openedBy(rs.getString("opened_by")).openedAt(lng(rs, "opened_at"))
                .context(strMap(rs.getString("context_json"))).masterEpoch(rs.getLong("master_epoch")).alerted(rs.getBoolean("alerted")).build();
    }

    private static PipelineRecord mapPipeline(ResultSet rs) throws SQLException {
        return PipelineRecord.builder().roundId(rs.getString("round_id")).pipelineId(rs.getString("pipeline_id")).runId(rs.getString("run_id"))
                .status(PipelineStatus.valueOf(rs.getString("status"))).parallelism(rs.getInt("parallelism")).enumDone(rs.getBoolean("enum_done"))
                .masterEpoch(rs.getLong("master_epoch")).startedAt(lng(rs, "started_at")).finishedAt(lng(rs, "finished_at")).error(rs.getString("error")).summaryCounts(longMap(rs.getString("sum_counts")))
                .summaryRowsIn(lng(rs, "sum_rows_in")).summaryRowsOut(lng(rs, "sum_rows_out")).build();
    }

    private static ChunkRecord mapChunk(ResultSet rs) throws SQLException {
        int cost = rs.getInt("cost_ms");
        Integer costMs = rs.wasNull() ? null : cost;
        return ChunkRecord.builder().roundId(rs.getString("round_id")).pipelineId(rs.getString("pipeline_id")).chunkId(rs.getString("chunk_id"))
                .runId(rs.getString("run_id")).status(ChunkStatus.valueOf(rs.getString("status"))).attempt(rs.getInt("attempt")).owner(rs.getString("owner"))
                .inputEdge(rs.getString("input_edge")).splitSpec(rs.getString("split_spec")).rowsIn(lng(rs, "rows_in"))
                .rowsOut(longMap(rs.getString("rows_out_json"))).costMs(costMs).error(rs.getString("error")).updatedAt(lng(rs, "updated_at")).build();
    }

    @Override
    public RoundRecord createRound(RoundRecord r, List<RunRecord> runs, List<GateRecord> gates, List<PipelineRecord> pipelines) {
        RoundRecord existing = findRoundByBizKey(r.getGraphName(), r.getBizKey());
        if (existing != null) return existing;
        if (r.getCreatedAt() == null) r.setCreatedAt(System.currentTimeMillis());
        if (r.getStatus() == null) r.setStatus(RoundStatus.CREATED);
        try {
            tx(c -> {
                exec(c, "INSERT INTO pg_round (" + R_COLS + ") VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", r.getRoundId(), r.getGraphName(), r.getBizKey(),
                        r.getPlanHash(), r.getPlanJson(), json(r.getParams()), r.getStatus().name(), r.getMasterEpoch(),
                        r.getJournalOffset() == null ? "0-0" : r.getJournalOffset(), r.getCreatedAt(), r.getStartedAt(), r.getFinishedAt(), r.getError(),
                        r.isIncomplete(), r.getSeq(), r.isCompacted());
                for (RunRecord u : runs == null ? Collections.<RunRecord>emptyList() : runs)
                    exec(c, "INSERT INTO pg_run (round_id,run_id,stage,mode,status,attempt,assignee,master_epoch,started_at,finished_at,error) VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                            r.getRoundId(), u.getRunId(), u.getStage(), u.getMode() == null ? null : u.getMode().name(),
                            u.getStatus() == null ? RunStatus.PENDING.name() : u.getStatus().name(), u.getAttempt(), u.getAssignee(), u.getMasterEpoch(),
                            u.getStartedAt(), u.getFinishedAt(), u.getError());
                for (GateRecord g : gates == null ? Collections.<GateRecord>emptyList() : gates)
                    exec(c, "INSERT INTO pg_gate (round_id,gate_id,type,before_runs,status,armed_at,deadline_at,opened_by,opened_at,context_json,master_epoch,alerted) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
                            r.getRoundId(), g.getGateId(), g.getType() == null ? null : g.getType().name(), json(g.getBeforeRuns()),
                            g.getStatus() == null ? GateStatus.CLOSED.name() : g.getStatus().name(), g.getArmedAt(), g.getDeadlineAt(), g.getOpenedBy(),
                            g.getOpenedAt(), json(g.getContext()), g.getMasterEpoch(), g.isAlerted());
                for (PipelineRecord p : pipelines == null ? Collections.<PipelineRecord>emptyList() : pipelines)
                    exec(c, "INSERT INTO pg_pipeline (round_id,pipeline_id,run_id,status,parallelism,enum_done,master_epoch,started_at,finished_at,error) VALUES (?,?,?,?,?,?,?,?,?,?)",
                            r.getRoundId(), p.getPipelineId(), p.getRunId(), p.getStatus() == null ? PipelineStatus.PENDING.name() : p.getStatus().name(),
                            p.getParallelism(), p.isEnumDone(), p.getMasterEpoch(), p.getStartedAt(), p.getFinishedAt(), p.getError());
                return null;
            });
            return r;
        } catch (RuntimeException e) {
            if (isDuplicate(e)) {
                RoundRecord again = findRoundByBizKey(r.getGraphName(), r.getBizKey());
                if (again != null) return again;
                if (loadRoundRecord(r.getRoundId()) != null) throw new DuplicateRoundIdException(r.getRoundId());   // same id, other biz key
            }
            throw e;
        }
    }

    private static boolean isDuplicate(Throwable e) {
        for (Throwable t = e; t != null; t = t.getCause())
            if (t instanceof SQLIntegrityConstraintViolationException || (t instanceof SQLException && ((SQLException) t).getErrorCode() == 1062)) return true;
        return false;
    }

    @Override
    public RoundRecord findRoundByBizKey(String graphName, String bizKey) {
        List<RoundRecord> l = query("SELECT " + R_COLS_LIGHT + " FROM pg_round WHERE graph_name=? AND biz_key=?", MySqlControlPlaneStore::mapRound, graphName, bizKey);
        return l.isEmpty() ? null : l.get(0);
    }

    @Override
    public RoundRecord loadRoundRecord(String roundId) {
        List<RoundRecord> l = query("SELECT " + R_COLS_LIGHT + " FROM pg_round WHERE round_id=?", MySqlControlPlaneStore::mapRound, roundId);
        return l.isEmpty() ? null : l.get(0);
    }

    @Override
    public RoundView loadRound(String roundId) {
        List<RoundRecord> rl = query("SELECT " + R_COLS + " FROM pg_round WHERE round_id=?", MySqlControlPlaneStore::mapRound, roundId);
        if (rl.isEmpty()) return null;
        RoundRecord round = rl.get(0);
        List<RunRecord> runs = query("SELECT * FROM pg_run WHERE round_id=? ORDER BY stage, run_id", MySqlControlPlaneStore::mapRun, roundId);
        List<GateRecord> gates = query("SELECT * FROM pg_gate WHERE round_id=? ORDER BY gate_id", MySqlControlPlaneStore::mapGate, roundId);
        List<PipelineRecord> pls = query("SELECT * FROM pg_pipeline WHERE round_id=? ORDER BY pipeline_id", MySqlControlPlaneStore::mapPipeline, roundId);
        Map<String, Map<ChunkStatus, Long>> counts = new LinkedHashMap<>();
        for (Object[] row : query("SELECT pipeline_id, status, COUNT(*) FROM pg_chunk WHERE round_id=? GROUP BY pipeline_id, status",
                rs -> new Object[]{rs.getString(1), rs.getString(2), rs.getLong(3)}, roundId))
            counts.computeIfAbsent((String) row[0], k -> new EnumMap<>(ChunkStatus.class)).put(ChunkStatus.valueOf((String) row[1]), (Long) row[2]);
        Map<String, Long> rowsIn = new LinkedHashMap<>();
        Map<String, Long> rowsOut = new LinkedHashMap<>();
        for (Object[] row : query("SELECT pipeline_id, COALESCE(SUM(rows_in),0), COALESCE(SUM(rows_out_total),0) FROM pg_chunk WHERE round_id=? AND status='DONE' GROUP BY pipeline_id",
                rs -> new Object[]{rs.getString(1), rs.getLong(2), rs.getLong(3)}, roundId)) {
            rowsIn.put((String) row[0], (Long) row[1]);
            rowsOut.put((String) row[0], (Long) row[2]);
        }
        StoreViews.mergeSummaries(pls, counts, rowsIn, rowsOut);   // compacted pipelines: chunk rows are gone, the summary stays
        return RoundView.builder().round(round).runs(runs).gates(gates).pipelines(pls).chunkCounts(counts).rowsIn(rowsIn).rowsOut(rowsOut).build();
    }

    @Override
    public RoundView loadRoundState(String roundId) {
        RoundRecord round = loadRoundRecord(roundId);
        if (round == null) return null;
        List<RunRecord> runs = query("SELECT * FROM pg_run WHERE round_id=? ORDER BY stage, run_id", MySqlControlPlaneStore::mapRun, roundId);
        List<GateRecord> gates = query("SELECT * FROM pg_gate WHERE round_id=? ORDER BY gate_id", MySqlControlPlaneStore::mapGate, roundId);
        List<PipelineRecord> pls = query("SELECT * FROM pg_pipeline WHERE round_id=? ORDER BY pipeline_id", MySqlControlPlaneStore::mapPipeline, roundId);
        return RoundView.builder().round(round).runs(runs).gates(gates).pipelines(pls).chunkCounts(new LinkedHashMap<String, Map<ChunkStatus, Long>>())
                .rowsIn(new LinkedHashMap<String, Long>()).rowsOut(new LinkedHashMap<String, Long>()).build();
    }

    @Override
    public boolean fenceRound(String roundId, long epoch) {
        return withConn(c -> fenceUpdate(c, "UPDATE pg_round SET master_epoch=? WHERE round_id=? AND master_epoch<=?", roundId, epoch, epoch, roundId, epoch));
    }

    @Override
    public long maxMasterEpoch() {
        List<Long> r = query("SELECT GREATEST(COALESCE((SELECT max_epoch FROM pg_master_epoch WHERE id=1),0), COALESCE((SELECT MAX(master_epoch) FROM pg_round),0))",
                rs -> rs.getLong(1));
        return r.isEmpty() ? 0L : r.get(0);
    }

    @Override
    public boolean reserveMasterEpoch(long epoch) {
        // matched rows == changed rows here (the WHERE only matches when the value rises); above every round's epoch too
        return update("UPDATE pg_master_epoch SET max_epoch=? WHERE id=1 AND max_epoch<? AND ? > (SELECT COALESCE(MAX(master_epoch),0) FROM pg_round)",
                epoch, epoch, epoch) == 1;
    }

    @Override
    public long maxRoundSeq() {
        List<Long> r = query("SELECT COALESCE(MAX(seq),0) FROM pg_round", rs -> rs.getLong(1));
        return r.isEmpty() ? 0L : r.get(0);
    }

    @Override
    public List<String> findActiveRounds() {
        return query("SELECT round_id FROM pg_round WHERE status IN " + ACTIVE_IN + " ORDER BY created_at, round_id", rs -> rs.getString(1));
    }

    private static final String R_COLS_LIGHT = "round_id,graph_name,biz_key,plan_hash,NULL AS plan_json,params_json,status,master_epoch,journal_offset,"
            + "created_at,started_at,finished_at,error,incomplete,seq,compacted";

    @Override
    public List<RoundRecord> listRecentRounds(String graphName, int limit) {
        List<RoundRecord> out = new ArrayList<>();
        if (limit <= 0) return out;
        boolean all = graphName == null || graphName.trim().isEmpty();
        String g = all ? "" : " AND graph_name=?";
        Object[] ga = all ? new Object[0] : new Object[]{graphName};
        // active first (idx_status), then the newest of the rest (idx_graph_created / idx_created)
        List<RoundRecord> active = query("SELECT " + R_COLS_LIGHT + " FROM pg_round WHERE status IN " + ACTIVE_IN + g
                + " ORDER BY created_at DESC, round_id DESC LIMIT ?", MySqlControlPlaneStore::mapRound, append(ga, limit));
        out.addAll(active);
        if (out.size() >= limit) return out;
        List<RoundRecord> rest = query("SELECT " + R_COLS_LIGHT + " FROM pg_round WHERE status NOT IN " + ACTIVE_IN + g
                + " ORDER BY created_at DESC, round_id DESC LIMIT ?", MySqlControlPlaneStore::mapRound, append(ga, limit - out.size()));
        out.addAll(rest);
        return out;
    }

    private static Object[] append(Object[] a, Object x) {
        Object[] r = java.util.Arrays.copyOf(a, a.length + 1);
        r[a.length] = x;
        return r;
    }

    /**
     * Runs a round-row UPDATE that bumps master_epoch and returns whether the round is NOT fenced. An update count of 0 is ambiguous (fenced,
     * missing, or - with useAffectedRows=true - matched but unchanged because the epoch was already stored), so 0 is settled by a SELECT.
     */
    private static boolean fenceUpdate(Connection c, String sql, String roundId, long epoch, Object... args) throws SQLException {
        if (exec(c, sql, args) > 0) return true;
        return exists(c, "SELECT 1 FROM pg_round WHERE round_id=? AND master_epoch<=?", roundId, epoch);
    }

    /** Round-level epoch fence appended to every master UPDATE of a non-round table; the share lock serialises against fenceRound. */
    private static final String FENCE = " AND (SELECT master_epoch FROM pg_round WHERE round_id=? LOCK IN SHARE MODE) <= ?";

    /**
     * Generic fenced status CAS; sets started/finished stamps on the given transitions. A FAILED to RUNNING (revive) clears error and
     * finished_at, any other transition keeps the stored error when none is given. An update count of 0 is settled by a SELECT (it may
     * only mean "matched but unchanged" under useAffectedRows=true).
     */
    private boolean cas(String table, String where, Object[] keys, String expect, String target, String error, long epoch, boolean toRunning, boolean terminal) {
        long now = System.currentTimeMillis();
        boolean revive = FAILED.equals(expect) && RUNNING.equals(target);
        StringBuilder sb = new StringBuilder("UPDATE " + table + " SET status=?, master_epoch=?, error=" + (revive ? "?" : "COALESCE(?,error)"));
        List<Object> args = new ArrayList<>();
        args.add(target); args.add(epoch); args.add(error);
        if (toRunning) { sb.append(", started_at=COALESCE(started_at,?)"); args.add(now); }
        if (revive) sb.append(", finished_at=NULL");
        if (terminal) { sb.append(", finished_at=?"); args.add(now); }
        List<Object> match = new ArrayList<>();
        StringBuilder cond = new StringBuilder(" WHERE ").append(where).append(" AND status=? AND master_epoch<=?");
        for (Object k : keys) match.add(k);
        match.add(expect); match.add(epoch);
        if (!table.equals("pg_round")) { cond.append(FENCE); match.add(keys[0]); match.add(epoch); }
        args.addAll(match);
        String upd = sb.append(cond).toString();
        String sel = "SELECT 1 FROM " + table + cond;
        return withConn(c -> exec(c, upd, args.toArray()) > 0 || exists(c, sel, match.toArray()));
    }

    private static final String FAILED = RunStatus.FAILED.name(), RUNNING = RunStatus.RUNNING.name();

    @Override
    public boolean casRoundStatus(String roundId, RoundStatus expect, RoundStatus target, String error, long epoch) {
        return cas("pg_round", "round_id=?", new Object[]{roundId}, expect.name(), target.name(), error, epoch, target == RoundStatus.RUNNING, target.terminal());
    }

    @Override
    public void setRoundIncomplete(String roundId) { update("UPDATE pg_round SET incomplete=1 WHERE round_id=?", roundId); }

    @Override
    public boolean casRunStatus(String roundId, String runId, RunStatus expect, RunStatus target, String error, long epoch) {
        return cas("pg_run", "round_id=? AND run_id=?", new Object[]{roundId, runId}, expect.name(), target.name(), error, epoch,
                target == RunStatus.RUNNING, target.terminal());
    }

    @Override
    public boolean updateRunAssignment(String roundId, String runId, String assignee, int attempt, long epoch) {
        String cond = " WHERE round_id=? AND run_id=? AND master_epoch<=?" + FENCE;
        return withConn(c -> exec(c, "UPDATE pg_run SET assignee=?, attempt=?, master_epoch=?" + cond, assignee, attempt, epoch, roundId, runId, epoch, roundId, epoch) > 0
                || exists(c, "SELECT 1 FROM pg_run" + cond, roundId, runId, epoch, roundId, epoch));   // 0 rows may mean unchanged (useAffectedRows)
    }

    @Override
    public boolean casPipelineStatus(String roundId, String pipelineId, PipelineStatus expect, PipelineStatus target, String error, long epoch) {
        return cas("pg_pipeline", "round_id=? AND pipeline_id=?", new Object[]{roundId, pipelineId}, expect.name(), target.name(), error, epoch,
                target == PipelineStatus.RUNNING, target.terminal());
    }

    @Override
    public void setPipelineEnumDone(String roundId, String pipelineId, boolean done, long epoch) {
        update("UPDATE pg_pipeline SET enum_done=?, master_epoch=? WHERE round_id=? AND pipeline_id=? AND master_epoch<=?" + FENCE,
                done, epoch, roundId, pipelineId, epoch, roundId, epoch);
    }

    @Override
    public void setPipelineParallelism(String roundId, String pipelineId, int parallelism, long epoch) {
        update("UPDATE pg_pipeline SET parallelism=?, master_epoch=? WHERE round_id=? AND pipeline_id=? AND master_epoch<=?" + FENCE,
                parallelism, epoch, roundId, pipelineId, epoch, roundId, epoch);
    }

    // ---------------------------------------------------------------- gates

    @Override
    public void armGate(String roundId, String gateId, long armedAt, Long deadlineAt, long epoch) {
        update("UPDATE pg_gate SET armed_at=?, deadline_at=?, master_epoch=? WHERE round_id=? AND gate_id=? AND master_epoch<=?" + FENCE,
                armedAt, deadlineAt, epoch, roundId, gateId, epoch, roundId, epoch);
    }

    @Override
    public boolean casGateStatus(String roundId, String gateId, GateStatus expect, GateStatus target, String openedBy, Map<String, String> context, long epoch) {
        String cond = " WHERE round_id=? AND gate_id=? AND status=? AND master_epoch<=?" + FENCE;
        return withConn(c -> exec(c, "UPDATE pg_gate SET status=?, master_epoch=?, opened_by=COALESCE(?,opened_by), context_json=COALESCE(?,context_json), "
                        + "opened_at=CASE WHEN ?='CLOSED' THEN opened_at ELSE ? END" + cond,
                target.name(), epoch, openedBy, json(context), target.name(), System.currentTimeMillis(), roundId, gateId, expect.name(), epoch, roundId, epoch) > 0
                || exists(c, "SELECT 1 FROM pg_gate" + cond, roundId, gateId, expect.name(), epoch, roundId, epoch));
    }

    @Override
    public void markGateAlerted(String roundId, String gateId) { update("UPDATE pg_gate SET alerted=1 WHERE round_id=? AND gate_id=?", roundId, gateId); }

    @Override
    public void saveGateSignal(String roundId, String gateId, String signalKey, String payload) {
        if (signalKey == null || signalKey.isEmpty() || signalKey.length() > MAX_SIGNAL_KEY_LENGTH)   // INSERT IGNORE would truncate silently
            throw new IllegalArgumentException("signal key must be 1.." + MAX_SIGNAL_KEY_LENGTH + " characters, got " + (signalKey == null ? "null" : String.valueOf(signalKey.length())));
        update("INSERT IGNORE INTO pg_gate_signal (round_id,gate_id,signal_key,payload,created_at) VALUES (?,?,?,?,?)",
                roundId, gateId, signalKey, payload, System.currentTimeMillis());
    }

    @Override
    public Map<String, String> loadGateSignals(String roundId, String gateId) {
        Map<String, String> out = new LinkedHashMap<>();
        for (String[] kv : query("SELECT signal_key, payload FROM pg_gate_signal WHERE round_id=? AND gate_id=? ORDER BY created_at, signal_key",
                rs -> new String[]{rs.getString(1), rs.getString(2)}, roundId, gateId)) out.put(kv[0], kv[1]);
        return out;
    }

    // ---------------------------------------------------------------- chunks

    private static final String CHUNK_INS = "INSERT %s INTO pg_chunk (round_id,pipeline_id,chunk_id,run_id,status,attempt,owner,input_edge,split_spec,rows_in,rows_out_json,rows_out_total,sink_rows,cost_ms,error,updated_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";

    @Override
    public void savePendingChunks(String roundId, List<ChunkRecord> chunks, long epoch) {
        if (chunks == null || chunks.isEmpty()) return;
        boolean ok = tx(c -> {
            // fence: the share lock on the round row serialises against fenceRound; a deposed master inserts nothing
            if (!exists(c, "SELECT 1 FROM pg_round WHERE round_id=? AND master_epoch<=? LOCK IN SHARE MODE", roundId, epoch)) return false;
            try (PreparedStatement ps = c.prepareStatement(String.format(CHUNK_INS, "IGNORE"))) {
                long now = System.currentTimeMillis();
                for (ChunkRecord k : chunks) {
                    bind(ps, roundId, k.getPipelineId(), k.getChunkId(), k.getRunId(), k.getStatus() == null ? "PENDING" : k.getStatus().name(), k.getAttempt(),
                            k.getOwner(), k.getInputEdge(), k.getSplitSpec(), k.getRowsIn(), json(k.getRowsOut()), k.getRowsOut() == null ? null : totalOf(k.getRowsOut()), sinkRowsOf(k.getRowsOut()),
                            k.getCostMs(), k.getError(), k.getUpdatedAt() == null ? now : k.getUpdatedAt());
                    ps.addBatch();
                }
                ps.executeBatch();
            }
            return true;
        });
        if (!ok) log.warn("savePendingChunks for round {} fenced or missing (epoch {}); nothing stored", roundId, epoch);
    }

    private static final String[] UPSERT_COLS = {"run_id", "attempt", "owner", "input_edge", "rows_in", "rows_out_json", "rows_out_total", "sink_rows", "cost_ms", "error", "updated_at"};

    private static String upsertSql(boolean dead) {
        StringBuilder sb = new StringBuilder(String.format(CHUNK_INS, "")).append(" ON DUPLICATE KEY UPDATE ");
        for (String col : UPSERT_COLS)
            sb.append(col).append(dead ? "=IF(status='DONE'," + col + ",VALUES(" + col + ")), " : "=VALUES(" + col + "), ");
        // MySQL assigns left to right: status goes last so the DONE test above still sees the old status.
        return sb.append(dead ? "status=IF(status='DONE',status,VALUES(status))" : "status=VALUES(status)").toString();
    }

    private static final String UPSERT_DONE = upsertSql(false);
    private static final String UPSERT_DEAD = upsertSql(true);

    @Override
    public void applyChunkEvents(String roundId, List<ChunkEvent> events, String newJournalOffset, long epoch) {
        boolean applied = tx(c -> {
            // fence first: a stale master must not write anything
            if (!fenceUpdate(c, "UPDATE pg_round SET journal_offset=?, master_epoch=? WHERE round_id=? AND master_epoch<=?", roundId, epoch, newJournalOffset, epoch, roundId, epoch)) return false;
            String curType = null;
            PreparedStatement ps = null;
            try {
                for (ChunkEvent e : events == null ? Collections.<ChunkEvent>emptyList() : events) {
                    String t = e.getType();
                    if (!"DONE".equals(t) && !"DEAD".equals(t)) continue;
                    if (!t.equals(curType)) {
                        if (ps != null) { ps.executeBatch(); ps.close(); }
                        ps = c.prepareStatement("DONE".equals(t) ? UPSERT_DONE : UPSERT_DEAD);
                        curType = t;
                    }
                    bind(ps, roundId, e.getPipelineId(), e.getChunkId(), e.getRunId(), t, e.getAttempt(), e.getOwner(), e.getInputEdge(), null, e.getRowsIn(),
                            json(e.getRowsOut()), e.getRowsOut() == null ? null : totalOf(e.getRowsOut()), sinkRowsOf(e.getRowsOut()), e.getCostMs(), e.getError(), e.getTs());
                    ps.addBatch();
                }
                if (ps != null) ps.executeBatch();
            } finally {
                if (ps != null) ps.close();
            }
            return true;
        });
        if (!applied) throw new StaleEpochException("applyChunkEvents for round " + roundId + " fenced or missing (epoch " + epoch + ")");
    }

    @Override
    public void markChunksSkipped(String roundId, String runId, long epoch) {
        update("UPDATE pg_chunk SET status='SKIPPED', updated_at=? WHERE round_id=? AND run_id=? AND status='DEAD'" + FENCE,
                System.currentTimeMillis(), roundId, runId, roundId, epoch);
    }

    @Override
    public void resetRuns(String roundId, List<String> runIds, List<String> gateIds, long epoch) {
        tx(c -> {
            // fence first (row lock on the round serialises against fenceRound and every other master write)
            if (!fenceUpdate(c, "UPDATE pg_round SET master_epoch=? WHERE round_id=? AND master_epoch<=?", roundId, epoch, epoch, roundId, epoch))
                throw new StaleEpochException("resetRuns for round " + roundId + " fenced or missing (epoch " + epoch + ")");
            // all-or-nothing: check every listed run before writing; a missing run is skipped (like the Redis store), a run written by a
            // newer epoch aborts the whole reset (the exception makes tx() roll the fence bump back)
            List<String> present = new ArrayList<>();
            for (String runId : runIds) {
                Long re = null;
                try (PreparedStatement ps = c.prepareStatement("SELECT master_epoch FROM pg_run WHERE round_id=? AND run_id=? FOR UPDATE")) {
                    ps.setString(1, roundId);
                    ps.setString(2, runId);
                    try (ResultSet rs = ps.executeQuery()) { if (rs.next()) re = rs.getLong(1); }
                }
                if (re == null) continue;
                if (re > epoch) throw new StaleEpochException("resetRuns for round " + roundId + ": run " + runId + " was written by epoch " + re + " > " + epoch);
                present.add(runId);
            }
            for (String runId : present) {
                exec(c, "UPDATE pg_run SET status='PENDING', attempt=attempt+1, assignee=NULL, started_at=NULL, finished_at=NULL, error=NULL, master_epoch=? "
                        + "WHERE round_id=? AND run_id=?", epoch, roundId, runId);
                exec(c, "UPDATE pg_pipeline SET status='PENDING', enum_done=0, started_at=NULL, finished_at=NULL, error=NULL, sum_counts=NULL, sum_rows_in=NULL, sum_rows_out=NULL, master_epoch=? WHERE round_id=? AND run_id=?",
                        epoch, roundId, runId);
                exec(c, "DELETE FROM pg_chunk WHERE round_id=? AND run_id=?", roundId, runId);
            }
            for (String gateId : gateIds) {
                exec(c, "UPDATE pg_gate SET status='CLOSED', armed_at=NULL, deadline_at=NULL, opened_by=NULL, opened_at=NULL, context_json=NULL, alerted=0, master_epoch=? "
                        + "WHERE round_id=? AND gate_id=?", epoch, roundId, gateId);
                exec(c, "DELETE FROM pg_gate_signal WHERE round_id=? AND gate_id=?", roundId, gateId);
            }
            exec(c, "UPDATE pg_round SET incomplete=0 WHERE round_id=?", roundId);
            exec(c, "UPDATE pg_round SET status='RUNNING', finished_at=NULL, error=NULL, compacted=0 WHERE round_id=? AND status IN " + REVIVABLE_IN, roundId);
            return null;
        });
    }

    @Override
    public List<ChunkRecord> queryChunks(ChunkQuery q) {
        StringBuilder sb = new StringBuilder("SELECT * FROM pg_chunk WHERE 1=1");
        List<Object> a = new ArrayList<>();
        if (q.getRoundId() != null) { sb.append(" AND round_id=?"); a.add(q.getRoundId()); }
        if (q.getPipelineId() != null) { sb.append(" AND pipeline_id=?"); a.add(q.getPipelineId()); }
        if (q.getRunId() != null) { sb.append(" AND run_id=?"); a.add(q.getRunId()); }
        if (q.getStatus() != null) { sb.append(" AND status=?"); a.add(q.getStatus().name()); }
        sb.append(" ORDER BY pipeline_id, chunk_id LIMIT ? OFFSET ?");
        a.add(q.getLimit()); a.add(q.getOffset());
        return query(sb.toString(), MySqlControlPlaneStore::mapChunk, a.toArray());
    }

    @Override
    public long countChunks(String roundId, String pipelineId, ChunkStatus status) {
        StringBuilder sb = new StringBuilder("SELECT COUNT(*) FROM pg_chunk WHERE round_id=?");
        List<Object> a = new ArrayList<>();
        a.add(roundId);
        if (pipelineId != null) { sb.append(" AND pipeline_id=?"); a.add(pipelineId); }
        if (status != null) { sb.append(" AND status=?"); a.add(status.name()); }
        return query(sb.toString(), rs -> rs.getLong(1), a.toArray()).get(0);
    }

    @Override
    public long countSinkChunks(String roundId, String pipelineId) {
        return query("SELECT COUNT(*) FROM pg_chunk WHERE round_id=? AND pipeline_id=? AND status='DONE' AND sink_rows>0", rs -> rs.getLong(1), roundId, pipelineId).get(0);
    }

    @Override
    public Map<String, Long> sumRowsOutByEdge(String roundId, String runId) {
        Map<String, Long> out = new LinkedHashMap<>();
        for (String j : query("SELECT rows_out_json FROM pg_chunk WHERE round_id=? AND run_id=? AND status='DONE' AND rows_out_json IS NOT NULL",
                rs -> rs.getString(1), roundId, runId)) {
            Map<String, Long> m = longMap(j);
            for (Map.Entry<String, Long> e : m.entrySet()) if (e.getKey().startsWith("e")) out.merge(e.getKey(), e.getValue(), Long::sum);
        }
        return out;
    }

    @Override
    public Map<String, Long> sumRowsInByEdge(String roundId, String runId) {
        Map<String, Long> out = new LinkedHashMap<>();
        for (Object[] r : query("SELECT input_edge, COALESCE(SUM(rows_in),0) FROM pg_chunk WHERE round_id=? AND run_id=? AND status='DONE' "
                + "AND input_edge IS NOT NULL AND input_edge<>'split' GROUP BY input_edge", rs -> new Object[]{rs.getString(1), rs.getLong(2)}, roundId, runId))
            out.put((String) r[0], (Long) r[1]);
        return out;
    }

    // ---------------------------------------------------------------- retention

    private static boolean terminalName(String st) { return RoundStatus.valueOf(st).terminal(); }

    /** Locks the round row; returns {status, finishedAt, compacted} or null when missing. */
    private static Object[] lockRound(Connection c, String roundId) throws SQLException {
        try (PreparedStatement ps = c.prepareStatement("SELECT status, finished_at, compacted FROM pg_round WHERE round_id=? FOR UPDATE")) {
            ps.setString(1, roundId);
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) return null;
                long fin = rs.getLong(2);
                return new Object[]{rs.getString(1), rs.wasNull() ? null : fin, rs.getBoolean(3)};
            }
        }
    }

    @Override
    public int compactRound(String roundId, long finishedBeforeMillis) {
        return tx(c -> {
            Object[] r = lockRound(c, roundId);
            if (r == null || !terminalName((String) r[0]) || r[1] == null || (Long) r[1] >= finishedBeforeMillis || (Boolean) r[2]) return 0;
            Map<String, Map<String, Long>> counts = new LinkedHashMap<>();
            Map<String, long[]> rows = new LinkedHashMap<>();
            try (PreparedStatement ps = c.prepareStatement("SELECT pipeline_id, status, COUNT(*), COALESCE(SUM(CASE WHEN status='DONE' THEN rows_in END),0), "
                    + "COALESCE(SUM(CASE WHEN status='DONE' THEN rows_out_total END),0) FROM pg_chunk WHERE round_id=? GROUP BY pipeline_id, status")) {
                ps.setString(1, roundId);
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        counts.computeIfAbsent(rs.getString(1), k -> new LinkedHashMap<String, Long>()).put(rs.getString(2), rs.getLong(3));
                        if ("DONE".equals(rs.getString(2))) {   // row sums only exist for pipelines with DONE chunks (like the live view)
                            long[] t = rows.computeIfAbsent(rs.getString(1), k -> new long[2]);
                            t[0] += rs.getLong(4);
                            t[1] += rs.getLong(5);
                        }
                    }
                }
            }
            for (Map.Entry<String, Map<String, Long>> e : counts.entrySet()) {
                long[] t = rows.get(e.getKey());
                exec(c, "UPDATE pg_pipeline SET sum_counts=?, sum_rows_in=?, sum_rows_out=? WHERE round_id=? AND pipeline_id=?",
                        json(e.getValue()), t == null ? null : t[0], t == null ? null : t[1], roundId, e.getKey());
            }
            int removed = exec(c, "DELETE FROM pg_chunk WHERE round_id=?", roundId);
            exec(c, "UPDATE pg_round SET compacted=1 WHERE round_id=?", roundId);
            return removed;
        });
    }

    @Override
    public List<String> purgeRounds(long finishedBeforeMillis, int max) {
        List<String> purged = new ArrayList<>();
        if (max <= 0) return purged;
        List<String> candidates = query("SELECT round_id FROM pg_round WHERE status IN " + TERMINAL_IN + " AND finished_at < ? "
                + "ORDER BY finished_at, round_id LIMIT ?", rs -> rs.getString(1), finishedBeforeMillis, max);
        for (String rid : candidates) {
            boolean done = tx(c -> {
                Object[] r = lockRound(c, rid);   // re-check under the row lock: a rerun may have flipped the round back to RUNNING
                if (r == null || !terminalName((String) r[0]) || r[1] == null || (Long) r[1] >= finishedBeforeMillis) return false;
                for (String t : new String[]{"pg_chunk", "pg_gate_signal", "pg_gate", "pg_pipeline", "pg_run"}) exec(c, "DELETE FROM " + t + " WHERE round_id=?", rid);
                exec(c, "DELETE FROM pg_round WHERE round_id=?", rid);
                return true;
            });
            if (done) purged.add(rid);
        }
        return purged;
    }

    @Override
    public List<String> findTerminalRounds(long finishedBeforeMillis, int max) {
        if (max <= 0) return new ArrayList<>();
        return query("SELECT round_id FROM pg_round WHERE status IN " + TERMINAL_IN + " AND finished_at < ? AND compacted=0 "
                + "ORDER BY finished_at, round_id LIMIT ?", rs -> rs.getString(1), finishedBeforeMillis, max);
    }
}
