package io.github.amwtke.pipeliner.store.mysql;

import static org.junit.jupiter.api.Assertions.*;

import com.zaxxer.hikari.HikariDataSource;
import io.github.amwtke.pipeliner.core.graph.ExecMode;
import io.github.amwtke.pipeliner.core.graph.GateType;
import io.github.amwtke.pipeliner.core.store.*;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Runs the shared contract against a live MySQL (skipped when unreachable), plus the MySQL specific behaviour. */
class MySqlControlPlaneStoreTest extends AbstractControlPlaneStoreContractTest {
    private static HikariDataSource ds;
    private static MySqlControlPlaneStore shared;

    @BeforeAll
    static void connect() {
        Assumptions.assumeTrue(MySqlTestDb.up(), "MySQL not reachable");
        ds = MySqlTestDb.open("", true);
        shared = new MySqlControlPlaneStore(ds);
        shared.init();
        shared.init(); // idempotent
    }

    @AfterAll
    static void close() { if (ds != null) ds.close(); }

    @BeforeEach
    void setUp() { store = shared; }

    private static String chars(int n, char c) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < n; i++) sb.append(c);
        return sb.toString();
    }

    private static void sql(String... stmts) throws Exception {
        try (Connection c = ds.getConnection(); Statement st = c.createStatement()) { for (String s : stmts) st.execute(s); }
    }

    private static int columnLength(String table, String column) throws Exception {
        try (Connection c = ds.getConnection(); Statement st = c.createStatement();
             ResultSet rs = st.executeQuery("SELECT CHARACTER_MAXIMUM_LENGTH FROM information_schema.COLUMNS WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='" + table
                     + "' AND COLUMN_NAME='" + column + "'")) {
            assertTrue(rs.next());
            return rs.getInt(1);
        }
    }

    private static void dropLongRows() throws Exception {
        sql("DELETE FROM pg_chunk WHERE CHAR_LENGTH(round_id)>64 OR CHAR_LENGTH(pipeline_id)>64",
            "DELETE FROM pg_gate_signal WHERE CHAR_LENGTH(round_id)>64 OR CHAR_LENGTH(gate_id)>64",
            "DELETE FROM pg_gate WHERE CHAR_LENGTH(round_id)>64 OR CHAR_LENGTH(gate_id)>64",
            "DELETE FROM pg_pipeline WHERE CHAR_LENGTH(round_id)>64 OR CHAR_LENGTH(pipeline_id)>64",
            "DELETE FROM pg_run WHERE CHAR_LENGTH(round_id)>64",
            "DELETE FROM pg_round WHERE CHAR_LENGTH(round_id)>64");
    }

    @Test
    void maximumLengthIdentifiersFitEveryColumnAndPrimaryKey() throws Exception {
        String graph = chars(ControlPlaneStore.MAX_GRAPH_NAME_LENGTH, 'g');
        String rid = graph + "-20260101-120000-" + Long.toHexString(System.nanoTime() | 0x1000000000000L).substring(0, 12);
        assertEquals(ControlPlaneStore.MAX_ROUND_ID_LENGTH, rid.length());
        String gate = "barrier:" + chars(ControlPlaneStore.MAX_GATE_ID_LENGTH - 8, 'd');
        String pl = chars(ControlPlaneStore.MAX_PIPELINE_ID_LENGTH, 'p');
        Map<String, String> params = new LinkedHashMap<>();
        params.put("a", "b");
        try {
            shared.createRound(RoundRecord.builder().roundId(rid).graphName(graph).bizKey("long").planHash("h").planJson("{}").params(params)
                            .status(RoundStatus.CREATED).createdAt(1L).build(),
                    Collections.singletonList(RunRecord.builder().roundId(rid).runId("R1").stage(0).mode(ExecMode.CLUSTER).status(RunStatus.PENDING).build()),
                    Collections.singletonList(GateRecord.builder().roundId(rid).gateId(gate).type(GateType.BARRIER).status(GateStatus.CLOSED).build()),
                    Collections.singletonList(PipelineRecord.builder().roundId(rid).pipelineId(pl).runId("R1").status(PipelineStatus.PENDING).build()));
            shared.savePendingChunks(rid, Collections.singletonList(ChunkRecord.builder().pipelineId(pl).chunkId(chars(255, 'c')).runId("R1").inputEdge("split").build()), 1);
            String key = chars(ControlPlaneStore.MAX_SIGNAL_KEY_LENGTH, 'k');
            shared.saveGateSignal(rid, gate, key, "v");
            assertEquals("v", shared.loadGateSignals(rid, gate).get(key));
            assertEquals(1, shared.countChunks(rid, pl, ChunkStatus.PENDING));
            assertEquals(rid, shared.findRoundByBizKey(graph, "long").getRoundId());
            assertEquals(gate, shared.loadRound(rid).getGates().get(0).getGateId());
            assertTrue(shared.casRoundStatus(rid, RoundStatus.CREATED, RoundStatus.RUNNING, null, 1));
        } finally {
            dropLongRows();
        }
    }

    @Test
    void initWidensColumnsOfTablesCreatedWithNarrowerOnes() throws Exception {
        dropLongRows();
        sql("ALTER TABLE pg_gate MODIFY gate_id VARCHAR(64) NOT NULL", "ALTER TABLE pg_round MODIFY round_id VARCHAR(64) NOT NULL",
            "ALTER TABLE pg_chunk MODIFY pipeline_id VARCHAR(64) NOT NULL");
        assertEquals(64, columnLength("pg_gate", "gate_id"));
        shared.init();
        assertEquals(128, columnLength("pg_gate", "gate_id"));
        assertEquals(96, columnLength("pg_round", "round_id"));
        assertEquals(128, columnLength("pg_chunk", "pipeline_id"));
        assertEquals(512, columnLength("pg_gate_signal", "signal_key"));
    }

    /** An Error mid-transaction must roll back (the autocommit restore would otherwise commit the partial work). */
    @Test
    void anErrorInsideATransactionIsRolledBackNotCommitted() {
        String rid = "err-" + Long.toString(System.nanoTime(), 36);
        shared.createRound(RoundRecord.builder().roundId(rid).graphName("err-g").bizKey(rid).planHash("h").planJson("{}").status(RoundStatus.CREATED).createdAt(1L).build(),
                Collections.singletonList(RunRecord.builder().roundId(rid).runId("R1").stage(0).status(RunStatus.PENDING).build()),
                Collections.<GateRecord>emptyList(), Collections.<PipelineRecord>emptyList());
        Map<String, Long> boom = new LinkedHashMap<String, Long>() {
            @Override public java.util.Set<Map.Entry<String, Long>> entrySet() { throw new AssertionError("injected Error"); }
        };
        ChunkEvent e = ChunkEvent.builder().type("DONE").roundId(rid).runId("R1").pipelineId("p").chunkId("c").inputEdge("split").rowsIn(1L).rowsOut(boom).ts(1L).build();
        assertThrows(AssertionError.class, () -> shared.applyChunkEvents(rid, Collections.singletonList(e), "9-9", 1));
        assertEquals("0-0", shared.loadRoundRecord(rid).getJournalOffset(), "the journal offset update was rolled back");
        assertEquals(0, shared.loadRoundRecord(rid).getMasterEpoch());
    }
}
