package io.github.amwtke.pipeliner.spring;

import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import io.github.amwtke.pipeliner.api.BatchRunner;
import io.github.amwtke.pipeliner.api.Chunk;
import io.github.amwtke.pipeliner.api.Row;
import io.github.amwtke.pipeliner.api.Sink;
import io.github.amwtke.pipeliner.api.SinkContext;
import io.github.amwtke.pipeliner.api.Split;
import io.github.amwtke.pipeliner.api.SourceContext;
import io.github.amwtke.pipeliner.api.SplitSource;
import io.github.amwtke.pipeliner.core.dataplane.DataPlane;
import io.github.amwtke.pipeliner.core.engine.PipelinerEngine;
import io.github.amwtke.pipeliner.core.engine.RuntimeConfig;
import io.github.amwtke.pipeliner.core.graph.ExecMode;
import io.github.amwtke.pipeliner.core.graph.Gates;
import io.github.amwtke.pipeliner.core.graph.PipelineGraph;
import io.github.amwtke.pipeliner.core.store.ControlPlaneStore;
import io.github.amwtke.pipeliner.core.store.RoundRecord;
import io.github.amwtke.pipeliner.core.store.RoundStatus;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

class AdminUiEndpointsTest {
    static class Src implements SplitSource {
        public String datasetName() { return "src_ds"; }
        public List<Split> enumerate(SourceContext ctx) { return Collections.emptyList(); }
        public List<Row> read(Split split, SourceContext ctx) { return Collections.emptyList(); }
    }

    static class Snk implements Sink {
        final String ds;
        Snk(String ds) { this.ds = ds; }
        public String datasetName() { return ds; }
        public void write(Chunk chunk, SinkContext ctx) { }
    }

    static class Batch implements BatchRunner {
        public void process(List<Row> rows, io.github.amwtke.pipeliner.api.Collector out, io.github.amwtke.pipeliner.api.RunContext ctx) { }
    }

    PipelinerEngine engine;
    ControlPlaneStore store;
    DataPlane plane;
    MockMvc mvc;

    @BeforeEach
    void setUp() {
        PipelineGraph g = PipelineGraph.builder("demo")
                .pipeline("load", p -> p.source(new Src()).batch(new Batch()).sink(new Snk("mid")).mode(ExecMode.CLUSTER).parallelism(3).chunkKey("id"))
                .pipeline("post", p -> p.batch(new Batch()).sink(new Snk("res")).chunkKey("id"))
                .pipeline("publish", p -> p.source(new Src() { public String datasetName() { return "res"; } }).batch(new Batch()).sink(new Snk("out")))
                .edge("load", "post")
                .gate("approve", Gates.manual(), gt -> gt.after("post").before("publish"))
                .build();
        engine = mock(PipelinerEngine.class);
        when(engine.graphNames()).thenReturn(Collections.singletonList("demo"));
        when(engine.plan("demo")).thenReturn(g.plan());
        store = mock(ControlPlaneStore.class);
        plane = mock(DataPlane.class);
        mvc = MockMvcBuilders.standaloneSetup(new PipelinerAdminController(engine, new RuntimeConfig(), store, plane)).build();
    }

    @Test
    void planJsonShape() throws Exception {
        mvc.perform(get("/pg/graphs/demo/plan.json"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.graphName").value("demo"))
                .andExpect(jsonPath("$.hash").isNotEmpty())
                .andExpect(jsonPath("$.stageCount").isNumber())
                .andExpect(jsonPath("$.runs[0].runId").isNotEmpty())
                .andExpect(jsonPath("$.runs[0].pipelineIds").isArray())
                .andExpect(jsonPath("$.runs[0].inGateIds").isArray())
                .andExpect(jsonPath("$.runs[0].upstreamRunIds").isArray())
                .andExpect(jsonPath("$.pipelines[?(@.id=='load')].source.type").value("Src"))
                .andExpect(jsonPath("$.pipelines[?(@.id=='load')].source.dataset").value("src_ds"))
                .andExpect(jsonPath("$.pipelines[?(@.id=='load')].runner.kind").value("batch"))
                .andExpect(jsonPath("$.pipelines[?(@.id=='load')].runner.class").value("Batch"))
                .andExpect(jsonPath("$.pipelines[?(@.id=='load')].runner.perThread").value(false))
                .andExpect(jsonPath("$.pipelines[?(@.id=='load')].parallelism").value(3))
                .andExpect(jsonPath("$.pipelines[?(@.id=='load')].sinks.out[0].dataset").value("mid"))
                .andExpect(jsonPath("$.pipelines[?(@.id=='post')].source").value((Object) null))
                .andExpect(jsonPath("$.pipelines[?(@.id=='post')].inputEdgeIds[0]").value("e1"))
                .andExpect(jsonPath("$.channels[0].edgeId").value("e1"))
                .andExpect(jsonPath("$.channels[0].from").value("load"))
                .andExpect(jsonPath("$.channels[0].to").value("post"))
                .andExpect(jsonPath("$.gates[?(@.gateId=='approve')].type").value("MANUAL"))
                .andExpect(jsonPath("$.gates[?(@.gateId=='approve')].auto").value(false))
                .andExpect(jsonPath("$.gates[?(@.gateId=='approve')].afterPipelines[0]").value("post"))
                .andExpect(jsonPath("$.gates[?(@.auto==true)].dataset").isNotEmpty());
    }

    @Test
    void planJsonUnknownGraphIs404() throws Exception {
        mvc.perform(get("/pg/graphs/nope/plan.json")).andExpect(status().isNotFound()).andExpect(jsonPath("$.error").isNotEmpty());
    }

    @Test
    void roundsShape() throws Exception {
        RoundRecord r = RoundRecord.builder().roundId("r1").graphName("demo").bizKey("b1").status(RoundStatus.RUNNING).createdAt(10L).startedAt(11L)
                .incomplete(true).params(Collections.singletonMap("planVersion", "V1")).build();
        when(store.listRecentRounds(eq("demo"), eq(5))).thenReturn(Collections.singletonList(r));
        mvc.perform(get("/pg/rounds").param("graph", "demo").param("limit", "5"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$[0].roundId").value("r1"))
                .andExpect(jsonPath("$[0].graph").value("demo"))
                .andExpect(jsonPath("$[0].bizKey").value("b1"))
                .andExpect(jsonPath("$[0].status").value("RUNNING"))
                .andExpect(jsonPath("$[0].createdAt").value(10))
                .andExpect(jsonPath("$[0].startedAt").value(11))
                .andExpect(jsonPath("$[0].finishedAt").value((Object) null))
                .andExpect(jsonPath("$[0].incomplete").value(true))
                .andExpect(jsonPath("$[0].compacted").value(false));
    }

    @Test
    void clusterWorkersShape() throws Exception {
        long now = System.currentTimeMillis();
        Map<String, Long> live = new LinkedHashMap<>();
        live.put("pod1", now - 1500);
        live.put("pod2", now - 100);
        when(plane.liveWorkers(anyLong())).thenReturn(live);
        when(plane.currentLeader()).thenReturn("pod2");
        when(plane.workerInfo("pod1")).thenReturn(Collections.singletonMap("live", "[[\"r1\",\"load\",0,3],[\"r1\",\"post\",0,1]]"));
        when(plane.workerInfo("pod2")).thenReturn(Collections.<String, String>emptyMap());
        mvc.perform(get("/pg/cluster/workers"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.length()").value(2))
                .andExpect(jsonPath("$[0].podId").value("pod1"))
                .andExpect(jsonPath("$[0].leader").value(false))
                .andExpect(jsonPath("$[0].heartbeatAgeMillis").isNumber())
                .andExpect(jsonPath("$[0].instances[0].round").value("r1"))
                .andExpect(jsonPath("$[0].instances[0].pipeline").value("load"))
                .andExpect(jsonPath("$[0].instances[0].attempt").value(0))
                .andExpect(jsonPath("$[0].instances[0].threads").value(3))
                .andExpect(jsonPath("$[0].instances.length()").value(2))
                .andExpect(jsonPath("$[1].podId").value("pod2"))
                .andExpect(jsonPath("$[1].leader").value(true))
                .andExpect(jsonPath("$[1].instances.length()").value(0));
    }

    @Test
    void chunksLimitAndOffsetAreValidated() throws Exception {
        when(engine.chunks(org.mockito.ArgumentMatchers.any())).thenReturn(Collections.emptyList());
        mvc.perform(get("/pg/rounds/r1/chunks").param("limit", "-1")).andExpect(status().isBadRequest());
        mvc.perform(get("/pg/rounds/r1/chunks").param("limit", "0")).andExpect(status().isBadRequest());
        mvc.perform(get("/pg/rounds/r1/chunks").param("limit", "2147483647")).andExpect(status().isBadRequest());
        mvc.perform(get("/pg/rounds/r1/chunks").param("limit", "1001")).andExpect(status().isBadRequest());
        mvc.perform(get("/pg/rounds/r1/chunks").param("offset", "-5")).andExpect(status().isBadRequest());
        mvc.perform(get("/pg/rounds/r1/chunks").param("limit", "1000").param("offset", "0")).andExpect(status().isOk());
        mvc.perform(get("/pg/rounds/r1/chunks").param("limit", "1")).andExpect(status().isOk());
        mvc.perform(get("/pg/rounds/r1/chunks").param("status", "pending")).andExpect(status().isOk());
    }

    @Test
    void chunkStatusParsingIsLocaleIndependent() throws Exception {
        java.util.Locale old = java.util.Locale.getDefault();
        java.util.Locale.setDefault(new java.util.Locale("tr", "TR"));
        try {
            when(engine.chunks(org.mockito.ArgumentMatchers.any())).thenReturn(Collections.emptyList());
            mvc.perform(get("/pg/rounds/r1/chunks").param("status", "pending")).andExpect(status().isOk());
            mvc.perform(get("/pg/rounds/r1/chunks").param("status", "skipped")).andExpect(status().isOk());
        } finally {
            java.util.Locale.setDefault(old);
        }
    }

    @Test
    void clientErrorsAre4xx() throws Exception {
        mvc.perform(get("/pg/rounds").param("limit", "abc")).andExpect(status().isBadRequest()).andExpect(jsonPath("$.error").isNotEmpty());
        mvc.perform(get("/pg/rounds/r1/chunks").param("status", "bogus")).andExpect(status().isBadRequest());
        mvc.perform(org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post("/pg/rounds")
                .contentType("application/json").content("{not json")).andExpect(status().isBadRequest());
        mvc.perform(org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post("/pg/rounds")
                .contentType("application/json").content("{\"graph\":\"demo\"}")).andExpect(status().isBadRequest());
        mvc.perform(org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post("/pg/rounds")
                .contentType("text/plain").content("x")).andExpect(status().isUnsupportedMediaType());
        mvc.perform(org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post("/pg/rounds")
                .contentType("application/json")).andExpect(status().isBadRequest());
    }

    @Test
    void unknownRoundOrGraphIs404AndUnexpectedIs500WithShortMessage() throws Exception {
        org.mockito.Mockito.doThrow(new io.github.amwtke.pipeliner.api.PipelinerException("unknown round: nope")).when(engine).cancel("nope");
        org.mockito.Mockito.doThrow(new RuntimeException("Unable to connect to 10.1.2.3:6380 secret")).when(engine).pause("r1");
        mvc.perform(org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post("/pg/rounds/nope/cancel")).andExpect(status().isNotFound());
        when(engine.submit(eq("ghost"), org.mockito.ArgumentMatchers.anyString(), org.mockito.ArgumentMatchers.anyMap()))
                .thenThrow(new io.github.amwtke.pipeliner.api.PipelinerException("unknown graph: ghost"));
        mvc.perform(org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post("/pg/rounds").contentType("application/json")
                .content("{\"graph\":\"ghost\",\"bizKey\":\"k\"}")).andExpect(status().isNotFound());
        mvc.perform(org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post("/pg/rounds/r1/pause"))
                .andExpect(status().isInternalServerError())
                .andExpect(jsonPath("$.error").value("internal error (RuntimeException)"));
    }

    @Test
    void masterTickInfrastructureFailureIs200WithErrorField() throws Exception {
        when(engine.tick()).thenThrow(new RuntimeException("Unable to connect to 127.0.0.1:6380"));
        mvc.perform(org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post("/pg/master/tick"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.skipped").value(true))
                .andExpect(jsonPath("$.error").value("internal error (RuntimeException)"));
    }

    @Test
    void backpressureLoadsTheViewOnce() throws Exception {
        io.github.amwtke.pipeliner.core.store.RoundView v = io.github.amwtke.pipeliner.core.store.RoundView.builder()
                .round(RoundRecord.builder().roundId("r1").graphName("demo").status(RoundStatus.RUNNING).build())
                .runs(Collections.emptyList()).build();
        when(engine.view("r1")).thenReturn(v);
        when(engine.channelLengths(v)).thenReturn(Collections.singletonMap("e1", 3L));
        mvc.perform(get("/pg/rounds/r1/backpressure")).andExpect(status().isOk()).andExpect(jsonPath("$.channels.e1").value(3));
        org.mockito.Mockito.verify(engine, org.mockito.Mockito.times(1)).view("r1");
        org.mockito.Mockito.verify(engine, org.mockito.Mockito.never()).channelLengths(org.mockito.ArgumentMatchers.anyString());
    }
}
