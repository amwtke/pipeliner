package io.github.amwtke.pipeliner.spring;

import io.github.amwtke.pipeliner.core.engine.PipelinerEngine;
import io.github.amwtke.pipeliner.core.engine.RuntimeConfig;
import io.github.amwtke.pipeliner.core.plan.ExecutionPlan;
import io.github.amwtke.pipeliner.core.plan.PlanChannel;
import io.github.amwtke.pipeliner.core.dataplane.DataPlane;
import io.github.amwtke.pipeliner.core.store.ControlPlaneStore;
import io.github.amwtke.pipeliner.core.store.RoundRecord;
import io.github.amwtke.pipeliner.core.store.ChunkQuery;
import io.github.amwtke.pipeliner.core.store.ChunkRecord;
import io.github.amwtke.pipeliner.core.store.ChunkStatus;
import io.github.amwtke.pipeliner.core.store.RoundView;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import javax.servlet.http.HttpServletRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.github.amwtke.pipeliner.spring.security.AdminSecurityFilter;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/** Admin REST API over the engine, base path /pg. */
@RestController
@RequestMapping("/pg")
public class PipelinerAdminController {
    private static final Logger log = LoggerFactory.getLogger(PipelinerAdminController.class);
    private static final com.fasterxml.jackson.databind.ObjectMapper MAPPER = new com.fasterxml.jackson.databind.ObjectMapper();
    private static final Logger AUDIT = LoggerFactory.getLogger("pipeliner.audit");
    private final PipelinerEngine engine;
    private final RuntimeConfig cfg;
    private final ControlPlaneStore store;
    private final DataPlane plane;

    public PipelinerAdminController(PipelinerEngine engine, RuntimeConfig cfg) {
        this(engine, cfg, null, null);
    }

    public PipelinerAdminController(PipelinerEngine engine, RuntimeConfig cfg, ControlPlaneStore store, DataPlane plane) {
        this.engine = engine;
        this.cfg = cfg;
        this.store = store;
        this.plane = plane;
    }

    @GetMapping("/graphs")
    public List<String> graphs() {
        return new ArrayList<>(engine.graphNames());
    }

    @GetMapping(value = "/graphs/{name}/plan", produces = MediaType.TEXT_PLAIN_VALUE)
    public String plan(@PathVariable String name) {
        return engine.plan(name).explain();
    }

    /** Structured plan (runs, pipelines, channels, gates) for the UI. */
    @GetMapping("/graphs/{name}/plan.json")
    public ResponseEntity<?> planJson(@PathVariable String name) {
        if (!engine.graphNames().contains(name)) return notFound("graph not found: " + name);
        return ResponseEntity.ok(PlanViews.plan(engine.plan(name)));
    }

    /** Recent rounds: active first, then newest by createdAt. */
    @GetMapping("/rounds")
    public List<Map<String, Object>> rounds(@RequestParam(required = false) String graph, @RequestParam(defaultValue = "20") int limit) {
        List<Map<String, Object>> out = new ArrayList<>();
        if (store == null) return out;
        for (RoundRecord r : store.listRecentRounds(blankToNull(graph), Math.max(1, Math.min(limit, 200)))) {
            Map<String, Object> m = new LinkedHashMap<>();
            m.put("roundId", r.getRoundId());
            m.put("graph", r.getGraphName());
            m.put("bizKey", r.getBizKey());
            m.put("status", r.getStatus() == null ? null : r.getStatus().name());
            m.put("createdAt", r.getCreatedAt());
            m.put("startedAt", r.getStartedAt());
            m.put("finishedAt", r.getFinishedAt());
            m.put("incomplete", r.isIncomplete());
            m.put("compacted", r.isCompacted());
            m.put("params", r.getParams());
            out.add(m);
        }
        return out;
    }

    /** Every live pod with its heartbeat age and live pipeline instances (from the heartbeat info field "live"). */
    @GetMapping("/cluster/workers")
    public List<Map<String, Object>> clusterWorkers() {
        List<Map<String, Object>> out = new ArrayList<>();
        if (plane == null) return out;
        long now = System.currentTimeMillis();
        String leader = plane.currentLeader();
        Map<String, Long> live = plane.liveWorkers(cfg.getWorkerDeadMillis());
        for (Map.Entry<String, Long> e : new java.util.TreeMap<>(live).entrySet()) {
            Map<String, Object> m = new LinkedHashMap<>();
            m.put("podId", e.getKey());
            m.put("leader", e.getKey().equals(leader));
            m.put("heartbeatAgeMillis", Math.max(0, now - e.getValue()));
            List<Map<String, Object>> inst = new ArrayList<>();
            String json = plane.workerInfo(e.getKey()).get("live");
            if (json != null && !json.isEmpty()) {
                try {
                    for (List<?> row : MAPPER.readValue(json, List[].class)) {
                        Map<String, Object> i = new LinkedHashMap<>();
                        i.put("round", row.get(0));
                        i.put("pipeline", row.get(1));
                        i.put("attempt", row.get(2));
                        i.put("threads", row.get(3));
                        inst.add(i);
                    }
                } catch (Exception ex) {
                    log.debug("bad live field from {}: {}", e.getKey(), ex.toString());
                }
            }
            m.put("instances", inst);
            out.add(m);
        }
        return out;
    }

    @PostMapping("/rounds")
    public Map<String, String> submit(@RequestBody Map<String, Object> body, HttpServletRequest req) {
        return audited(req, "-", "-", () -> doSubmit(body));
    }

    private Map<String, String> doSubmit(Map<String, Object> body) {
        Object graph = body.get("graph");
        Object bizKey = body.get("bizKey");
        if (graph == null || bizKey == null) throw new IllegalArgumentException("graph and bizKey are required");
        Map<String, String> params = new LinkedHashMap<>();
        Object p = body.get("params");
        if (p instanceof Map) ((Map<?, ?>) p).forEach((k, v) -> params.put(String.valueOf(k), String.valueOf(v)));
        return Collections.singletonMap("roundId", engine.submit(graph.toString(), bizKey.toString(), params));
    }

    @GetMapping("/rounds/{id}")
    public ResponseEntity<?> round(@PathVariable String id) {
        RoundView v = engine.view(id);
        if (v == null) return notFound("round not found: " + id);
        return ResponseEntity.ok(v);
    }

    @GetMapping("/rounds/{id}/chunks")
    public List<ChunkRecord> chunks(@PathVariable String id, @RequestParam(required = false) String pipeline,
                                    @RequestParam(required = false) String status,
                                    @RequestParam(defaultValue = "100") int limit, @RequestParam(defaultValue = "0") int offset) {
        ChunkQuery q = ChunkQuery.builder().roundId(id).pipelineId(blankToNull(pipeline))
                .status(blankToNull(status) == null ? null : ChunkStatus.valueOf(status.toUpperCase()))
                .limit(limit).offset(offset).build();
        return engine.chunks(q);
    }

    @GetMapping("/rounds/{id}/channels")
    public Map<String, Long> channels(@PathVariable String id) {
        return engine.channelLengths(id);
    }

    @GetMapping("/rounds/{id}/backpressure")
    public ResponseEntity<?> backpressure(@PathVariable String id) {
        RoundView v = engine.view(id);
        if (v == null) return notFound("round not found: " + id);
        Map<String, Long> lengths = engine.channelLengths(id);
        ExecutionPlan plan = engine.plan(v.getRound().getGraphName());
        List<String> bottlenecks = new ArrayList<>();
        for (String pid : plan.getPipelines().keySet()) {
            long in = 0;
            boolean hasIn = false;
            Long split = lengths.get("split:" + pid);
            if (split != null) { in += split; hasIn = true; }
            for (PlanChannel c : plan.inputsOf(pid)) {
                Long l = lengths.get(c.getEdgeId());
                if (l != null) { in += l; hasIn = true; }
            }
            if (!hasIn || in < cfg.getHighWatermark()) continue;
            boolean outputsLow = true;
            for (PlanChannel c : plan.outputsOf(pid)) {
                Long l = lengths.get(c.getEdgeId());
                if (l != null && l >= cfg.getHighWatermark()) outputsLow = false;
            }
            if (outputsLow) bottlenecks.add(pid);
        }
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("highWatermark", cfg.getHighWatermark());
        out.put("lowWatermark", cfg.getLowWatermark());
        out.put("channels", lengths);
        out.put("bottlenecks", bottlenecks);
        return ResponseEntity.ok(out);
    }

    @PostMapping("/rounds/{id}/pause")
    public Map<String, String> pause(@PathVariable String id, HttpServletRequest req) {
        return audited(req, id, "-", () -> { engine.pause(id); return ok(); });
    }

    @PostMapping("/rounds/{id}/resume")
    public Map<String, String> resume(@PathVariable String id, HttpServletRequest req) {
        return audited(req, id, "-", () -> { engine.resume(id); return ok(); });
    }

    @PostMapping("/rounds/{id}/cancel")
    public Map<String, String> cancel(@PathVariable String id, HttpServletRequest req) {
        return audited(req, id, "-", () -> { engine.cancel(id); return ok(); });
    }

    @PostMapping("/runs/{roundId}/{runId}/retry")
    public Map<String, String> retry(@PathVariable String roundId, @PathVariable String runId, HttpServletRequest req) {
        return audited(req, roundId, "run=" + runId, () -> { engine.retryRun(roundId, runId); return ok(); });
    }

    @PostMapping("/runs/{roundId}/{runId}/rerun")
    public Map<String, String> rerun(@PathVariable String roundId, @PathVariable String runId, HttpServletRequest req) {
        return audited(req, roundId, "run=" + runId, () -> { engine.rerunFrom(roundId, runId); return ok(); });
    }

    @PostMapping("/runs/{roundId}/{runId}/skip")
    public Map<String, String> skip(@PathVariable String roundId, @PathVariable String runId, HttpServletRequest req) {
        return audited(req, roundId, "run=" + runId, () -> { engine.skipDeadLetters(roundId, runId); return ok(); });
    }

    @PostMapping("/gates/{roundId}/{gateId}/signal")
    public Map<String, String> signal(@PathVariable String roundId, @PathVariable String gateId, @RequestBody Map<String, Object> body,
                                      HttpServletRequest req) {
        return audited(req, roundId, "gate=" + gateId, () -> {
            Object key = body.get("key");
            if (key == null) throw new IllegalArgumentException("key is required");
            Object payload = body.get("payload");
            engine.signalGate(roundId, gateId, key.toString(), payload == null ? "" : payload.toString());
            return ok();
        });
    }

    @PostMapping("/gates/{roundId}/{gateId}/open")
    public Map<String, String> open(@PathVariable String roundId, @PathVariable String gateId,
                                    @RequestParam(defaultValue = "admin") String operator, HttpServletRequest req) {
        // the authenticated principal wins; the query param is only honoured when no principal exists (security disabled)
        Object principal = req.getAttribute(AdminSecurityFilter.PRINCIPAL_ATTR);
        String op = principal != null ? principal.toString() : operator;
        return audited(req, roundId, "gate=" + gateId, () -> { engine.openGate(roundId, gateId, op); return ok(); });
    }

    /** Runs a mutating call and writes one audit line with the resulting HTTP status (never logs tokens). */
    private <T> T audited(HttpServletRequest req, String round, String target, Supplier<T> call) {
        int status = 200;
        try {
            return call.get();
        } catch (IllegalArgumentException e) {
            status = 400;
            throw e;
        } catch (RuntimeException e) {
            status = 500;
            throw e;
        } finally {
            Object p = req.getAttribute(AdminSecurityFilter.PRINCIPAL_ATTR);
            AUDIT.info("principal={} method={} path={} round={} {} status={}", p == null ? "-" : p, req.getMethod(),
                    req.getRequestURI(), round, target.contains("=") ? target : "run/gate=" + target, status);
        }
    }

    /** Runs one master pass on this pod (for external schedulers such as xxl-job; ADMIN token). Always 200: see skipped/skipReason. */
    @PostMapping("/master/tick")
    public io.github.amwtke.pipeliner.core.engine.TickResult masterTick() {
        return engine.tick();   // not audited per call (fires every second or so); rejected calls are audited by the security filter
    }

    @GetMapping("/cluster")
    public Map<String, Object> cluster() {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("podId", engine.podId());
        m.put("isLeader", engine.isLeader());
        m.put("liveInstances", engine.liveInstances());   // pipelines instantiated on this pod right now
        return m;
    }

    @ExceptionHandler(IllegalArgumentException.class)
    public ResponseEntity<Map<String, String>> badRequest(IllegalArgumentException e) {
        return error(HttpStatus.BAD_REQUEST, e);
    }

    @ExceptionHandler(Exception.class)
    public ResponseEntity<Map<String, String>> failure(Exception e) {
        return error(HttpStatus.INTERNAL_SERVER_ERROR, e);
    }

    private static ResponseEntity<Map<String, String>> error(HttpStatus s, Exception e) {
        String msg = e.getMessage() == null ? e.getClass().getSimpleName() : e.getMessage();
        return ResponseEntity.status(s).body(Collections.singletonMap("error", msg));
    }

    private static ResponseEntity<Map<String, String>> notFound(String msg) {
        return ResponseEntity.status(HttpStatus.NOT_FOUND).body(Collections.singletonMap("error", msg));
    }

    private static Map<String, String> ok() { return Collections.singletonMap("status", "ok"); }

    private static String blankToNull(String s) { return s == null || s.trim().isEmpty() ? null : s; }
}
