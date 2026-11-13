package io.github.amwtke.pipeliner.spring;

import io.github.amwtke.pipeliner.core.engine.PipelinerEngine;
import io.github.amwtke.pipeliner.core.engine.RuntimeConfig;
import io.github.amwtke.pipeliner.core.plan.ExecutionPlan;
import io.github.amwtke.pipeliner.core.plan.PlanChannel;
import io.github.amwtke.pipeliner.core.store.ChunkQuery;
import io.github.amwtke.pipeliner.core.store.ChunkRecord;
import io.github.amwtke.pipeliner.core.store.ChunkStatus;
import io.github.amwtke.pipeliner.core.store.RoundView;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
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
    private final PipelinerEngine engine;
    private final RuntimeConfig cfg;

    public PipelinerAdminController(PipelinerEngine engine, RuntimeConfig cfg) {
        this.engine = engine;
        this.cfg = cfg;
    }

    @GetMapping("/graphs")
    public List<String> graphs() {
        return new ArrayList<>(engine.graphNames());
    }

    @GetMapping(value = "/graphs/{name}/plan", produces = MediaType.TEXT_PLAIN_VALUE)
    public String plan(@PathVariable String name) {
        return engine.plan(name).explain();
    }

    @PostMapping("/rounds")
    public Map<String, String> submit(@RequestBody Map<String, Object> body) {
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
    public Map<String, String> pause(@PathVariable String id) { engine.pause(id); return ok(); }

    @PostMapping("/rounds/{id}/resume")
    public Map<String, String> resume(@PathVariable String id) { engine.resume(id); return ok(); }

    @PostMapping("/rounds/{id}/cancel")
    public Map<String, String> cancel(@PathVariable String id) { engine.cancel(id); return ok(); }

    @PostMapping("/runs/{roundId}/{runId}/retry")
    public Map<String, String> retry(@PathVariable String roundId, @PathVariable String runId) { engine.retryRun(roundId, runId); return ok(); }

    @PostMapping("/runs/{roundId}/{runId}/rerun")
    public Map<String, String> rerun(@PathVariable String roundId, @PathVariable String runId) { engine.rerunFrom(roundId, runId); return ok(); }

    @PostMapping("/runs/{roundId}/{runId}/skip")
    public Map<String, String> skip(@PathVariable String roundId, @PathVariable String runId) { engine.skipDeadLetters(roundId, runId); return ok(); }

    @PostMapping("/gates/{roundId}/{gateId}/signal")
    public Map<String, String> signal(@PathVariable String roundId, @PathVariable String gateId, @RequestBody Map<String, Object> body) {
        Object key = body.get("key");
        if (key == null) throw new IllegalArgumentException("key is required");
        Object payload = body.get("payload");
        engine.signalGate(roundId, gateId, key.toString(), payload == null ? "" : payload.toString());
        return ok();
    }

    @PostMapping("/gates/{roundId}/{gateId}/open")
    public Map<String, String> open(@PathVariable String roundId, @PathVariable String gateId,
                                    @RequestParam(defaultValue = "admin") String operator) {
        engine.openGate(roundId, gateId, operator);
        return ok();
    }

    @GetMapping("/cluster")
    public Map<String, Object> cluster() {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("podId", engine.podId());
        m.put("isLeader", engine.isLeader());
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
