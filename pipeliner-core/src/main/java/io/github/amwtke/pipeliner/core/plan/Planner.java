package io.github.amwtke.pipeliner.core.plan;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.amwtke.pipeliner.api.GateCondition;
import io.github.amwtke.pipeliner.api.PipelinerException;
import io.github.amwtke.pipeliner.api.Sink;
import io.github.amwtke.pipeliner.core.graph.EdgeSpec;
import io.github.amwtke.pipeliner.core.graph.ExecMode;
import io.github.amwtke.pipeliner.core.graph.GateSpec;
import io.github.amwtke.pipeliner.core.graph.GateType;
import io.github.amwtke.pipeliner.core.graph.PipelineGraph;
import io.github.amwtke.pipeliner.core.graph.PipelineSpec;
import io.github.amwtke.pipeliner.core.graph.TimeoutPolicy;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

/** Turns a PipelineGraph into an ExecutionPlan. */
public final class Planner {
    private static final ObjectMapper CANON = new ObjectMapper();

    private final PlanOptions options;

    public Planner(PlanOptions o) { this.options = o == null ? PlanOptions.defaults() : o; }

    /** Internal gate (declared or auto). */
    private static final class G {
        String id; GateType type; boolean auto; String dataset; GateCondition condition;
        List<String> after; List<String> before; long timeout; TimeoutPolicy policy;
    }

    public ExecutionPlan plan(PipelineGraph g) {
        final Map<String, PipelineSpec> specs = g.getPipelines();
        final List<String> ids = new ArrayList<>(specs.keySet());
        final Map<String, Integer> idx = new HashMap<>();
        for (int i = 0; i < ids.size(); i++) idx.put(ids.get(i), i);
        final List<EdgeSpec> edges = g.getEdges();

        validate(specs, edges, g);

        // 2. gates (declared first, then auto barriers for materialized datasets)
        List<G> gates = new ArrayList<>();
        Set<String> gateIds = new HashSet<>();
        for (GateSpec s : g.getGates()) {
            G x = new G();
            x.id = s.getId(); x.type = s.getType(); x.condition = s.getCondition();
            x.after = sortedByDecl(s.getAfter(), idx); x.before = sortedByDecl(s.getBefore(), idx);
            x.timeout = s.getTimeoutMillis(); x.policy = s.getOnTimeout() == null ? TimeoutPolicy.FAIL : s.getOnTimeout();
            gates.add(x);
            gateIds.add(x.id);
        }
        Map<String, Set<String>> writers = new TreeMap<>(), readers = new TreeMap<>();
        for (String pid : ids) {
            PipelineSpec p = specs.get(pid);
            for (String d : readDatasets(p)) {
                Set<String> w = writers.get(d);
                if (w == null) writers.put(d, w = new LinkedHashSet<>());
                for (String q : ids) if (!q.equals(pid) && writesDataset(specs.get(q), d)) w.add(q);
                Set<String> r = readers.get(d);
                if (r == null) readers.put(d, r = new LinkedHashSet<>());
                r.add(pid);
            }
        }
        for (Map.Entry<String, Set<String>> e : writers.entrySet()) {
            if (e.getValue().isEmpty()) continue;
            G x = new G();
            x.id = "barrier:" + e.getKey(); x.type = GateType.BARRIER; x.auto = true; x.dataset = e.getKey();
            x.after = sortedByDecl(e.getValue(), idx); x.before = sortedByDecl(readers.get(e.getKey()), idx);
            x.timeout = 0; x.policy = TimeoutPolicy.FAIL;
            if (!gateIds.add(x.id)) throw new PlanException("gate id collides with auto gate: " + x.id);
            gates.add(x);
        }

        // 3. regions (union-find over stream edges)
        int[] uf = new int[ids.size()];
        for (int i = 0; i < uf.length; i++) uf[i] = i;
        for (EdgeSpec e : edges) uf[find(uf, idx.get(e.getFromPipeline()))] = find(uf, idx.get(e.getToPipeline()));

        // 4. merge regions into runs by in-gate set
        Map<Integer, TreeSet<String>> regionKey = new HashMap<>();
        for (G x : gates)
            for (String b : x.before) {
                int r = find(uf, idx.get(b));
                TreeSet<String> k = regionKey.get(r);
                if (k == null) regionKey.put(r, k = new TreeSet<>());
                k.add(x.id);
            }
        Map<List<String>, Integer> keyToRun = new LinkedHashMap<>();
        List<List<String>> runPipes = new ArrayList<>();   // declaration order within each
        List<List<String>> runGates = new ArrayList<>();
        int[] runOf = new int[ids.size()];
        for (int i = 0; i < ids.size(); i++) {
            TreeSet<String> k = regionKey.get(find(uf, i));
            List<String> key = k == null ? new ArrayList<String>() : new ArrayList<>(k);
            Integer r = keyToRun.get(key);
            if (r == null) {
                r = runPipes.size();
                keyToRun.put(key, r);
                runPipes.add(new ArrayList<String>());
                runGates.add(key);
            }
            runPipes.get(r).add(ids.get(i));
            runOf[i] = r;
        }
        int n = runPipes.size();

        // 5. deadlock check
        for (G x : gates) {
            for (String a : x.after)
                for (String b : x.before)
                    if (runOf[idx.get(a)] == runOf[idx.get(b)]) {
                        boolean direct = false;
                        for (EdgeSpec e : edges) if (e.getFromPipeline().equals(a) && e.getToPipeline().equals(b)) direct = true;
                        throw new PlanException("gate " + x.id + ": after (" + a + ") and before (" + b + ") are in the same run; change the stream edge "
                                + a + "->" + b + (direct ? "" : " (or the stream path connecting them)") + " into a materialized dataset");
                    }
        }

        // 6. run dependencies, stages
        Map<String, G> gateById = new LinkedHashMap<>();
        for (G x : gates) gateById.put(x.id, x);
        List<Set<Integer>> deps = new ArrayList<>();
        for (int r = 0; r < n; r++) {
            Set<Integer> d = new TreeSet<>();
            for (String gid : runGates.get(r)) for (String a : gateById.get(gid).after) d.add(runOf[idx.get(a)]);
            deps.add(d);
        }
        int[] stage = new int[n];
        int done = 0;
        boolean[] fin = new boolean[n];
        boolean progress = true;
        while (progress) {
            progress = false;
            for (int r = 0; r < n; r++) {
                if (fin[r]) continue;
                int st = 1;
                boolean ready = true;
                for (int d : deps.get(r)) {
                    if (!fin[d]) { ready = false; break; }
                    st = Math.max(st, stage[d] + 1);
                }
                if (ready) { fin[r] = true; stage[r] = st; done++; progress = true; }
            }
        }
        if (done < n) {
            List<String> stuck = new ArrayList<>();
            for (int r = 0; r < n; r++) if (!fin[r]) stuck.add(runPipes.get(r).toString());
            throw new PlanException("cyclic dependency between runs through gates/datasets: " + stuck);
        }
        List<Integer> order = new ArrayList<>();
        for (int r = 0; r < n; r++) order.add(r);
        final int[] stageF = stage;
        Collections.sort(order, (a, b) -> stageF[a] != stageF[b] ? Integer.compare(stageF[a], stageF[b]) : Integer.compare(a, b));
        int[] newId = new int[n];   // old index -> position
        for (int i = 0; i < n; i++) newId[order.get(i)] = i;
        String[] runName = new String[n];
        for (int r = 0; r < n; r++) runName[r] = "R" + (newId[r] + 1);

        // channels
        List<PlanChannel> channels = new ArrayList<>();
        Map<String, Integer> outCount = new HashMap<>(), inCount = new HashMap<>();
        for (int i = 0; i < edges.size(); i++) {
            EdgeSpec e = edges.get(i);
            channels.add(PlanChannel.builder().edgeId("e" + (i + 1)).ordinal(i + 1).fromPipeline(e.getFromPipeline())
                    .fromPort(e.getFromPort()).toPipeline(e.getToPipeline()).runId(runName[runOf[idx.get(e.getFromPipeline())]]).build());
            inc(outCount, e.getFromPipeline());
            inc(inCount, e.getToPipeline());
        }

        // runs
        List<PlanRun> runs = new ArrayList<>();
        for (int pos = 0; pos < n; pos++) {
            int r = order.get(pos);
            List<String> topo = topo(runPipes.get(r), edges, idx);
            List<String> inGates = new ArrayList<>();
            for (G x : gates) if (runGates.get(r).contains(x.id)) inGates.add(x.id);
            List<String> up = new ArrayList<>();
            for (int d : deps.get(r)) up.add(runName[d]);
            Collections.sort(up, (a, b) -> Integer.compare(Integer.parseInt(a.substring(1)), Integer.parseInt(b.substring(1))));
            runs.add(PlanRun.builder().runId(runName[r]).stage(stage[r]).mode(selectMode(runPipes.get(r), specs, inCount, outCount))
                    .pipelineIds(topo).inGateIds(inGates).upstreamRunIds(up).build());
        }

        // plan pipelines
        Map<String, PlanPipeline> pipes = new LinkedHashMap<>();
        for (String pid : ids) {
            PipelineSpec s = specs.get(pid);
            List<String> inE = new ArrayList<>(), ups = new ArrayList<>();
            Map<String, String> p2e = new LinkedHashMap<>();
            for (PlanChannel c : channels) {
                if (c.getToPipeline().equals(pid)) {
                    inE.add(c.getEdgeId());
                    if (!ups.contains(c.getFromPipeline())) ups.add(c.getFromPipeline());
                }
                if (c.getFromPipeline().equals(pid)) p2e.put(c.getFromPort(), c.getEdgeId());
            }
            pipes.put(pid, PlanPipeline.builder().pipelineId(pid).runId(runName[runOf[idx.get(pid)]]).source(s.getSource() != null)
                    .parallelism(s.getParallelism() != null ? s.getParallelism() : options.getDefaultParallelism())
                    .chunkRows(s.getChunkRows()).chunkBytes(s.getChunkBytes()).chunkKey(new ArrayList<>(s.getChunkKey()))
                    .inputEdgeIds(inE).portToEdge(p2e).upstreamPipelines(ups).build());
        }

        // plan gates
        List<PlanGate> planGates = new ArrayList<>();
        for (G x : gates) {
            planGates.add(PlanGate.builder().gateId(x.id).type(x.type).auto(x.auto).dataset(x.dataset)
                    .afterPipelines(x.after).afterRuns(runsOf(x.after, runOf, idx, newId)).beforePipelines(x.before)
                    .beforeRuns(runsOf(x.before, runOf, idx, newId)).timeoutMillis(x.timeout).onTimeout(x.policy).build());
        }

        int stages = 0;
        for (PlanRun r : runs) stages = Math.max(stages, r.getStage());
        ExecutionPlan plan = ExecutionPlan.builder().graphName(g.getName()).runs(runs).gates(planGates).channels(channels)
                .pipelines(pipes).stageCount(stages).graph(g).hash(hash(g, specs, gates, channels, runs)).build();
        return plan;
    }

    // ---------------------------------------------------------------- validation

    private void validate(Map<String, PipelineSpec> specs, List<EdgeSpec> edges, PipelineGraph g) {
        Set<String> usedPorts = new HashSet<>();
        Map<String, Integer> in = new HashMap<>();
        for (EdgeSpec e : edges) {
            PipelineSpec from = specs.get(e.getFromPipeline());
            PipelineSpec to = specs.get(e.getToPipeline());
            if (from == null || to == null) throw new PlanException("edge " + e.getFromPipeline() + " -> " + e.getToPipeline() + ": unknown pipeline");
            if (!from.getPorts().contains(e.getFromPort()))
                throw new PlanException("edge " + e.getFromPipeline() + "." + e.getFromPort() + " -> " + e.getToPipeline()
                        + ": port '" + e.getFromPort() + "' is not declared on pipeline " + e.getFromPipeline() + " " + from.getPorts());
            if (e.getFromPipeline().equals(e.getToPipeline())) throw new PlanException("edge " + e.getFromPipeline() + " -> itself: self loop");
            if (!usedPorts.add(e.getFromPipeline() + "." + e.getFromPort()))
                throw new PlanException("port " + e.getFromPipeline() + "." + e.getFromPort() + " has more than one outgoing stream edge");
            if (to.getSource() != null)
                throw new PlanException("pipeline " + e.getToPipeline() + " has a source and cannot have an incoming stream edge (from " + e.getFromPipeline() + ")");
            inc(in, e.getToPipeline());
        }
        for (PipelineSpec p : specs.values()) {
            if (p.getSource() == null && !in.containsKey(p.getId()))
                throw new PlanException("pipeline " + p.getId() + " has no source and no incoming stream edge");
            if (p.runnerCount() > 1)
                throw new PlanException("pipeline " + p.getId() + " sets both a row runner and a batch runner, or more than one runner (instance or type)");
            if (p.isPerThread() && !p.lazyRunner())
                throw new PlanException("pipeline " + p.getId() + ": runnerPerThread() needs a type-based runner (row(Class) / batch(Class))");
        }
        for (GateSpec s : g.getGates()) {
            if (s.getAfter().isEmpty()) throw new PlanException("gate " + s.getId() + ": 'after' must not be empty");
            if (s.getBefore().isEmpty()) throw new PlanException("gate " + s.getId() + ": 'before' must not be empty");
            for (String p : s.getAfter()) if (!specs.containsKey(p)) throw new PlanException("gate " + s.getId() + ": unknown pipeline " + p);
            for (String p : s.getBefore()) if (!specs.containsKey(p)) throw new PlanException("gate " + s.getId() + ": unknown pipeline " + p);
        }
    }

    // ---------------------------------------------------------------- helpers

    private static Set<String> readDatasets(PipelineSpec p) {
        Set<String> d = new LinkedHashSet<>();
        if (p.getSource() != null) {
            if (p.getSource().datasetName() != null) d.add(p.getSource().datasetName());
            if (p.getSource() instanceof ReadsDatasets) d.addAll(((ReadsDatasets) p.getSource()).readDatasets());
        }
        return d;
    }

    private static boolean writesDataset(PipelineSpec p, String d) {
        for (List<Sink> l : p.getSinks().values()) for (Sink s : l) if (s != null && d.equals(s.datasetName())) return true;
        return false;
    }

    private static List<String> sortedByDecl(java.util.Collection<String> c, final Map<String, Integer> idx) {
        List<String> l = new ArrayList<>(new LinkedHashSet<>(c));
        Collections.sort(l, (a, b) -> Integer.compare(idx.get(a), idx.get(b)));
        return l;
    }

    private static int find(int[] uf, int x) {
        while (uf[x] != x) { uf[x] = uf[uf[x]]; x = uf[x]; }
        return x;
    }

    private static void inc(Map<String, Integer> m, String k) {
        Integer v = m.get(k);
        m.put(k, v == null ? 1 : v + 1);
    }

    private static List<String> runsOf(List<String> pipes, int[] runOf, Map<String, Integer> idx, int[] newId) {
        TreeSet<Integer> s = new TreeSet<>();
        for (String p : pipes) s.add(newId[runOf[idx.get(p)]] + 1);
        List<String> l = new ArrayList<>();
        for (int i : s) l.add("R" + i);
        return l;
    }

    /** Kahn topological order over stream edges inside a run; ties by declaration order. */
    private static List<String> topo(List<String> members, List<EdgeSpec> edges, Map<String, Integer> idx) {
        Set<String> set = new HashSet<>(members);
        Map<String, Integer> indeg = new HashMap<>();
        for (String m : members) indeg.put(m, 0);
        for (EdgeSpec e : edges) if (set.contains(e.getFromPipeline()) && set.contains(e.getToPipeline())) inc(indeg, e.getToPipeline());
        List<String> out = new ArrayList<>();
        Set<String> left = new LinkedHashSet<>(members);
        while (!left.isEmpty()) {
            String pick = null;
            for (String m : left) if (indeg.get(m) == 0) { pick = m; break; }   // members are in declaration order
            if (pick == null) throw new PlanException("cycle through stream edges among pipelines " + left);
            left.remove(pick);
            out.add(pick);
            for (EdgeSpec e : edges) if (e.getFromPipeline().equals(pick) && set.contains(e.getToPipeline())) indeg.put(e.getToPipeline(), indeg.get(e.getToPipeline()) - 1);
        }
        return out;
    }

    private ExecMode selectMode(List<String> members, Map<String, PipelineSpec> specs, Map<String, Integer> inCount, Map<String, Integer> outCount) {
        ExecMode hint = null;
        for (String m : members) {
            ExecMode h = specs.get(m).getMode();
            if (h != null && (hint == null || h.ordinal() > hint.ordinal())) hint = h;
        }
        if (hint != null) return hint;
        if (options.getForceMode() != null) return options.getForceMode();
        long rows = 0;
        boolean linear = true;
        for (String m : members) {
            long e = specs.get(m).getEstimatedRows();
            if (e < 0) return ExecMode.CLUSTER;
            rows += e;
            if (valueOf(inCount, m) > 1 || valueOf(outCount, m) > 1) linear = false;
        }
        if (linear && rows <= options.getInlineMaxRows()) return ExecMode.INLINE;
        if (rows <= options.getLocalMaxRows()) return ExecMode.LOCAL;
        return ExecMode.CLUSTER;
    }

    private static int valueOf(Map<String, Integer> m, String k) {
        Integer v = m.get(k);
        return v == null ? 0 : v;
    }

    // ---------------------------------------------------------------- hash

    private static String cn(Object o) {
        return o == null ? null : stripLambda(o.getClass().getName());
    }

    private static String stripLambda(String n) {
        int i = n.indexOf("$$Lambda");
        return i >= 0 ? n.substring(0, i) + "$$Lambda" : n;
    }

    private static Map<String, Object> condition(GateCondition c) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("class", cn(c));
        if (c == null) return m;
        Map<String, Object> fields = new TreeMap<>();
        for (Field f : c.getClass().getDeclaredFields()) {
            if (Modifier.isStatic(f.getModifiers()) || f.isSynthetic()) continue;
            Class<?> t = f.getType();
            if (!(t.isPrimitive() || t == String.class || Number.class.isAssignableFrom(t) || t == Boolean.class)) continue;
            try {
                f.setAccessible(true);
                fields.put(f.getName(), f.get(c));
            } catch (Exception | LinkageError ignore) {
                // not hashable; class name only
            }
        }
        if (!fields.isEmpty()) m.put("params", fields);
        return m;
    }

    private String hash(PipelineGraph g, Map<String, PipelineSpec> specs, List<G> gates, List<PlanChannel> channels, List<PlanRun> runs) {
        Map<String, Object> root = new LinkedHashMap<>();
        root.put("graph", g.getName());
        List<Object> ps = new ArrayList<>();
        for (PipelineSpec s : specs.values()) {
            Map<String, Object> p = new LinkedHashMap<>();
            p.put("id", s.getId());
            p.put("source", s.getSource() == null ? null : cn(s.getSource()));
            p.put("sourceDataset", s.getSource() == null ? null : s.getSource().datasetName());
            p.put("runner", s.hasRunner() ? s.runnerKind() + ":" + stripLambda(s.runnerClassName()) : null);
            if (s.isPerThread()) p.put("perThread", true);   // only when set, so instance-based hashes stay unchanged
            p.put("ports", new ArrayList<>(s.getPorts()));
            Map<String, Object> sinks = new LinkedHashMap<>();
            for (String port : new TreeSet<>(s.getSinks().keySet())) {
                List<Object> l = new ArrayList<>();
                for (Sink k : s.getSinks().get(port)) {
                    Map<String, Object> sm = new LinkedHashMap<>();
                    sm.put("class", cn(k));
                    sm.put("dataset", k == null ? null : k.datasetName());
                    l.add(sm);
                }
                sinks.put(port, l);
            }
            p.put("sinks", sinks);
            p.put("parallelism", s.getParallelism() != null ? s.getParallelism() : options.getDefaultParallelism());
            p.put("chunkRows", s.getChunkRows());
            p.put("chunkBytes", s.getChunkBytes());
            p.put("chunkKey", new ArrayList<>(s.getChunkKey()));
            ps.add(p);
        }
        root.put("pipelines", ps);
        List<Object> cs = new ArrayList<>();
        for (PlanChannel c : channels) cs.add(Arrays.asList(c.getEdgeId(), c.getFromPipeline(), c.getFromPort(), c.getToPipeline()));
        root.put("channels", cs);
        List<Object> gs = new ArrayList<>();
        for (G x : gates) {
            Map<String, Object> m = new LinkedHashMap<>();
            m.put("id", x.id); m.put("type", x.type.name()); m.put("auto", x.auto); m.put("dataset", x.dataset);
            m.put("condition", condition(x.condition));
            m.put("after", x.after); m.put("before", x.before); m.put("timeout", x.timeout); m.put("policy", x.policy.name());
            gs.add(m);
        }
        root.put("gates", gs);
        List<Object> rs = new ArrayList<>();
        for (PlanRun r : runs) {
            Map<String, Object> m = new LinkedHashMap<>();
            m.put("id", r.getRunId()); m.put("stage", r.getStage()); m.put("mode", r.getMode().name());
            m.put("pipelines", r.getPipelineIds()); m.put("inGates", r.getInGateIds());
            rs.add(m);
        }
        root.put("runs", rs);
        try {
            byte[] json = CANON.writeValueAsString(root).getBytes(StandardCharsets.UTF_8);
            byte[] d = MessageDigest.getInstance("SHA-256").digest(json);
            StringBuilder sb = new StringBuilder();
            for (byte b : d) sb.append(String.format("%02x", b));
            return sb.substring(0, 32);
        } catch (Exception e) {
            throw new PipelinerException("cannot hash plan", e);
        }
    }
}
