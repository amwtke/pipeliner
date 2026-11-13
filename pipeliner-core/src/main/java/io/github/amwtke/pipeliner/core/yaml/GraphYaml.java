package io.github.amwtke.pipeliner.core.yaml;

import io.github.amwtke.pipeliner.api.BatchRunner;
import io.github.amwtke.pipeliner.api.GateCondition;
import io.github.amwtke.pipeliner.api.PipelinerException;
import io.github.amwtke.pipeliner.api.RowRunner;
import io.github.amwtke.pipeliner.api.Sink;
import io.github.amwtke.pipeliner.api.SplitSource;
import io.github.amwtke.pipeliner.core.graph.ExecMode;
import io.github.amwtke.pipeliner.core.graph.GateDef;
import io.github.amwtke.pipeliner.core.graph.Gates;
import io.github.amwtke.pipeliner.core.graph.PipelineGraph;
import io.github.amwtke.pipeliner.core.graph.TimeoutPolicy;
import java.io.InputStream;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.yaml.snakeyaml.Yaml;

/** Loads a PipelineGraph from YAML. */
public final class GraphYaml {
    private static final Pattern DURATION = Pattern.compile("(\\d+)\\s*(ms|s|m|h|d)");

    private GraphYaml() {}

    public static PipelineGraph load(InputStream in, ComponentResolver resolver) {
        Object doc = new Yaml().load(in);
        if (!(doc instanceof Map)) throw new PipelinerException("graph yaml: top level must be a mapping");
        Map<?, ?> root = (Map<?, ?>) doc;
        Object name = root.get("graph");
        if (name == null) name = root.get("name");
        if (name == null) throw new PipelinerException("graph yaml: missing 'graph' name");
        PipelineGraph.Builder b = PipelineGraph.builder(String.valueOf(name));
        for (Object po : list(root.get("pipelines"))) {
            final Map<?, ?> p = map(po, "pipeline");
            final String id = str(p.get("id"));
            if (id == null) throw new PipelinerException("graph yaml: pipeline without id");
            b.pipeline(id, pb -> configure(pb, p, id, resolver));
        }
        for (Object eo : list(root.get("edges"))) {
            Map<?, ?> e = map(eo, "edge");
            b.edge(req(e, "from", "edge"), req(e, "to", "edge"));
        }
        for (Object go : list(root.get("gates"))) gate(b, map(go, "gate"), resolver);
        return b.build();
    }

    private static void configure(PipelineGraph.PipelineBuilder pb, Map<?, ?> p, String id, ComponentResolver resolver) {
        if (p.get("source") != null) pb.source(bean(resolver, str(p.get("source")), SplitSource.class, "pipeline " + id + " source"));
        Object runner = p.get("runner");
        if (runner != null) {
            Map<?, ?> r = map(runner, "runner of " + id);
            String type = String.valueOf(r.get("type")).toLowerCase(Locale.ROOT);
            String beanName = req(r, "bean", "runner of " + id);
            if ("row".equals(type)) pb.row(bean(resolver, beanName, RowRunner.class, "pipeline " + id + " runner"));
            else if ("batch".equals(type)) pb.batch(bean(resolver, beanName, BatchRunner.class, "pipeline " + id + " runner"));
            else throw new PipelinerException("pipeline " + id + ": runner type must be row or batch, got " + type);
        }
        List<Object> ports = list(p.get("ports"));
        if (!ports.isEmpty()) {
            String[] arr = new String[ports.size()];
            for (int i = 0; i < arr.length; i++) arr[i] = String.valueOf(ports.get(i));
            pb.ports(arr);
        }
        Object sinks = p.get("sinks");
        if (sinks != null) {
            for (Map.Entry<?, ?> e : map(sinks, "sinks of " + id).entrySet()) {
                for (Object s : list(e.getValue()))
                    pb.sink(String.valueOf(e.getKey()), bean(resolver, String.valueOf(s), Sink.class, "pipeline " + id + " sink"));
            }
        }
        if (p.get("parallelism") != null) pb.parallelism(num(p.get("parallelism")).intValue());
        if (p.get("mode") != null) pb.mode(ExecMode.valueOf(str(p.get("mode")).toUpperCase(Locale.ROOT)));
        if (p.get("estimatedRows") != null) pb.estimatedRows(num(p.get("estimatedRows")).longValue());
        if (p.get("chunkRows") != null) pb.chunkRows(num(p.get("chunkRows")).intValue());
        if (p.get("chunkBytes") != null) pb.chunkBytes(num(p.get("chunkBytes")).intValue());
        List<Object> ck = list(p.get("chunkKey"));
        if (!ck.isEmpty()) {
            String[] arr = new String[ck.size()];
            for (int i = 0; i < arr.length; i++) arr[i] = String.valueOf(ck.get(i));
            pb.chunkKey(arr);
        }
    }

    private static void gate(PipelineGraph.Builder b, final Map<?, ?> g, ComponentResolver resolver) {
        final String id = req(g, "id", "gate");
        String type = req(g, "type", "gate " + id).toLowerCase(Locale.ROOT);
        String condName = str(g.get("condition"));
        GateDef def;
        switch (type) {
            case "barrier": def = Gates.barrier(); break;
            case "manual": def = Gates.manual(); break;
            case "condition":
                if (condName == null) throw new PipelinerException("gate " + id + ": condition gate needs 'condition'");
                def = Gates.condition(bean(resolver, condName, GateCondition.class, "gate " + id + " condition"));
                break;
            case "external":
                if (g.get("expectFrom") != null) def = Gates.externalCountOf(str(g.get("expectFrom")));
                else if (condName != null) def = Gates.external(bean(resolver, condName, GateCondition.class, "gate " + id + " condition"));
                else if (g.get("expectCount") != null) def = Gates.externalCount(num(g.get("expectCount")).intValue());
                else throw new PipelinerException("gate " + id + ": external gate needs expectFrom, expectCount or condition");
                break;
            case "timer":
                if (g.get("at") != null) def = Gates.timerAt(num(g.get("at")).longValue());
                else if (g.get("delay") != null) def = Gates.delay(duration(g.get("delay")));
                else throw new PipelinerException("gate " + id + ": timer gate needs 'at' or 'delay'");
                break;
            default: throw new PipelinerException("gate " + id + ": unknown type " + type);
        }
        final String[] after = strings(g.get("after")), before = strings(g.get("before"));
        final Object to = g.get("timeout");
        final Object pol = g.get("onTimeout");
        b.gate(id, def, gb -> {
            gb.after(after).before(before);
            if (to != null || pol != null)
                gb.timeout(to == null ? Duration.ZERO : duration(to),
                        pol == null ? TimeoutPolicy.FAIL : TimeoutPolicy.valueOf(str(pol).toUpperCase(Locale.ROOT)));
        });
    }

    /** Parses 2h, 30m, 10s, 500ms, 1d (also sums like "1h30m"); a bare number means milliseconds. */
    static Duration duration(Object o) {
        if (o instanceof Number) return Duration.ofMillis(((Number) o).longValue());
        String s = String.valueOf(o).trim().toLowerCase(Locale.ROOT);
        Matcher m = DURATION.matcher(s);
        long ms = 0;
        int end = 0;
        while (m.find()) {
            if (!s.substring(end, m.start()).trim().isEmpty()) throw new PipelinerException("bad duration: " + o);
            long v = Long.parseLong(m.group(1));
            switch (m.group(2)) {
                case "ms": ms += v; break;
                case "s": ms += v * 1000; break;
                case "m": ms += v * 60_000; break;
                case "h": ms += v * 3_600_000; break;
                default: ms += v * 86_400_000L;
            }
            end = m.end();
        }
        if (end == 0 || !s.substring(end).trim().isEmpty()) {
            if (s.matches("\\d+")) return Duration.ofMillis(Long.parseLong(s));
            throw new PipelinerException("bad duration: " + o);
        }
        return Duration.ofMillis(ms);
    }

    private static <T> T bean(ComponentResolver r, String name, Class<T> type, String what) {
        Object o = r.resolve(name);
        if (o == null) throw new PipelinerException(what + ": bean '" + name + "' not found");
        if (!type.isInstance(o)) throw new PipelinerException(what + ": bean '" + name + "' is " + o.getClass().getName() + ", expected " + type.getSimpleName());
        return type.cast(o);
    }

    private static String[] strings(Object o) {
        List<Object> l = list(o);
        String[] a = new String[l.size()];
        for (int i = 0; i < a.length; i++) a[i] = String.valueOf(l.get(i));
        return a;
    }

    @SuppressWarnings("unchecked")
    private static List<Object> list(Object o) {
        if (o == null) return Collections.emptyList();
        if (o instanceof Collection) return new ArrayList<>((Collection<Object>) o);
        return Collections.singletonList(o);
    }

    private static Map<?, ?> map(Object o, String what) {
        if (!(o instanceof Map)) throw new PipelinerException("graph yaml: " + what + " must be a mapping");
        return (Map<?, ?>) o;
    }

    private static String str(Object o) { return o == null ? null : String.valueOf(o); }

    private static String req(Map<?, ?> m, String key, String what) {
        Object v = m.get(key);
        if (v == null) throw new PipelinerException("graph yaml: " + what + " missing '" + key + "'");
        return String.valueOf(v);
    }

    private static Number num(Object o) {
        if (o instanceof Number) return (Number) o;
        try { return Long.valueOf(String.valueOf(o).trim()); }
        catch (NumberFormatException e) { throw new PipelinerException("graph yaml: not a number: " + o); }
    }
}
