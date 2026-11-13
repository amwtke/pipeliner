package io.github.amwtke.pipeliner.api;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/** A slice of a bounded source; id assigned by the framework (S0000, S0001, ...). */
public final class Split {
    private final String splitId;
    private final Map<String, String> spec;

    public Split(String splitId, Map<String, String> spec) {
        this.splitId = splitId;
        this.spec = Collections.unmodifiableMap(spec == null ? new LinkedHashMap<String, String>() : new LinkedHashMap<>(spec));
    }

    public String getSplitId() { return splitId; }
    public Map<String, String> getSpec() { return spec; }
    public Split withId(String id) { return new Split(id, spec); }

    @Override public String toString() { return "Split[" + splitId + ", " + spec + "]"; }
}
