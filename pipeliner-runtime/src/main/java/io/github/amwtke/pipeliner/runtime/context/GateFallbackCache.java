package io.github.amwtke.pipeliner.runtime.context;

import io.github.amwtke.pipeliner.core.store.ControlPlaneStore;
import io.github.amwtke.pipeliner.core.store.GateRecord;
import io.github.amwtke.pipeliner.core.store.RoundView;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/** Short-lived per-round cache of gates that were opened by the OPEN_WITH_FALLBACK timeout policy. */
public final class GateFallbackCache {
    private static final long TTL_MILLIS = 5000;

    private static final class Entry {
        final long loadedAt;
        final Set<String> gates;
        Entry(long loadedAt, Set<String> gates) { this.loadedAt = loadedAt; this.gates = gates; }
    }

    private final ControlPlaneStore store;
    private final ConcurrentMap<String, Entry> cache = new ConcurrentHashMap<>();

    public GateFallbackCache(ControlPlaneStore store) { this.store = store; }

    public boolean isFallback(String roundId, String gateId) {
        long now = System.currentTimeMillis();
        Entry e = cache.get(roundId);
        if (e == null || now - e.loadedAt > TTL_MILLIS) {
            Set<String> gates = new HashSet<>();
            RoundView v = store.loadRoundState(roundId);
            if (v != null && v.getGates() != null)
                for (GateRecord g : v.getGates())
                    if (g.getContext() != null && "true".equals(g.getContext().get("fallback"))) gates.add(g.getGateId());
            e = new Entry(now, gates);
            cache.put(roundId, e);
        }
        return e.gates.contains(gateId);
    }
}
