package io.github.amwtke.pipeliner.runtime.context;

import io.github.amwtke.pipeliner.core.store.ControlPlaneStore;
import io.github.amwtke.pipeliner.core.store.GateRecord;
import io.github.amwtke.pipeliner.core.store.GateStatus;
import io.github.amwtke.pipeliner.core.store.RoundView;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Short-lived per-round cache of whether an OPEN gate was opened by the OPEN_WITH_FALLBACK timeout policy. Only answers about gates that are
 * already OPEN are cached (an OPEN gate's answer cannot change until a reset): a gate that is still CLOSED answers false WITHOUT being cached,
 * so a gate that opens (with or without fallback) right afterwards is seen by the very next call and the answer never depends on timing. Entries
 * expire after a short TTL (a reset gate is re-read soon) and expired entries are swept when the map grows, so it stays bounded.
 */
public final class GateFallbackCache {
    private static final long TTL_MILLIS = 2000;
    private static final int SWEEP_ABOVE = 256;

    private static final class Entry {
        final long loadedAt;
        final Map<String, Boolean> openGates;   // gate id -> opened with fallback; only OPEN gates
        Entry(long loadedAt, Map<String, Boolean> openGates) { this.loadedAt = loadedAt; this.openGates = openGates; }
    }

    private final ControlPlaneStore store;
    private final ConcurrentMap<String, Entry> cache = new ConcurrentHashMap<>();

    public GateFallbackCache(ControlPlaneStore store) { this.store = store; }

    public boolean isFallback(String roundId, String gateId) {
        long now = System.currentTimeMillis();
        Entry e = cache.get(roundId);
        if (e != null && now - e.loadedAt <= TTL_MILLIS) {
            Boolean known = e.openGates.get(gateId);
            if (known != null) return known;
        }
        Map<String, Boolean> open = new HashMap<>();
        RoundView v = store.loadRoundState(roundId);
        if (v != null && v.getGates() != null)
            for (GateRecord g : v.getGates())
                if (g.getStatus() == GateStatus.OPEN) open.put(g.getGateId(), g.getContext() != null && "true".equals(g.getContext().get("fallback")));
        if (cache.size() > SWEEP_ABOVE) cache.values().removeIf(x -> now - x.loadedAt > TTL_MILLIS);
        cache.put(roundId, new Entry(now, open));
        Boolean r = open.get(gateId);
        return r != null && r;
    }

    /** Drops the round's entry (round finished or reset). */
    public void evict(String roundId) { cache.remove(roundId); }
}
