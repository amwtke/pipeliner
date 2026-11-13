package io.github.amwtke.pipeliner.runtime.master;

import io.github.amwtke.pipeliner.runtime.Services;

/** Services plus the fencing epoch of the current leadership term. */
final class MasterContext {
    final Services s;
    private volatile long epoch;

    MasterContext(Services s) { this.s = s; }

    long epoch() { return epoch; }

    void epoch(long e) { this.epoch = e; }
}
