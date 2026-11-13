package io.github.amwtke.pipeliner.core.engine;

/** Outcome of one {@link PipelinerEngine#tick()} call (JSON friendly: plain getters). */
public final class TickResult {
    public static final String NOT_LEADER = "not leader";
    public static final String IN_PROGRESS = "tick in progress";
    public static final String MASTER_DISABLED = "master disabled";

    private final boolean leader;
    private final long epoch;
    private final int roundsTicked;
    private final int commandsExecuted;
    private final long durationMillis;
    private final boolean skipped;
    private final String skipReason;

    public TickResult(boolean leader, long epoch, int roundsTicked, int commandsExecuted, long durationMillis, boolean skipped, String skipReason) {
        this.leader = leader;
        this.epoch = epoch;
        this.roundsTicked = roundsTicked;
        this.commandsExecuted = commandsExecuted;
        this.durationMillis = durationMillis;
        this.skipped = skipped;
        this.skipReason = skipReason;
    }

    public static TickResult skipped(String reason, boolean leader, long epoch, long durationMillis) {
        return new TickResult(leader, epoch, 0, 0, durationMillis, true, reason);
    }

    /** True when this pod holds the lease after the tick. */
    public boolean isLeader() { return leader; }
    /** Epoch this pod leads with (last known epoch when not leader). */
    public long getEpoch() { return epoch; }
    public int getRoundsTicked() { return roundsTicked; }
    public int getCommandsExecuted() { return commandsExecuted; }
    public long getDurationMillis() { return durationMillis; }
    /** True when nothing was advanced; see {@link #getSkipReason()}. */
    public boolean isSkipped() { return skipped; }
    /** Null unless skipped: "not leader", "tick in progress", "master disabled", ... */
    public String getSkipReason() { return skipReason; }

    @Override public String toString() {
        return "TickResult{leader=" + leader + ", epoch=" + epoch + ", roundsTicked=" + roundsTicked + ", commandsExecuted=" + commandsExecuted
                + ", durationMillis=" + durationMillis + ", skipped=" + skipped + (skipReason == null ? "" : ", skipReason=" + skipReason) + "}";
    }
}
