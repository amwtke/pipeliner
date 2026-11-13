package io.github.amwtke.pipeliner.core.dataplane;

/** Redis key builders. Per-round keys share the hash tag {roundId}. */
public final class Keys {
    public static final String GROUP = "pg";

    private Keys() {}

    public static String leader() { return "pg:leader"; }
    public static String epoch() { return "pg:epoch"; }
    public static String workers() { return "pg:workers"; }
    public static String workerInfo(String pod) { return "pg:worker:" + pod; }
    public static String activeRounds() { return "pg:rounds:active"; }
    public static String commands() { return "pg:commands"; }
    public static String commandResult(String id) { return "pg:cmdres:" + id; }
    public static String throttle() { return "pg:throttle"; }
    public static String desired(String r) { return "pg:{" + r + "}:desired"; }
    public static String assign(String r) { return "pg:{" + r + "}:assign"; }
    public static String paused(String r) { return "pg:{" + r + "}:paused"; }
    /** Highest master epoch that fenced the round's data plane (see DataPlane.fenceRound). */
    public static String fence(String r) { return "pg:{" + r + "}:fence"; }
    public static String roundSeq() { return "pg:roundseq"; }
    /** Data keys are scoped by the run attempt: a rerun never shares a key with an older attempt. */
    public static String channel(String r, String edgeId, int attempt) { return "pg:{" + r + "}:ch:" + edgeId + ":" + attempt; }
    public static String splitQueue(String r, String p, int attempt) { return "pg:{" + r + "}:split:" + p + ":" + attempt; }
    public static String splitIndex(String r, String p, int attempt) { return "pg:{" + r + "}:splitidx:" + p + ":" + attempt; }
    public static String dlq(String r, String p, int attempt) { return "pg:{" + r + "}:dlq:" + p + ":" + attempt; }
    public static String journal(String r) { return "pg:{" + r + "}:journal"; }
}
