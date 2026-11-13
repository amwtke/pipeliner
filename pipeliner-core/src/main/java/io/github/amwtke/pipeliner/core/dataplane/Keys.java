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
    public static String channel(String r, String edgeId) { return "pg:{" + r + "}:ch:" + edgeId; }
    public static String splitQueue(String r, String p) { return "pg:{" + r + "}:split:" + p; }
    public static String splitIndex(String r, String p) { return "pg:{" + r + "}:splitidx:" + p; }
    public static String dlq(String r, String p) { return "pg:{" + r + "}:dlq:" + p; }
    public static String journal(String r) { return "pg:{" + r + "}:journal"; }
}
