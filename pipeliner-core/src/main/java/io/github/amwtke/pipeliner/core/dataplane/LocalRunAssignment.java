package io.github.amwtke.pipeliner.core.dataplane;

/**
 * The single codec of the local-run assignment value stored under a run id: {@code pod|attempt}. The pod id may itself contain '|'
 * (the attempt is everything after the LAST bar). {@link #parse} never throws: a malformed value yields {@code null}.
 */
public final class LocalRunAssignment {
    public final String pod;
    public final int attempt;

    private LocalRunAssignment(String pod, int attempt) { this.pod = pod; this.attempt = attempt; }

    public static String format(String pod, int attempt) { return pod + "|" + attempt; }

    /** Null when the value is null, has no '|', has an empty pod or a non-numeric attempt. */
    public static LocalRunAssignment parse(String value) {
        if (value == null) return null;
        int i = value.lastIndexOf('|');
        if (i <= 0 || i == value.length() - 1) return null;
        try {
            return new LocalRunAssignment(value.substring(0, i), Integer.parseInt(value.substring(i + 1)));
        } catch (NumberFormatException e) {
            return null;
        }
    }

    /** Pod of the value, or null when malformed. */
    public static String podOf(String value) { LocalRunAssignment a = parse(value); return a == null ? null : a.pod; }
}
