package io.github.amwtke.pipeliner.runtime;

import io.github.amwtke.pipeliner.api.PipelinerException;
import io.github.amwtke.pipeliner.core.codec.ChunkCodec;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;

/**
 * Engine side of the admin command queue: pushes a command (id + deadline = now + client timeout) for the leader and waits for its result.
 * On timeout the command is withdrawn from the queue; a command that was already popped is waited for briefly. The leader skips expired
 * commands, so a timed-out command is never executed later.
 */
final class AdminClient {
    private static final long POPPED_GRACE_MILLIS = 2000;
    private final Services s;

    AdminClient(Services s) { this.s = s; }

    void execute(String op, String roundId, String target, String operator) { execute(op, roundId, target, operator, false); }

    /** Blocks until the leader executed the command; throws PipelinerException with the leader's error or on timeout ("not executed"). */
    void execute(String op, String roundId, String target, String operator, boolean force) {
        Map<String, String> cmd = new LinkedHashMap<>();
        String id = UUID.randomUUID().toString();
        long timeout = s.cfg.getAdminTimeoutMillis();
        cmd.put("id", id);
        cmd.put("op", op);
        cmd.put("roundId", roundId);
        if (target != null) cmd.put("target", target);
        if (operator != null) cmd.put("operator", operator);
        if (force) cmd.put("force", "true");
        cmd.put("deadline", String.valueOf(System.currentTimeMillis() + timeout));
        String json = ChunkCodec.toJson(cmd);
        s.plane.pushCommand(json);
        try {
            if (await(id, op, timeout)) return;
            if (s.plane.withdrawCommand(json))
                throw new PipelinerException(op + " timed out after " + timeout + " ms: no leader executed the command; it was withdrawn (not executed)");
            // already popped by a leader: it is running or about to answer
            if (await(id, op, POPPED_GRACE_MILLIS)) return;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            s.plane.withdrawCommand(json);
            throw new PipelinerException(op + " interrupted while waiting for the leader");
        }
        throw new PipelinerException(op + " timed out after " + timeout + " ms: a leader picked the command up but did not answer; its outcome is unknown, check the round before retrying");
    }

    /** True when the command succeeded; throws when it failed; false when no result arrived within {@code millis}. */
    private boolean await(String id, String op, long millis) throws InterruptedException {
        long end = System.currentTimeMillis() + millis;
        while (true) {
            String res = s.plane.getCommandResult(id);
            if (res != null) {
                Map<String, String> r = ChunkCodec.toStringMap(res);
                if (!"true".equals(r.get("ok"))) throw new PipelinerException(r.get("error") == null ? op + " failed" : r.get("error"));
                return true;
            }
            if (System.currentTimeMillis() >= end) return false;
            Thread.sleep(100);
        }
    }
}
