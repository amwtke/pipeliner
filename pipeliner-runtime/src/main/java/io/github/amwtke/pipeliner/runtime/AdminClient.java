package io.github.amwtke.pipeliner.runtime;

import io.github.amwtke.pipeliner.api.PipelinerException;
import io.github.amwtke.pipeliner.core.codec.ChunkCodec;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;

/** Engine side of the admin command queue: pushes a command for the leader and waits for its result. */
final class AdminClient {
    private final Services s;

    AdminClient(Services s) { this.s = s; }

    /** Blocks until the leader executed the command; throws PipelinerException with the leader's error or on timeout. */
    void execute(String op, String roundId, String target, String operator) {
        Map<String, String> cmd = new LinkedHashMap<>();
        String id = UUID.randomUUID().toString();
        cmd.put("id", id);
        cmd.put("op", op);
        cmd.put("roundId", roundId);
        if (target != null) cmd.put("target", target);
        if (operator != null) cmd.put("operator", operator);
        s.plane.pushCommand(ChunkCodec.toJson(cmd));
        long deadline = System.currentTimeMillis() + s.cfg.getAdminTimeoutMillis();
        try {
            while (System.currentTimeMillis() < deadline) {
                String res = s.plane.getCommandResult(id);
                if (res != null) {
                    Map<String, String> r = ChunkCodec.toStringMap(res);
                    if (!"true".equals(r.get("ok"))) throw new PipelinerException(r.get("error") == null ? op + " failed" : r.get("error"));
                    return;
                }
                Thread.sleep(100);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PipelinerException(op + " interrupted while waiting for the leader");
        }
        throw new PipelinerException(op + " timed out after " + s.cfg.getAdminTimeoutMillis() + " ms: no leader executed the command");
    }
}
