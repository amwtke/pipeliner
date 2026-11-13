package io.github.amwtke.pipeliner.store.redis;

import io.github.amwtke.pipeliner.core.codec.ChunkCodec;
import io.github.amwtke.pipeliner.core.graph.ExecMode;
import io.github.amwtke.pipeliner.core.graph.GateType;
import io.github.amwtke.pipeliner.core.store.*;
import io.lettuce.core.RedisClient;
import io.lettuce.core.ScriptOutputType;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.EnumMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import lombok.extern.slf4j.Slf4j;

/**
 * Redis control plane store. Per-round keys share the hash tag {roundId}:
 * pg:{r}:round, :run:ID, :gate:ID, :signal:ID, :pl:ID, :chunk:PID (chunkId to JSON), :cnt:PID (status counters and row sums),
 * and the enumeration sets :runs, :gates, :pls, :cpls. Globals: pg:store:rounds (zset), pg:store:biz (hash), pg:store:active (set).
 * Mutations are Lua scripts so CAS, epoch fencing and chunk upserts are atomic.
 */
@Slf4j
public class RedisControlPlaneStore implements ControlPlaneStore {
    private static final String ROUNDS = "pg:store:rounds";
    private static final String BIZ = "pg:store:biz";
    private static final String ACTIVE = "pg:store:active";
    /** Retention indexes (score = finishedAt, member = roundId): all terminal rounds / terminal rounds not yet compacted. Candidates only: the scripts re-check the round. */
    private static final String FINISHED = "pg:store:finished";
    private static final String UNCOMPACTED = "pg:store:uncompacted";

    /** KEYS[1]=hash, KEYS[2]=round hash (round-level epoch fence); ARGV[1]=expected status or ''; ARGV[2]=epoch or -1 (no fence); then (op,field,value) triples: S set, N set-if-absent, D delete, I incr. */
    private static final String MUTATE =
            "local k=KEYS[1]\n"
            + "if redis.call('EXISTS',k)==0 then return 0 end\n"
            + "if ARGV[1]~='' and redis.call('HGET',k,'status')~=ARGV[1] then return 0 end\n"
            + "if tonumber(ARGV[2])>=0 then\n"
            + "  if tonumber(redis.call('HGET',KEYS[2],'epoch') or '0')>tonumber(ARGV[2]) then return 0 end\n"
            + "  local ep=tonumber(redis.call('HGET',k,'epoch') or '0')\n"
            + "  if ep>tonumber(ARGV[2]) then return 0 end\n"
            + "  redis.call('HSET',k,'epoch',ARGV[2])\n"
            + "end\n"
            + "for i=3,#ARGV,3 do\n"
            + "  local op,f,v=ARGV[i],ARGV[i+1],ARGV[i+2]\n"
            + "  if op=='S' then redis.call('HSET',k,f,v)\n"
            + "  elseif op=='N' then redis.call('HSETNX',k,f,v)\n"
            + "  elseif op=='D' then redis.call('HDEL',k,f)\n"
            + "  elseif op=='I' then redis.call('HINCRBY',k,f,v) end\n"
            + "end\n"
            + "return 1\n";

    /** KEYS[1]=biz hash; ARGV: bizField, roundId, nItems, items (H key n f v.. | S key member | Z key score member). Returns winning roundId. */
    private static final String CREATE =
            "if redis.call('HSETNX',KEYS[1],ARGV[1],ARGV[2])==0 then return redis.call('HGET',KEYS[1],ARGV[1]) end\n"
            + "local p=4\n"
            + "for _=1,tonumber(ARGV[3]) do\n"
            + "  local t=ARGV[p]\n"
            + "  if t=='H' then\n"
            + "    local n=tonumber(ARGV[p+2])\n"
            + "    for j=0,n-1 do redis.call('HSET',ARGV[p+1],ARGV[p+3+2*j],ARGV[p+4+2*j]) end\n"
            + "    p=p+3+2*n\n"
            + "  elseif t=='S' then redis.call('SADD',ARGV[p+1],ARGV[p+2]); p=p+3\n"
            + "  else redis.call('ZADD',ARGV[p+1],ARGV[p+2],ARGV[p+3]); p=p+4 end\n"
            + "end\n"
            + "return ARGV[2]\n";

    /**
     * KEYS[1]=round hash; ARGV[1]=epoch or -1 (no fence), ARGV[2]=journal offset or '', then 7-tuples (mode,pid,cid,status,json,rowsIn,rowsOutTotal).
     * mode O overwrite, D overwrite unless DONE, N insert if absent. Keeps per-pipeline status counters and DONE row sums in pg:{r}:cnt:PID.
     * Returns 1 ok, 0 fenced, -1 round missing.
     */
    private static final String CHUNKS =
            "local rk=KEYS[1]\n"
            + "local pre=string.sub(rk,1,#rk-5)\n"
            + "if tonumber(ARGV[1])>=0 then\n"
            + "  local cur=redis.call('HGET',rk,'epoch')\n"
            + "  if not cur then return -1 end\n"
            + "  if tonumber(cur)>tonumber(ARGV[1]) then return 0 end\n"
            + "  redis.call('HSET',rk,'epoch',ARGV[1])\n"
            + "  if ARGV[2]~='' then redis.call('HSET',rk,'journalOffset',ARGV[2]) end\n"
            + "end\n"
            + "local function num(x) if type(x)=='number' then return x end return 0 end\n"
            + "for i=3,#ARGV,7 do\n"
            + "  local mode,pid,cid,st,js=ARGV[i],ARGV[i+1],ARGV[i+2],ARGV[i+3],ARGV[i+4]\n"
            + "  local ck,nk=pre..'chunk:'..pid,pre..'cnt:'..pid\n"
            + "  local old=redis.call('HGET',ck,cid)\n"
            + "  local orec,ost\n"
            + "  if old then orec=cjson.decode(old); ost=orec.status end\n"
            + "  local write=true\n"
            + "  if mode=='N' and old then write=false end\n"
            + "  if mode=='D' and ost=='DONE' then write=false end\n"
            + "  if write then\n"
            + "    if old then\n"
            + "      redis.call('HINCRBY',nk,ost,-1)\n"
            + "      if ost=='DONE' then\n"
            + "        redis.call('HINCRBYFLOAT',nk,'_in',-num(orec.rowsIn))\n"
            + "        local ot=0\n"
            + "        if type(orec.rowsOut)=='table' then for _,v in pairs(orec.rowsOut) do ot=ot+num(v) end end\n"
            + "        redis.call('HINCRBYFLOAT',nk,'_out',-ot)\n"
            + "      end\n"
            + "      if type(orec.splitSpec)=='string' and not string.find(js,'\"splitSpec\":',1,true) then\n"
            + "        js=string.sub(js,1,-2)..',\"splitSpec\":'..cjson.encode(orec.splitSpec)..'}'\n"
            + "      end\n"
            + "    else redis.call('SADD',pre..'cpls',pid) end\n"
            + "    redis.call('HSET',ck,cid,js)\n"
            + "    redis.call('HINCRBY',nk,st,1)\n"
            + "    if st=='DONE' then\n"
            + "      redis.call('HINCRBYFLOAT',nk,'_in',ARGV[i+5])\n"
            + "      redis.call('HINCRBYFLOAT',nk,'_out',ARGV[i+6])\n"
            + "    end\n"
            + "  end\n"
            + "end\n"
            + "return 1\n";

    /**
     * KEYS[1]=round hash; ARGV[1]=epoch, ARGV[2]=nRuns, run ids..., nGates, gate ids.... Atomic rerun reset (see ControlPlaneStore.resetRuns).
     * Returns 1 ok, 0 fenced, -1 round missing.
     */
    private static final String RESET_RUNS =
            "local rk=KEYS[1]\n"
            + "local pre=string.sub(rk,1,#rk-5)\n"
            + "local cur=redis.call('HGET',rk,'epoch')\n"
            + "if not cur then return -1 end\n"
            + "if tonumber(cur)>tonumber(ARGV[1]) then return 0 end\n"
            + "local nr=tonumber(ARGV[2])\n"
            + "for i=1,nr do\n"
            + "  local re=redis.call('HGET',pre..'run:'..ARGV[2+i],'epoch')\n"
            + "  if re and tonumber(re)>tonumber(ARGV[1]) then return 0 end\n"
            + "end\n"
            + "redis.call('HSET',rk,'epoch',ARGV[1])\n"
            + "local ep=ARGV[1]\n"
            + "local runs={}\n"
            + "for i=1,nr do\n"
            + "  local id=ARGV[2+i]\n"
            + "  runs[id]=true\n"
            + "  local k=pre..'run:'..id\n"
            + "  if redis.call('EXISTS',k)==1 then\n"
            + "    redis.call('HSET',k,'status','PENDING','epoch',ep)\n"
            + "    redis.call('HINCRBY',k,'attempt',1)\n"
            + "    redis.call('HDEL',k,'assignee','startedAt','finishedAt','error')\n"
            + "  end\n"
            + "end\n"
            + "for _,pid in ipairs(redis.call('SMEMBERS',pre..'pls')) do\n"
            + "  local pk=pre..'pl:'..pid\n"
            + "  if runs[redis.call('HGET',pk,'runId')] then\n"
            + "    redis.call('HSET',pk,'status','PENDING','enumDone','0','epoch',ep)\n"
            + "    redis.call('HDEL',pk,'startedAt','finishedAt','error','sumCounts','sumIn','sumOut')\n"
            + "    redis.call('DEL',pre..'chunk:'..pid,pre..'cnt:'..pid)\n"
            + "    redis.call('SREM',pre..'cpls',pid)\n"
            + "  end\n"
            + "end\n"
            + "local ng=tonumber(ARGV[3+nr])\n"
            + "for i=1,ng do\n"
            + "  local id=ARGV[3+nr+i]\n"
            + "  local k=pre..'gate:'..id\n"
            + "  if redis.call('EXISTS',k)==1 then\n"
            + "    redis.call('HSET',k,'status','CLOSED','alerted','0','epoch',ep)\n"
            + "    redis.call('HDEL',k,'armedAt','deadlineAt','openedBy','openedAt','context')\n"
            + "  end\n"
            + "  redis.call('DEL',pre..'signal:'..id)\n"
            + "end\n"
            + "local st=redis.call('HGET',rk,'status')\n"
            + "if st=='FAILED' or st=='SUCCEEDED' then\n"
            + "  redis.call('HSET',rk,'status','RUNNING')\n"
            + "  redis.call('HDEL',rk,'finishedAt','error','compacted')\n"
            + "  redis.call('SADD','pg:store:active',string.sub(pre,5,#pre-2))\n"
            + "  redis.call('ZREM','pg:store:finished',string.sub(pre,5,#pre-2))\n"
            + "  redis.call('ZREM','pg:store:uncompacted',string.sub(pre,5,#pre-2))\n"
            + "end\n"
            + "return 1\n";

    /**
     * KEYS[1]=round hash; ARGV[1]=cutoff. Compaction (see ControlPlaneStore.compactRound): returns the number of removed chunk records,
     * -1 round missing, -2 not terminal / not old enough, -3 already compacted.
     */
    private static final String COMPACT =
            "local rk=KEYS[1]\n"
            + "local pre=string.sub(rk,1,#rk-5)\n"
            + "local rid=string.sub(pre,5,#pre-2)\n"
            + "local st=redis.call('HGET',rk,'status')\n"
            + "if not st then return -1 end\n"
            + "if st~='SUCCEEDED' and st~='FAILED' and st~='CANCELLED' then redis.call('ZREM','pg:store:uncompacted',rid); return -2 end\n"
            + "if redis.call('HGET',rk,'compacted')=='1' then redis.call('ZREM','pg:store:uncompacted',rid); return -3 end\n"
            + "local fin=tonumber(redis.call('HGET',rk,'finishedAt') or '')\n"
            + "if (not fin) or fin>=tonumber(ARGV[1]) then return -2 end\n"
            + "local removed=0\n"
            + "for _,pid in ipairs(redis.call('SMEMBERS',pre..'cpls')) do\n"
            + "  local ck,nk,pk=pre..'chunk:'..pid,pre..'cnt:'..pid,pre..'pl:'..pid\n"
            + "  local h=redis.call('HGETALL',nk)\n"
            + "  local counts,ins,outs={},nil,nil\n"
            + "  for i=1,#h,2 do\n"
            + "    if h[i]=='_in' then ins=h[i+1] elseif h[i]=='_out' then outs=h[i+1]\n"
            + "    elseif tonumber(h[i+1])>0 then counts[h[i]]=tonumber(h[i+1]) end\n"
            + "  end\n"
            + "  removed=removed+redis.call('HLEN',ck)\n"
            + "  if redis.call('EXISTS',pk)==1 then\n"
            + "    local js='{'\n"
            + "    for f,v in pairs(counts) do if #js>1 then js=js..',' end js=js..'\"'..f..'\":'..string.format('%d',v) end\n"
            + "    redis.call('HSET',pk,'sumCounts',js..'}')\n"
            + "    if ins then redis.call('HSET',pk,'sumIn',ins) end\n"
            + "    if outs then redis.call('HSET',pk,'sumOut',outs) end\n"
            + "  end\n"
            + "  redis.call('UNLINK',ck,nk)\n"
            + "end\n"
            + "redis.call('UNLINK',pre..'cpls')\n"
            + "redis.call('HSET',rk,'compacted','1')\n"
            + "redis.call('ZREM','pg:store:uncompacted',rid)\n"
            + "return removed\n";

    /**
     * KEYS[1]=round hash; ARGV[1]=cutoff. Atomic status check + delete of every key of the round (see ControlPlaneStore.purgeRounds).
     * Returns 1 purged, 0 not eligible (stale index entries are dropped), -1 missing.
     */
    private static final String PURGE =
            "local rk=KEYS[1]\n"
            + "local pre=string.sub(rk,1,#rk-5)\n"
            + "local rid=string.sub(pre,5,#pre-2)\n"
            + "local st=redis.call('HGET',rk,'status')\n"
            + "if not st then redis.call('ZREM','pg:store:finished',rid); redis.call('ZREM','pg:store:uncompacted',rid); return -1 end\n"
            + "if st~='SUCCEEDED' and st~='FAILED' and st~='CANCELLED' then redis.call('ZREM','pg:store:finished',rid); redis.call('ZREM','pg:store:uncompacted',rid); return 0 end\n"
            + "local fin=tonumber(redis.call('HGET',rk,'finishedAt') or '')\n"
            + "if (not fin) or fin>=tonumber(ARGV[1]) then return 0 end\n"
            + "local del={rk}\n"
            + "local function add(k) del[#del+1]=k end\n"
            + "for _,id in ipairs(redis.call('SMEMBERS',pre..'runs')) do add(pre..'run:'..id) end\n"
            + "for _,id in ipairs(redis.call('SMEMBERS',pre..'gates')) do add(pre..'gate:'..id); add(pre..'signal:'..id) end\n"
            + "local seen={}\n"
            + "for _,set in ipairs({'pls','cpls'}) do\n"
            + "  for _,id in ipairs(redis.call('SMEMBERS',pre..set)) do\n"
            + "    if not seen[id] then seen[id]=true; add(pre..'pl:'..id); add(pre..'chunk:'..id); add(pre..'cnt:'..id) end\n"
            + "  end\n"
            + "end\n"
            + "for _,suf in ipairs({'runs','gates','pls','cpls','desired','assign','paused','fence','journal'}) do add(pre..suf) end\n"
            + "local biz=(redis.call('HGET',rk,'graphName') or '')..'|'..(redis.call('HGET',rk,'bizKey') or '')\n"
            + "if redis.call('HGET','pg:store:biz',biz)==rid then redis.call('HDEL','pg:store:biz',biz) end\n"
            + "redis.call('ZREM','pg:store:rounds',rid)\n"
            + "redis.call('ZREM','pg:store:finished',rid)\n"
            + "redis.call('ZREM','pg:store:uncompacted',rid)\n"
            + "redis.call('SREM','pg:store:active',rid)\n"
            + "for i=1,#del,500 do redis.call('UNLINK',unpack(del,i,math.min(i+499,#del))) end\n"
            + "return 1\n";

    private final RedisClient client;
    private volatile StatefulRedisConnection<String, String> conn;

    public RedisControlPlaneStore(RedisClient client) { this.client = client; }

    private RedisCommands<String, String> r() {
        StatefulRedisConnection<String, String> c = conn;
        if (c == null || !c.isOpen()) {
            synchronized (this) {
                c = conn;
                if (c == null || !c.isOpen()) conn = c = client.connect();
            }
        }
        return c.sync();
    }

    @Override
    public void init() {
        RedisCommands<String, String> c = r();
        c.ping();
        // retention indexes: backfill terminal rounds created before they existed (idempotent, ZADD NX)
        long total = c.zcard(ROUNDS);
        for (long from = 0; from < total; from += 1000) {
            for (String rid : c.zrange(ROUNDS, from, from + 999)) {
                List<io.lettuce.core.KeyValue<String, String>> v = c.hmget(k(rid, "round"), "status", "finishedAt", "createdAt", "compacted");
                String st = v.get(0).hasValue() ? v.get(0).getValue() : null;
                if (st == null || !RoundStatus.valueOf(st).terminal()) continue;
                String fin = v.get(1).hasValue() ? v.get(1).getValue() : (v.get(2).hasValue() ? v.get(2).getValue() : "0");
                c.zadd(FINISHED, io.lettuce.core.ZAddArgs.Builder.nx(), Double.parseDouble(fin), rid);
                if (!(v.get(3).hasValue() && "1".equals(v.get(3).getValue())))
                    c.zadd(UNCOMPACTED, io.lettuce.core.ZAddArgs.Builder.nx(), Double.parseDouble(fin), rid);
            }
        }
    }

    // ---------------------------------------------------------------- keys & helpers

    private static String k(String rid, String suffix) { return "pg:{" + rid + "}:" + suffix; }

    private long evalLong(String script, String[] keys, List<String> args) {
        Long v = r().eval(script, ScriptOutputType.INTEGER, keys, args.toArray(new String[0]));
        return v == null ? 0 : v;
    }

    private static void put(List<String> out, Object v, String field) { if (v != null) { out.add(field); out.add(String.valueOf(v)); } }

    private static void putBool(List<String> out, boolean v, String field) { out.add(field); out.add(v ? "1" : "0"); }

    private static String json(Object o) { return o == null ? null : ChunkCodec.toJson(o); }

    private static Long lng(Map<String, String> h, String f) { String v = h.get(f); return v == null ? null : Long.valueOf(v); }

    private static long lng0(Map<String, String> h, String f) { Long v = lng(h, f); return v == null ? 0 : v; }

    private static int int0(Map<String, String> h, String f) { String v = h.get(f); return v == null ? 0 : Integer.parseInt(v); }

    private static boolean bool(Map<String, String> h, String f) { return "1".equals(h.get(f)); }

    private static long totalOf(Map<String, Long> m) {
        long t = 0;
        if (m != null) for (Long v : m.values()) if (v != null) t += v;
        return t;
    }

    /** Builds an op triple list for the MUTATE script. */
    private static final class Ops {
        final List<String> args = new ArrayList<>();
        Ops(String expect, long epoch) { args.add(expect == null ? "" : expect); args.add(String.valueOf(epoch)); }
        Ops set(String f, Object v) { return v == null ? op("D", f, "") : op("S", f, String.valueOf(v)); }
        Ops setIfNull(String f, Object v) { return op("N", f, String.valueOf(v)); }
        Ops del(String f) { return op("D", f, ""); }
        Ops incr(String f, long by) { return op("I", f, String.valueOf(by)); }
        Ops setIfNotNull(String f, Object v) { return v == null ? this : op("S", f, String.valueOf(v)); }
        private Ops op(String op, String f, String v) { args.add(op); args.add(f); args.add(v); return this; }
    }

    private boolean mutate(String roundId, String key, Ops o) { return evalLong(MUTATE, new String[]{key, k(roundId, "round")}, o.args) == 1; }

    /** KEYS[1]=round hash; ARGV[1]=epoch. 1 fenced ok, 0 higher epoch holds the round or it is missing. */
    private static final String FENCE =
            "local cur=redis.call('HGET',KEYS[1],'epoch')\n"
            + "if not cur then return 0 end\n"
            + "if tonumber(cur)>tonumber(ARGV[1]) then return 0 end\n"
            + "redis.call('HSET',KEYS[1],'epoch',ARGV[1])\n"
            + "return 1\n";

    @Override
    public boolean fenceRound(String roundId, long epoch) {
        return evalLong(FENCE, new String[]{k(roundId, "round")}, java.util.Collections.singletonList(String.valueOf(epoch))) == 1;
    }

    // ---------------------------------------------------------------- rounds

    private static List<String> roundFields(RoundRecord r) {
        List<String> f = new ArrayList<>();
        put(f, r.getRoundId(), "roundId"); put(f, r.getGraphName(), "graphName"); put(f, r.getBizKey(), "bizKey");
        put(f, r.getPlanHash(), "planHash"); put(f, r.getPlanJson(), "planJson"); put(f, json(r.getParams()), "params");
        put(f, r.getStatus().name(), "status"); put(f, r.getMasterEpoch(), "epoch");
        put(f, r.getJournalOffset() == null ? "0-0" : r.getJournalOffset(), "journalOffset");
        put(f, r.getCreatedAt(), "createdAt"); put(f, r.getStartedAt(), "startedAt"); put(f, r.getFinishedAt(), "finishedAt");
        put(f, r.getError(), "error"); putBool(f, r.isIncomplete(), "incomplete"); put(f, r.getSeq(), "seq");
        return f;
    }

    private static RoundRecord toRound(Map<String, String> h) {
        if (h.isEmpty()) return null;
        String params = h.get("params");
        return RoundRecord.builder().roundId(h.get("roundId")).graphName(h.get("graphName")).bizKey(h.get("bizKey")).planHash(h.get("planHash"))
                .planJson(h.get("planJson")).params(params == null ? null : ChunkCodec.toStringMap(params)).status(RoundStatus.valueOf(h.get("status")))
                .masterEpoch(lng0(h, "epoch")).journalOffset(h.get("journalOffset")).createdAt(lng(h, "createdAt")).startedAt(lng(h, "startedAt"))
                .finishedAt(lng(h, "finishedAt")).error(h.get("error")).incomplete(bool(h, "incomplete")).seq(lng0(h, "seq")).compacted(bool(h, "compacted")).build();
    }

    private static RunRecord toRun(String rid, String id, Map<String, String> h) {
        String mode = h.get("mode");
        return RunRecord.builder().roundId(rid).runId(id).stage(int0(h, "stage")).mode(mode == null ? null : ExecMode.valueOf(mode))
                .status(RunStatus.valueOf(h.get("status"))).attempt(int0(h, "attempt")).assignee(h.get("assignee")).masterEpoch(lng0(h, "epoch"))
                .startedAt(lng(h, "startedAt")).finishedAt(lng(h, "finishedAt")).error(h.get("error")).build();
    }

    @SuppressWarnings("unchecked")
    private static GateRecord toGate(String rid, String id, Map<String, String> h) {
        String type = h.get("type");
        String br = h.get("beforeRuns");
        String ctx = h.get("context");
        return GateRecord.builder().roundId(rid).gateId(id).type(type == null ? null : GateType.valueOf(type))
                .beforeRuns(br == null ? null : (List<String>) ChunkCodec.fromJson(br, List.class)).status(GateStatus.valueOf(h.get("status")))
                .armedAt(lng(h, "armedAt")).deadlineAt(lng(h, "deadlineAt")).openedBy(h.get("openedBy")).openedAt(lng(h, "openedAt"))
                .context(ctx == null ? null : ChunkCodec.toStringMap(ctx)).masterEpoch(lng0(h, "epoch")).alerted(bool(h, "alerted")).build();
    }

    private static PipelineRecord toPipeline(String rid, String id, Map<String, String> h) {
        return PipelineRecord.builder().roundId(rid).pipelineId(id).runId(h.get("runId")).status(PipelineStatus.valueOf(h.get("status")))
                .parallelism(int0(h, "parallelism")).enumDone(bool(h, "enumDone")).masterEpoch(lng0(h, "epoch")).startedAt(lng(h, "startedAt"))
                .finishedAt(lng(h, "finishedAt")).error(h.get("error")).summaryCounts(summaryCounts(h.get("sumCounts")))
                .summaryRowsIn(h.containsKey("sumIn") ? Long.valueOf(Math.round(Double.parseDouble(h.get("sumIn")))) : null)
                .summaryRowsOut(h.containsKey("sumOut") ? Long.valueOf(Math.round(Double.parseDouble(h.get("sumOut")))) : null).build();
    }

    private static Map<String, Long> summaryCounts(String js) {
        if (js == null) return null;
        Map<?, ?> raw = ChunkCodec.fromJson(js, Map.class);
        Map<String, Long> out = new LinkedHashMap<>();
        for (Map.Entry<?, ?> e : raw.entrySet()) out.put(String.valueOf(e.getKey()), ((Number) e.getValue()).longValue());
        return out;
    }

    @Override
    public RoundRecord createRound(RoundRecord round, List<RunRecord> runs, List<GateRecord> gates, List<PipelineRecord> pipelines) {
        if (round.getCreatedAt() == null) round.setCreatedAt(System.currentTimeMillis());
        if (round.getStatus() == null) round.setStatus(RoundStatus.CREATED);
        String rid = round.getRoundId();
        List<String> items = new ArrayList<>();
        int n = 0;
        n += hashItem(items, k(rid, "round"), roundFields(round));
        for (RunRecord u : runs == null ? Collections.<RunRecord>emptyList() : runs) {
            List<String> f = new ArrayList<>();
            put(f, u.getStage(), "stage"); put(f, u.getMode(), "mode"); put(f, u.getStatus() == null ? RunStatus.PENDING : u.getStatus(), "status");
            put(f, u.getAttempt(), "attempt"); put(f, u.getAssignee(), "assignee"); put(f, u.getMasterEpoch(), "epoch");
            put(f, u.getStartedAt(), "startedAt"); put(f, u.getFinishedAt(), "finishedAt"); put(f, u.getError(), "error");
            n += hashItem(items, k(rid, "run:" + u.getRunId()), f);
            n += setItem(items, k(rid, "runs"), u.getRunId());
        }
        for (GateRecord g : gates == null ? Collections.<GateRecord>emptyList() : gates) {
            List<String> f = new ArrayList<>();
            put(f, g.getType(), "type"); put(f, json(g.getBeforeRuns()), "beforeRuns"); put(f, g.getStatus() == null ? GateStatus.CLOSED : g.getStatus(), "status");
            put(f, g.getArmedAt(), "armedAt"); put(f, g.getDeadlineAt(), "deadlineAt"); put(f, g.getOpenedBy(), "openedBy"); put(f, g.getOpenedAt(), "openedAt");
            put(f, json(g.getContext()), "context"); put(f, g.getMasterEpoch(), "epoch"); putBool(f, g.isAlerted(), "alerted");
            n += hashItem(items, k(rid, "gate:" + g.getGateId()), f);
            n += setItem(items, k(rid, "gates"), g.getGateId());
        }
        for (PipelineRecord p : pipelines == null ? Collections.<PipelineRecord>emptyList() : pipelines) {
            List<String> f = new ArrayList<>();
            put(f, p.getRunId(), "runId"); put(f, p.getStatus() == null ? PipelineStatus.PENDING : p.getStatus(), "status"); put(f, p.getParallelism(), "parallelism");
            putBool(f, p.isEnumDone(), "enumDone"); put(f, p.getMasterEpoch(), "epoch"); put(f, p.getStartedAt(), "startedAt");
            put(f, p.getFinishedAt(), "finishedAt"); put(f, p.getError(), "error");
            n += hashItem(items, k(rid, "pl:" + p.getPipelineId()), f);
            n += setItem(items, k(rid, "pls"), p.getPipelineId());
        }
        items.add("Z"); items.add(ROUNDS); items.add(String.valueOf(round.getCreatedAt())); items.add(rid); n++;
        if (!round.getStatus().terminal()) n += setItem(items, ACTIVE, rid);
        List<String> args = new ArrayList<>();
        args.add(round.getGraphName() + "|" + round.getBizKey());
        args.add(rid);
        args.add(String.valueOf(n));
        args.addAll(items);
        String winner = r().eval(CREATE, ScriptOutputType.VALUE, new String[]{BIZ}, args.toArray(new String[0]));
        return rid.equals(winner) ? round : loadRoundRecord(winner);
    }

    private static int hashItem(List<String> items, String key, List<String> fields) {
        items.add("H"); items.add(key); items.add(String.valueOf(fields.size() / 2)); items.addAll(fields);
        return 1;
    }

    private static int setItem(List<String> items, String key, String member) { items.add("S"); items.add(key); items.add(member); return 1; }

    @Override
    public RoundRecord findRoundByBizKey(String graphName, String bizKey) {
        String rid = r().hget(BIZ, graphName + "|" + bizKey);
        return rid == null ? null : loadRoundRecord(rid);
    }

    @Override
    public RoundRecord loadRoundRecord(String roundId) { return toRound(r().hgetall(k(roundId, "round"))); }

    @Override
    public RoundView loadRoundState(String roundId) {
        RoundRecord round = loadRoundRecord(roundId);
        if (round == null) return null;
        RedisCommands<String, String> c = r();
        return RoundView.builder().round(round).runs(loadRuns(c, roundId)).gates(loadGates(c, roundId)).pipelines(loadPipelines(c, roundId))
                .chunkCounts(new LinkedHashMap<String, Map<ChunkStatus, Long>>()).rowsIn(new LinkedHashMap<String, Long>())
                .rowsOut(new LinkedHashMap<String, Long>()).build();
    }

    private List<RunRecord> loadRuns(RedisCommands<String, String> c, String roundId) {
        List<RunRecord> runs = new ArrayList<>();
        for (String id : c.smembers(k(roundId, "runs"))) runs.add(toRun(roundId, id, c.hgetall(k(roundId, "run:" + id))));
        runs.sort(Comparator.comparingInt(RunRecord::getStage).thenComparing(RunRecord::getRunId));
        return runs;
    }

    private List<GateRecord> loadGates(RedisCommands<String, String> c, String roundId) {
        List<GateRecord> gates = new ArrayList<>();
        for (String id : new TreeSet<>(c.smembers(k(roundId, "gates")))) gates.add(toGate(roundId, id, c.hgetall(k(roundId, "gate:" + id))));
        return gates;
    }

    private List<PipelineRecord> loadPipelines(RedisCommands<String, String> c, String roundId) {
        List<PipelineRecord> pls = new ArrayList<>();
        for (String id : new TreeSet<>(c.smembers(k(roundId, "pls")))) pls.add(toPipeline(roundId, id, c.hgetall(k(roundId, "pl:" + id))));
        return pls;
    }

    @Override
    public RoundView loadRound(String roundId) {
        RoundRecord round = loadRoundRecord(roundId);
        if (round == null) return null;
        RedisCommands<String, String> c = r();
        List<RunRecord> runs = loadRuns(c, roundId);
        List<GateRecord> gates = loadGates(c, roundId);
        List<PipelineRecord> pls = loadPipelines(c, roundId);
        Map<String, Map<ChunkStatus, Long>> counts = new LinkedHashMap<>();
        Map<String, Long> rowsIn = new LinkedHashMap<>();
        Map<String, Long> rowsOut = new LinkedHashMap<>();
        for (String pid : new TreeSet<>(c.smembers(k(roundId, "cpls")))) {
            Map<String, String> h = c.hgetall(k(roundId, "cnt:" + pid));
            Map<ChunkStatus, Long> m = new EnumMap<>(ChunkStatus.class);
            for (ChunkStatus s : ChunkStatus.values()) {
                long n = lng0(h, s.name());
                if (n > 0) m.put(s, n);
            }
            if (m.isEmpty()) continue;
            counts.put(pid, m);
            if (h.containsKey("_in") || h.containsKey("_out")) {
                rowsIn.put(pid, Math.round(Double.parseDouble(h.getOrDefault("_in", "0"))));
                rowsOut.put(pid, Math.round(Double.parseDouble(h.getOrDefault("_out", "0"))));
            }
        }
        for (PipelineRecord p : pls) {   // compacted pipelines: chunk records are gone, the summary stays
            if (p.getSummaryCounts() == null || counts.containsKey(p.getPipelineId())) continue;
            Map<ChunkStatus, Long> m = new EnumMap<>(ChunkStatus.class);
            for (Map.Entry<String, Long> e : p.getSummaryCounts().entrySet()) if (e.getValue() > 0) m.put(ChunkStatus.valueOf(e.getKey()), e.getValue());
            if (m.isEmpty()) continue;
            counts.put(p.getPipelineId(), m);
            if (p.getSummaryRowsIn() != null) rowsIn.put(p.getPipelineId(), p.getSummaryRowsIn());
            if (p.getSummaryRowsOut() != null) rowsOut.put(p.getPipelineId(), p.getSummaryRowsOut());
        }
        return RoundView.builder().round(round).runs(runs).gates(gates).pipelines(pls).chunkCounts(counts).rowsIn(rowsIn).rowsOut(rowsOut).build();
    }

    @Override
    public List<String> findActiveRounds() {
        RedisCommands<String, String> c = r();
        List<Object[]> found = new ArrayList<>();
        for (String rid : c.smembers(ACTIVE)) {
            List<io.lettuce.core.KeyValue<String, String>> v = c.hmget(k(rid, "round"), "status", "createdAt");
            String st = v.get(0).hasValue() ? v.get(0).getValue() : null;
            if (st == null) { c.srem(ACTIVE, rid); continue; }
            if (RoundStatus.valueOf(st).terminal()) { c.srem(ACTIVE, rid); continue; }
            found.add(new Object[]{rid, v.get(1).hasValue() ? Long.parseLong(v.get(1).getValue()) : 0L});
        }
        found.sort(Comparator.<Object[], Long>comparing(o -> (Long) o[1]).thenComparing(o -> (String) o[0]));
        List<String> out = new ArrayList<>();
        for (Object[] o : found) out.add((String) o[0]);
        return out;
    }

    @Override
    public List<RoundRecord> listRecentRounds(String graphName, int limit) {
        List<RoundRecord> out = new ArrayList<>();
        if (limit <= 0) return out;
        boolean all = graphName == null || graphName.trim().isEmpty();
        RedisCommands<String, String> c = r();
        java.util.Set<String> seen = new java.util.HashSet<>();
        List<RoundRecord> active = new ArrayList<>();
        for (String rid : findActiveRounds()) {
            RoundRecord rr = loadLight(c, rid);
            if (rr != null && (all || graphName.equals(rr.getGraphName()))) { active.add(rr); seen.add(rid); }
        }
        active.sort(recentFirst());
        for (RoundRecord rr : active) { if (out.size() >= limit) return out; out.add(rr); }
        // newest first from the created-at sorted index; bounded scan when filtering by graph
        long scanned = 0;
        final int batch = 100;
        final long maxScan = all ? limit + seen.size() + batch : 5000;
        for (long from = 0; out.size() < limit && scanned < maxScan; from += batch) {
            List<String> ids = c.zrevrange(ROUNDS, from, from + batch - 1);
            if (ids.isEmpty()) break;
            for (String rid : ids) {
                scanned++;
                if (seen.contains(rid)) continue;
                RoundRecord rr = loadLight(c, rid);
                if (rr == null || !(all || graphName.equals(rr.getGraphName()))) continue;
                out.add(rr);
                if (out.size() >= limit) break;
            }
        }
        return out;
    }

    private static Comparator<RoundRecord> recentFirst() {
        return (a, b) -> {
            long x = a.getCreatedAt() == null ? 0 : a.getCreatedAt(), y = b.getCreatedAt() == null ? 0 : b.getCreatedAt();
            return x != y ? Long.compare(y, x) : b.getRoundId().compareTo(a.getRoundId());
        };
    }

    /** Round hash without the (large) plan json. */
    private RoundRecord loadLight(RedisCommands<String, String> c, String rid) {
        List<io.lettuce.core.KeyValue<String, String>> v = c.hmget(k(rid, "round"), "roundId", "graphName", "bizKey", "planHash", "params",
                "status", "epoch", "createdAt", "startedAt", "finishedAt", "error", "incomplete", "seq", "compacted");
        Map<String, String> h = new LinkedHashMap<>();
        String[] names = {"roundId", "graphName", "bizKey", "planHash", "params", "status", "epoch", "createdAt", "startedAt", "finishedAt", "error", "incomplete", "seq", "compacted"};
        for (int i = 0; i < names.length; i++) if (v.get(i).hasValue()) h.put(names[i], v.get(i).getValue());
        if (!h.containsKey("status")) return null;
        return toRound(h);
    }

    private static boolean isRunTerminal(RunStatus s) { return s == RunStatus.SUCCEEDED || s == RunStatus.FAILED; }

    private static boolean isPlTerminal(PipelineStatus s) { return s == PipelineStatus.SUCCEEDED || s == PipelineStatus.FAILED; }

    private Ops statusOps(String expect, String target, String error, long epoch, boolean running, boolean terminal) {
        long now = System.currentTimeMillis();
        Ops o = new Ops(expect, epoch).set("status", target).setIfNotNull("error", error);
        if (running) o.setIfNull("startedAt", now);
        if (terminal) o.set("finishedAt", now);
        return o;
    }

    @Override
    public boolean casRoundStatus(String roundId, RoundStatus expect, RoundStatus target, String error, long epoch) {
        boolean ok = mutate(roundId, k(roundId, "round"), statusOps(expect.name(), target.name(), error, epoch, target == RoundStatus.RUNNING, target.terminal()));
        if (ok) {
            RedisCommands<String, String> c = r();
            if (target.terminal()) {
                c.srem(ACTIVE, roundId);
                String fin = c.hget(k(roundId, "round"), "finishedAt");
                double score = fin == null ? (double) System.currentTimeMillis() : Double.parseDouble(fin);
                c.zadd(FINISHED, score, roundId);
                c.zadd(UNCOMPACTED, score, roundId);
            } else {
                c.sadd(ACTIVE, roundId);
                c.zrem(FINISHED, roundId);
                c.zrem(UNCOMPACTED, roundId);
            }
        }
        return ok;
    }

    @Override
    public void setRoundIncomplete(String roundId) { mutate(roundId, k(roundId, "round"), new Ops(null, -1).set("incomplete", "1")); }

    @Override
    public boolean casRunStatus(String roundId, String runId, RunStatus expect, RunStatus target, String error, long epoch) {
        return mutate(roundId, k(roundId, "run:" + runId), statusOps(expect.name(), target.name(), error, epoch, target == RunStatus.RUNNING, isRunTerminal(target)));
    }

    @Override
    public boolean updateRunAssignment(String roundId, String runId, String assignee, int attempt, long epoch) {
        return mutate(roundId, k(roundId, "run:" + runId), new Ops(null, epoch).set("assignee", assignee).set("attempt", attempt));
    }

    @Override
    public boolean casPipelineStatus(String roundId, String pipelineId, PipelineStatus expect, PipelineStatus target, String error, long epoch) {
        return mutate(roundId, k(roundId, "pl:" + pipelineId), statusOps(expect.name(), target.name(), error, epoch, target == PipelineStatus.RUNNING, isPlTerminal(target)));
    }

    @Override
    public void setPipelineEnumDone(String roundId, String pipelineId, boolean done, long epoch) {
        mutate(roundId, k(roundId, "pl:" + pipelineId), new Ops(null, epoch).set("enumDone", done ? "1" : "0"));
    }

    @Override
    public void setPipelineParallelism(String roundId, String pipelineId, int parallelism, long epoch) {
        mutate(roundId, k(roundId, "pl:" + pipelineId), new Ops(null, epoch).set("parallelism", parallelism));
    }

    // ---------------------------------------------------------------- gates

    @Override
    public void armGate(String roundId, String gateId, long armedAt, Long deadlineAt, long epoch) {
        mutate(roundId, k(roundId, "gate:" + gateId), new Ops(null, epoch).set("armedAt", armedAt).set("deadlineAt", deadlineAt));
    }

    @Override
    public boolean casGateStatus(String roundId, String gateId, GateStatus expect, GateStatus target, String openedBy, Map<String, String> context, long epoch) {
        Ops o = new Ops(expect.name(), epoch).set("status", target).setIfNotNull("openedBy", openedBy).setIfNotNull("context", json(context));
        if (target != GateStatus.CLOSED) o.set("openedAt", System.currentTimeMillis());
        return mutate(roundId, k(roundId, "gate:" + gateId), o);
    }

    @Override
    public void markGateAlerted(String roundId, String gateId) { mutate(roundId, k(roundId, "gate:" + gateId), new Ops(null, -1).set("alerted", "1")); }

    @Override
    public void resetGate(String roundId, String gateId, long epoch) {
        mutate(roundId, k(roundId, "gate:" + gateId), new Ops(null, epoch).set("status", GateStatus.CLOSED).del("armedAt").del("deadlineAt").del("openedBy")
                .del("openedAt").del("context").set("alerted", "0"));
    }

    @Override
    public void saveGateSignal(String roundId, String gateId, String signalKey, String payload) {
        r().hsetnx(k(roundId, "signal:" + gateId), signalKey, payload == null ? "" : payload);
    }

    @Override
    public Map<String, String> loadGateSignals(String roundId, String gateId) {
        return new LinkedHashMap<>(r().hgetall(k(roundId, "signal:" + gateId)));
    }

    // ---------------------------------------------------------------- chunks

    private static void tuple(List<String> args, String mode, ChunkRecord c) {
        args.add(mode); args.add(c.getPipelineId()); args.add(c.getChunkId()); args.add(c.getStatus().name()); args.add(ChunkCodec.toJson(c));
        args.add(String.valueOf(c.getRowsIn() == null ? 0 : c.getRowsIn())); args.add(String.valueOf(totalOf(c.getRowsOut())));
    }

    private long chunkScript(String roundId, long epoch, String offset, List<String> tuples) {
        List<String> args = new ArrayList<>();
        args.add(String.valueOf(epoch)); args.add(offset == null ? "" : offset);
        args.addAll(tuples);
        return evalLong(CHUNKS, new String[]{k(roundId, "round")}, args);
    }

    @Override
    public void savePendingChunks(String roundId, List<ChunkRecord> chunks) {
        if (chunks == null || chunks.isEmpty()) return;
        long now = System.currentTimeMillis();
        List<String> t = new ArrayList<>();
        for (ChunkRecord c : chunks) {
            c.setRoundId(roundId);
            if (c.getStatus() == null) c.setStatus(ChunkStatus.PENDING);
            if (c.getUpdatedAt() == null) c.setUpdatedAt(now);
            tuple(t, "N", c);
        }
        chunkScript(roundId, -1, null, t);
    }

    @Override
    public void applyChunkEvents(String roundId, List<ChunkEvent> events, String newJournalOffset, long epoch) {
        List<String> t = new ArrayList<>();
        for (ChunkEvent e : events == null ? Collections.<ChunkEvent>emptyList() : events) {
            boolean done = "DONE".equals(e.getType());
            if (!done && !"DEAD".equals(e.getType())) continue;
            ChunkRecord c = ChunkRecord.builder().roundId(roundId).pipelineId(e.getPipelineId()).chunkId(e.getChunkId()).runId(e.getRunId())
                    .status(done ? ChunkStatus.DONE : ChunkStatus.DEAD).attempt(e.getAttempt()).owner(e.getOwner()).inputEdge(e.getInputEdge())
                    .rowsIn(e.getRowsIn()).rowsOut(e.getRowsOut()).costMs(e.getCostMs()).error(e.getError()).updatedAt(e.getTs()).build();
            tuple(t, done ? "O" : "D", c);
        }
        long res = chunkScript(roundId, epoch, newJournalOffset, t);
        if (res != 1) throw new StaleEpochException("applyChunkEvents for round " + roundId + " fenced or missing (epoch " + epoch + ")");
    }

    private List<ChunkRecord> allChunks(String roundId, String pipelineId) {
        RedisCommands<String, String> c = r();
        Set<String> pls = pipelineId != null ? Collections.singleton(pipelineId) : c.smembers(k(roundId, "cpls"));
        List<ChunkRecord> out = new ArrayList<>();
        for (String pid : pls) {   // HSCAN in pages: never one huge HGETALL
            io.lettuce.core.MapScanCursor<String, String> cur = c.hscan(k(roundId, "chunk:" + pid), io.lettuce.core.ScanArgs.Builder.limit(1000));
            while (true) {
                for (String js : cur.getMap().values()) out.add(ChunkCodec.fromJson(js, ChunkRecord.class));
                if (cur.isFinished()) break;
                cur = c.hscan(k(roundId, "chunk:" + pid), cur, io.lettuce.core.ScanArgs.Builder.limit(1000));
            }
        }
        return out;
    }

    @Override
    public void markChunksSkipped(String roundId, String runId, long epoch) {
        List<String> t = new ArrayList<>();
        long now = System.currentTimeMillis();
        for (ChunkRecord c : allChunks(roundId, null)) {
            if (!runId.equals(c.getRunId()) || c.getStatus() != ChunkStatus.DEAD) continue;
            c.setStatus(ChunkStatus.SKIPPED);
            c.setUpdatedAt(now);
            tuple(t, "D", c);
        }
        if (!t.isEmpty() && chunkScript(roundId, epoch, null, t) != 1) log.warn("markChunksSkipped for round {} fenced (epoch {}); nothing applied", roundId, epoch);
    }

    @Override
    public void resetRuns(String roundId, List<String> runIds, List<String> gateIds, long epoch) {
        List<String> args = new ArrayList<>();
        args.add(String.valueOf(epoch));
        args.add(String.valueOf(runIds.size()));
        args.addAll(runIds);
        args.add(String.valueOf(gateIds.size()));
        args.addAll(gateIds);
        long res = evalLong(RESET_RUNS, new String[]{k(roundId, "round")}, args);
        if (res != 1) throw new StaleEpochException("resetRuns for round " + roundId + " fenced or missing (epoch " + epoch + ")");
    }

    @Override
    public List<ChunkRecord> queryChunks(ChunkQuery q) {
        List<ChunkRecord> list = new ArrayList<>();
        for (ChunkRecord c : allChunks(q.getRoundId(), q.getPipelineId())) {
            if (q.getRunId() != null && !q.getRunId().equals(c.getRunId())) continue;
            if (q.getStatus() != null && q.getStatus() != c.getStatus()) continue;
            list.add(c);
        }
        list.sort(Comparator.comparing(ChunkRecord::getPipelineId).thenComparing(ChunkRecord::getChunkId));
        int from = Math.min(Math.max(q.getOffset(), 0), list.size());
        return new ArrayList<>(list.subList(from, (int) Math.min((long) from + Math.max(q.getLimit(), 0), list.size())));
    }

    @Override
    public long countChunks(String roundId, String pipelineId, ChunkStatus status) {
        RedisCommands<String, String> c = r();
        Set<String> pls = pipelineId != null ? Collections.singleton(pipelineId) : c.smembers(k(roundId, "cpls"));
        long total = 0;
        for (String pid : pls) {
            Map<String, String> h = c.hgetall(k(roundId, "cnt:" + pid));
            for (ChunkStatus s : ChunkStatus.values()) if (status == null || status == s) total += Math.max(0, lng0(h, s.name()));
        }
        return total;
    }

    @Override
    public Map<String, Long> sumRowsOutByEdge(String roundId, String runId) {
        Map<String, Long> out = new LinkedHashMap<>();
        for (ChunkRecord c : allChunks(roundId, null)) {
            if (c.getStatus() != ChunkStatus.DONE || !runId.equals(c.getRunId()) || c.getRowsOut() == null) continue;
            for (Map.Entry<String, Long> e : c.getRowsOut().entrySet()) if (e.getKey().startsWith("e")) out.merge(e.getKey(), e.getValue(), Long::sum);
        }
        return out;
    }

    @Override
    public Map<String, Long> sumRowsInByEdge(String roundId, String runId) {
        Map<String, Long> out = new LinkedHashMap<>();
        for (ChunkRecord c : allChunks(roundId, null)) {
            if (c.getStatus() != ChunkStatus.DONE || !runId.equals(c.getRunId()) || c.getInputEdge() == null || "split".equals(c.getInputEdge())) continue;
            out.merge(c.getInputEdge(), c.getRowsIn() == null ? 0L : c.getRowsIn(), Long::sum);
        }
        return out;
    }

    // ---------------------------------------------------------------- retention

    @Override
    public int compactRound(String roundId, long finishedBeforeMillis) {
        long n = evalLong(COMPACT, new String[]{k(roundId, "round")}, Collections.singletonList(String.valueOf(finishedBeforeMillis)));
        return n < 0 ? 0 : (int) n;
    }

    @Override
    public List<String> purgeRounds(long finishedBeforeMillis, int max) {
        List<String> purged = new ArrayList<>();
        if (max <= 0) return purged;
        RedisCommands<String, String> c = r();
        io.lettuce.core.Range<Double> range = io.lettuce.core.Range.from(io.lettuce.core.Range.Boundary.unbounded(),
                io.lettuce.core.Range.Boundary.excluding((double) finishedBeforeMillis));
        // stale index entries are dropped by the script, so ask for a few more candidates than needed
        for (String rid : c.zrangebyscore(FINISHED, range, io.lettuce.core.Limit.create(0, max * 2))) {
            if (purged.size() >= max) break;
            if (evalLong(PURGE, new String[]{k(rid, "round")}, Collections.singletonList(String.valueOf(finishedBeforeMillis))) == 1) purged.add(rid);
        }
        return purged;
    }

    @Override
    public List<String> findTerminalRounds(long finishedBeforeMillis, int max) {
        List<String> out = new ArrayList<>();
        if (max <= 0) return out;
        RedisCommands<String, String> c = r();
        io.lettuce.core.Range<Double> range = io.lettuce.core.Range.from(io.lettuce.core.Range.Boundary.unbounded(),
                io.lettuce.core.Range.Boundary.excluding((double) finishedBeforeMillis));
        for (String rid : c.zrangebyscore(UNCOMPACTED, range, io.lettuce.core.Limit.create(0, max * 2))) {
            if (out.size() >= max) break;
            List<io.lettuce.core.KeyValue<String, String>> v = c.hmget(k(rid, "round"), "status", "compacted");
            String st = v.get(0).hasValue() ? v.get(0).getValue() : null;
            boolean ok = st != null && RoundStatus.valueOf(st).terminal() && !(v.get(1).hasValue() && "1".equals(v.get(1).getValue()));
            if (ok) out.add(rid); else c.zrem(UNCOMPACTED, rid);
        }
        return out;
    }
}
