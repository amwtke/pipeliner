package io.github.amwtke.pipeliner.store.redis;

import io.github.amwtke.pipeliner.core.codec.ChunkCodec;
import io.github.amwtke.pipeliner.core.dataplane.Keys;
import io.github.amwtke.pipeliner.core.graph.ExecMode;
import io.github.amwtke.pipeliner.core.graph.GateType;
import io.github.amwtke.pipeliner.core.store.*;
import io.lettuce.core.KeyValue;
import io.lettuce.core.Limit;
import io.lettuce.core.Range;
import io.lettuce.core.RedisClient;
import io.lettuce.core.ScanArgs;
import io.lettuce.core.ScriptOutputType;
import io.lettuce.core.ZAddArgs;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.EnumMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import lombok.extern.slf4j.Slf4j;

/**
 * Redis control plane store. Per-round keys share the hash tag {roundId}:
 * pg:{r}:round, :run:ID, :gate:ID, :signal:ID, :pl:ID, :chunk:PID (chunkId to JSON), :cnt:PID (status counters, row sums, per-edge sums),
 * :cidx:PID (zset of the chunk ids, lex order) and :cst:PID (zset of "STATUS|chunkId") as chunk indexes, and the enumeration sets
 * :runs, :gates, :pls, :cpls. Globals: pg:store:rounds (zset), pg:store:graph:G (per-graph zset), pg:store:biz (hash), pg:store:active (set),
 * the retention indexes and the schema marker.
 * Mutations are Lua scripts so CAS, epoch fencing and chunk upserts are atomic. Every key name used by a script is passed in (KEYS / the
 * ARGV layout header built by {@link #hdr}) from the constants below: the scripts hard-code no key layout.
 */
@Slf4j
public class RedisControlPlaneStore implements ControlPlaneStore {
    private static final String ROUNDS = "pg:store:rounds";
    private static final String BIZ = "pg:store:biz";
    private static final String ACTIVE = "pg:store:active";
    /** Per-graph created-at index (score = createdAt, member = roundId); the key is this prefix plus the graph name. */
    private static final String GRAPH_ROUNDS = "pg:store:graph:";
    /** Retention indexes (score = finishedAt, member = roundId): all terminal rounds / terminal rounds not yet compacted. Candidates only: the scripts re-check the round. */
    private static final String FINISHED = "pg:store:finished";
    private static final String UNCOMPACTED = "pg:store:uncompacted";
    /** Highest master epoch reserved ({@link #reserveMasterEpoch}) or stored on a round (fence/backfill); one number, compare-and-set only. */
    private static final String MAX_EPOCH = "pg:store:maxepoch";
    /** Highest round seq ever created (raised in the createRound script and by the init backfill). */
    private static final String MAX_SEQ = "pg:store:maxseq";
    /** Rounds purged from the control plane whose data-plane keys still have to be purged (see ControlPlaneStore.pendingPurges). */
    private static final String PENDING_PURGE = "pg:store:pendingpurge";
    /** Schema marker: the version of the one-time init backfill that has completed (raised only). */
    private static final String SCHEMA = "pg:store:schema";
    private static final long SCHEMA_VERSION = 2;

    // per-round key layout (pg:{rid}:<suffix>)
    private static final String ROUND = "round", RUNS = "runs", GATES = "gates", PLS = "pls", CPLS = "cpls";
    private static final String P_RUN = "run:", P_GATE = "gate:", P_SIGNAL = "signal:", P_PL = "pl:", P_CHUNK = "chunk:", P_CNT = "cnt:", P_CIDX = "cidx:", P_CST = "cst:";
    /** Chunk counter fields (in pg:{r}:cnt:PID) that are not a ChunkStatus: sink-chunk count, index-present marker, per-edge row sums "o:EDGE" / "i:EDGE". */
    private static final String SINK_CNT = "_sink", IX = "_ix", OUT_PREFIX = "o:", IN_PREFIX = "i:";

    /** The fields of a round hash except the large planJson. */
    private static final String[] ROUND_LIGHT = {"roundId", "graphName", "bizKey", "planHash", "params", "status", "epoch", "journalOffset", "createdAt",
            "startedAt", "finishedAt", "error", "incomplete", "seq", "compacted"};

    /** Lua prelude of every per-round script: reads the ARGV layout header (see {@link #hdr}). Script specific ARGV start at 15. */
    private static final String HDR =
            "local pre,rid=ARGV[1],ARGV[2]\n"
            + "local S_RUNS,S_GATES,S_PLS,S_CPLS=ARGV[3],ARGV[4],ARGV[5],ARGV[6]\n"
            + "local P_RUN,P_GATE,P_SIGNAL,P_PL,P_CHUNK,P_CNT,P_CIDX,P_CST=ARGV[7],ARGV[8],ARGV[9],ARGV[10],ARGV[11],ARGV[12],ARGV[13],ARGV[14]\n";

    /** The ARGV layout header: key prefix of the round, its id and the key suffix / prefix constants, in the order {@link #HDR} reads them. */
    private static List<String> hdr(String rid) {
        List<String> a = new ArrayList<>();
        a.add(k(rid, "")); a.add(rid);
        a.add(RUNS); a.add(GATES); a.add(PLS); a.add(CPLS);
        a.add(P_RUN); a.add(P_GATE); a.add(P_SIGNAL); a.add(P_PL); a.add(P_CHUNK); a.add(P_CNT); a.add(P_CIDX); a.add(P_CST);
        return a;
    }

    /** Reads {@code n} then n names from ARGV at p into a set, returns the set and the new position (Lua helper, as source text). */
    private static final String READ_SET =
            "local function readset(p)\n"
            + "  local n=tonumber(ARGV[p]); p=p+1\n"
            + "  local s={}\n"
            + "  for _=1,n do s[ARGV[p]]=true; p=p+1 end\n"
            + "  return s,p\n"
            + "end\n";

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

    /**
     * Round status CAS together with the active / finished / uncompacted index updates, all in one script (one atomic, fenced step).
     * KEYS: 1 round hash, 2 active set, 3 finished zset, 4 uncompacted zset. ARGV: 1 expect, 2 epoch, 3 target, 4 hasError (1/0), 5 error,
     * 6 now, 7 setStartedAt (1/0), 8 target is terminal (1/0), 9 revive (FAILED to RUNNING: clears error and finishedAt), 10 round id. Returns 1 / 0.
     */
    private static final String CAS_ROUND =
            "local rk=KEYS[1]\n"
            + "if redis.call('EXISTS',rk)==0 then return 0 end\n"
            + "if redis.call('HGET',rk,'status')~=ARGV[1] then return 0 end\n"
            + "if tonumber(redis.call('HGET',rk,'epoch') or '0')>tonumber(ARGV[2]) then return 0 end\n"
            + "redis.call('HSET',rk,'epoch',ARGV[2],'status',ARGV[3])\n"
            + "if ARGV[9]=='1' then redis.call('HDEL',rk,'error','finishedAt') end\n"
            + "if ARGV[4]=='1' then redis.call('HSET',rk,'error',ARGV[5]) end\n"
            + "if ARGV[7]=='1' then redis.call('HSETNX',rk,'startedAt',ARGV[6]) end\n"
            + "if ARGV[8]=='1' then\n"
            + "  redis.call('HSET',rk,'finishedAt',ARGV[6])\n"
            + "  redis.call('SREM',KEYS[2],ARGV[10])\n"
            + "  redis.call('ZADD',KEYS[3],ARGV[6],ARGV[10])\n"
            + "  redis.call('ZADD',KEYS[4],ARGV[6],ARGV[10])\n"
            + "else\n"
            + "  redis.call('SADD',KEYS[2],ARGV[10])\n"
            + "  redis.call('ZREM',KEYS[3],ARGV[10])\n"
            + "  redis.call('ZREM',KEYS[4],ARGV[10])\n"
            + "end\n"
            + "return 1\n";

    /**
     * KEYS[1]=biz hash, KEYS[2]=round hash; ARGV: bizField, roundId, nItems, items (H key n f v.. | S key member | Z key score member | M key value: raise a max).
     * Returns {1, roundId} created, {0, winningRoundId} when the biz key exists, {-1} when the round id is already used (nothing written).
     */
    private static final String CREATE =
            "local w=redis.call('HGET',KEYS[1],ARGV[1])\n"
            + "if w then return {0,w} end\n"
            + "if redis.call('EXISTS',KEYS[2])==1 then return {-1} end\n"
            + "redis.call('HSET',KEYS[1],ARGV[1],ARGV[2])\n"
            + "local p=4\n"
            + "for _=1,tonumber(ARGV[3]) do\n"
            + "  local t=ARGV[p]\n"
            + "  if t=='H' then\n"
            + "    local n=tonumber(ARGV[p+2])\n"
            + "    for j=0,n-1 do redis.call('HSET',ARGV[p+1],ARGV[p+3+2*j],ARGV[p+4+2*j]) end\n"
            + "    p=p+3+2*n\n"
            + "  elseif t=='S' then redis.call('SADD',ARGV[p+1],ARGV[p+2]); p=p+3\n"
            + "  elseif t=='M' then\n"
            + "    if tonumber(ARGV[p+2])>tonumber(redis.call('GET',ARGV[p+1]) or '0') then redis.call('SET',ARGV[p+1],ARGV[p+2]) end\n"
            + "    p=p+3\n"
            + "  else redis.call('ZADD',ARGV[p+1],ARGV[p+2],ARGV[p+3]); p=p+4 end\n"
            + "end\n"
            + "return {1,ARGV[2]}\n";

    /** Per-edge sums of a DONE chunk record into the counter hash (sign +1 / -1): rowsOut keys starting with 'e' and the input edge (not 'split'). Needs num/fmt. */
    private static final String EDGES_LUA =
            "local function num(x) if type(x)=='number' then return x end return 0 end\n"
            + "local function fmt(x) return string.format('%.0f',x) end\n"
            + "local function edges(nk,rec,sign)\n"
            + "  if type(rec.rowsOut)=='table' then\n"
            + "    for k,v in pairs(rec.rowsOut) do if string.sub(k,1,1)=='e' then redis.call('HINCRBYFLOAT',nk,'" + OUT_PREFIX + "'..k,fmt(sign*num(v))) end end\n"
            + "  end\n"
            + "  if type(rec.inputEdge)=='string' and rec.inputEdge~='split' then redis.call('HINCRBYFLOAT',nk,'" + IN_PREFIX + "'..rec.inputEdge,fmt(sign*num(rec.rowsIn))) end\n"
            + "end\n";

    /**
     * KEYS[1]=round hash; hdr, then ARGV[15]=epoch (always fenced), ARGV[16]=journal offset or '', then 8-tuples (mode,pid,cid,status,json,rowsIn,rowsOutTotal,sinkRows).
     * mode O overwrite, D overwrite unless DONE, N insert if absent. Keeps per-pipeline status counters, DONE row sums, per-edge sums, the count of
     * DONE chunks with sink rows (_sink) in pg:{r}:cnt:PID and the chunk indexes (cidx / cst). The journal offset is written LAST, after every event
     * was applied, so a script that fails midway leaves the offset behind and the batch is retried.
     * Returns 1 ok, 0 fenced, -1 round missing.
     */
    private static final String CHUNKS =
            "local rk=KEYS[1]\n"
            + HDR
            + "local epoch=tonumber(ARGV[15])\n"
            + "local cur=redis.call('HGET',rk,'epoch')\n"
            + "if not cur then return -1 end\n"
            + "if tonumber(cur)>epoch then return 0 end\n"
            + "redis.call('HSET',rk,'epoch',ARGV[15])\n"
            + EDGES_LUA
            + "local function sinkOf(rec)\n"
            + "  local t=0\n"
            + "  if type(rec.rowsOut)=='table' then for k,v in pairs(rec.rowsOut) do if string.sub(k,1,5)=='sink:' then t=t+num(v) end end end\n"
            + "  return t\n"
            + "end\n"
            + "for i=17,#ARGV,8 do\n"
            + "  local mode,pid,cid,st,js=ARGV[i],ARGV[i+1],ARGV[i+2],ARGV[i+3],ARGV[i+4]\n"
            + "  local ck,nk,ik,sk=pre..P_CHUNK..pid,pre..P_CNT..pid,pre..P_CIDX..pid,pre..P_CST..pid\n"
            + "  local old=redis.call('HGET',ck,cid)\n"
            + "  local orec,ost\n"
            + "  if old then orec=cjson.decode(old); ost=orec.status end\n"
            + "  local write=true\n"
            + "  if mode=='N' and old then write=false end\n"
            + "  if mode=='D' and ost=='DONE' then write=false end\n"
            + "  if write then\n"
            + "    if old then\n"
            + "      redis.call('HINCRBY',nk,ost,-1)\n"
            + "      redis.call('ZREM',sk,ost..'|'..cid)\n"
            + "      if ost=='DONE' then\n"
            + "        redis.call('HINCRBYFLOAT',nk,'_in',fmt(-num(orec.rowsIn)))\n"
            + "        local ot=0\n"
            + "        if type(orec.rowsOut)=='table' then for _,v in pairs(orec.rowsOut) do ot=ot+num(v) end end\n"
            + "        redis.call('HINCRBYFLOAT',nk,'_out',fmt(-ot))\n"
            + "        if sinkOf(orec)>0 then redis.call('HINCRBY',nk,'" + SINK_CNT + "',-1) end\n"
            + "        edges(nk,orec,-1)\n"
            + "      end\n"
            + "      if type(orec.splitSpec)=='string' and not string.find(js,'\"splitSpec\":',1,true) then\n"
            + "        js=string.sub(js,1,-2)..',\"splitSpec\":'..cjson.encode(orec.splitSpec)..'}'\n"
            + "      end\n"
            + "    else\n"
            + "      if redis.call('SADD',pre..S_CPLS,pid)==1 then redis.call('HSET',nk,'" + IX + "','1') end\n"
            + "      redis.call('ZADD',ik,0,cid)\n"
            + "    end\n"
            + "    redis.call('HSET',ck,cid,js)\n"
            + "    redis.call('ZADD',sk,0,st..'|'..cid)\n"
            + "    redis.call('HINCRBY',nk,st,1)\n"
            + "    if st=='DONE' then\n"
            + "      redis.call('HINCRBYFLOAT',nk,'_in',ARGV[i+5])\n"
            + "      redis.call('HINCRBYFLOAT',nk,'_out',ARGV[i+6])\n"
            + "      if tonumber(ARGV[i+7])>0 then redis.call('HINCRBY',nk,'" + SINK_CNT + "',1) end\n"
            + "      edges(nk,cjson.decode(js),1)\n"
            + "    end\n"
            + "  end\n"
            + "end\n"
            + "if ARGV[16]~='' then redis.call('HSET',rk,'journalOffset',ARGV[16]) end\n"
            + "return 1\n";

    /** KEYS[1]=chunk hash of one pipeline; hdr, ARGV[15]=pipeline id. Rebuilds the chunk indexes and the per-edge sums of a pipeline written before they existed. */
    private static final String REINDEX =
            HDR
            + "local pid=ARGV[15]\n"
            + "local ck,nk,ik,sk=pre..P_CHUNK..pid,pre..P_CNT..pid,pre..P_CIDX..pid,pre..P_CST..pid\n"
            + EDGES_LUA
            + "redis.call('DEL',ik,sk)\n"
            + "for _,f in ipairs(redis.call('HKEYS',nk)) do\n"
            + "  local a=string.sub(f,1,2)\n"
            + "  if a=='" + OUT_PREFIX + "' or a=='" + IN_PREFIX + "' then redis.call('HDEL',nk,f) end\n"
            + "end\n"
            + "local h=redis.call('HGETALL',ck)\n"
            + "for i=1,#h,2 do\n"
            + "  local rec=cjson.decode(h[i+1])\n"
            + "  redis.call('ZADD',ik,0,h[i])\n"
            + "  redis.call('ZADD',sk,0,rec.status..'|'..h[i])\n"
            + "  if rec.status=='DONE' then edges(nk,rec,1) end\n"
            + "end\n"
            + "redis.call('HSET',nk,'" + IX + "','1')\n"
            + "return #h/2\n";

    /**
     * KEYS: 1 round hash, 2 active set, 3 finished zset, 4 uncompacted zset; hdr, then ARGV[15]=epoch, a revivable-status list (n, names...), nRuns,
     * run ids..., nGates, gate ids.... Atomic rerun reset (see ControlPlaneStore.resetRuns): nothing is applied when the round or an existing listed
     * run is fenced; a run that does not exist is skipped. Returns 1 ok, 0 fenced, -1 round missing.
     */
    private static final String RESET_RUNS =
            "local rk=KEYS[1]\n"
            + HDR
            + READ_SET
            + "local ep=ARGV[15]\n"
            + "local cur=redis.call('HGET',rk,'epoch')\n"
            + "if not cur then return -1 end\n"
            + "if tonumber(cur)>tonumber(ep) then return 0 end\n"
            + "local rev,p=readset(16)\n"
            + "local nr=tonumber(ARGV[p]); p=p+1\n"
            + "local rids={}\n"
            + "for i=1,nr do rids[i]=ARGV[p]; p=p+1 end\n"
            + "local ng=tonumber(ARGV[p]); p=p+1\n"
            + "local gids={}\n"
            + "for i=1,ng do gids[i]=ARGV[p]; p=p+1 end\n"
            + "for i=1,nr do\n"
            + "  local re=redis.call('HGET',pre..P_RUN..rids[i],'epoch')\n"
            + "  if re and tonumber(re)>tonumber(ep) then return 0 end\n"
            + "end\n"
            + "redis.call('HSET',rk,'epoch',ep,'incomplete','0')\n"
            + "local runs={}\n"
            + "for i=1,nr do\n"
            + "  local k=pre..P_RUN..rids[i]\n"
            + "  if redis.call('EXISTS',k)==1 then\n"
            + "    runs[rids[i]]=true\n"
            + "    redis.call('HSET',k,'status','PENDING','epoch',ep)\n"
            + "    redis.call('HINCRBY',k,'attempt',1)\n"
            + "    redis.call('HDEL',k,'assignee','startedAt','finishedAt','error')\n"
            + "  end\n"
            + "end\n"
            + "for _,pid in ipairs(redis.call('SMEMBERS',pre..S_PLS)) do\n"
            + "  local pk=pre..P_PL..pid\n"
            + "  if runs[redis.call('HGET',pk,'runId')] then\n"
            + "    redis.call('HSET',pk,'status','PENDING','enumDone','0','epoch',ep)\n"
            + "    redis.call('HDEL',pk,'startedAt','finishedAt','error','sumCounts','sumIn','sumOut')\n"
            + "    redis.call('DEL',pre..P_CHUNK..pid,pre..P_CNT..pid,pre..P_CIDX..pid,pre..P_CST..pid)\n"
            + "    redis.call('SREM',pre..S_CPLS,pid)\n"
            + "  end\n"
            + "end\n"
            + "for i=1,ng do\n"
            + "  local k=pre..P_GATE..gids[i]\n"
            + "  if redis.call('EXISTS',k)==1 then\n"
            + "    redis.call('HSET',k,'status','CLOSED','alerted','0','epoch',ep)\n"
            + "    redis.call('HDEL',k,'armedAt','deadlineAt','openedBy','openedAt','context')\n"
            + "  end\n"
            + "  redis.call('DEL',pre..P_SIGNAL..gids[i])\n"
            + "end\n"
            + "if rev[redis.call('HGET',rk,'status')] then\n"
            + "  redis.call('HSET',rk,'status','RUNNING')\n"
            + "  redis.call('HDEL',rk,'finishedAt','error','compacted')\n"
            + "  redis.call('SADD',KEYS[2],rid)\n"
            + "  redis.call('ZREM',KEYS[3],rid)\n"
            + "  redis.call('ZREM',KEYS[4],rid)\n"
            + "end\n"
            + "return 1\n";

    /**
     * KEYS: 1 round hash, 2 uncompacted zset; hdr, ARGV[15]=cutoff, a terminal-status list (n, names...), a chunk-status list (n, names...).
     * Compaction (see ControlPlaneStore.compactRound): returns the number of removed chunk records,
     * -1 round missing, -2 not terminal / not old enough, -3 already compacted.
     */
    private static final String COMPACT =
            "local rk=KEYS[1]\n"
            + HDR
            + READ_SET
            + "local term,p=readset(16)\n"
            + "local isSt\n"
            + "isSt,p=readset(p)\n"
            + "local st=redis.call('HGET',rk,'status')\n"
            + "if not st then return -1 end\n"
            + "if not term[st] then redis.call('ZREM',KEYS[2],rid); return -2 end\n"
            + "if redis.call('HGET',rk,'compacted')=='1' then redis.call('ZREM',KEYS[2],rid); return -3 end\n"
            + "local fin=tonumber(redis.call('HGET',rk,'finishedAt') or '')\n"
            + "if (not fin) or fin>=tonumber(ARGV[15]) then return -2 end\n"
            + "local removed=0\n"
            + "for _,pid in ipairs(redis.call('SMEMBERS',pre..S_CPLS)) do\n"
            + "  local ck,nk,pk=pre..P_CHUNK..pid,pre..P_CNT..pid,pre..P_PL..pid\n"
            + "  local h=redis.call('HGETALL',nk)\n"
            + "  local counts,ins,outs={},nil,nil\n"
            + "  for i=1,#h,2 do\n"
            + "    if h[i]=='_in' then ins=h[i+1] elseif h[i]=='_out' then outs=h[i+1]\n"
            + "    elseif isSt[h[i]] and tonumber(h[i+1])>0 then counts[h[i]]=tonumber(h[i+1]) end\n"
            + "  end\n"
            + "  removed=removed+redis.call('HLEN',ck)\n"
            + "  if redis.call('EXISTS',pk)==1 then\n"
            + "    local js='{'\n"
            + "    for f,v in pairs(counts) do if #js>1 then js=js..',' end js=js..'\"'..f..'\":'..string.format('%d',v) end\n"
            + "    redis.call('HSET',pk,'sumCounts',js..'}')\n"
            + "    if ins then redis.call('HSET',pk,'sumIn',ins) end\n"
            + "    if outs then redis.call('HSET',pk,'sumOut',outs) end\n"
            + "  end\n"
            + "  redis.call('UNLINK',ck,nk,pre..P_CIDX..pid,pre..P_CST..pid)\n"
            + "end\n"
            + "redis.call('UNLINK',pre..S_CPLS)\n"
            + "redis.call('HSET',rk,'compacted','1')\n"
            + "redis.call('ZREM',KEYS[2],rid)\n"
            + "return removed\n";

    /**
     * KEYS: 1 round hash, 2 rounds zset, 3 biz hash, 4 active set, 5 finished zset, 6 uncompacted zset; hdr, ARGV[15]=cutoff, 16=per-graph zset key prefix,
     * a terminal-status list (n, names...), then the data plane keys of the round (n, keys...). Atomic status check + delete of every key of the round
     * (see ControlPlaneStore.purgeRounds). Returns 1 purged, 0 not eligible (stale index entries are dropped), -1 missing.
     */
    private static final String PURGE =
            "local rk=KEYS[1]\n"
            + HDR
            + READ_SET
            + "local term,p=readset(17)\n"
            + "local st=redis.call('HGET',rk,'status')\n"
            + "if not st then redis.call('ZREM',KEYS[5],rid); redis.call('ZREM',KEYS[6],rid); return -1 end\n"
            + "if not term[st] then redis.call('ZREM',KEYS[5],rid); redis.call('ZREM',KEYS[6],rid); return 0 end\n"
            + "local fin=tonumber(redis.call('HGET',rk,'finishedAt') or '')\n"
            + "if (not fin) or fin>=tonumber(ARGV[15]) then return 0 end\n"
            + "local del={rk}\n"
            + "local function add(k) del[#del+1]=k end\n"
            + "for _,id in ipairs(redis.call('SMEMBERS',pre..S_RUNS)) do add(pre..P_RUN..id) end\n"
            + "for _,id in ipairs(redis.call('SMEMBERS',pre..S_GATES)) do add(pre..P_GATE..id); add(pre..P_SIGNAL..id) end\n"
            + "local seen={}\n"
            + "for _,set in ipairs({S_PLS,S_CPLS}) do\n"
            + "  for _,id in ipairs(redis.call('SMEMBERS',pre..set)) do\n"
            + "    if not seen[id] then seen[id]=true; add(pre..P_PL..id); add(pre..P_CHUNK..id); add(pre..P_CNT..id); add(pre..P_CIDX..id); add(pre..P_CST..id) end\n"
            + "  end\n"
            + "end\n"
            + "for _,suf in ipairs({S_RUNS,S_GATES,S_PLS,S_CPLS}) do add(pre..suf) end\n"
            + "local nd=tonumber(ARGV[p]); p=p+1\n"
            + "for _=1,nd do add(ARGV[p]); p=p+1 end\n"
            + "local graph=redis.call('HGET',rk,'graphName') or ''\n"
            + "local biz=graph..'|'..(redis.call('HGET',rk,'bizKey') or '')\n"
            + "if redis.call('HGET',KEYS[3],biz)==rid then redis.call('HDEL',KEYS[3],biz) end\n"
            + "redis.call('ZREM',ARGV[16]..graph,rid)\n"
            + "redis.call('ZREM',KEYS[2],rid)\n"
            + "redis.call('ZREM',KEYS[5],rid)\n"
            + "redis.call('ZREM',KEYS[6],rid)\n"
            + "redis.call('SREM',KEYS[4],rid)\n"
            + "redis.call('SADD',KEYS[7],rid)\n"
            + "for i=1,#del,500 do redis.call('UNLINK',unpack(del,i,math.min(i+499,#del))) end\n"
            + "return 1\n";

    /**
     * KEYS[1]=round hash; hdr, ARGV[15]=include planJson (1/0), ARGV[16]=include chunk counters (1/0). One round trip for the whole round state:
     * returns {round fields, runs, gates, pipelines, counters}, each group a flat list (id, fields-list, id, fields-list, ...), the round a flat field list.
     */
    private static final String LOAD =
            HDR
            + "local rkey=KEYS[1]\n"
            + "local r=redis.call('HGETALL',rkey)\n"
            + "if #r==0 then return {{}} end\n"
            + "if ARGV[15]~='1' then\n"
            + "  local o={}\n"
            + "  for i=1,#r,2 do if r[i]~='planJson' then o[#o+1]=r[i]; o[#o+1]=r[i+1] end end\n"
            + "  r=o\n"
            + "end\n"
            + "local function grp(setName,prefix)\n"
            + "  local out={}\n"
            + "  for _,id in ipairs(redis.call('SMEMBERS',pre..setName)) do out[#out+1]=id; out[#out+1]=redis.call('HGETALL',pre..prefix..id) end\n"
            + "  return out\n"
            + "end\n"
            + "local res={r,grp(S_RUNS,P_RUN),grp(S_GATES,P_GATE),grp(S_PLS,P_PL)}\n"
            + "if ARGV[16]=='1' then res[5]=grp(S_CPLS,P_CNT) else res[5]={} end\n"
            + "return res\n";

    /** ARGV[1]=n, ARGV[2..n+1]=hash fields, then hash keys: HMGET of every key in one round trip; one row (nils for absent fields) per key. */
    private static final String HMGET_MANY =
            "local n=tonumber(ARGV[1])\n"
            + "local out={}\n"
            + "for i=n+2,#ARGV do out[#out+1]=redis.call('HMGET',ARGV[i],unpack(ARGV,2,n+1)) end\n"
            + "return out\n";

    private final RedisClient client;
    private volatile StatefulRedisConnection<String, String> conn;

    public RedisControlPlaneStore(RedisClient client) { this.client = client; }

    private RedisCommands<String, String> r() {
        StatefulRedisConnection<String, String> c = conn;
        if (c == null) {   // isOpen() is false during Lettuce's auto-reconnect: never replace (and orphan) that connection
            synchronized (this) {
                c = conn;
                if (c == null) conn = c = client.connect();
            }
        }
        return c.sync();
    }

    /**
     * Creates nothing but runs the one-time backfill (behind the {@link #SCHEMA} marker): retention indexes for terminal rounds created before they
     * existed, the per-graph index, the maximum epoch / seq of the existing rounds (the floors of new epochs and sequences) and the chunk indexes of
     * pipelines written before they existed. Later starts only ping.
     */
    @Override
    public void init() {
        RedisCommands<String, String> c = r();
        c.ping();
        String v = c.get(SCHEMA);
        if (v != null && Long.parseLong(v) >= SCHEMA_VERSION) return;
        backfill(c);
        evalLong(RAISE_MAX, new String[]{SCHEMA}, Collections.singletonList(String.valueOf(SCHEMA_VERSION)));
    }

    private void backfill(RedisCommands<String, String> c) {
        long maxEpoch = 0, maxSeq = 0;
        long total = c.zcard(ROUNDS);
        for (long from = 0; from < total; from += 1000) {
            List<String> ids = c.zrange(ROUNDS, from, from + 999);
            List<String> keys = new ArrayList<>();
            for (String rid : ids) keys.add(k(rid, ROUND));
            List<Map<String, String>> rows = hmgetMany(keys, "graphName", "status", "finishedAt", "createdAt", "compacted", "epoch", "seq");
            for (int i = 0; i < ids.size(); i++) {
                String rid = ids.get(i);
                Map<String, String> h = rows.get(i);
                if (h.containsKey("epoch")) maxEpoch = Math.max(maxEpoch, Long.parseLong(h.get("epoch")));
                if (h.containsKey("seq")) maxSeq = Math.max(maxSeq, Long.parseLong(h.get("seq")));
                String st = h.get("status");
                if (st == null) continue;
                String created = h.containsKey("createdAt") ? h.get("createdAt") : "0";
                if (h.containsKey("graphName")) c.zadd(GRAPH_ROUNDS + h.get("graphName"), ZAddArgs.Builder.nx(), Double.parseDouble(created), rid);
                boolean compacted = "1".equals(h.get("compacted"));
                if (!compacted) {   // chunk indexes for pipelines that predate them
                    for (String pid : c.smembers(k(rid, CPLS)))
                        if (!c.hexists(k(rid, P_CNT + pid), IX)) reindex(rid, pid);
                }
                if (!RoundStatus.valueOf(st).terminal()) continue;
                String fin = h.containsKey("finishedAt") ? h.get("finishedAt") : created;
                c.zadd(FINISHED, ZAddArgs.Builder.nx(), Double.parseDouble(fin), rid);
                if (!compacted) c.zadd(UNCOMPACTED, ZAddArgs.Builder.nx(), Double.parseDouble(fin), rid);
            }
        }
        if (maxEpoch > 0) evalLong(RAISE_MAX, new String[]{MAX_EPOCH}, Collections.singletonList(String.valueOf(maxEpoch)));
        if (maxSeq > 0) evalLong(RAISE_MAX, new String[]{MAX_SEQ}, Collections.singletonList(String.valueOf(maxSeq)));
    }

    private void reindex(String rid, String pid) {
        List<String> a = hdr(rid);
        a.add(pid);
        evalLong(REINDEX, new String[]{k(rid, P_CHUNK + pid)}, a);
    }

    // ---------------------------------------------------------------- keys & helpers

    private static String k(String rid, String suffix) { return "pg:{" + rid + "}:" + suffix; }

    private long evalLong(String script, String[] keys, List<String> args) {
        Long v = r().eval(script, ScriptOutputType.INTEGER, keys, args.toArray(new String[0]));
        return v == null ? 0 : v;
    }

    private List<Object> evalList(String script, String[] keys, List<String> args) {
        return r().eval(script, ScriptOutputType.MULTI, keys, args.toArray(new String[0]));
    }

    /** HMGET of the same fields of many hashes in one round trip; one map (present fields only) per key, in order. */
    private List<Map<String, String>> hmgetMany(List<String> keys, String... fields) {
        List<Map<String, String>> out = new ArrayList<>();
        if (keys.isEmpty()) return out;
        List<String> args = new ArrayList<>();
        args.add(String.valueOf(fields.length));
        Collections.addAll(args, fields);
        args.addAll(keys);
        List<Object> rows = evalList(HMGET_MANY, new String[0], args);
        for (Object row : rows) {
            List<?> vals = (List<?>) row;
            Map<String, String> m = new LinkedHashMap<>();
            for (int i = 0; i < fields.length; i++) if (vals.get(i) != null) m.put(fields[i], String.valueOf(vals.get(i)));
            out.add(m);
        }
        return out;
    }

    private static void put(List<String> out, Object v, String field) { if (v != null) { out.add(field); out.add(String.valueOf(v)); } }

    private static void putBool(List<String> out, boolean v, String field) { out.add(field); out.add(v ? "1" : "0"); }

    private static String json(Object o) { return StoreViews.json(o); }

    private static Long lng(Map<String, String> h, String f) { String v = h.get(f); return v == null ? null : Long.valueOf(v); }

    private static long lng0(Map<String, String> h, String f) { Long v = lng(h, f); return v == null ? 0 : v; }

    private static int int0(Map<String, String> h, String f) { String v = h.get(f); return v == null ? 0 : Integer.parseInt(v); }

    private static boolean bool(Map<String, String> h, String f) { return "1".equals(h.get(f)); }

    private static long sinkRowsOf(Map<String, Long> m) { return StoreViews.sinkRowsOf(m); }

    private static long totalOf(Map<String, Long> m) { return StoreViews.totalOf(m); }

    /** A flat field/value list (HGETALL shape) as a map. */
    private static Map<String, String> flat(Object o) {
        Map<String, String> m = new LinkedHashMap<>();
        List<?> l = (List<?>) o;
        for (int i = 0; i + 1 < l.size(); i += 2) m.put(String.valueOf(l.get(i)), String.valueOf(l.get(i + 1)));
        return m;
    }

    /** A group (id, fields, id, fields, ...) as id to fields, in id order. */
    private static Map<String, Map<String, String>> group(Object o) {
        Map<String, Map<String, String>> m = new java.util.TreeMap<>();
        List<?> l = (List<?>) o;
        for (int i = 0; i + 1 < l.size(); i += 2) m.put(String.valueOf(l.get(i)), flat(l.get(i + 1)));
        return m;
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

    private boolean mutate(String roundId, String key, Ops o) { return evalLong(MUTATE, new String[]{key, k(roundId, ROUND)}, o.args) == 1; }

    /** KEYS[1]=max key; ARGV[1]=value. Raises the stored number to the value if it is higher; returns 1 when it did. */
    private static final String RAISE_MAX =
            "if tonumber(ARGV[1])>tonumber(redis.call('GET',KEYS[1]) or '0') then redis.call('SET',KEYS[1],ARGV[1]); return 1 end\n"
            + "return 0\n";

    /** KEYS[1]=round hash, KEYS[2]=max epoch key; ARGV[1]=epoch. 1 fenced ok, 0 higher epoch holds the round or it is missing. Raises the max only when the round's epoch actually rises (not on every tick). */
    private static final String FENCE =
            "local cur=redis.call('HGET',KEYS[1],'epoch')\n"
            + "if not cur then return 0 end\n"
            + "if tonumber(cur)>tonumber(ARGV[1]) then return 0 end\n"
            + "if tonumber(cur)<tonumber(ARGV[1]) then\n"
            + "  redis.call('HSET',KEYS[1],'epoch',ARGV[1])\n"
            + "  if tonumber(ARGV[1])>tonumber(redis.call('GET',KEYS[2]) or '0') then redis.call('SET',KEYS[2],ARGV[1]) end\n"
            + "end\n"
            + "return 1\n";

    /** KEYS[1]=max epoch key; ARGV[1]=epoch. Compare-and-set: stores the epoch and returns 1 iff it is above the stored maximum, else 0. */
    private static final String RESERVE_EPOCH =
            "if tonumber(ARGV[1])>tonumber(redis.call('GET',KEYS[1]) or '0') then redis.call('SET',KEYS[1],ARGV[1]); return 1 end\n"
            + "return 0\n";

    @Override
    public boolean fenceRound(String roundId, long epoch) {
        return evalLong(FENCE, new String[]{k(roundId, ROUND), MAX_EPOCH}, Collections.singletonList(String.valueOf(epoch))) == 1;
    }

    @Override
    public boolean reserveMasterEpoch(long epoch) {
        return evalLong(RESERVE_EPOCH, new String[]{MAX_EPOCH}, Collections.singletonList(String.valueOf(epoch))) == 1;
    }

    @Override
    public long maxMasterEpoch() { return longOf(r().get(MAX_EPOCH)); }

    @Override
    public long maxRoundSeq() { return longOf(r().get(MAX_SEQ)); }

    private static long longOf(String v) { return v == null ? 0L : Long.parseLong(v); }

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
        if (h.isEmpty() || !h.containsKey("status")) return null;
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
                .finishedAt(lng(h, "finishedAt")).error(h.get("error")).summaryCounts(StoreViews.longMap(h.get("sumCounts")))
                .summaryRowsIn(h.containsKey("sumIn") ? Long.valueOf(Math.round(Double.parseDouble(h.get("sumIn")))) : null)
                .summaryRowsOut(h.containsKey("sumOut") ? Long.valueOf(Math.round(Double.parseDouble(h.get("sumOut")))) : null).build();
    }

    @Override
    public RoundRecord createRound(RoundRecord round, List<RunRecord> runs, List<GateRecord> gates, List<PipelineRecord> pipelines) {
        if (round.getCreatedAt() == null) round.setCreatedAt(System.currentTimeMillis());
        if (round.getStatus() == null) round.setStatus(RoundStatus.CREATED);
        String rid = round.getRoundId();
        List<String> items = new ArrayList<>();
        int n = 0;
        n += hashItem(items, k(rid, ROUND), roundFields(round));
        for (RunRecord u : runs == null ? Collections.<RunRecord>emptyList() : runs) {
            List<String> f = new ArrayList<>();
            put(f, u.getStage(), "stage"); put(f, u.getMode(), "mode"); put(f, u.getStatus() == null ? RunStatus.PENDING : u.getStatus(), "status");
            put(f, u.getAttempt(), "attempt"); put(f, u.getAssignee(), "assignee"); put(f, u.getMasterEpoch(), "epoch");
            put(f, u.getStartedAt(), "startedAt"); put(f, u.getFinishedAt(), "finishedAt"); put(f, u.getError(), "error");
            n += hashItem(items, k(rid, P_RUN + u.getRunId()), f);
            n += setItem(items, k(rid, RUNS), u.getRunId());
        }
        for (GateRecord g : gates == null ? Collections.<GateRecord>emptyList() : gates) {
            List<String> f = new ArrayList<>();
            put(f, g.getType(), "type"); put(f, json(g.getBeforeRuns()), "beforeRuns"); put(f, g.getStatus() == null ? GateStatus.CLOSED : g.getStatus(), "status");
            put(f, g.getArmedAt(), "armedAt"); put(f, g.getDeadlineAt(), "deadlineAt"); put(f, g.getOpenedBy(), "openedBy"); put(f, g.getOpenedAt(), "openedAt");
            put(f, json(g.getContext()), "context"); put(f, g.getMasterEpoch(), "epoch"); putBool(f, g.isAlerted(), "alerted");
            n += hashItem(items, k(rid, P_GATE + g.getGateId()), f);
            n += setItem(items, k(rid, GATES), g.getGateId());
        }
        for (PipelineRecord p : pipelines == null ? Collections.<PipelineRecord>emptyList() : pipelines) {
            List<String> f = new ArrayList<>();
            put(f, p.getRunId(), "runId"); put(f, p.getStatus() == null ? PipelineStatus.PENDING : p.getStatus(), "status"); put(f, p.getParallelism(), "parallelism");
            putBool(f, p.isEnumDone(), "enumDone"); put(f, p.getMasterEpoch(), "epoch"); put(f, p.getStartedAt(), "startedAt");
            put(f, p.getFinishedAt(), "finishedAt"); put(f, p.getError(), "error");
            n += hashItem(items, k(rid, P_PL + p.getPipelineId()), f);
            n += setItem(items, k(rid, PLS), p.getPipelineId());
        }
        items.add("Z"); items.add(ROUNDS); items.add(String.valueOf(round.getCreatedAt())); items.add(rid); n++;
        items.add("Z"); items.add(GRAPH_ROUNDS + round.getGraphName()); items.add(String.valueOf(round.getCreatedAt())); items.add(rid); n++;
        items.add("M"); items.add(MAX_SEQ); items.add(String.valueOf(round.getSeq())); n++;
        if (!round.getStatus().terminal()) n += setItem(items, ACTIVE, rid);
        List<String> args = new ArrayList<>();
        args.add(round.getGraphName() + "|" + round.getBizKey());
        args.add(rid);
        args.add(String.valueOf(n));
        args.addAll(items);
        List<Object> res = evalList(CREATE, new String[]{BIZ, k(rid, ROUND)}, args);
        long code = ((Number) res.get(0)).longValue();
        if (code == -1) throw new DuplicateRoundIdException(rid);
        return code == 1 ? round : loadRoundRecord(String.valueOf(res.get(1)));
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

    /** The round without planJson (one HMGET of the light fields). */
    @Override
    public RoundRecord loadRoundRecord(String roundId) {
        List<Map<String, String>> rows = hmgetMany(Collections.singletonList(k(roundId, ROUND)), ROUND_LIGHT);
        return toRound(rows.get(0));
    }

    private List<RoundRecord> loadLight(List<String> rids) {
        List<String> keys = new ArrayList<>();
        for (String rid : rids) keys.add(k(rid, ROUND));
        List<RoundRecord> out = new ArrayList<>();
        for (Map<String, String> h : hmgetMany(keys, ROUND_LIGHT)) {
            RoundRecord rr = toRound(h);
            if (rr != null) out.add(rr);
        }
        return out;
    }

    /** Loads the round, runs, gates and pipelines (and the chunk counters when asked) in one round trip. */
    @SuppressWarnings("unchecked")
    private RoundView loadView(String roundId, boolean withPlan, boolean withCounts) {
        List<String> a = hdr(roundId);
        a.add(withPlan ? "1" : "0");
        a.add(withCounts ? "1" : "0");
        List<Object> res = evalList(LOAD, new String[]{k(roundId, ROUND)}, a);
        RoundRecord round = toRound(flat(res.get(0)));
        if (round == null) return null;
        List<RunRecord> runs = new ArrayList<>();
        for (Map.Entry<String, Map<String, String>> e : group(res.get(1)).entrySet()) runs.add(toRun(roundId, e.getKey(), e.getValue()));
        runs.sort(Comparator.comparingInt(RunRecord::getStage).thenComparing(RunRecord::getRunId));
        List<GateRecord> gates = new ArrayList<>();
        for (Map.Entry<String, Map<String, String>> e : group(res.get(2)).entrySet()) gates.add(toGate(roundId, e.getKey(), e.getValue()));
        List<PipelineRecord> pls = new ArrayList<>();
        for (Map.Entry<String, Map<String, String>> e : group(res.get(3)).entrySet()) pls.add(toPipeline(roundId, e.getKey(), e.getValue()));
        Map<String, Map<ChunkStatus, Long>> counts = new LinkedHashMap<>();
        Map<String, Long> rowsIn = new LinkedHashMap<>();
        Map<String, Long> rowsOut = new LinkedHashMap<>();
        if (withCounts) {
            for (Map.Entry<String, Map<String, String>> e : group(res.get(4)).entrySet()) {
                Map<String, String> h = e.getValue();
                Map<ChunkStatus, Long> m = new EnumMap<>(ChunkStatus.class);
                for (ChunkStatus s : ChunkStatus.values()) {
                    long n = lng0(h, s.name());
                    if (n > 0) m.put(s, n);
                }
                if (m.isEmpty()) continue;
                counts.put(e.getKey(), m);
                if (h.containsKey("_in") || h.containsKey("_out")) {
                    rowsIn.put(e.getKey(), Math.round(Double.parseDouble(h.getOrDefault("_in", "0"))));
                    rowsOut.put(e.getKey(), Math.round(Double.parseDouble(h.getOrDefault("_out", "0"))));
                }
            }
            StoreViews.mergeSummaries(pls, counts, rowsIn, rowsOut);   // compacted pipelines: chunk records are gone, the summary stays
        }
        return RoundView.builder().round(round).runs(runs).gates(gates).pipelines(pls).chunkCounts(counts).rowsIn(rowsIn).rowsOut(rowsOut).build();
    }

    @Override
    public RoundView loadRoundState(String roundId) { return loadView(roundId, false, false); }

    @Override
    public RoundView loadRound(String roundId) { return loadView(roundId, true, true); }

    @Override
    public List<String> findActiveRounds() {
        RedisCommands<String, String> c = r();
        List<String> ids = new ArrayList<>(c.smembers(ACTIVE));
        List<String> keys = new ArrayList<>();
        for (String rid : ids) keys.add(k(rid, ROUND));
        List<Map<String, String>> rows = hmgetMany(keys, "status", "createdAt");
        List<Object[]> found = new ArrayList<>();
        for (int i = 0; i < ids.size(); i++) {
            String rid = ids.get(i);
            String st = rows.get(i).get("status");
            if (st == null || RoundStatus.valueOf(st).terminal()) { c.srem(ACTIVE, rid); continue; }
            found.add(new Object[]{rid, rows.get(i).containsKey("createdAt") ? Long.parseLong(rows.get(i).get("createdAt")) : 0L});
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
        Set<String> seen = new HashSet<>();
        List<RoundRecord> active = new ArrayList<>();
        for (RoundRecord rr : loadLight(findActiveRounds())) {
            if (all || graphName.equals(rr.getGraphName())) { active.add(rr); seen.add(rr.getRoundId()); }
        }
        active.sort(recentFirst());
        for (RoundRecord rr : active) { if (out.size() >= limit) return out; out.add(rr); }
        // newest first from a created-at sorted index: the graph's own index (one ZREVRANGE per page), else the global one
        String key = ROUNDS;
        boolean filter = !all;
        if (!all) {
            String gk = GRAPH_ROUNDS + graphName;
            if (c.zcard(gk) > 0) { key = gk; filter = false; }
        }
        final int batch = Math.max(limit + seen.size(), 50);
        final long maxScan = key.equals(ROUNDS) && filter ? 5000 : Long.MAX_VALUE;   // the legacy global scan stays bounded
        long scanned = 0;
        for (long from = 0; out.size() < limit && scanned < maxScan; from += batch) {
            List<String> ids = c.zrevrange(key, from, from + batch - 1);
            if (ids.isEmpty()) break;
            scanned += ids.size();
            List<String> fresh = new ArrayList<>();
            for (String rid : ids) if (!seen.contains(rid)) fresh.add(rid);
            for (RoundRecord rr : loadLight(fresh)) {
                if (filter && !graphName.equals(rr.getGraphName())) continue;
                out.add(rr);
                if (out.size() >= limit) break;
            }
            if (ids.size() < batch) break;
        }
        return out;
    }

    private static Comparator<RoundRecord> recentFirst() {
        return (a, b) -> {
            long x = a.getCreatedAt() == null ? 0 : a.getCreatedAt(), y = b.getCreatedAt() == null ? 0 : b.getCreatedAt();
            return x != y ? Long.compare(y, x) : b.getRoundId().compareTo(a.getRoundId());
        };
    }

    private Ops statusOps(String expect, String target, String error, long epoch, boolean running, boolean terminal) {
        long now = System.currentTimeMillis();
        Ops o = new Ops(expect, epoch).set("status", target);
        if ("FAILED".equals(expect) && "RUNNING".equals(target)) o.del("error").del("finishedAt");   // revive (contract C1)
        o.setIfNotNull("error", error);
        if (running) o.setIfNull("startedAt", now);
        if (terminal) o.set("finishedAt", now);
        return o;
    }

    @Override
    public boolean casRoundStatus(String roundId, RoundStatus expect, RoundStatus target, String error, long epoch) {
        List<String> a = new ArrayList<>();
        a.add(expect.name()); a.add(String.valueOf(epoch)); a.add(target.name());
        a.add(error == null ? "0" : "1"); a.add(error == null ? "" : error);
        a.add(String.valueOf(System.currentTimeMillis()));
        a.add(target == RoundStatus.RUNNING ? "1" : "0");
        a.add(target.terminal() ? "1" : "0");
        a.add(expect == RoundStatus.FAILED && target == RoundStatus.RUNNING ? "1" : "0");
        a.add(roundId);
        return evalLong(CAS_ROUND, new String[]{k(roundId, ROUND), ACTIVE, FINISHED, UNCOMPACTED}, a) == 1;
    }

    @Override
    public boolean setRoundIncomplete(String roundId, long epoch) { return mutate(roundId, k(roundId, ROUND), new Ops(null, epoch).set("incomplete", "1")); }

    @Override
    public boolean casRunStatus(String roundId, String runId, RunStatus expect, RunStatus target, String error, long epoch) {
        return mutate(roundId, k(roundId, P_RUN + runId), statusOps(expect.name(), target.name(), error, epoch, target == RunStatus.RUNNING, target.terminal()));
    }

    @Override
    public boolean updateRunAssignment(String roundId, String runId, String assignee, int attempt, long epoch) {
        return mutate(roundId, k(roundId, P_RUN + runId), new Ops(null, epoch).set("assignee", assignee).set("attempt", attempt));
    }

    @Override
    public boolean casPipelineStatus(String roundId, String pipelineId, PipelineStatus expect, PipelineStatus target, String error, long epoch) {
        return mutate(roundId, k(roundId, P_PL + pipelineId), statusOps(expect.name(), target.name(), error, epoch, target == PipelineStatus.RUNNING, target.terminal()));
    }

    @Override
    public void setPipelineEnumDone(String roundId, String pipelineId, boolean done, long epoch) {
        mutate(roundId, k(roundId, P_PL + pipelineId), new Ops(null, epoch).set("enumDone", done ? "1" : "0"));
    }

    @Override
    public void setPipelineParallelism(String roundId, String pipelineId, int parallelism, long epoch) {
        mutate(roundId, k(roundId, P_PL + pipelineId), new Ops(null, epoch).set("parallelism", parallelism));
    }

    // ---------------------------------------------------------------- gates

    @Override
    public void armGate(String roundId, String gateId, long armedAt, Long deadlineAt, long epoch) {
        mutate(roundId, k(roundId, P_GATE + gateId), new Ops(null, epoch).set("armedAt", armedAt).set("deadlineAt", deadlineAt));
    }

    @Override
    public boolean casGateStatus(String roundId, String gateId, GateStatus expect, GateStatus target, String openedBy, Map<String, String> context, long epoch) {
        Ops o = new Ops(expect.name(), epoch).set("status", target).setIfNotNull("openedBy", openedBy).setIfNotNull("context", json(context));
        if (target != GateStatus.CLOSED) o.set("openedAt", System.currentTimeMillis());
        return mutate(roundId, k(roundId, P_GATE + gateId), o);
    }

    @Override
    public void markGateAlerted(String roundId, String gateId) { mutate(roundId, k(roundId, P_GATE + gateId), new Ops(null, -1).set("alerted", "1")); }

    @Override
    public void saveGateSignal(String roundId, String gateId, String signalKey, String payload) {
        if (signalKey == null || signalKey.isEmpty() || signalKey.length() > MAX_SIGNAL_KEY_LENGTH)
            throw new IllegalArgumentException("signal key must be 1.." + MAX_SIGNAL_KEY_LENGTH + " characters, got " + (signalKey == null ? "null" : String.valueOf(signalKey.length())));
        r().hsetnx(k(roundId, P_SIGNAL + gateId), signalKey, payload == null ? "" : payload);
    }

    @Override
    public Map<String, String> loadGateSignals(String roundId, String gateId) {
        return new LinkedHashMap<>(r().hgetall(k(roundId, P_SIGNAL + gateId)));
    }

    // ---------------------------------------------------------------- chunks

    private static void tuple(List<String> args, String mode, ChunkRecord c) {
        args.add(mode); args.add(c.getPipelineId()); args.add(c.getChunkId()); args.add(c.getStatus().name()); args.add(ChunkCodec.toJson(c));
        args.add(String.valueOf(c.getRowsIn() == null ? 0 : c.getRowsIn())); args.add(String.valueOf(totalOf(c.getRowsOut())));
        args.add(String.valueOf(sinkRowsOf(c.getRowsOut())));
    }

    private long chunkScript(String roundId, long epoch, String offset, List<String> tuples) {
        List<String> args = hdr(roundId);
        args.add(String.valueOf(epoch)); args.add(offset == null ? "" : offset);
        args.addAll(tuples);
        return evalLong(CHUNKS, new String[]{k(roundId, ROUND)}, args);
    }

    @Override
    public void savePendingChunks(String roundId, List<ChunkRecord> chunks, long epoch) {
        if (chunks == null || chunks.isEmpty()) return;
        long now = System.currentTimeMillis();
        List<String> t = new ArrayList<>();
        for (ChunkRecord c : chunks) {
            c.setRoundId(roundId);
            if (c.getStatus() == null) c.setStatus(ChunkStatus.PENDING);
            if (c.getUpdatedAt() == null) c.setUpdatedAt(now);
            tuple(t, "N", c);
        }
        if (chunkScript(roundId, epoch, null, t) != 1) log.warn("savePendingChunks for round {} fenced or missing (epoch {}); nothing stored", roundId, epoch);
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

    /** Every chunk record of one pipeline (HSCAN in pages: never one huge HGETALL). Used for pipelines that predate the chunk indexes. */
    private List<ChunkRecord> scanChunks(String roundId, String pipelineId) {
        RedisCommands<String, String> c = r();
        List<ChunkRecord> out = new ArrayList<>();
        String key = k(roundId, P_CHUNK + pipelineId);
        io.lettuce.core.MapScanCursor<String, String> cur = c.hscan(key, ScanArgs.Builder.limit(1000));
        while (true) {
            for (String js : cur.getMap().values()) out.add(ChunkCodec.fromJson(js, ChunkRecord.class));
            if (cur.isFinished()) break;
            cur = c.hscan(key, cur, ScanArgs.Builder.limit(1000));
        }
        return out;
    }

    private boolean indexed(RedisCommands<String, String> c, String roundId, String pipelineId) {
        return c.hexists(k(roundId, P_CNT + pipelineId), IX);
    }

    /** Pipelines that have chunk records, sorted; when {@code runId} is given, only those of that run (by the pipeline record). */
    private List<String> chunkPipelines(RedisCommands<String, String> c, String roundId, String runId) {
        List<String> pids = new ArrayList<>(new TreeSet<>(c.smembers(k(roundId, CPLS))));
        if (runId == null) return pids;
        List<String> keys = new ArrayList<>();
        for (String pid : pids) keys.add(k(roundId, P_PL + pid));
        List<Map<String, String>> rows = hmgetMany(keys, "runId");
        List<String> out = new ArrayList<>();
        for (int i = 0; i < pids.size(); i++) if (runId.equals(rows.get(i).get("runId"))) out.add(pids.get(i));
        return out;
    }

    private static Range<String> statusRange(ChunkStatus s) { return Range.from(Range.Boundary.including(s.name() + "|"), Range.Boundary.excluding(s.name() + "}")); }

    private List<ChunkRecord> loadChunks(RedisCommands<String, String> c, String roundId, String pipelineId, List<String> chunkIds) {
        List<ChunkRecord> out = new ArrayList<>();
        if (chunkIds.isEmpty()) return out;
        for (KeyValue<String, String> kv : c.hmget(k(roundId, P_CHUNK + pipelineId), chunkIds.toArray(new String[0])))
            if (kv.hasValue()) out.add(ChunkCodec.fromJson(kv.getValue(), ChunkRecord.class));
        return out;
    }

    @Override
    public void markChunksSkipped(String roundId, String runId, long epoch) {
        RedisCommands<String, String> c = r();
        long now = System.currentTimeMillis();
        for (String pid : chunkPipelines(c, roundId, runId)) {
            if (!indexed(c, roundId, pid)) {   // legacy pipeline without indexes
                List<String> t = new ArrayList<>();
                for (ChunkRecord ck : scanChunks(roundId, pid)) {
                    if (ck.getStatus() != ChunkStatus.DEAD) continue;
                    ck.setStatus(ChunkStatus.SKIPPED);
                    ck.setUpdatedAt(now);
                    tuple(t, "D", ck);
                }
                if (!t.isEmpty() && chunkScript(roundId, epoch, null, t) != 1) { log.warn("markChunksSkipped for round {} fenced (epoch {}); nothing applied", roundId, epoch); return; }
                continue;
            }
            String sk = k(roundId, P_CST + pid);
            String prefix = ChunkStatus.DEAD.name() + "|";
            for (int guard = 0; guard < 100_000; guard++) {   // each pass moves up to 500 chunks out of the DEAD set
                List<String> members = c.zrangebylex(sk, statusRange(ChunkStatus.DEAD), Limit.create(0, 500));
                if (members.isEmpty()) break;
                List<String> ids = new ArrayList<>();
                for (String m : members) ids.add(m.substring(prefix.length()));
                List<String> t = new ArrayList<>();
                for (ChunkRecord ck : loadChunks(c, roundId, pid, ids)) {
                    ck.setStatus(ChunkStatus.SKIPPED);
                    ck.setUpdatedAt(now);
                    tuple(t, "D", ck);
                }
                if (t.isEmpty() || chunkScript(roundId, epoch, null, t) != 1) {
                    if (!t.isEmpty()) log.warn("markChunksSkipped for round {} fenced (epoch {}); nothing applied", roundId, epoch);
                    return;
                }
            }
        }
    }

    @Override
    public void resetRuns(String roundId, List<String> runIds, List<String> gateIds, long epoch) {
        List<String> args = hdr(roundId);
        args.add(String.valueOf(epoch));
        addList(args, RoundStatus.revivableNames());
        addList(args, runIds);
        addList(args, gateIds);
        long res = evalLong(RESET_RUNS, new String[]{k(roundId, ROUND), ACTIVE, FINISHED, UNCOMPACTED}, args);
        if (res != 1) throw new StaleEpochException("resetRuns for round " + roundId + " fenced or missing (epoch " + epoch + ")");
    }

    private static void addList(List<String> args, List<String> l) {
        args.add(String.valueOf(l.size()));
        args.addAll(l);
    }

    @Override
    public List<ChunkRecord> queryChunks(ChunkQuery q) {
        List<ChunkRecord> out = new ArrayList<>();
        if (q.getRoundId() == null) return out;
        RedisCommands<String, String> c = r();
        String roundId = q.getRoundId();
        long skip = Math.max(q.getOffset(), 0);
        int limit = Math.max(q.getLimit(), 0);
        List<String> pids = q.getPipelineId() != null ? Collections.singletonList(q.getPipelineId()) : chunkPipelines(c, roundId, q.getRunId());
        for (String pid : pids) {
            if (out.size() >= limit) break;
            boolean ix = indexed(c, roundId, pid);
            boolean runOk = q.getRunId() == null || q.getPipelineId() == null || runOf(c, roundId, pid).equals(q.getRunId());
            if (!ix || !runOk) {
                if (ix) continue;   // indexed pipeline of another run
                // legacy pipeline: decode, filter, sort, slice (this pipeline only)
                List<ChunkRecord> l = new ArrayList<>();
                for (ChunkRecord ck : scanChunks(roundId, pid)) {
                    if (q.getRunId() != null && !q.getRunId().equals(ck.getRunId())) continue;
                    if (q.getStatus() != null && q.getStatus() != ck.getStatus()) continue;
                    l.add(ck);
                }
                l.sort(Comparator.comparing(ChunkRecord::getChunkId));
                for (ChunkRecord ck : l) {
                    if (skip > 0) { skip--; continue; }
                    if (out.size() >= limit) break;
                    out.add(ck);
                }
                continue;
            }
            String key = q.getStatus() != null ? k(roundId, P_CST + pid) : k(roundId, P_CIDX + pid);
            long n = q.getStatus() != null ? c.zlexcount(key, statusRange(q.getStatus())) : c.zcard(key);
            if (skip >= n) { skip -= n; continue; }
            int want = limit - out.size();
            List<String> members = q.getStatus() != null
                    ? c.zrangebylex(key, statusRange(q.getStatus()), Limit.create(skip, want))
                    : c.zrangebylex(key, Range.unbounded(), Limit.create(skip, want));
            skip = 0;
            List<String> ids = new ArrayList<>();
            int cut = q.getStatus() != null ? q.getStatus().name().length() + 1 : 0;
            for (String m : members) ids.add(m.substring(cut));
            out.addAll(loadChunks(c, roundId, pid, ids));
        }
        return out;
    }

    private String runOf(RedisCommands<String, String> c, String roundId, String pipelineId) {
        String v = c.hget(k(roundId, P_PL + pipelineId), "runId");
        return v == null ? "" : v;
    }

    @Override
    public long countChunks(String roundId, String pipelineId, ChunkStatus status) {
        RedisCommands<String, String> c = r();
        Set<String> pls = pipelineId != null ? Collections.singleton(pipelineId) : c.smembers(k(roundId, CPLS));
        long total = 0;
        for (String pid : pls) {
            Map<String, String> h = c.hgetall(k(roundId, P_CNT + pid));
            for (ChunkStatus s : ChunkStatus.values()) if (status == null || status == s) total += Math.max(0, lng0(h, s.name()));
        }
        return total;
    }

    @Override
    public long countSinkChunks(String roundId, String pipelineId) {
        String v = r().hget(k(roundId, P_CNT + pipelineId), SINK_CNT);
        return v == null ? 0L : Math.max(0L, Long.parseLong(v));
    }

    /** Per-edge DONE row sums of a run from the per-pipeline counters (a pipeline that predates them is summed from its records). */
    private Map<String, Long> edgeSums(String roundId, String runId, boolean out) {
        Map<String, Long> res = new LinkedHashMap<>();
        RedisCommands<String, String> c = r();
        String prefix = out ? OUT_PREFIX : IN_PREFIX;
        for (String pid : chunkPipelines(c, roundId, runId)) {
            Map<String, String> h = c.hgetall(k(roundId, P_CNT + pid));
            if (h.containsKey(IX)) {
                for (Map.Entry<String, String> e : h.entrySet())
                    if (e.getKey().startsWith(prefix)) res.merge(e.getKey().substring(prefix.length()), Math.round(Double.parseDouble(e.getValue())), Long::sum);
                continue;
            }
            for (ChunkRecord ck : scanChunks(roundId, pid)) {
                if (ck.getStatus() != ChunkStatus.DONE) continue;
                if (out) {
                    if (ck.getRowsOut() != null) for (Map.Entry<String, Long> e : ck.getRowsOut().entrySet()) if (e.getKey().startsWith("e")) res.merge(e.getKey(), e.getValue(), Long::sum);
                } else if (ck.getInputEdge() != null && !"split".equals(ck.getInputEdge())) {
                    res.merge(ck.getInputEdge(), ck.getRowsIn() == null ? 0L : ck.getRowsIn(), Long::sum);
                }
            }
        }
        return res;
    }

    @Override
    public Map<String, Long> sumRowsOutByEdge(String roundId, String runId) { return edgeSums(roundId, runId, true); }

    @Override
    public Map<String, Long> sumRowsInByEdge(String roundId, String runId) { return edgeSums(roundId, runId, false); }

    // ---------------------------------------------------------------- retention

    @Override
    public int compactRound(String roundId, long finishedBeforeMillis) {
        List<String> args = hdr(roundId);
        args.add(String.valueOf(finishedBeforeMillis));
        addList(args, RoundStatus.terminalNames());
        List<String> statuses = new ArrayList<>();
        for (ChunkStatus s : ChunkStatus.values()) statuses.add(s.name());
        addList(args, statuses);
        long n = evalLong(COMPACT, new String[]{k(roundId, ROUND), UNCOMPACTED}, args);
        return n < 0 ? 0 : (int) n;
    }

    private boolean purge(String rid, long finishedBeforeMillis) {
        List<String> args = hdr(rid);
        args.add(String.valueOf(finishedBeforeMillis));
        args.add(GRAPH_ROUNDS);
        addList(args, RoundStatus.terminalNames());
        // the data plane's per-round keys that die with the round
        addList(args, java.util.Arrays.asList(Keys.desired(rid), Keys.assign(rid), Keys.paused(rid), Keys.fence(rid), Keys.journal(rid)));
        return evalLong(PURGE, new String[]{k(rid, ROUND), ROUNDS, BIZ, ACTIVE, FINISHED, UNCOMPACTED, PENDING_PURGE}, args) == 1;
    }

    @Override
    public List<String> purgeRounds(long finishedBeforeMillis, int max) {
        List<String> purged = new ArrayList<>();
        if (max <= 0) return purged;
        RedisCommands<String, String> c = r();
        Range<Double> range = Range.from(Range.Boundary.unbounded(), Range.Boundary.excluding((double) finishedBeforeMillis));
        // stale index entries are dropped by the script, so ask for a few more candidates than needed
        for (String rid : c.zrangebyscore(FINISHED, range, Limit.create(0, max * 2))) {
            if (purged.size() >= max) break;
            if (purge(rid, finishedBeforeMillis)) purged.add(rid);
        }
        return purged;
    }

    @Override
    public List<String> pendingPurges(int max) {
        List<String> out = new ArrayList<>();
        if (max <= 0) return out;
        out.addAll(r().srandmember(PENDING_PURGE, max));
        return out;
    }

    @Override
    public void clearPendingPurges(java.util.Collection<String> roundIds) {
        if (roundIds == null || roundIds.isEmpty()) return;
        r().srem(PENDING_PURGE, roundIds.toArray(new String[0]));
    }

    @Override
    public List<String> findTerminalRounds(long finishedBeforeMillis, int max) {
        List<String> out = new ArrayList<>();
        if (max <= 0) return out;
        RedisCommands<String, String> c = r();
        Range<Double> range = Range.from(Range.Boundary.unbounded(), Range.Boundary.excluding((double) finishedBeforeMillis));
        for (String rid : c.zrangebyscore(UNCOMPACTED, range, Limit.create(0, max * 2))) {
            if (out.size() >= max) break;
            List<Map<String, String>> v = hmgetMany(Collections.singletonList(k(rid, ROUND)), "status", "compacted");
            String st = v.get(0).get("status");
            boolean ok = st != null && RoundStatus.valueOf(st).terminal() && !"1".equals(v.get(0).get("compacted"));
            if (ok) out.add(rid); else c.zrem(UNCOMPACTED, rid);
        }
        return out;
    }
}
