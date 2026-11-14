#!/usr/bin/env python3
"""Admin-operations x zombie-process chaos scenarios for pipeliner-example (invoked by chaos-admin.sh; env parameters see there).

A  rerun R2 with CHANGED input while a worker of the old attempt is SIGSTOPped (zombie), then SIGCONT.
B  injected transient fault -> R2 FAILED + dead letters -> fault off -> retry (variant 1 plain, variant 2 with the leader frozen right after).
C  cancel a round with a frozen straggler, resubmit the SAME planVersion under another bizKey, wake the straggler during the new round.
D  admin rerunFrom (variant 1: R2 via a follower, variant 2: R3 via the leader itself) immediately followed by a leader SIGSTOP.
Reuses the harness of chaos.py (pods, redis hygiene, verification). A background chaos loop (kill / kill_leader / stop_leader / stop_worker)
runs during the scenarios unless BG_CHAOS=0; it never touches a pod a scenario has reserved and stays quiet during critical windows.
"""
import json
import os
import signal
import subprocess
import sys
import threading
import time
import urllib.request
import urllib.error

import chaos as C

E = os.environ.get
SCENARIOS = E("SCENARIOS", "A,B,C,D").split(",")
ITER = int(E("ITER", "1"))
BG_CHAOS = E("BG_CHAOS", "1") != "0"
ROUND_TIMEOUT = int(E("TIMEOUT_SEC", "420"))
A_DELTA, A_BIG, A_FINE = int(E("A_DELTA", "7")), int(E("A_BIG", "20")), int(E("A_FINE", "20"))
FAULT_MOD = int(E("FAULT_MOD", "7"))
FREEZE_WORKER = 2 * C.CLAIM_IDLE
FREEZE_LEADER = 2 * C.LEADER_TTL
DEBUG_LOGS = ["--logging.level.io.github.amwtke.pipeliner.runtime.master.JournalDrainer=DEBUG",
              "--logging.level.io.github.amwtke.pipeliner.runtime.worker.ClusterPipelineWorker=DEBUG"]
log = C.log
CTX = C.CTX
rng = C.rng

TABLE_RUNS = {"mysql:ex_nd1_big": "R2", "ch:ex_fine_gd": "R2", "ch:ex_grade_result": "R2", "ch:ex_refurb_plan": "R3", "ch:ex_scrap": "R3",
              "mysql:ex_publish_log": "R6"}
SCAN = [("fenced", "fenced"), ("steps down", "steps down"), ("CONFLICT", "commit conflict"), ("drop stale/old-attempt event", "dropping"),
        ("ignoring stale", "ignoring stale"), ("rerun", "rerun from"), ("admin cmd failed", "admin command"), ("claim-renew-failed", "claim renew failed"),
        ("dead-lettered", "dead-lettered"), ("injected fault", "injected fault"), ("ERROR", " ERROR "), ("worker-loop-error", "loop error")]


# ---------------------------------------------------------------- helpers
def post_json(port, path, body, timeout=40):
    req = urllib.request.Request("http://127.0.0.1:%d%s" % (port, path), method="POST", data=json.dumps(body).encode(),
                                 headers=dict(C.AUTH, **{"content-type": "application/json"}))
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.status, json.loads(r.read().decode())
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode()
    except Exception as e:  # noqa
        return 0, "%s: %s" % (type(e).__name__, e)


def healthy_port(exclude=()):
    h = [p for p in CTX.healthy() if p.name not in exclude]
    return rng.choice(h).port if h else None


def admin(outcomes, label, path, port=None, timeout=40, tries=1, pause=1.0):
    """POST an admin path; record every attempt in outcomes. Returns True once a 200 was seen."""
    for i in range(tries):
        pt = port if (port and i == 0) else healthy_port()
        t0 = time.time()
        code, body = C.http(pt, "POST", path, timeout) if pt else (0, "no healthy pod")
        outcomes.append({"call": label, "try": i + 1, "port": pt, "code": code, "body": str(body)[:200], "secs": round(time.time() - t0, 1)})
        log("ADMIN %s try %d port %s -> %s %s (%.1fs)" % (label, i + 1, pt, code, str(body)[:120], time.time() - t0))
        if code == 200:
            return True
        time.sleep(pause)
    return False


def submit_pg(pv, biz):
    for _ in range(30):
        port = healthy_port()
        if port:
            code, r = post_json(port, "/pg/rounds", {"graph": "net-demand", "bizKey": biz, "params": {"planVersion": pv}}, 30)
            if code == 200 and isinstance(r, dict) and r.get("roundId"):
                return r["roundId"]
        time.sleep(1)
    raise RuntimeError("submit failed for %s/%s" % (pv, biz))


def vw(rid, timeout=45):
    """Round view that survives transient unavailability (all reachable pods down/frozen by the background chaos)."""
    end = time.time() + timeout
    while True:
        v = C.view(rid)
        if v or time.time() > end:
            return v
        time.sleep(0.5)


def run_of(v, rid_run):
    return next((r for r in v["runs"] if r["runId"] == rid_run), None) if v else None


def strip(v):
    if v:
        v = json.loads(json.dumps(v))
        v["round"].pop("planJson", None)
    return v


def wait_for(cond, timeout, step=0.2):
    end = time.time() + timeout
    while time.time() < end:
        r = cond()
        if r:
            return r
        time.sleep(step)
    return None


def wait_run(rid, runid, statuses, timeout=180):
    def cond():
        v = vw(rid)
        r = run_of(v, runid)
        return v if r and r["status"] in statuses else None
    return wait_for(cond, timeout)


def holders(rid, pipes=("big-net", "gd-fix")):
    """Pods that currently hold live instances of the given pipelines of the round: [(pod, isLeader, {pipelines})]."""
    out = []
    for p in CTX.healthy():
        code, c = C.http(p.port, "GET", "/pg/cluster", 3)
        if code == 200 and isinstance(c, dict):
            pl = {i["pipelineId"] for i in c.get("liveInstances", []) if i.get("roundId") == rid and i.get("pipelineId") in pipes}
            if pl:
                out.append((p, bool(c.get("isLeader")), pl))
    return out


def wait_done(rid, timeout=ROUND_TIMEOUT, until=("SUCCEEDED", "FAILED", "CANCELLED")):
    """Poll a round until its status is in `until`, opening the approve gate whenever it is (re-)armed. Returns (status, secs, view)."""
    t0 = time.time()
    opened = set()
    while time.time() - t0 < timeout:
        v = vw(rid)
        if v:
            st = v["round"]["status"]
            g = [x for x in v.get("gates", []) if x["gateId"] == "approve"]
            if g and g[0]["status"] == "CLOSED" and g[0].get("armedAt") and (rid, g[0]["armedAt"]) not in opened:
                port = healthy_port()
                if port and C.http(port, "POST", "/pg/gates/%s/approve/open?operator=chaos" % rid, 40)[0] == 200:
                    opened.add((rid, g[0]["armedAt"]))
                    log("approve opened for %s" % rid)
            if st in until:
                return st, time.time() - t0, v
        time.sleep(0.7)
    return "TIMEOUT", time.time() - t0, vw(rid)


def freeze(pod, reserved):
    pod.signal(signal.SIGSTOP)
    pod.state = "stopped"
    reserved.add(pod.name)
    log("SIGSTOP %s" % pod.name)


def thaw(pod, reserved):
    pod.signal(signal.SIGCONT)
    pod.state = "up"
    reserved.discard(pod.name)
    log("SIGCONT %s" % pod.name)


def stop_pod(p):
    if p.pid and p.alive():
        p.signal(signal.SIGCONT)
        p.signal(signal.SIGTERM)
        for _ in range(40):
            if not p.alive():
                break
            time.sleep(0.5)
        p.signal(signal.SIGKILL)
    p.state = "down"


def redis_keys(rid):
    return sorted(k.split("}:", 1)[1] for k in C.Redis().cmd("KEYS", "pg:{%s}:*" % rid))


def jhash(s):
    h = 0
    for ch in s:
        h = (31 * h + ord(ch)) & 0xFFFFFFFF
    return h - (1 << 32) if h & 0x80000000 else h


# ---------------------------------------------------------------- background chaos
class Bg:
    KINDS = ["kill", "kill_leader", "stop_leader", "stop_worker"]

    def __init__(self):
        self.reserved, self.quiet, self.actions, self.timers = set(), False, [], []
        self.ev, self.th = threading.Event(), None

    def start(self):
        if BG_CHAOS:
            self.ev.clear()
            self.th = threading.Thread(target=self.loop, daemon=True)
            self.th.start()

    def stop(self):
        self.ev.set()
        if self.th:
            self.th.join(60)
            self.th = None
        for t in self.timers:
            t()
        self.timers.clear()

    def loop(self):
        nxt = time.time() + rng.uniform(4, 8)
        due = []
        while not self.ev.is_set():
            now = time.time()
            for d in [d for d in due if d[0] <= now]:
                due.remove(d)
                d[1]()
            for p in CTX.pods:
                p.poll_ready()
            if now >= nxt and not self.quiet:
                t = self.act()
                if t:
                    due.append(t)
                nxt = time.time() + rng.uniform(7, 14)
            self.timers = [d[1] for d in due]
            time.sleep(0.25)
        for d in due:
            d[1]()

    def act(self):
        if any(p.state != "up" or not p.alive() for p in CTX.pods):
            return None   # one disruption at a time
        kind = rng.choice(self.KINDS)
        ld = CTX.leader()
        cands = [p for p in CTX.pods if p.name not in self.reserved]
        if kind in ("kill_leader", "stop_leader"):
            if not ld or ld.name in self.reserved:
                return None
            p = ld
        else:
            cands = [p for p in cands if p is not ld] or cands
            if not cands:
                return None
            p = rng.choice(cands)
        now = time.time()
        if kind in ("kill", "kill_leader"):
            d = rng.uniform(2, 5)
            p.signal(signal.SIGKILL)
            p.state = "down"
            self.actions.append("%s %s" % (kind, p.name))
            log("BG %s %s restart_in=%.1fs" % (kind, p.name, d))
            return (now + d, lambda p=p: (p.start(), log("BG restarted %s" % p.name)))
        d = rng.uniform(1.5, 3) * (C.LEADER_TTL if kind == "stop_leader" else C.CLAIM_IDLE)
        p.signal(signal.SIGSTOP)
        p.state = "stopped"
        self.actions.append("%s %s" % (kind, p.name))
        log("BG %s %s for %.1fs" % (kind, p.name, d))
        return (now + d, lambda p=p: (p.signal(signal.SIGCONT), setattr(p, "state", "up"), log("BG SIGCONT %s" % p.name)))


BG = Bg()


def heal():
    for p in CTX.pods:
        if p.state == "down" or not p.alive():
            p.start()
        elif p.state == "stopped":
            p.signal(signal.SIGCONT)
            p.state = "up"
    if not C.wait_ready(CTX.pods, 120):
        log("WARN heal: not every pod is ready after 120s: %s" % [(p.name, p.state) for p in CTX.pods])
    BG.reserved.clear()


# ---------------------------------------------------------------- cluster modes
PODS = []
BASEPOD = None


def mk_pods():
    global PODS
    PODS = [C.Pod(i + 1, extra=DEBUG_LOGS) for i in range(C.PODS)]


def cluster_mode():
    if BASEPOD and BASEPOD.alive():
        stop_pod(BASEPOD)
    CTX.pods = PODS
    for p in PODS:
        if p.state == "down" or not p.alive():
            p.start()
    if not C.wait_ready(PODS, 120):
        raise RuntimeError("pods did not come up")


def base_mode():
    global BASEPOD
    for p in PODS:
        if p.alive():
            stop_pod(p)
    BASEPOD = C.Pod(0, extra=["--pipeliner.plan.force-mode=INLINE"] + DEBUG_LOGS, name="chaos-base")
    CTX.pods = [BASEPOD]
    BASEPOD.start()
    if not C.wait_ready([BASEPOD], 120):
        raise RuntimeError("base pod did not come up")
    return BASEPOD


def run_base(pv):
    """Single-pod INLINE reference round on the current data; returns the base dict used by C.verify_round."""
    rid = submit_pg(pv, pv)
    st, secs, v = wait_done(rid, 600)
    if st != "SUCCEEDED":
        raise RuntimeError("BASE %s did not succeed: %s" % (pv, json.dumps(strip(v))[:1500]))
    port = BASEPOD.port
    _, cs = C.http(port, "GET", "/demo/checksum?planVersion=" + pv, 60)
    _, vf = C.http(port, "GET", "/demo/verify?planVersion=" + pv, 300)
    log("BASE %s checksum %s (%.0fs)" % (pv, json.dumps(cs["kinds"]), secs))
    return {"pv": pv, "checksum": cs, "verify": vf, "rid": rid}


# ---------------------------------------------------------------- shared checks
def check_versions(rid, pv, v, viol, info):
    """Every result row of pv must carry pg_version == roundSeq*1000+attempt of the run that wrote it (MySQL and ClickHouse FINAL)."""
    code, vs = C.http(healthy_port() or CTX.pods[0].port, "GET", "/demo/versions?planVersion=" + pv, 120)
    if code != 200:
        viol.append("versions endpoint failed: %s" % str(vs)[:200])
        return
    seq = v["round"]["seq"]
    att = {r["runId"]: r["attempt"] for r in v["runs"]}
    ver = lambda run: seq * 1000 + att[run]  # noqa
    exp = dict(TABLE_RUNS)
    info["expectedVersions"] = {k: ver(r) for k, r in exp.items()}
    info["expectedVersions"].update({"mysql:ex_nd_final BIG": ver("R4"), "mysql:ex_nd_final FINE": ver("R5")})
    info["versions"] = vs
    for tbl, run in exp.items():
        for suffix in ("", ":final"):
            hist = vs.get(tbl + suffix)
            if hist is None:
                continue
            bad = {k: n for k, n in hist.items() if int(k) != ver(run)}
            if bad:
                viol.append("version: %s%s has rows with pg_version != %d (%s attempt %d): %s" % (tbl, suffix, ver(run), run, att[run], bad))
    for k, n in vs["mysql:ex_nd_final"].items():
        kind, version = k.split("/")
        if int(version) != ver("R4" if kind == "BIG" else "R5"):
            viol.append("version: ex_nd_final %s rows=%d pg_version %s != %d" % (kind, n, version, ver("R4" if kind == "BIG" else "R5")))
    old = {}
    for tbl, run in exp.items():
        raw = vs.get(tbl + ":raw")
        if raw:
            o = sum(n for k, n in raw.items() if int(k) != ver(run))
            if o:
                old[tbl] = o
    info["rawRowsOfOtherVersions"] = old   # ClickHouse raw rows of superseded attempts (merged away by FINAL) - informational
    info["attempts"] = att


def hygiene_extra(rid, viol, info, label=""):
    left, jinfo = C.redis_leftovers(rid)
    if left:
        viol.append("e%s: redis leftovers: %s" % (label, left[:10]))
    info["redisKeysAfter" + label] = redis_keys(rid)
    live = {}
    for p in CTX.pods:
        code, c = C.http(p.port, "GET", "/pg/cluster", 5)
        if code == 200 and rid in json.dumps(c.get("liveInstances")):
            live[p.name] = c["liveInstances"]
    if live:
        viol.append("f%s: liveInstances left on %s" % (label, json.dumps(live)[:300]))


def log_scan(offs, path):
    counts, per_pod = {n: 0 for n, _ in SCAN}, {}
    with open(path, "w") as ex:
        for p in list(PODS) + ([BASEPOD] if BASEPOD else []):
            if not os.path.exists(p.logpath()):
                continue
            with open(p.logpath(), "rb") as f:
                f.seek(offs.get(p.name, 0))
                text = f.read().decode(errors="replace")
            pc = {}
            for line in text.splitlines():
                for n, needle in SCAN:
                    if needle in line:
                        counts[n] += 1
                        pc[n] = pc.get(n, 0) + 1
                        if pc[n] <= 25:
                            ex.write("[%s] %s\n" % (p.name, line[:300]))
            per_pod[p.name] = pc
    return {k: v for k, v in counts.items() if v}, per_pod


def log_offsets():
    return {p.name: os.path.getsize(p.logpath()) if os.path.exists(p.logpath()) else 0 for p in list(PODS) + ([BASEPOD] if BASEPOD else [])}


def finish_verify(rid, pv, base, expect, viol, dets, label):
    v2, d2 = C.verify_round(rid, pv, base, None, expect)
    viol += ["%s: %s" % (label, x) for x in v2]
    dets["verify"] = {k: d2.get(k) for k in ("checksum", "splitChunks", "chunksWithAttemptGt1", "rawDup", "redisLeftovers", "liveInstances")}
    vf = d2.get("verify")
    dets["diffs"] = vf.get("diffsVsBase") if isinstance(vf, dict) else None
    return d2.get("view")


# ---------------------------------------------------------------- scenarios
BUMP_PATH = "/demo/bump?nBig=%d&nFine=%d&delta=%d"


def restore_bump(info, viol):
    """Undo scenario A's input change on every exit path (liveness early return, exception). The shared MySQL input must be back to the
    original before B/C/D, which are verified against base0."""
    BG.quiet = False
    BG.stop()
    heal()   # a frozen/killed pod must not swallow the restore
    for i in range(5):
        port = healthy_port()
        if port:
            code, body = C.http(port, "POST", BUMP_PATH % (A_BIG, A_FINE, -A_DELTA), 60)
            if code == 200:
                info["restore"] = body
                log("A: input restored")
                return True
            log("A: restore try %d -> %s %s" % (i + 1, code, str(body)[:100]))
        time.sleep(2)
    viol.append("A: HARNESS: could not restore the bumped input (+%d for %d BIG / %d FINE); B/C/D checksums will be wrong" % (A_DELTA, A_BIG, A_FINE))
    return False


def sc_A(ctx, n):
    base0, expect = ctx["base"], ctx["expect"]
    pv = "A%02d-%s" % (n, C.STAMP)
    viol, info, out = [], {"pv": pv}, []
    reserved = BG.reserved
    rid = submit_pg(pv, pv)
    info["rid"] = rid
    log("A: round %s" % rid)
    v = wait_run(rid, "R2", ("RUNNING",), 120)
    att0 = run_of(v, "R2")["attempt"] if v else None
    # let the pipelines spin up on some pod, pick a (non-leader) holder of big-net / gd-fix chunks
    hs = wait_for(lambda: holders(rid), 60, 0.2) or []
    cand = [h for h in hs if not h[1]] or hs
    zombie = max(cand, key=lambda h: len(h[2]))[0] if cand else rng.choice(CTX.healthy())
    info["zombie"] = zombie.name
    info["holders"] = [(h[0].name, h[1], sorted(h[2])) for h in hs]
    BG.quiet = True
    t_stop = time.time()
    freeze(zombie, reserved)
    code, bm = C.http(healthy_port(), "POST", BUMP_PATH % (A_BIG, A_FINE, A_DELTA), 60)
    info["bump"] = bm
    bumped = code == 200
    if not bumped:
        viol.append("A: bump did not return 200 (%s %s): input state unknown, not restoring" % (code, str(bm)[:100]))
    try:
        ok = admin(out, "rerun R2", "/pg/runs/%s/R2/rerun" % rid, tries=5)
        if not ok:
            viol.append("A: rerun command not accepted after 5 tries")
        time.sleep(max(0, FREEZE_WORKER - (time.time() - t_stop)))
        thaw(zombie, reserved)
        info["frozenSecs"] = round(time.time() - t_stop, 1)
        BG.quiet = False
        st, secs, v = wait_done(rid)
        info["status"], info["secs"] = st, round(secs)
        if st != "SUCCEEDED":
            viol.append("LIVENESS: A round %s after %.0fs" % (st, secs))
            return viol, info, out
        r2 = run_of(v, "R2")
        info["R2attempt"] = {"before": att0, "after": r2["attempt"]}
        if att0 is None or r2["attempt"] <= att0:
            viol.append("A: R2 attempt did not advance (%s -> %s)" % (att0, r2["attempt"]))
        time.sleep(2)
        check_versions(rid, pv, v, viol, info)
        BG.stop()
        heal()
        hygiene_extra(rid, viol, info, "(4 pods)")
        # BASE on the CHANGED data: single pod INLINE
        base_mode()
        basec = run_base("BASEC%02d-%s" % (n, C.STAMP))
        if basec["checksum"]["kinds"] == base0["checksum"]["kinds"]:
            viol.append("A: INVALID TEST: changed input did not change the BASE checksum")
        info["baseChanged"] = basec["checksum"]["kinds"]
        info["baseOrig"] = base0["checksum"]["kinds"]
        finish_verify(rid, pv, basec, expect, viol, info, "A")
        return viol, info, out
    finally:
        if bumped:
            restore_bump(info, viol)


def fault(on):
    mod = FAULT_MOD if on else 0
    for _ in range(10):
        port = healthy_port()
        if port and C.http(port, "POST", "/demo/fault?pipeline=big-net&mod=%d" % mod, 20)[0] == 200:
            log("fault big-net mod=%d" % mod)
            return True
        time.sleep(1)
    return False


def sc_B_one(ctx, n, variant):
    base0, expect = ctx["base"], ctx["expect"]
    pv = "B%02d%s-%s" % (n, variant, C.STAMP)
    viol, info, out = [], {"pv": pv, "variant": variant}, []
    reserved = BG.reserved
    if not fault(True):
        return ["B: could not enable fault"], info, out
    try:
        rid = submit_pg(pv, pv)
        info["rid"] = rid
        st, secs, v = wait_done(rid, 240, until=("FAILED", "SUCCEEDED", "CANCELLED"))
        info["afterFault"] = {"round": st, "secs": round(secs), "error": ((v or {}).get("round") or {}).get("error") if isinstance(v, dict) else None,
                              "runs": {r["runId"]: (r["status"], r["attempt"]) for r in v.get("runs", [])} if isinstance(v, dict) else {},
                              "chunkCounts": v.get("chunkCounts") if isinstance(v, dict) else None}
        if st != "FAILED":
            viol.append("B: expected FAILED with fault on, got %s (fault mod %d may miss every chunk)" % (st, FAULT_MOD))
        else:
            r2 = run_of(v, "R2")
            if r2["status"] != "FAILED":
                viol.append("B: R2 status %s expected FAILED" % r2["status"])
            chunks, cerr = C.get_chunks(rid, "big-net")
            if cerr:
                info["deadChunkCheckSkipped"] = cerr   # could not read the chunk records: skip the DEAD-chunk checks instead of reporting "no dead chunks"
                C.log("NOTE B: dead chunk checks skipped: %s" % cerr)
            dead = [c["chunkId"] for c in (chunks or []) if c["status"] == "DEAD"]
            info["deadChunks"] = dead
            info["deadChunksAllHashMatch"] = all(jhash(c) % FAULT_MOD == 0 or (jhash(c) % FAULT_MOD) == 0 for c in dead)
            bad = [c for c in dead if jhash(c) % FAULT_MOD != 0]
            if bad:
                viol.append("B: DEAD chunks that the fault rule does not select: %s" % bad[:5])
            if not dead and not cerr:
                viol.append("B: run FAILED but no DEAD chunks")
            info["dlqKeysWhileFailed"] = [k for k in redis_keys(rid) if k.startswith("dlq:")]
    finally:
        if not fault(False):
            viol.append("B: could not disable fault")
    if st != "FAILED":
        return viol, info, out
    time.sleep(1)
    att_before = run_of(vw(rid), "R2")["attempt"]
    if variant == "2":
        # retry via a follower, freeze the leader right after the call was sent
        ld = CTX.leader()
        fol = [p for p in CTX.healthy() if p is not ld]
        port = rng.choice(fol).port
        BG.quiet = True
        res = {}

        def call():
            t0 = time.time()
            res["r"] = C.http(port, "POST", "/pg/runs/%s/R2/retry" % rid, 45)
            res["secs"] = round(time.time() - t0, 1)
        th = threading.Thread(target=call)
        th.start()
        time.sleep(0.15)
        t_stop = time.time()
        if ld:
            freeze(ld, reserved)
        time.sleep(FREEZE_LEADER)
        if ld:
            thaw(ld, reserved)
        th.join(60)
        BG.quiet = False
        code, body = res.get("r", (0, "no result"))
        out.append({"call": "retry R2 (leader frozen right after)", "port": port, "code": code, "body": str(body)[:200], "secs": res.get("secs"),
                    "leader": ld.name if ld else None})
        log("ADMIN retry R2 under leader freeze -> %s %s" % (code, str(body)[:100]))
        if code != 200:
            if not admin(out, "retry R2 (second)", "/pg/runs/%s/R2/retry" % rid, tries=3):
                viol.append("B: retry never accepted")
    else:
        if not admin(out, "retry R2", "/pg/runs/%s/R2/retry" % rid, tries=4):
            viol.append("B: retry not accepted")
    st, secs, v = wait_done(rid)
    info["status"], info["secs"] = st, round(secs)
    if st != "SUCCEEDED":
        viol.append("LIVENESS: B after retry round %s after %.0fs" % (st, secs))
        return viol, info, out
    info["R2attemptBeforeAfterRetry"] = (att_before, run_of(v, "R2")["attempt"])
    time.sleep(2)
    check_versions(rid, pv, v, viol, info)
    finish_verify(rid, pv, base0, expect, viol, info, "B")
    left_dlq = [k for k in redis_keys(rid) if k.startswith("dlq:")]
    info["dlqKeysAfterSuccess"] = left_dlq
    if left_dlq:
        viol.append("B: dlq keys left after success: %s" % left_dlq)
    return viol, info, out


def sc_B(ctx, n):
    allv, allinfo, allout = [], {}, []
    for variant in ("1", "2"):
        v, i, o = sc_B_one(ctx, n, variant)
        allv += ["[v%s] %s" % (variant, x) for x in v]
        allinfo["variant" + variant] = i
        allout += o
        if variant == "1":
            heal()
    return allv, allinfo, allout


def sc_C(ctx, n):
    base0, expect = ctx["base"], ctx["expect"]
    pv = "C%02d-%s" % (n, C.STAMP)
    viol, info, out = [], {"pv": pv}, []
    reserved = BG.reserved
    old = submit_pg(pv, pv + "-a")
    info["oldRid"] = old
    log("C: old round %s" % old)
    wait_run(old, "R2", ("RUNNING",), 120)
    hs = wait_for(lambda: holders(old), 60, 0.2) or []
    cand = [h for h in hs if not h[1]] or hs
    zombie = max(cand, key=lambda h: len(h[2]))[0] if cand else rng.choice(CTX.healthy())
    info["straggler"] = zombie.name
    BG.quiet = True
    t_stop = time.time()
    freeze(zombie, reserved)
    if not admin(out, "cancel old", "/pg/rounds/%s/cancel" % old, tries=5):
        viol.append("C: cancel not accepted")
    new = submit_pg(pv, pv + "-b")
    info["newRid"] = new
    log("C: new round %s" % new)
    time.sleep(max(0, FREEZE_WORKER - (time.time() - t_stop)))
    thaw(zombie, reserved)
    BG.quiet = False
    st, secs, v = wait_done(new)
    info["status"], info["secs"] = st, round(secs)
    vo = vw(old)
    info["oldStatus"] = vo["round"]["status"] if vo else None
    if info["oldStatus"] != "CANCELLED":
        viol.append("C: old round status %s expected CANCELLED" % info["oldStatus"])
    if st != "SUCCEEDED":
        viol.append("LIVENESS: C new round %s after %.0fs" % (st, secs))
        return viol, info, out
    time.sleep(3)
    info["oldSeq"], info["newSeq"] = vo["round"]["seq"], v["round"]["seq"]
    check_versions(new, pv, v, viol, info)
    low = {t: h for t, h in (info.get("versions") or {}).items() if not t.endswith(":raw") and "ex_algo_gd" not in t and any(int(k.split("/")[-1]) < info["newSeq"] * 1000 for k in h)}
    if low:
        viol.append("C: result rows below newSeq*1000: %s" % low)
    BG.stop()
    heal()
    finish_verify(new, pv, base0, expect, viol, info, "C")
    # cancelled round: hygiene + stray keys
    left, _ = C.redis_leftovers(old)
    live, unreachable = {}, []
    for p in CTX.pods:
        code, cl = C.http(p.port, "GET", "/pg/cluster", 5)
        if code != 200 or not isinstance(cl, dict):   # error string / non-JSON: say so, keep the violations collected so far
            unreachable.append("%s(%s)" % (p.name, code))
            continue
        mine = [i for i in (cl.get("liveInstances") or []) if i.get("roundId") == old]
        if mine:
            live[p.name] = mine
    if unreachable:
        info["oldLiveInstancesUnknown"] = unreachable
        log("NOTE C: liveInstances of the cancelled round not checked on %s" % unreachable)
    info["oldKeysAfter"] = redis_keys(old)
    info["oldLeftovers"] = left
    if left:
        viol.append("C: cancelled round %s hygiene leaks: %s" % (old, left[:10]))
    if live:
        viol.append("C: cancelled round has liveInstances: %s" % json.dumps(live)[:300])
    return viol, info, out


def sc_D_one(ctx, n, variant):
    base0, expect = ctx["base"], ctx["expect"]
    run, to_leader = ("R2", False) if variant == "1" else ("R3", True)
    pv = "D%02d%s-%s" % (n, variant, C.STAMP)
    viol, info, out = [], {"pv": pv, "variant": variant, "rerunFrom": run, "viaLeader": to_leader}, []
    reserved = BG.reserved
    rid = submit_pg(pv, pv)
    info["rid"] = rid
    v = wait_run(rid, run, ("RUNNING",) if run == "R2" else ("RUNNING", "SUCCEEDED"), 180)
    if run == "R2":
        wait_for(lambda: holders(rid), 30, 0.2)
    att0 = run_of(v, run)["attempt"] if v else None
    BG.quiet = True
    ld = None
    for _ in range(40):
        ld = CTX.leader()
        if ld:
            break
        time.sleep(0.25)
    if not ld:
        BG.quiet = False
        return ["D: no leader found"], info, out
    fol = [p for p in CTX.healthy() if p is not ld]
    port = ld.port if to_leader else rng.choice(fol).port
    res = {}

    def call():
        t0 = time.time()
        res["r"] = C.http(port, "POST", "/pg/runs/%s/%s/rerun" % (rid, run), 45)
        res["secs"] = round(time.time() - t0, 1)
    th = threading.Thread(target=call)
    th.start()
    time.sleep(0.15)
    freeze(ld, reserved)
    time.sleep(FREEZE_LEADER)
    thaw(ld, reserved)
    th.join(60)
    code, body = res.get("r", (0, "no result"))
    out.append({"call": "rerun %s (leader %s frozen right after)" % (run, ld.name), "port": port, "code": code, "body": str(body)[:200], "secs": res.get("secs")})
    log("ADMIN rerun %s under leader freeze -> %s %s (%ss)" % (run, code, str(body)[:100], res.get("secs")))
    info["firstCall"] = {"code": code, "body": str(body)[:200], "secs": res.get("secs")}
    if code != 200:
        info["firstCallErrored"] = True
        if not admin(out, "rerun %s retry-once" % run, "/pg/runs/%s/%s/rerun" % (rid, run), tries=1):
            admin(out, "rerun %s retry-extra" % run, "/pg/runs/%s/%s/rerun" % (rid, run), tries=3)
            viol.append("D: first call errored and the one allowed retry failed too (extra tries were needed)")
    BG.quiet = False
    st, secs, v = wait_done(rid)
    info["status"], info["secs"] = st, round(secs)
    if st != "SUCCEEDED":
        viol.append("LIVENESS: D round %s after %.0fs" % (st, secs))
        return viol, info, out
    att1 = run_of(v, run)["attempt"]
    info["attempt"] = {"before": att0, "after": att1}
    if att0 is not None and att1 <= att0:
        viol.append("D: %s attempt did not advance (%s -> %s): rerun reported %s but was not applied" % (run, att0, att1, code))
    time.sleep(2)
    check_versions(rid, pv, v, viol, info)
    finish_verify(rid, pv, base0, expect, viol, info, "D")
    return viol, info, out


def sc_D(ctx, n):
    allv, allinfo, allout = [], {}, []
    for variant in ("1", "2"):
        v, i, o = sc_D_one(ctx, n, variant)
        allv += ["[v%s] %s" % (variant, x) for x in v]
        allinfo["variant" + variant] = i
        allout += o
        if variant == "1":
            heal()
    return allv, allinfo, allout


# ---------------------------------------------------------------- main
def main():
    os.makedirs(C.LOGS, exist_ok=True)
    open(os.path.join(C.OUT, "chaos.log"), "a").close()
    log("chaos-admin start store=%s PODS=%d MATERIALS=%d SCENARIOS=%s ITER=%d BG=%s seed=%d vmload=%s out=%s" % (
        C.STORE_PROFILE or "redis", C.PODS, C.MATERIALS, SCENARIOS, ITER, BG_CHAOS, C.SEED, C.vm_load(), C.OUT))
    subprocess.run([os.path.join(C.SCRIPTS, "stop-pods.sh")], env=dict(os.environ, PIPELINER_LOGS=C.LOGS), stdout=subprocess.DEVNULL)
    nbig = int(C.MATERIALS * 0.4)
    expect = {"nd_final": C.MATERIALS * 5 * 16, "nd1_big": nbig * 5 * 16}
    summary = {"store": C.STORE_PROFILE or "redis", "seed": C.SEED, "bg": BG_CHAOS, "scenarios": [], "violations": 0}
    mk_pods()
    try:
        base_mode()
        c, r = C.http(BASEPOD.port, "POST", "/demo/seed?materials=%d&seed=42" % C.MATERIALS, 300)
        log("seed: %s %s" % (c, json.dumps(r)[:200]))
        base0 = run_base("BASE-" + C.STAMP)
        bvf = base0["verify"]["tables"]
        if bvf["mysql:ex_nd_final"]["rows"] != expect["nd_final"] or bvf["mysql:ex_nd1_big"]["rows"] != expect["nd1_big"]:
            raise RuntimeError("BASE self-check failed")
        json.dump(base0, open(os.path.join(C.OUT, "base.json"), "w"), indent=1)
        ctx = {"base": base0, "expect": expect}
        fns = {"A": sc_A, "B": sc_B, "C": sc_C, "D": sc_D}
        for it in range(1, ITER + 1):
            for sc in SCENARIOS:
                sc = sc.strip()
                log("=== scenario %s iteration %d (vmload %s)" % (sc, it, C.vm_load()))
                cluster_mode()
                offs = log_offsets()
                load0 = C.vm_load()
                t0 = time.time()
                BG.actions = []
                BG.start()
                try:
                    viol, info, out = fns[sc](ctx, it)
                except Exception as e:  # noqa
                    import traceback
                    traceback.print_exc()
                    viol, info, out = ["EXC: %r" % e], {}, []   # scenarios guard their own http results, so collected violations are not lost here
                finally:
                    BG.quiet = False
                    BG.stop()
                    heal()
                    for p in PODS:   # never leave a pod frozen
                        if p.alive():
                            p.signal(signal.SIGCONT)
                    fault(False)
                dirp = os.path.join(C.OUT, "%s%02d" % (sc, it))
                os.makedirs(dirp, exist_ok=True)
                counts, per_pod = log_scan(offs, os.path.join(dirp, "log-excerpt.txt"))
                live = [x for x in viol if x.split("] ")[-1].startswith("LIVENESS")]
                rec = {"scenario": sc, "iteration": it, "store": summary["store"], "durationSec": round(time.time() - t0), "vmLoadStart": load0,
                       "vmLoadEnd": C.vm_load(), "bgActions": list(BG.actions), "adminCalls": out, "logCounts": counts, "logCountsPerPod": per_pod,
                       "correctnessViolations": [x for x in viol if x not in live], "livenessViolations": live, "info": info}
                rec["verdict"] = "PASS" if not viol else ("FAIL(liveness)" if len(live) == len(viol) else "FAIL")
                json.dump(rec, open(os.path.join(dirp, "result.json"), "w"), indent=1, default=str)
                summary["scenarios"].append({k: rec[k] for k in ("scenario", "iteration", "verdict", "durationSec", "vmLoadStart", "vmLoadEnd", "bgActions",
                                                                 "logCounts", "correctnessViolations", "livenessViolations")})
                summary["violations"] += len(viol)
                log("=== %s%d verdict %s bg=%s logs=%s admin=%s" % (sc, it, rec["verdict"], rec["bgActions"], counts,
                                                                   [(o.get("call"), o.get("code")) for o in out]))
                for x in viol:
                    log("    VIOLATION: %s" % x[:700])
    except Exception as e:  # noqa
        import traceback
        traceback.print_exc()
        summary["fatal"] = repr(e)
        summary["violations"] += 1
        log("FATAL: %r" % e)
    finally:
        BG.stop()
        for p in PODS + ([BASEPOD] if BASEPOD else []):
            if p.pid and p.alive():
                p.signal(signal.SIGCONT)
        subprocess.run([os.path.join(C.SCRIPTS, "stop-pods.sh")], env=dict(os.environ, PIPELINER_LOGS=C.LOGS), stdout=subprocess.DEVNULL)
        json.dump(summary, open(os.path.join(C.OUT, "summary.json"), "w"), indent=1)
    log("DONE violations=%d summary=%s" % (summary["violations"], os.path.join(C.OUT, "summary.json")))
    sys.exit(1 if summary["violations"] else 0)


if __name__ == "__main__":
    main()
