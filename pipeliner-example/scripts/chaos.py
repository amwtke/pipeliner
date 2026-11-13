#!/usr/bin/env python3
"""Chaos driver for pipeliner-example (invoked by chaos.sh; see that file for the env parameters).

Flow: seed -> BASE (single pod, force-mode INLINE) -> CL1 (single pod, normal CLUSTER) -> N iterations; each iteration submits
2 rounds concurrently on PODS pods while a chaos loop kills / freezes / pauses pods; then 6 verification groups (a..f) per round.
"""
import concurrent.futures as cf
import datetime
import json
import os
import random
import re
import signal
import socket
import subprocess
import sys
import threading
import time
import urllib.request
import urllib.error

E = os.environ.get
PODS = int(E("PODS", "4"))
ROUNDS = int(E("ROUNDS", "6"))
MATERIALS = int(E("MATERIALS", "600"))
STORE_PROFILE = E("STORE_PROFILE", "")
SEED = int(E("SEED", str(int(time.time()) % 100000)))
TIMEOUT_SEC = int(E("TIMEOUT_SEC", "900"))          # per iteration
CHAOS_MAX_SEC = int(E("CHAOS_MAX_SEC", "150"))      # chaos actions stop after this, then the rounds converge
BASE_PORT = int(E("BASE_PORT", "9101"))
REDIS_PORT = int(E("REDIS_PORT", "6380"))
REDIS_DB = int(E("REDIS_DB", "5"))
MYSQL_DB = E("MYSQL_DB", "pipeliner_chaos")
CP_DB = E("CP_DB", "pipeliner_chaos_cp")
CH_DB = E("CH_DB", "pipeliner_chaos")
OUT = E("OUT")
SCRIPTS = os.path.dirname(os.path.abspath(__file__))
LOGS = os.path.join(OUT, "pods")
# chaos-profile timings (must mirror application-chaos.yml)
LEADER_TTL = 3.0
CLAIM_IDLE = 3.0
ACTIONS = ["kill", "kill_leader", "stop_leader", "stop_worker", "pause"]
rng = random.Random(SEED)
STAMP = str(int(time.time()))
t_start = time.time()
_loglock = threading.Lock()


def log(msg):
    line = "%s %s" % (datetime.datetime.now().strftime("%H:%M:%S.%f")[:-3], msg)
    with _loglock:
        print(line, flush=True)
        with open(os.path.join(OUT, "chaos.log"), "a") as f:
            f.write(line + "\n")


def vm_load():
    """Load average of the shared Lima VM (contention from other sessions can explain slowness, not correctness violations)."""
    try:
        r = subprocess.run(["limactl", "shell", "cg", "--", "uptime"], capture_output=True, text=True, timeout=25)
        return r.stdout.strip().split("load average:")[-1].strip()
    except Exception as e:  # noqa
        return "n/a (%s)" % type(e).__name__


LIVENESS_PREFIXES = ("rounds did not all reach SUCCEEDED", "retry was needed")


# ---------------------------------------------------------------- http / redis
def http(port, method, path, timeout=10):
    req = urllib.request.Request("http://127.0.0.1:%d%s" % (port, path), method=method, data=b"" if method == "POST" else None)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            body = r.read().decode()
            code = r.status
    except urllib.error.HTTPError as e:
        body, code = e.read().decode(), e.code
    except Exception as e:  # noqa
        return 0, str(e)
    try:
        return code, json.loads(body)
    except ValueError:
        return code, body


class Redis:
    def __init__(self):
        self.s = socket.create_connection(("127.0.0.1", REDIS_PORT), timeout=10)
        self.f = self.s.makefile("rb")
        self.cmd("SELECT", REDIS_DB)

    def cmd(self, *args):
        out = b"*%d\r\n" % len(args)
        for a in args:
            a = str(a).encode()
            out += b"$%d\r\n%s\r\n" % (len(a), a)
        self.s.sendall(out)
        return self._read()

    def _read(self):
        line = self.f.readline().rstrip(b"\r\n")
        t, rest = line[:1], line[1:]
        if t in (b"+", b":"):
            return rest.decode() if t == b"+" else int(rest)
        if t == b"-":
            raise RuntimeError(rest.decode())
        if t == b"$":
            n = int(rest)
            if n < 0:
                return None
            d = self.f.read(n + 2)[:-2]
            return d.decode()
        if t == b"*":
            n = int(rest)
            return None if n < 0 else [self._read() for _ in range(n)]
        raise RuntimeError("bad resp %r" % line)


def redis_leftovers(rid):
    """Hygiene keys of a round (e): ch:*, split:*, splitidx:*, dlq:*, desired, assign, paused + membership in pg:rounds:active."""
    r = Redis()
    bad = []
    for k in r.cmd("KEYS", "pg:{%s}:*" % rid):
        suffix = k.split("}:", 1)[1]
        if re.match(r"(ch:|split:|splitidx:|dlq:)", suffix) or suffix in ("desired", "assign", "paused"):
            bad.append(k)
    typ = r.cmd("TYPE", "pg:rounds:active")
    members = {"set": lambda: r.cmd("SMEMBERS", "pg:rounds:active"), "zset": lambda: r.cmd("ZRANGE", "pg:rounds:active", 0, -1),
               "hash": lambda: r.cmd("HKEYS", "pg:rounds:active"), "list": lambda: r.cmd("LRANGE", "pg:rounds:active", 0, -1)}.get(typ, lambda: [])()
    if rid in (members or []):
        bad.append("pg:rounds:active contains %s" % rid)
    info = {"journalKeyLeft": bool(r.cmd("EXISTS", "pg:{%s}:journal" % rid))}
    return bad, info


# ---------------------------------------------------------------- pods
class Pod:
    def __init__(self, idx, extra=None, name=None):
        self.name = name or "chaos-p%d" % idx
        self.port = BASE_PORT + idx
        self.extra = extra or []
        self.state = "down"          # down | starting | up | stopped
        self.pid = None

    def args(self):
        prof = "chaos" + ("," + STORE_PROFILE if STORE_PROFILE else "")
        mysql = "jdbc:mysql://127.0.0.1:3306/%s?createDatabaseIfNotExist=true&useSSL=false&allowPublicKeyRetrieval=true&rewriteBatchedStatements=true&serverTimezone=UTC" % MYSQL_DB
        cp = "jdbc:mysql://127.0.0.1:3306/%s?createDatabaseIfNotExist=true&useSSL=false&allowPublicKeyRetrieval=true&rewriteBatchedStatements=true&serverTimezone=UTC" % CP_DB
        a = ["--spring.profiles.active=" + prof, "--pipeliner.redis.uri=redis://127.0.0.1:%d/%d" % (REDIS_PORT, REDIS_DB),
             "--pipeliner.datasources.mysql.url=" + mysql]
        if STORE_PROFILE:
            a.append("--pipeliner.datasources.pgstore.url=" + cp)
        return a + self.extra

    def start(self):
        env = dict(os.environ, PIPELINER_JAR=os.path.join(OUT, "pipeliner-example.jar"), PIPELINER_LOGS=LOGS, JAVA_OPTS="-Ddemo.ch-db=" + CH_DB)
        subprocess.run([os.path.join(SCRIPTS, "run-pod.sh"), self.name, str(self.port)] + self.args(), env=env, check=True, stdout=subprocess.DEVNULL)
        self.pid = int(open(os.path.join(LOGS, self.name + ".pid")).read())
        self.state = "starting"
        self.started = time.time()

    def alive(self):
        if self.pid is None:
            return False
        try:
            os.kill(self.pid, 0)
            return True
        except OSError:
            return False

    def poll_ready(self):
        if self.state == "starting":
            if not self.alive():
                self.state = "down"
            elif http(self.port, "GET", "/pg/cluster", 2)[0] == 200:
                self.state = "up"
        return self.state == "up"

    def signal(self, sig):
        try:
            os.kill(self.pid, sig)
        except OSError:
            pass

    def logpath(self):
        return os.path.join(LOGS, self.name + ".log")


def wait_ready(pods, timeout=120):
    end = time.time() + timeout
    while time.time() < end:
        if all(p.poll_ready() for p in pods):
            return True
        time.sleep(0.5)
    return False


# ---------------------------------------------------------------- round helpers
class Ctx:
    def __init__(self):
        self.pods = []

    def healthy(self):
        return [p for p in self.pods if p.state == "up" and p.alive()]

    def any_port(self):
        h = self.healthy()
        return rng.choice(h).port if h else None

    def leader(self):
        for p in self.healthy():
            code, c = http(p.port, "GET", "/pg/cluster", 2)
            if code == 200 and isinstance(c, dict) and c.get("isLeader"):
                return p
        return None


CTX = Ctx()


def submit(pv):
    for _ in range(30):
        port = CTX.any_port()
        if port:
            code, r = http(port, "POST", "/demo/rounds?planVersion=" + pv, 30)
            if code == 200 and isinstance(r, dict) and r.get("roundId"):
                return r["roundId"]
        time.sleep(1)
    raise RuntimeError("submit failed for " + pv)


def view(rid):
    port = CTX.any_port()
    if not port:
        return None
    code, v = http(port, "GET", "/pg/rounds/" + rid, 8)
    return v if code == 200 and isinstance(v, dict) else None


def approve_armed(v):
    g = [x for x in v.get("gates", []) if x["gateId"] == "approve"]
    return bool(g) and g[0]["status"] == "CLOSED" and g[0].get("armedAt")


def drive(label, rounds, chaos, timeout):
    """Wait for all rounds {name: {rid,pv}} to reach SUCCEEDED while optionally applying chaos. Returns (ok, actions, retries, notes)."""
    actions, retries, notes = [], [], []
    timers = []       # (due, fn, desc)
    pool = cf.ThreadPoolExecutor(8)
    t0 = time.time()
    next_chaos = t0 + rng.uniform(3, 6)
    next_poll = 0
    final = {}
    opened = set()

    def act(kind, detail):
        actions.append({"t": round(time.time() - t0, 1), "kind": kind, "detail": detail})
        log("[%s] CHAOS %s %s" % (label, kind, detail))

    def restart(p):
        def f():
            p.start()
            log("[%s] restarted %s pid=%s" % (label, p.name, p.pid))
        return f

    def cont(p):
        def f():
            p.signal(signal.SIGCONT)
            p.state = "up"
            log("[%s] SIGCONT %s" % (label, p.name))
        return f

    def do_chaos():
        healthy = CTX.healthy()
        if len(healthy) < 2:
            log("[%s] chaos skipped (only %d healthy pods)" % (label, len(healthy)))
            return
        kind = rng.choice(ACTIONS)
        now = time.time()
        if kind == "kill":
            p = rng.choice(healthy)
            p.signal(signal.SIGKILL)
            p.state = "down"
            d = rng.uniform(2, 5)
            act("kill", "%s restart_in=%.1fs" % (p.name, d))
            timers.append((now + d, restart(p), "restart " + p.name))
        elif kind == "kill_leader":
            p = CTX.leader()
            if not p or len(healthy) < 2:
                log("[%s] chaos kill_leader skipped (no leader found)" % label)
                return
            p.signal(signal.SIGKILL)
            p.state = "down"
            d = rng.uniform(2, 5)
            act("kill_leader", "%s restart_in=%.1fs" % (p.name, d))
            timers.append((now + d, restart(p), "restart " + p.name))
        elif kind == "stop_leader":
            p = CTX.leader()
            if not p:
                log("[%s] chaos stop_leader skipped (no leader found)" % label)
                return
            d = rng.uniform(1.5, 3) * LEADER_TTL
            p.signal(signal.SIGSTOP)
            p.state = "stopped"
            act("stop_leader", "%s for %.1fs" % (p.name, d))
            timers.append((now + d, cont(p), "cont " + p.name))
        elif kind == "stop_worker":
            ld = CTX.leader()
            cands = [p for p in healthy if p is not ld] or healthy
            p = rng.choice(cands)
            d = rng.uniform(1.5, 3) * CLAIM_IDLE
            p.signal(signal.SIGSTOP)
            p.state = "stopped"
            act("stop_worker", "%s for %.1fs" % (p.name, d))
            timers.append((now + d, cont(p), "cont " + p.name))
        elif kind == "pause":
            live = [r for r in rounds.values() if r["rid"] not in final]
            if not live:
                return
            r = rng.choice(live)
            port = rng.choice(healthy).port
            act("pause", "%s via port %d, resume in 2s" % (r["pv"], port))
            pool.submit(lambda: http(port, "POST", "/pg/rounds/%s/pause" % r["rid"], 40))

            def resume(r=r):
                p2 = CTX.any_port()
                pool.submit(lambda: http(p2, "POST", "/pg/rounds/%s/resume" % r["rid"], 40))
                log("[%s] resume %s" % (label, r["pv"]))
            timers.append((now + 2, resume, "resume"))

    ok = True
    while True:
        now = time.time()
        for t in [t for t in timers if t[0] <= now]:
            timers.remove(t)
            t[1]()
        for p in CTX.pods:
            p.poll_ready()
        if chaos and now >= next_chaos and now - t0 < CHAOS_MAX_SEC:
            do_chaos()
            next_chaos = time.time() + rng.uniform(2, 6)
        if now >= next_poll:
            next_poll = now + 1.0
            for name, r in rounds.items():
                if r["rid"] in final:
                    continue
                v = view(r["rid"])
                if not v:
                    continue
                st = v["round"]["status"]
                if approve_armed(v) and r["rid"] not in opened:
                    port = CTX.any_port()
                    if port:
                        c, _ = http(port, "POST", "/pg/gates/%s/approve/open?operator=chaos" % r["rid"], 40)
                        if c == 200:
                            opened.add(r["rid"])
                            log("[%s] approve opened for %s" % (label, r["pv"]))
                if st == "SUCCEEDED":
                    final[r["rid"]] = st
                    log("[%s] %s SUCCEEDED after %.0fs" % (label, r["pv"], time.time() - t0))
                elif st in ("FAILED", "CANCELLED"):
                    failed_runs = [x["runId"] for x in v["runs"] if x["status"] == "FAILED"]
                    if st == "FAILED" and r["rid"] not in [x["rid"] for x in retries] and failed_runs:
                        retries.append({"rid": r["rid"], "pv": r["pv"], "runs": failed_runs, "error": v["round"].get("error"),
                                        "view": v})
                        log("[%s] %s FAILED (%s) runs=%s -> retry once" % (label, r["pv"], v["round"].get("error"), failed_runs))
                        for rn in failed_runs:
                            http(CTX.any_port(), "POST", "/pg/runs/%s/%s/retry" % (r["rid"], rn), 40)
                    else:
                        final[r["rid"]] = st
                        log("[%s] %s ended %s" % (label, r["pv"], st))
        if len(final) == len(rounds):
            ok = all(s == "SUCCEEDED" for s in final.values())
            break
        if time.time() - t0 > timeout:
            ok = False
            notes.append("timeout after %ds" % timeout)
            log("[%s] TIMEOUT (vm load: %s)" % (label, vm_load()))
            break
        time.sleep(0.25)
    # heal: fire every pending timer now, make sure all pods are up and unfrozen
    for t in timers:
        t[1]()
    timers.clear()
    pool.shutdown(wait=True)
    for p in CTX.pods:
        if p.state in ("down",) or not p.alive():
            p.start()
        elif p.state == "stopped":
            p.signal(signal.SIGCONT)
            p.state = "up"
    wait_ready(CTX.pods, 120)
    return ok, actions, retries, notes, final


# ---------------------------------------------------------------- verification
SOURCE_PIPELINES = ["algo-submit", "gd-fix", "cap-alloc", "big-final", "fine-net", "publish"]


def get_chunks(rid, pipeline):
    port = CTX.any_port()
    code, c = http(port, "GET", "/pg/rounds/%s/chunks?pipeline=%s&limit=100000" % (rid, pipeline), 30)
    return c if code == 200 and isinstance(c, list) else []


def verify_round(rid, pv, base, ref_splits, expect, check_chunks=True):
    """Returns (violations[], details{}). base = {"checksum":..., "verify":...}."""
    viol, det = [], {}
    port = CTX.any_port()
    # a. checksum vs BASE
    code, cs = http(port, "GET", "/demo/checksum?planVersion=" + pv, 60)
    det["checksum"] = cs
    if code != 200 or cs.get("kinds") != base["checksum"]["kinds"]:
        viol.append("a: checksum differs from BASE: %s vs %s" % (json.dumps(cs.get("kinds") if isinstance(cs, dict) else cs), json.dumps(base["checksum"]["kinds"])))
    # b/c. MySQL + ClickHouse results
    code, vf = http(port, "GET", "/demo/verify?planVersion=%s&base=%s" % (pv, base["pv"]), 300)
    det["verify"] = vf
    if code != 200:
        viol.append("b/c: verify endpoint failed: %s" % vf)
    else:
        T = vf["tables"]
        for tbl, want in (("mysql:ex_nd_final", expect["nd_final"]), ("mysql:ex_nd1_big", expect["nd1_big"]), ("mysql:ex_publish_log", 2)):
            if T[tbl]["rows"] != want:
                viol.append("b: %s rows=%s expected %s" % (tbl, T[tbl]["rows"], want))
        for tbl in ("mysql:ex_nd_final", "mysql:ex_nd1_big"):
            if T[tbl]["duplicateKeys"]:
                viol.append("b: %s has %s duplicate business keys" % (tbl, T[tbl]["duplicateKeys"]))
        if T["mysql:ex_nd_final"].get("negativeQty"):
            viol.append("b: ex_nd_final has negative qty rows")
        for tbl, m in T.items():
            bm = base["verify"]["tables"][tbl]
            if m["rows"] != bm["rows"]:
                viol.append("c: %s FINAL rows=%s base=%s" % (tbl, m["rows"], bm["rows"]))
            if m["hash"] != bm["hash"]:
                viol.append("c: %s content hash differs from BASE (%s vs %s)" % (tbl, m["hash"], bm["hash"]))
            if m.get("identicalDuplicateRowsInFinal"):
                viol.append("c: %s FINAL contains %s repeated identical rows" % (tbl, m["identicalDuplicateRowsInFinal"]))
        if vf.get("diffsVsBase"):
            viol.append("c: row-level diffs vs BASE in %s" % list(vf["diffsVsBase"].keys()))
        det["rawDup"] = {t: m["rawRows"] - m["rows"] for t, m in T.items() if "rawRows" in m}
    # d. control plane
    v = view(rid)
    det["view"] = v
    if not v:
        viol.append("d: cannot read round view")
    else:
        if v["round"]["status"] != "SUCCEEDED":
            viol.append("d: round status %s" % v["round"]["status"])
        if v["round"].get("incomplete"):
            viol.append("d: round.incomplete=true")
        for r in v["runs"]:
            if r["status"] != "SUCCEEDED":
                viol.append("d: run %s status %s" % (r["runId"], r["status"]))
        for pid, cnt in (v.get("chunkCounts") or {}).items():
            if cnt.get("DEAD"):
                viol.append("d: pipeline %s has %s DEAD chunks" % (pid, cnt["DEAD"]))
        att = {}
        if check_chunks:
            splits = {}
            for pid in SOURCE_PIPELINES:
                ch = get_chunks(rid, pid)
                src = [c for c in ch if c.get("inputEdge") == "split"]
                splits[pid] = len(src)
                bad = [c["chunkId"] for c in src if c["status"] != "DONE"]
                if bad:
                    viol.append("d: source %s chunks not DONE: %s" % (pid, bad[:5]))
                att[pid] = sum(1 for c in ch if (c.get("attempt") or 0) > 1)
                dead = [c["chunkId"] for c in ch if c["status"] == "DEAD"]
                if dead:
                    viol.append("d: DEAD chunks in %s: %s" % (pid, dead[:5]))
            det["splitChunks"] = splits
            det["chunksWithAttemptGt1"] = att
            if ref_splits is not None and splits != ref_splits:
                viol.append("d: source chunk records %s != reference splits %s" % (splits, ref_splits))
    # e. redis hygiene (allow a short settle time)
    left = None
    for _ in range(30):
        left, info = redis_leftovers(rid)
        if not left:
            break
        time.sleep(1)
    det["redisLeftovers"] = left
    if left:
        viol.append("e: redis leftovers: %s" % left[:10])
    # f. live instances on every pod
    live = {}
    for _ in range(30):
        live = {}
        for p in CTX.pods:
            code, c = http(p.port, "GET", "/pg/cluster", 5)
            if code == 200 and rid in json.dumps(c.get("liveInstances")):
                live[p.name] = [i for i in c["liveInstances"] if i.get("roundId") == rid]
        if not live:
            break
        time.sleep(1)
    det["liveInstances"] = live
    if live:
        viol.append("f: liveInstances left for round on %s" % json.dumps(live)[:300])
    return viol, det


# ---------------------------------------------------------------- log scan
PATTERNS = [("ERROR", re.compile(r" ERROR ")), ("dead-lettered", re.compile(r"dead-lettered")), ("CONFLICT", re.compile(r"commit conflict|CONFLICT")),
            ("fenced", re.compile(r"fenced")), ("steps down", re.compile(r"steps down")), ("claim-renew-failed", re.compile(r"claim renew failed")),
            ("worker-loop-error", re.compile(r"loop error"))]


def log_offsets():
    return {p.name: (os.path.getsize(p.logpath()) if os.path.exists(p.logpath()) else 0) for p in CTX.pods}


def scan_logs(offsets, excerpt_path):
    counts = {k: 0 for k, _ in PATTERNS}
    per_pod = {}
    errs = {}
    with open(excerpt_path, "w") as ex:
        for p in CTX.pods:
            if not os.path.exists(p.logpath()):
                continue
            with open(p.logpath(), "rb") as f:
                f.seek(offsets.get(p.name, 0))
                text = f.read().decode(errors="replace")
            pc = {k: 0 for k, _ in PATTERNS}
            for line in text.splitlines():
                for k, rx in PATTERNS:
                    if rx.search(line):
                        counts[k] += 1
                        pc[k] += 1
                        if k in ("ERROR", "dead-lettered", "worker-loop-error"):
                            key = re.sub(r"[0-9a-f]{4,}|\d+", "#", line.split(" : ", 1)[-1])[:160]
                            errs[key] = errs.get(key, 0) + 1
                        if pc[k] <= 40:
                            ex.write("[%s] %s\n" % (p.name, line[:400]))
            per_pod[p.name] = pc
    top = sorted(errs.items(), key=lambda kv: -kv[1])[:5]
    return counts, per_pod, top


# ---------------------------------------------------------------- main
def main():
    os.makedirs(LOGS, exist_ok=True)
    open(os.path.join(OUT, "chaos.log"), "a").close()
    log("chaos start store=%s PODS=%d ROUNDS=%d MATERIALS=%d SEED=%d redisdb=%d mysqldb=%s chdb=%s out=%s" % (
        STORE_PROFILE or "redis", PODS, ROUNDS, MATERIALS, SEED, REDIS_DB, MYSQL_DB, CH_DB, OUT))
    subprocess.run([os.path.join(SCRIPTS, "stop-pods.sh")], env=dict(os.environ, PIPELINER_LOGS=LOGS), stdout=subprocess.DEVNULL)
    nbig = int(MATERIALS * 0.4)
    expect = {"nd_final": MATERIALS * 5 * 16, "nd1_big": nbig * 5 * 16}
    summary = {"store": STORE_PROFILE or "redis", "pods": PODS, "materials": MATERIALS, "seed": SEED, "iterations": [], "violations": 0}
    try:
        # ---- BASE
        basepod = Pod(0, extra=["--pipeliner.plan.force-mode=INLINE"], name="chaos-base")
        CTX.pods = [basepod]
        basepod.start()
        if not wait_ready([basepod]):
            raise RuntimeError("base pod did not come up")
        c, r = http(basepod.port, "POST", "/demo/seed?materials=%d&seed=42" % MATERIALS, 300)
        log("seed: %s %s" % (c, json.dumps(r)[:300]))
        BASEPV = "BASE-" + STAMP
        rid = submit(BASEPV)
        log("BASE round %s (INLINE, 1 pod)" % rid)
        ok, _, _, notes, _ = drive("BASE", {"BASE": {"rid": rid, "pv": BASEPV}}, False, 600)
        if not ok:
            raise RuntimeError("BASE round did not succeed: %s %s" % (notes, json.dumps(view(rid))[:1500]))
        code, bcs = http(basepod.port, "GET", "/demo/checksum?planVersion=" + BASEPV, 60)
        code, bvf = http(basepod.port, "GET", "/demo/verify?planVersion=" + BASEPV, 300)
        base = {"pv": BASEPV, "checksum": bcs, "verify": bvf}
        json.dump(base, open(os.path.join(OUT, "base.json"), "w"), indent=1)
        log("BASE checksum: %s" % json.dumps(bcs["kinds"]))
        sane = []
        if bvf["tables"]["mysql:ex_nd_final"]["rows"] != expect["nd_final"]:
            sane.append("BASE nd_final rows %s != %s" % (bvf["tables"]["mysql:ex_nd_final"]["rows"], expect["nd_final"]))
        if bvf["tables"]["mysql:ex_nd1_big"]["rows"] != expect["nd1_big"]:
            sane.append("BASE nd1_big rows %s != %s" % (bvf["tables"]["mysql:ex_nd1_big"]["rows"], expect["nd1_big"]))
        if bvf["tables"]["mysql:ex_publish_log"]["rows"] != 2:
            sane.append("BASE publish_log rows != 2")
        left, _ = redis_leftovers(rid)
        if left:
            sane.append("BASE redis leftovers %s" % left)
        if sane:
            raise RuntimeError("BASE self-check failed: " + "; ".join(sane))
        basepod.signal(signal.SIGTERM)
        for _ in range(30):
            if not basepod.alive():
                break
            time.sleep(0.5)
        basepod.signal(signal.SIGKILL)
        # ---- CL1: normal single-pod CLUSTER run
        pods = [Pod(i + 1) for i in range(PODS)]
        CTX.pods = pods[:1]
        pods[0].start()
        if not wait_ready(CTX.pods):
            raise RuntimeError("pod1 did not come up")
        CL1 = "CL1-" + STAMP
        rid = submit(CL1)
        log("CL1 round %s (normal CLUSTER, 1 pod)" % rid)
        ok, _, _, notes, _ = drive("CL1", {"CL1": {"rid": rid, "pv": CL1}}, False, 600)
        if not ok:
            raise RuntimeError("CL1 round did not succeed: %s" % notes)
        viol, det = verify_round(rid, CL1, base, None, expect)
        ref_splits = det.get("splitChunks")
        summary["cl1"] = {"violations": viol, "splitChunks": ref_splits, "checksumEqualsBase": not any(v.startswith("a:") for v in viol)}
        log("CL1 verdict: %s splitChunks=%s" % ("PASS" if not viol else "FAIL " + str(viol), ref_splits))
        if viol:
            summary["violations"] += len(viol)
        # ---- iterations
        CTX.pods = pods
        for p in pods[1:]:
            p.start()
        for it in range(1, ROUNDS + 1):
            itdir = os.path.join(OUT, "iter%02d" % it)
            os.makedirs(itdir, exist_ok=True)
            for p in pods:
                if p.state == "down" or not p.alive():
                    p.start()
            if not wait_ready(pods):
                raise RuntimeError("pods did not come up for iteration %d" % it)
            offs = log_offsets()
            load_start = vm_load()
            pvA, pvB = "C%02dA-%s" % (it, STAMP), "C%02dB-%s" % (it, STAMP)
            rA, rB = submit(pvA), submit(pvB)
            log("=== iteration %d: rounds %s=%s %s=%s (leader=%s)" % (it, pvA, rA, pvB, rB, (CTX.leader() or Pod(0)).name))
            rounds = {"A": {"rid": rA, "pv": pvA}, "B": {"rid": rB, "pv": pvB}}
            t_it = time.time()
            ok, actions, retries, notes, final = drive("it%02d" % it, rounds, True, TIMEOUT_SEC)
            time.sleep(2)
            vio, dets = [], {}
            if not ok:
                vio.append("rounds did not all reach SUCCEEDED: %s %s" % (final, notes))
            for name, r in rounds.items():
                if final.get(r["rid"]) != "SUCCEEDED":
                    continue
                v2, d2 = verify_round(r["rid"], r["pv"], base, ref_splits, expect)
                vio += ["%s(%s) %s" % (name, r["pv"], x) for x in v2]
                dets[name] = d2
            for rt in retries:
                vio.append("retry was needed for %s (%s) runs=%s: %s" % (rt["pv"], rt["rid"], rt["runs"], rt["error"]))
            load_end = vm_load()
            counts, per_pod, toperr = scan_logs(offs, os.path.join(itdir, "log-excerpt.txt"))
            by_kind = {}
            for a in actions:
                by_kind[a["kind"]] = by_kind.get(a["kind"], 0) + 1
            rec = {"iteration": it, "rounds": {k: r for k, r in rounds.items()}, "durationSec": round(time.time() - t_it),
                   "actions": by_kind, "actionLog": actions, "retries": [{k: v for k, v in rt.items() if k != "view"} for rt in retries],
                   "logCounts": counts, "logCountsPerPod": per_pod, "topErrors": toperr,
                   "rawDuplicateInserts": {k: d.get("rawDup") for k, d in dets.items()},
                   "chunksWithAttemptGt1": {k: d.get("chunksWithAttemptGt1") for k, d in dets.items()},
                   "vmLoadStart": load_start, "vmLoadEnd": load_end,
                   "correctnessViolations": [x for x in vio if not x.startswith(LIVENESS_PREFIXES) and ") retry" not in x[:0]],
                   "livenessViolations": [x for x in vio if x.startswith(LIVENESS_PREFIXES)],
                   "violations": vio, "verdict": "PASS" if not vio else "FAIL"}
            rec["verdictCorrectness"] = "PASS" if not rec["correctnessViolations"] else "FAIL"
            rec["verdictLiveness"] = "PASS" if not rec["livenessViolations"] else "FAIL"
            json.dump(rec, open(os.path.join(itdir, "result.json"), "w"), indent=1)
            if vio:
                ev = {"violations": vio, "retries": retries, "details": dets}
                for k, r in rounds.items():
                    ev.setdefault("redisKeys", {})[k] = redis_leftovers(r["rid"])[0]
                json.dump(ev, open(os.path.join(itdir, "evidence.json"), "w"), indent=1, default=str)
                summary["violations"] += len(vio)
            summary["iterations"].append({k: rec[k] for k in ("iteration", "durationSec", "vmLoadStart", "vmLoadEnd", "verdictCorrectness", "verdictLiveness", "actions", "logCounts", "rawDuplicateInserts", "retries", "violations", "verdict")})
            log("=== iteration %d verdict %s (correctness=%s liveness=%s) vmload=%s->%s actions=%s log=%s rawDup=%s" % (it, rec["verdict"], rec["verdictCorrectness"], rec["verdictLiveness"], load_start, load_end, by_kind, counts, rec["rawDuplicateInserts"]))
            for x in vio:
                log("    VIOLATION: %s" % x[:600])
    except Exception as e:  # noqa
        summary["fatal"] = repr(e)
        summary["violations"] += 1
        log("FATAL: %r" % e)
    finally:
        for p in CTX.pods:
            if p.pid and p.alive():
                p.signal(signal.SIGCONT)
        subprocess.run([os.path.join(SCRIPTS, "stop-pods.sh")], env=dict(os.environ, PIPELINER_LOGS=LOGS), stdout=subprocess.DEVNULL)
        json.dump(summary, open(os.path.join(OUT, "summary.json"), "w"), indent=1)
    log("DONE violations=%d summary=%s" % (summary["violations"], os.path.join(OUT, "summary.json")))
    sys.exit(1 if summary["violations"] else 0)


if __name__ == "__main__":
    main()
