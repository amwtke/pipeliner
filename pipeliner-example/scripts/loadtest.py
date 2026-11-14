#!/usr/bin/env python3
"""Load-test driver for pipeliner-example (invoked by loadtest.sh; env documented there).

One scenario: start pod1, seed, start the other pods, submit one round, open the approve gate when armed, wait for a terminal state,
sample every 2 s (Redis memory, channel lengths, VM load, pod CPU/RSS), then collect Prometheus, log stats, checksum, duplicate check
and write OUT/report.json.
"""
import json
import os
import re
import socket
import subprocess
import sys
import threading
import time
import urllib.request
import urllib.error

E = os.environ.get
MATERIALS = int(E("MATERIALS", "3000"))
PODS = int(E("PODS", "3"))
STORE_PROFILE = E("STORE_PROFILE", "")
CAP_MODE = E("CAP_MODE", "LOCAL")
CAP_PAR = E("CAP_PARALLELISM", "3")
SEED = int(E("SEED", "42"))
TIMEOUT_SEC = int(E("TIMEOUT_SEC", "1800"))
OUT = E("OUT")
BASE_PORT = int(E("BASE_PORT", "8081"))
REDIS_PORT = int(E("REDIS_PORT", "6380"))
TOKEN = E("PG_ADMIN_TOKEN", "dev-admin-token-change-me")
SCRIPTS = os.path.dirname(os.path.abspath(__file__))
LOGS = os.path.join(OUT, "pods")
SAMPLE_SEC = 2.0
OURS = {"mysqld", "clickhouse", "clickhouse-serv", "redis-server", "limactl", "ssh"}
T0 = time.time()
lock = threading.Lock()


def log(msg):
    line = "%.1f %s" % (time.time() - T0, msg)
    print(line, flush=True)
    with lock:
        with open(os.path.join(OUT, "loadtest.log"), "a") as f:
            f.write(line + "\n")


# ---------- http ----------
def http(method, url, timeout=60, raw=False):
    req = urllib.request.Request(url, method=method, headers={"Authorization": "Bearer " + TOKEN})
    if method == "POST":
        req.data = b""
    with urllib.request.urlopen(req, timeout=timeout) as r:
        body = r.read().decode()
        return body if raw else json.loads(body)


def pod_port(i):
    return BASE_PORT + i


def start_pod(i):
    args = [os.path.join(SCRIPTS, "run-pod.sh"), "pod%d" % (i + 1), str(pod_port(i))]
    if STORE_PROFILE:
        args.append("--spring.profiles.active=" + STORE_PROFILE)
    args += ["--demo.cap-alloc-mode=" + CAP_MODE, "--demo.cap-alloc-parallelism=" + CAP_PAR,
             "--demo.algo-callback-timeout=%ds" % TIMEOUT_SEC]
    subprocess.run(args, check=True, capture_output=True)


def wait_up(i, secs=180):
    end = time.time() + secs
    while time.time() < end:
        try:
            http("GET", "http://127.0.0.1:%d/pg/cluster" % pod_port(i), timeout=5)
            return True
        except Exception:
            time.sleep(1)
    return False


def pod_pids():
    out = {}
    for i in range(PODS):
        f = os.path.join(LOGS, "pod%d.pid" % (i + 1))
        try:
            out["pod%d" % (i + 1)] = int(open(f).read().strip())
        except Exception:
            pass
    return out


# ---------- samplers ----------
def vm_snapshot():
    """loadavg, free mem, per-process cpu ticks of the Lima VM (read-only)."""
    script = ("cat /proc/loadavg; free -m | sed -n 2p; echo ---; "
              "for d in /proc/[0-9]*; do read -r l < $d/stat 2>/dev/null && echo \"$l\"; done")
    try:
        r = subprocess.run(["limactl", "shell", "cg", "--", "bash", "-c", script], capture_output=True, text=True, timeout=30)
    except Exception as e:
        return {"error": str(e)}
    lines = r.stdout.splitlines()
    if len(lines) < 3:
        return {"error": "short output"}
    la = lines[0].split()
    mem = lines[1].split()
    procs = {}
    for l in lines[3:]:
        m = re.match(r"(\d+) \((.*)\) (\S) (.*)", l)
        if not m:
            continue
        f = m.group(4).split()
        try:
            procs[int(m.group(1))] = (m.group(2), int(f[10]) + int(f[11]))   # utime+stime ticks
        except Exception:
            pass
    return {"t": time.time(), "load1": float(la[0]), "load5": float(la[1]), "load15": float(la[2]), "running": la[3],
            "mem_total_mb": int(mem[1]), "mem_used_mb": int(mem[2]), "mem_free_mb": int(mem[3]), "mem_avail_mb": int(mem[6]),
            "_procs": procs}


def foreign_summary(a, b):
    """Top cpu consumers between two VM snapshots (ticks=100/s) with ours/foreign flag."""
    if "_procs" not in a or "_procs" not in b:
        return []
    dt = max(b["t"] - a["t"], 0.001)
    rows = []
    for pid, (comm, t1) in b["_procs"].items():
        if pid in a["_procs"]:
            d = t1 - a["_procs"][pid][1]
            if d > 0:
                rows.append((round(d / 100.0 / dt, 2), comm, pid))
    rows.sort(reverse=True)
    return [{"cores": c, "comm": n, "pid": p, "ours": n in OURS} for c, n, p in rows[:5]]


def redis_cmd(*args):
    s = socket.create_connection(("127.0.0.1", REDIS_PORT), timeout=5)
    try:
        s.sendall(("*%d\r\n" % len(args) + "".join("$%d\r\n%s\r\n" % (len(a.encode()), a) for a in args)).encode())
        data = b""
        need = None
        while True:
            chunk = s.recv(65536)
            if not chunk:
                break
            data += chunk
            if need is None and b"\r\n" in data:
                need = int(data.split(b"\r\n", 1)[0][1:])   # $<len> bulk reply
                need = len(data.split(b"\r\n", 1)[0]) + 2 + need + 2
            if need is not None and len(data) >= need:
                break
        data = data.split(b"\r\n", 1)[1] if b"\r\n" in data else data
        return data.decode(errors="replace")
    finally:
        s.close()


def redis_mem():
    txt = redis_cmd("INFO", "memory")
    d = {}
    for l in txt.splitlines():
        if ":" in l and not l.startswith("#"):
            k, v = l.split(":", 1)
            d[k] = v
    return {"used_memory": int(d.get("used_memory", 0)), "used_memory_rss": int(d.get("used_memory_rss", 0)),
            "peak": int(d.get("used_memory_peak", 0))}


def cpu_secs(s):
    # ps cputime: [dd-]hh:mm:ss or mm:ss.ss
    s = s.strip()
    try:
        parts = s.split(":")
        v = 0.0
        for p in parts:
            v = v * 60 + float(p)
        return v
    except Exception:
        return 0.0


def pod_ps(pids):
    out = {}
    if not pids:
        return out
    r = subprocess.run(["ps", "-o", "pid=,%cpu=,rss=,cputime=", "-p", ",".join(str(p) for p in pids.values())], capture_output=True, text=True)
    byp = {v: k for k, v in pids.items()}
    for l in r.stdout.splitlines():
        f = l.split()
        if len(f) >= 4 and int(f[0]) in byp:
            out[byp[int(f[0])]] = {"pcpu": float(f[1]), "rss_mb": int(f[2]) // 1024, "cpu_s": cpu_secs(f[3])}
    return out


def host_load():
    try:
        return os.getloadavg()[0]
    except Exception:
        return None


class Sampler(threading.Thread):
    def __init__(self, rid, port_fn):
        super().__init__(daemon=True)
        self.rid = rid
        self.port_fn = port_fn
        self.samples = []
        self.stop_ev = threading.Event()
        self.prev_vm = None
        self.prev_ps = None

    def run(self):
        while not self.stop_ev.is_set():
            t = time.time()
            s = {"t": round(t - T0, 1)}
            try:
                s["redis"] = redis_mem()
            except Exception as e:
                s["redis_err"] = str(e)
            try:
                bp = http("GET", "http://127.0.0.1:%d/pg/rounds/%s/backpressure" % (self.port_fn(), self.rid), timeout=20)
                ch = bp.get("channels", {})
                s["channels"] = ch
                s["max_channel"] = max(ch.values()) if ch else 0
                s["bottlenecks"] = bp.get("bottlenecks", [])
            except Exception as e:
                s["bp_err"] = str(e)[:80]
            ps = pod_ps(pod_pids())
            s["pods"] = {}
            for k, v in ps.items():
                cores = None
                if self.prev_ps and k in self.prev_ps[1]:
                    dtp = t - self.prev_ps[0]
                    cores = round((v["cpu_s"] - self.prev_ps[1][k]["cpu_s"]) / dtp, 2) if dtp > 0 else None
                s["pods"][k] = {"rss_mb": v["rss_mb"], "cores": cores, "pcpu": v["pcpu"]}
            self.prev_ps = (t, ps)
            s["host_load1"] = host_load()
            vm = vm_snapshot()
            if "_procs" in vm:
                s["vm"] = {k: vm[k] for k in vm if not k.startswith("_") and k != "t"}
                s["vm_top"] = foreign_summary(self.prev_vm, vm) if self.prev_vm else []
                self.prev_vm = vm
            else:
                s["vm_err"] = vm.get("error")
            self.samples.append(s)
            self.stop_ev.wait(max(0.1, SAMPLE_SEC - (time.time() - t)))


# ---------- ancillary ----------
def mysql_q(sql):
    r = subprocess.run(["limactl", "shell", "cg", "--", "mysql", "-upipeliner", "-ppipeliner", "-h127.0.0.1", "pipeliner_demo", "-N", "-B", "-e", sql],
                       capture_output=True, text=True, timeout=600)
    return r.stdout.strip()


def ch_q(sql):
    req = urllib.request.Request("http://127.0.0.1:8123/?user=pipeliner&password=pipeliner&query=" + urllib.parse.quote(sql))
    with urllib.request.urlopen(req, timeout=600) as r:
        return r.read().decode().strip()


import urllib.parse  # noqa: E402


def table_sizes():
    out = {}
    try:
        for l in ch_q("select table, sum(rows), sum(bytes_on_disk) from system.parts where database='pipeliner_demo' and active group by table format TSV").splitlines():
            t, r, b = l.split("\t")
            out["ch." + t] = {"rows": int(r), "bytes": int(b)}
    except Exception as e:
        out["ch_error"] = str(e)
    try:
        for l in mysql_q("select table_name, table_rows, data_length+index_length from information_schema.tables where table_schema='pipeliner_demo'").splitlines():
            t, r, b = l.split("\t")
            out["mysql." + t] = {"rows_est": int(r), "bytes": int(b)}
    except Exception as e:
        out["mysql_error"] = str(e)
    return out


def parse_prom(text):
    """Sum all samples per (name, labels) except jvm/tomcat/process/system/hikari noise is kept separately by prefix filter."""
    res = {}
    for l in text.splitlines():
        if not l or l.startswith("#"):
            continue
        m = re.match(r"^([A-Za-z_:][A-Za-z0-9_:]*)(\{.*\})?\s+(\S+)$", l)
        if not m:
            continue
        try:
            v = float(m.group(3))
        except ValueError:
            continue
        key = m.group(1) + (m.group(2) or "")
        res[key] = v
    return res


NOISE = re.compile(r"^(jvm_|tomcat_|process_|system_|logback_|disk_|executor_|application_|jdbc_|spring_|http_server|lettuce_|hikaricp_connections_(idle|max|min|pending|creation|timeout|usage|acquire)_)")


def collect_prom(n_pods):
    agg = {}
    for i in range(n_pods):
        try:
            txt = http("GET", "http://127.0.0.1:%d/actuator/prometheus" % pod_port(i), timeout=30, raw=True)
        except Exception as e:
            log("prometheus pod%d failed: %s" % (i + 1, e))
            continue
        with open(os.path.join(OUT, "prom-pod%d.txt" % (i + 1)), "w") as f:
            f.write(txt)
        for k, v in parse_prom(txt).items():
            name = k.split("{")[0]
            if NOISE.match(name):
                continue
            if "quantile" in k or "_bucket" in name:
                continue
            agg[k] = max(agg.get(k, 0.0), v) if name.endswith("_max") else agg.get(k, 0.0) + v   # sum over pods; max for *_max
    return {k: round(v, 3) for k, v in sorted(agg.items())}


def log_stats():
    res = {"files": {}}
    norm_num = re.compile(r"\b[0-9a-f]{8}-[0-9a-f-]{27}\b|\b[0-9a-f]{12,}\b|\d+")
    for f in sorted(os.listdir(LOGS)):
        if not f.endswith(".log"):
            continue
        err = warn = 0
        top = {}
        with open(os.path.join(LOGS, f), errors="replace") as fh:
            for l in fh:
                m = re.search(r"\b(ERROR|WARN)\b", l[:120])
                if not m:
                    continue
                if m.group(1) == "ERROR":
                    err += 1
                else:
                    warn += 1
                msg = norm_num.sub("#", l[m.end():].strip())[:220]
                k = m.group(1) + " " + msg
                top[k] = top.get(k, 0) + 1
        res["files"][f] = {"error": err, "warn": warn}
        for k, v in top.items():
            res.setdefault("_top", {})
            res["_top"][k] = res["_top"].get(k, 0) + v
    res["error_total"] = sum(v["error"] for v in res["files"].values())
    res["warn_total"] = sum(v["warn"] for v in res["files"].values())
    res["top_messages"] = [{"count": c, "msg": m} for m, c in sorted(res.pop("_top", {}).items(), key=lambda x: -x[1])[:15]]
    return res


def ms(x):
    return None if x is None else round(x / 1000.0, 2)


def build_view_summary(view, extra=None):
    rd = view["round"]
    runs = []
    for r in sorted(view.get("runs", []), key=lambda r: (r.get("stage", 0), r["runId"])):
        s, f = r.get("startedAt"), r.get("finishedAt")
        runs.append({"runId": r["runId"], "stage": r.get("stage"), "mode": r.get("mode"), "status": r.get("status"), "attempt": r.get("attempt"),
                     "started_s": ms(s - rd["startedAt"]) if s and rd.get("startedAt") else None,
                     "duration_s": ms(f - s) if s and f else None, "error": r.get("error")})
    pipes = {}
    for p in view.get("pipelines", []):
        pid = p["pipelineId"]
        s, f = p.get("startedAt"), p.get("finishedAt")
        dur = (f - s) / 1000.0 if s and f else None
        ri = (view.get("rowsIn") or {}).get(pid)
        ro = (view.get("rowsOut") or {}).get(pid)
        cc = (view.get("chunkCounts") or {}).get(pid, {})
        pipes[pid] = {"run": p.get("runId"), "status": p.get("status"), "parallelism": p.get("parallelism"),
                      "started_s": ms(s - rd["startedAt"]) if s and rd.get("startedAt") else None,
                      "finished_s": ms(f - rd["startedAt"]) if f and rd.get("startedAt") else None,
                      "duration_s": round(dur, 2) if dur is not None else None,
                      "rows_in": ri, "rows_out": ro,
                      "rows_in_per_s": round(ri / dur, 1) if dur and ri else None,
                      "rows_out_per_s": round(ro / dur, 1) if dur and ro else None,
                      "chunks": cc, "chunks_total": sum(cc.values()) if cc else 0,
                      "summaryRowsIn": p.get("summaryRowsIn"), "summaryRowsOut": p.get("summaryRowsOut"), "summaryCounts": p.get("summaryCounts"),
                      "error": p.get("error")}
    gates = {}
    for g in view.get("gates", []):
        a, o = g.get("armedAt"), g.get("openedAt")
        gates[g["gateId"]] = {"type": g.get("type"), "status": g.get("status"), "armed_s": ms(a - rd["startedAt"]) if a and rd.get("startedAt") else None,
                              "wait_s": ms(o - a) if a and o else None, "openedBy": g.get("openedBy")}
    return runs, pipes, gates


def chunk_stats(port, rid, pipes):
    """Per pipeline: attempts>1 (retries), DEAD, costMs sum/max from the chunk list (paged)."""
    out = {}
    for pid in pipes:
        n = retried = dead = 0
        cost = 0
        cmax = 0
        off = 0
        try:
            while True:
                lst = http("GET", "http://127.0.0.1:%d/pg/rounds/%s/chunks?pipeline=%s&limit=1000&offset=%d" % (port, rid, pid, off), timeout=120)
                if not lst:
                    break
                for c in lst:
                    n += 1
                    if (c.get("attempt") or 0) > 1:
                        retried += 1
                    if c.get("status") == "DEAD":
                        dead += 1
                    cm = c.get("costMs") or 0
                    cost += cm
                    cmax = max(cmax, cm)
                off += len(lst)
                if len(lst) < 1000:
                    break
        except Exception as e:
            out[pid] = {"error": str(e)[:100]}
            continue
        if n:
            out[pid] = {"chunks": n, "retried_attempt_gt1": retried, "dead": dead, "cost_ms_sum": cost, "cost_ms_avg": round(cost / n, 1), "cost_ms_max": cmax}
    return out


def main():
    os.makedirs(LOGS, exist_ok=True)
    rep = {"config": {"materials": MATERIALS, "pods": PODS, "store_profile": STORE_PROFILE or "redis", "cap_mode": CAP_MODE, "cap_parallelism": CAP_PAR,
                      "seed": SEED, "timeout_sec": TIMEOUT_SEC, "java_opts": E("JAVA_OPTS"), "rows_per_stage_est": MATERIALS * 80,
                      "git": subprocess.run(["git", "rev-parse", "--short", "HEAD"], capture_output=True, text=True, cwd=SCRIPTS).stdout.strip()},
           "started_at": time.strftime("%Y-%m-%d %H:%M:%S")}
    vm0 = vm_snapshot()
    rep["vm_before"] = {k: v for k, v in vm0.items() if not k.startswith("_")}
    rep["redis_before"] = redis_mem()
    status = "UNKNOWN"
    rid = None
    sampler = None
    try:
        start_pod(0)
        if not wait_up(0):
            raise RuntimeError("pod1 did not come up")
        t = time.time()
        seed_res = http("POST", "http://127.0.0.1:%d/demo/seed?materials=%d&seed=%d" % (pod_port(0), MATERIALS, SEED), timeout=1800)
        rep["seed"] = {"seconds": round(time.time() - t, 2), "counts": seed_res}
        log("seeded %d materials in %.1fs" % (MATERIALS, rep["seed"]["seconds"]))
        t = time.time()
        for i in range(1, PODS):
            start_pod(i)
        for i in range(1, PODS):
            if not wait_up(i):
                raise RuntimeError("pod%d did not come up" % (i + 1))
        rep["pods_start_seconds"] = round(time.time() - t, 2)
        time.sleep(3)   # heartbeats
        port = pod_port(0)
        pv = "LT-%d" % int(time.time())
        rep["plan_version"] = pv
        rep["redis_pre_round"] = redis_mem()
        vm_pre = vm_snapshot()
        t_submit = time.time()
        rid = http("POST", "http://127.0.0.1:%d/demo/rounds?planVersion=%s" % (port, pv))["roundId"]
        log("round %s submitted (%s)" % (rid, pv))
        sampler = Sampler(rid, lambda: port)
        sampler.prev_vm = vm_pre
        sampler.start()
        deadline = time.time() + TIMEOUT_SEC
        approve_opened = False
        view = None
        last_log = 0
        progress_sig = None
        progress_t = time.time()
        while time.time() < deadline:
            try:
                view = http("GET", "http://127.0.0.1:%d/pg/rounds/%s" % (port, rid), timeout=60)
            except Exception as e:
                log("view error: %s" % e)
                time.sleep(2)
                continue
            status = view["round"]["status"]
            if status in ("SUCCEEDED", "FAILED", "CANCELLED"):
                t_end = time.time()
                break
            if not approve_opened:
                g = [x for x in view.get("gates", []) if x["gateId"] == "approve"]
                if g and g[0]["status"] == "CLOSED" and g[0].get("armedAt"):
                    http("POST", "http://127.0.0.1:%d/pg/gates/%s/approve/open?operator=loadtest" % (port, rid))
                    approve_opened = True
                    log("approve gate opened")
            sig = json.dumps([view.get("rowsIn"), view.get("rowsOut"), [(r["runId"], r["status"]) for r in view.get("runs", [])]], sort_keys=True)
            if sig != progress_sig:
                progress_sig, progress_t = sig, time.time()
            if time.time() - last_log > 30:
                last_log = time.time()
                log("status=%s runs=%s rowsOut=%s" % (status, {r["runId"]: r["status"] for r in view.get("runs", [])}, view.get("rowsOut")))
            time.sleep(1)
        else:
            t_end = time.time()
            status = "TIMEOUT"
            rep["timeout_stalled_seconds"] = round(time.time() - progress_t, 1)
        rep["round_status"] = status
        rep["round_id"] = rid
        rep["total_wall_s"] = round(t_end - t_submit, 2)
        if view:
            rep["round_error"] = view["round"].get("error")
            rd = view["round"]
            if rd.get("startedAt") and rd.get("finishedAt"):
                rep["round_view_duration_s"] = ms(rd["finishedAt"] - rd["startedAt"])
            rep["submit_to_start_s"] = ms(rd["startedAt"] - rd["createdAt"]) if rd.get("startedAt") and rd.get("createdAt") else None
            runs, pipes, gates = build_view_summary(view)
            rep["runs"], rep["pipelines"], rep["gates"] = runs, pipes, gates
            with open(os.path.join(OUT, "final-view.json"), "w") as f:
                json.dump(view, f, indent=1)
        log("round %s -> %s in %.1fs" % (rid, status, rep["total_wall_s"]))
        sampler.stop_ev.set()
        sampler.join(10)
        vm1 = vm_snapshot()
        rep["vm_after"] = {k: v for k, v in vm1.items() if not k.startswith("_")}
        try:
            rep["chunk_stats"] = chunk_stats(port, rid, rep.get("pipelines", {}))
        except Exception as e:
            rep["chunk_stats_error"] = str(e)
        try:
            rep["workers_final"] = http("GET", "http://127.0.0.1:%d/pg/cluster/workers" % port)
        except Exception as e:
            rep["workers_error"] = str(e)
        rep["prometheus"] = collect_prom(PODS)
        t = time.time()
        try:
            cs = http("GET", "http://127.0.0.1:%d/demo/checksum?planVersion=%s" % (port, pv), timeout=1800)
            rep["checksum"] = cs
            rep["checksum_seconds"] = round(time.time() - t, 2)
        except Exception as e:
            rep["checksum_error"] = str(e)
        try:
            q = mysql_q("select count(*), count(distinct material_id, node, month), count(distinct plan_version) from ex_nd_final where plan_version='%s'" % pv)
            c, d, v = q.split("\t")
            rep["duplicate_check"] = {"count": int(c), "count_distinct_keys": int(d), "duplicates": int(c) - int(d)}
            rep["table_counts"] = {"ex_nd_final_plan_version": int(c)}
            for tb in ("ex_nd1_big", "ex_publish_log"):
                rep["table_counts"][tb] = int(mysql_q("select count(*) from %s where plan_version='%s'" % (tb, pv)) or 0)
            for tb in ("ex_algo_gd", "ex_fine_gd", "ex_grade_result", "ex_refurb_plan", "ex_scrap"):
                rep["table_counts"][tb] = int(ch_q("select count() from pipeliner_demo.%s final where plan_version='%s'" % (tb, pv)) or 0)
                rep["table_counts"][tb + "_raw"] = int(ch_q("select count() from pipeliner_demo.%s where plan_version='%s'" % (tb, pv)) or 0)
        except Exception as e:
            rep["duplicate_check_error"] = str(e)
        try:
            rep["table_sizes"] = table_sizes()
        except Exception as e:
            rep["table_sizes_error"] = str(e)
        rep["redis_after"] = redis_mem()
    except Exception as e:
        log("scenario error: %r" % e)
        rep["scenario_error"] = repr(e)
        rep.setdefault("round_status", status)
        if sampler:
            sampler.stop_ev.set()
    # series summary
    if sampler and sampler.samples:
        ss = sampler.samples
        rep["samples"] = ss
        used = [s["redis"]["used_memory"] for s in ss if "redis" in s]
        rep["redis_peak_used_memory_mb"] = round(max(used) / 1048576.0, 1) if used else None
        rep["redis_peak_used_memory_reported_by_redis_mb"] = round(max(s["redis"]["peak"] for s in ss if "redis" in s) / 1048576.0, 1) if used else None
        mc = [(s.get("max_channel", 0), s) for s in ss if "max_channel" in s]
        if mc:
            m, s = max(mc, key=lambda x: x[0])
            rep["max_channel_length"] = m
            rep["max_channel_at"] = {k: v for k, v in s["channels"].items() if v == m}
        peak_ch = {}
        for s in ss:
            for k, v in s.get("channels", {}).items():
                peak_ch[k] = max(peak_ch.get(k, 0), v)
        rep["peak_channels"] = peak_ch
        bn = {}
        for s in ss:
            for b in s.get("bottlenecks", []):
                bn[b] = bn.get(b, 0) + 1
        rep["bottleneck_sample_counts"] = bn
        l1 = [s["vm"]["load1"] for s in ss if "vm" in s]
        rep["vm_load1_max_during"] = max(l1) if l1 else None
        rep["vm_load1_avg_during"] = round(sum(l1) / len(l1), 2) if l1 else None
        foreign = {}
        for s in ss:
            for p in s.get("vm_top", []):
                if not p["ours"]:
                    foreign[p["comm"]] = max(foreign.get(p["comm"], 0), p["cores"])
        rep["vm_foreign_max_cores_by_comm"] = dict(sorted(foreign.items(), key=lambda x: -x[1])[:6])
        ours = {}
        for s in ss:
            for p in s.get("vm_top", []):
                if p["ours"]:
                    ours.setdefault(p["comm"], []).append(p["cores"])
        rep["vm_ours_avg_max_cores"] = {k: {"avg": round(sum(v) / len(v), 2), "max": max(v)} for k, v in ours.items()}
        podagg = {}
        for s in ss:
            for k, v in s.get("pods", {}).items():
                a = podagg.setdefault(k, {"rss_max_mb": 0, "cores": []})
                a["rss_max_mb"] = max(a["rss_max_mb"], v["rss_mb"])
                if v["cores"] is not None:
                    a["cores"].append(v["cores"])
        rep["pod_summary"] = {k: {"rss_max_mb": a["rss_max_mb"], "cores_avg": round(sum(a["cores"]) / len(a["cores"]), 2) if a["cores"] else None,
                                  "cores_max": max(a["cores"]) if a["cores"] else None} for k, a in podagg.items()}
        fl = [s["vm"] for s in ss if "vm" in s]
        rep["vm_min_avail_mem_mb"] = min(v["mem_avail_mb"] for v in fl) if fl else None
    rep["flag_foreign_load_gt3"] = bool(rep.get("vm_load1_max_during", 0) and (
        rep["vm_load1_max_during"] > 3 or rep.get("vm_before", {}).get("load1", 0) > 3 or rep.get("vm_after", {}).get("load1", 0) > 3)
        and any(v > 0.5 for v in rep.get("vm_foreign_max_cores_by_comm", {}).values()))
    subprocess.run([os.path.join(SCRIPTS, "stop-pods.sh")], capture_output=True)
    rep["log_stats"] = log_stats()
    rep["finished_at"] = time.strftime("%Y-%m-%d %H:%M:%S")
    with open(os.path.join(OUT, "report.json"), "w") as f:
        json.dump(rep, f, indent=1)
    log("report written: " + os.path.join(OUT, "report.json"))
    return 0 if rep.get("round_status") == "SUCCEEDED" else 1


if __name__ == "__main__":
    sys.exit(main())
