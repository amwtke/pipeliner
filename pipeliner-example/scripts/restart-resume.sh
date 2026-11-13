#!/usr/bin/env bash
# Full-cluster restart: 2 pods, seed, run V1 to completion, submit V2, kill -9 ALL pods while R2 is RUNNING,
# restart them, let V2 resume to SUCCEEDED, compare V1/V2 checksums.
# Env: MATERIALS (default 600), STORE_PROFILE (default: none = Redis store; e.g. mysql-store), SKIP_BUILD=1, TIMEOUT_SEC (per round, default 600)
set -uo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
ROOT="$(cd "$HERE/.." && pwd)"
REPO="$(cd "$ROOT/.." && pwd)"
MATERIALS="${MATERIALS:-600}"
TIMEOUT_SEC="${TIMEOUT_SEC:-600}"
PROFILE_ARGS=()
[ -n "${STORE_PROFILE:-}" ] && PROFILE_ARGS=(--spring.profiles.active="$STORE_PROFILE")
STAMP=$(date +%s)
PV1="V1-$STAMP"; PV2="V2-$STAMP"
PODS=(pod1 pod2)
PORTS=(8081 8082)
EXTRA=(--demo.core-grade-delay-ms=3)

die() { echo "RESTART-RESUME FAILED: $*" >&2; exit 1; }
json() { python3 -c "import sys,json
d=json.load(sys.stdin)
for k in sys.argv[1].split('.'):
    d = d.get(k) if isinstance(d, dict) else None
print('' if d is None else (json.dumps(d) if isinstance(d,(dict,list)) else d))" "$1"; }
export PG_ADMIN_TOKEN="${PG_ADMIN_TOKEN:-dev-admin-token-change-me}"   # pods read it too (application.yml)
api() { curl -s -m 30 -H "Authorization: Bearer $PG_ADMIN_TOKEN" "$@"; }
port_of() { for i in 0 1; do [ "${PODS[$i]}" = "$1" ] && echo "${PORTS[$i]}"; done; }
start_pod() { "$HERE/run-pod.sh" "$1" "$(port_of "$1")" ${PROFILE_ARGS[@]+"${PROFILE_ARGS[@]}"} "${EXTRA[@]}"; }
wait_up() { for _ in $(seq 1 120); do [ "$(api -o /dev/null -w '%{http_code}' "http://127.0.0.1:$1/pg/cluster")" = 200 ] && return 0; sleep 1; done; return 1; }

if [ -z "${SKIP_BUILD:-}" ]; then
  echo "== building"
  (cd "$REPO" && mvn -o -q -DskipTests -pl pipeliner-spring-boot-starter,pipeliner-example package || mvn -q -DskipTests -pl pipeliner-spring-boot-starter,pipeliner-example package) || die "build"
fi

trap '"$HERE/stop-pods.sh" >/dev/null 2>&1' EXIT
"$HERE/stop-pods.sh" >/dev/null 2>&1
echo "== starting pods"
for p in "${PODS[@]}"; do start_pod "$p"; done
for i in 0 1; do wait_up "${PORTS[$i]}" || die "pod ${PODS[$i]} did not come up (see logs)"; done
P1=${PORTS[0]}

echo "== seeding $MATERIALS materials"
api -X POST "http://127.0.0.1:$P1/demo/seed?materials=$MATERIALS&seed=42" | head -c 400; echo

# await_round <port> <roundId>: opens the manual approve gate once it is armed
await_round() {
  local port=$1 rid=$2 deadline=$((SECONDS + TIMEOUT_SEC)) view st
  while [ $SECONDS -lt $deadline ]; do
    view=$(api "http://127.0.0.1:$port/pg/rounds/$rid")
    st=$(echo "$view" | json round.status)
    case "$st" in
      SUCCEEDED) echo "round $rid SUCCEEDED"; return 0;;
      FAILED|CANCELLED) echo "$view" | head -c 2000; die "round $rid ended $st";;
    esac
    echo "$view" | python3 -c "import sys,json
d=json.load(sys.stdin)
g=[x for x in d.get('gates',[]) if x['gateId']=='approve']
sys.exit(0 if g and g[0]['status']=='CLOSED' and g[0].get('armedAt') else 1)" 2>/dev/null &&
      { echo "opening approve gate"; api -X POST "http://127.0.0.1:$port/pg/gates/$rid/approve/open?operator=e2e" >/dev/null; }
    sleep 1
  done
  die "timeout waiting for round $rid"
}

T0=$SECONDS
echo "== round 1 ($PV1) baseline"
R1=$(api -X POST "http://127.0.0.1:$P1/demo/rounds?planVersion=$PV1" | json roundId)
[ -n "$R1" ] || die "submit V1 failed"
echo "roundId=$R1"
await_round "$P1" "$R1"
CS1=$(api "http://127.0.0.1:$P1/demo/checksum?planVersion=$PV1")
echo "checksum V1: $CS1 (baseline took $((SECONDS - T0))s)"

echo "== round 2 ($PV2): kill -9 ALL pods while R2 is RUNNING"
R2=$(api -X POST "http://127.0.0.1:$P1/demo/rounds?planVersion=$PV2" | json roundId)
[ -n "$R2" ] || die "submit V2 failed"
echo "roundId=$R2"
rs=""
for _ in $(seq 1 300); do
  rs=$(api "http://127.0.0.1:$P1/pg/rounds/$R2" | python3 -c "import sys,json
d=json.load(sys.stdin)
print(''.join(r['status'] for r in d.get('runs',[]) if r['runId']=='R2'))" 2>/dev/null)
  [ "$rs" = RUNNING ] && break
  sleep 0.5
done
[ "$rs" = RUNNING ] || die "run R2 never became RUNNING"
sleep 2
for p in "${PODS[@]}"; do kill -9 "$(cat "$ROOT/logs/$p.pid")" 2>/dev/null; done
echo "killed all pods; sleeping 5s"
sleep 5
T1=$SECONDS
echo "restarting pods"
for p in "${PODS[@]}"; do start_pod "$p"; done
for i in 0 1; do wait_up "${PORTS[$i]}" || die "pod ${PODS[$i]} did not come back up"; done
await_round "$P1" "$R2"
echo "resume took $((SECONDS - T1))s"
CS2=$(api "http://127.0.0.1:$P1/demo/checksum?planVersion=$PV2")
echo "checksum V2: $CS2"

python3 - "$CS1" "$CS2" <<'PY' || die "checksum mismatch between V1 and V2"
import sys, json
a, b = json.loads(sys.argv[1]), json.loads(sys.argv[2])
ka, kb = a["kinds"], b["kinds"]
assert all(v["rows"] > 0 for v in ka.values()), "no rows for some kind: %s" % ka
if ka != kb:
    print("MISMATCH\nV1:", json.dumps(ka, indent=1), "\nV2:", json.dumps(kb, indent=1)); sys.exit(1)
print("checksums match")
PY
echo "RESTART-RESUME PASSED"
