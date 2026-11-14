#!/usr/bin/env bash
# Chaos test: several pods run two concurrent PipelineGraph rounds per iteration while pods are kill -9'd, SIGSTOPped (zombie
# leader / worker) and rounds are paused. Each round is verified against a single-threaded INLINE baseline (see chaos.py).
# Env: PODS=4 ROUNDS=6 (iterations, 2 rounds each) MATERIALS=600 STORE_PROFILE= (or mysql-store) SEED=<chaos RNG seed>
#      SKIP_BUILD=1  TIMEOUT_SEC=420 (per iteration)  CHAOS_MAX_SEC=150  BASE_PORT=9101
#      Isolation (defaults): REDIS_DB=5 MYSQL_DB=pipeliner_chaos CP_DB=pipeliner_chaos_cp CH_DB=pipeliner_chaos
#      OUT=<dir for logs/evidence> (default pipeliner-example/chaos-out/<timestamp>-<store>)
# Exit status is non-zero on any violation. All pods are stopped at the end.
set -uo pipefail
if [ "${MASTER_TICK:-local}" = external ]; then
  echo "$(basename "$0"): MASTER_TICK=external is not supported (nothing would tick the pods; rounds would stay CREATED). Unset MASTER_TICK; only e2e.sh drives external mode." >&2
  exit 2
fi
HERE="$(cd "$(dirname "$0")" && pwd)"
ROOT="$(cd "$HERE/.." && pwd)"
REPO="$(cd "$ROOT/.." && pwd)"
export PG_ADMIN_TOKEN="${PG_ADMIN_TOKEN:-dev-admin-token-change-me}"   # sent on every /pg call; pods read it via application.yml
export PODS="${PODS:-4}" ROUNDS="${ROUNDS:-6}" MATERIALS="${MATERIALS:-600}" STORE_PROFILE="${STORE_PROFILE:-}"
export SEED="${SEED:-$(( $(date +%s) % 100000 ))}"
export OUT="${OUT:-$ROOT/chaos-out/$(date +%Y%m%d-%H%M%S)-${STORE_PROFILE:-redis}}"
mkdir -p "$OUT/pods"
if [ -z "${SKIP_BUILD:-}" ]; then
  echo "== building example and the framework modules of this tree (-am; never install)"
  (cd "$REPO" && mvn -o -q -DskipTests -pl pipeliner-example -am package) || { echo "build failed" >&2; exit 2; }
fi
JAR="$(ls "$ROOT"/target/pipeliner-example-*.jar | grep -v original | head -1)"
cp "$JAR" "$OUT/pipeliner-example.jar"   # private copy: a concurrent rebuild cannot change the pods under test
{ echo "jar sha256: $(shasum -a 256 "$OUT/pipeliner-example.jar" | cut -d' ' -f1)"; echo "git head: $(git -C "$REPO" rev-parse --short HEAD)";
  echo "uncommitted: $(git -C "$REPO" status --short | wc -l | tr -d ' ') files"; } > "$OUT/manifest.txt"
trap 'PIPELINER_LOGS="$OUT/pods" "$HERE/stop-pods.sh" >/dev/null 2>&1' EXIT
python3 "$HERE/chaos.py"
rc=$?
echo "output: $OUT (exit $rc)"
exit $rc
