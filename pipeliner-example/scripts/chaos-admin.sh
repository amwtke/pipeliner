#!/usr/bin/env bash
# Admin operations x zombie processes chaos (see chaos-admin.py): A rerun with changed input + frozen old-attempt worker, B injected fault -> DLQ -> retry,
# C cancel + resubmit same planVersion with a straggler, D rerunFrom vs leader freeze. Reuses the chaos.py harness; 4 pods, `chaos` profile.
# Env: SCENARIOS=A,B,C,D ITER=1 (repetitions of the whole scenario list) BG_CHAOS=1 (background kill/stop loop; 0 = off) FAULT_MOD=7
#      A_DELTA=7 A_BIG=20 A_FINE=20 plus everything chaos.sh takes (PODS MATERIALS STORE_PROFILE SEED SKIP_BUILD TIMEOUT_SEC OUT ...).
# Exit status is non-zero on any violation. All pods are stopped at the end.
set -uo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
ROOT="$(cd "$HERE/.." && pwd)"
REPO="$(cd "$ROOT/.." && pwd)"
export PODS="${PODS:-4}" ROUNDS="${ROUNDS:-6}" MATERIALS="${MATERIALS:-600}" STORE_PROFILE="${STORE_PROFILE:-}"
export SEED="${SEED:-$(( $(date +%s) % 100000 ))}"
export OUT="${OUT:-$ROOT/chaos-out/admin-$(date +%Y%m%d-%H%M%S)-${STORE_PROFILE:-redis}}"
export PG_ADMIN_TOKEN="${PG_ADMIN_TOKEN:-dev-admin-token-change-me}"   # sent on every /pg call; pods read it via application.yml
mkdir -p "$OUT/pods"
if [ -z "${SKIP_BUILD:-}" ]; then
  echo "== building example (framework jars are taken from ~/.m2, not rebuilt)"
  (cd "$REPO" && mvn -o -q -DskipTests -pl pipeliner-example package) || { echo "build failed" >&2; exit 2; }
fi
JAR="$(ls "$ROOT"/target/pipeliner-example-*.jar | grep -v original | head -1)"
cp "$JAR" "$OUT/pipeliner-example.jar"   # private copy: a concurrent rebuild cannot change the pods under test
{ echo "jar sha256: $(shasum -a 256 "$OUT/pipeliner-example.jar" | cut -d' ' -f1)"; echo "git head: $(git -C "$REPO" rev-parse --short HEAD)";
  echo "uncommitted: $(git -C "$REPO" status --short | wc -l | tr -d ' ') files"; } > "$OUT/manifest.txt"
trap 'PIPELINER_LOGS="$OUT/pods" "$HERE/stop-pods.sh" >/dev/null 2>&1' EXIT
python3 "$HERE/chaos-admin.py"
rc=$?
echo "output: $OUT (exit $rc)"
exit $rc
