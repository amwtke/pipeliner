#!/usr/bin/env bash
# Load test of ONE scenario. Env: MATERIALS (3000), PODS (3), STORE_PROFILE (none=Redis store | mysql-store), CAP_MODE (LOCAL|CLUSTER),
# CAP_PARALLELISM (3), SEED (42), TIMEOUT_SEC (1800), OUT (output dir), SKIP_BUILD=1, POD_XMX (4g), BASE_PORT (8081), KEEP_DATA (unused)
# Writes $OUT/report.json, $OUT/pods/*.log, $OUT/loadtest.log. Wrap in the global Lima lock when sharing the VM.
set -uo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
ROOT="$(cd "$HERE/.." && pwd)"
REPO="$(cd "$ROOT/.." && pwd)"
export OUT="${OUT:-$ROOT/loadtest-out/run-$(date +%s)}"
mkdir -p "$OUT"
export PIPELINER_LOGS="$OUT/pods"
mkdir -p "$PIPELINER_LOGS"
export JAVA_HOME="${JAVA_HOME:-/opt/homebrew/opt/openjdk@21}"
export PATH="$JAVA_HOME/bin:$PATH"
if [ -z "${SKIP_BUILD:-}" ]; then
  (cd "$REPO" && mvn -o -q -DskipTests -pl pipeliner-spring-boot-starter,pipeliner-example -am package) || { echo "build failed" >&2; exit 1; }
fi
export JAVA_OPTS="${JAVA_OPTS:--Xms512m -Xmx${POD_XMX:-4g}}"
"$HERE/stop-pods.sh" >/dev/null 2>&1
trap '"$HERE/stop-pods.sh" >/dev/null 2>&1' EXIT
exec python3 "$HERE/loadtest.py"
