#!/usr/bin/env bash
# Env: MASTER_TICK=external (see below), TICK_INTERVAL_MS (1000), LEADER_TTL_MS (3000)
# Usage: run-pod.sh <name> <port> [extra spring args...]
# Starts the fat jar in the background; log: ../logs/<name>.log, pid: ../logs/<name>.pid
set -euo pipefail
[ $# -ge 2 ] || { echo "usage: $0 <name> <port> [extra args]" >&2; exit 2; }
NAME=$1; PORT=$2; shift 2
HERE="$(cd "$(dirname "$0")" && pwd)"
ROOT="$(cd "$HERE/.." && pwd)"
JAR="${PIPELINER_JAR:-$(ls "$ROOT"/target/pipeliner-example-*.jar 2>/dev/null | grep -v original | head -1 || true)}"
[ -n "$JAR" ] || { echo "fat jar not found; run mvn package first" >&2; exit 1; }
LOGS="${PIPELINER_LOGS:-$ROOT/logs}"   # override to keep a test run's logs/pids separate
mkdir -p "$LOGS"
PIDFILE="$LOGS/$NAME.pid"
. "$HERE/lib.sh"
if pid=$(pod_pid "$NAME"); then
  echo "pod $NAME already running (pid $pid)"; exit 0
fi   # a pid file whose pid is not this pod's java process (stale, reused pid) is ignored and overwritten below
# MASTER_TICK=external: no pg-master thread; an external trigger (tick-loop.sh simulating xxl-job) must POST /pg/master/tick
TICK_ARGS=()
if [ "${MASTER_TICK:-local}" = external ]; then
  TICK_ARGS=(--pipeliner.runtime.master-tick-mode=external --pipeliner.runtime.external-tick-interval-millis="${TICK_INTERVAL_MS:-1000}" --pipeliner.runtime.leader-ttl-millis="${LEADER_TTL_MS:-3000}")
fi
JAVA_BIN="${JAVA_HOME:+$JAVA_HOME/bin/}java"
nohup "$JAVA_BIN" ${JAVA_OPTS:-} -jar "$JAR" --pipeliner.pod-id="$NAME" --server.port="$PORT" ${TICK_ARGS[@]+"${TICK_ARGS[@]}"} "$@" >> "$LOGS/$NAME.log" 2>&1 &
echo $! > "$PIDFILE"
echo "started pod $NAME on port $PORT (pid $(cat "$PIDFILE"))"
