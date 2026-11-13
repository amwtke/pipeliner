#!/usr/bin/env bash
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
if [ -f "$PIDFILE" ] && kill -0 "$(cat "$PIDFILE")" 2>/dev/null; then
  echo "pod $NAME already running (pid $(cat "$PIDFILE"))"; exit 0
fi
JAVA_BIN="${JAVA_HOME:+$JAVA_HOME/bin/}java"
nohup "$JAVA_BIN" ${JAVA_OPTS:-} -jar "$JAR" --pipeliner.pod-id="$NAME" --server.port="$PORT" "$@" >> "$LOGS/$NAME.log" 2>&1 &
echo $! > "$PIDFILE"
echo "started pod $NAME on port $PORT (pid $(cat "$PIDFILE"))"
