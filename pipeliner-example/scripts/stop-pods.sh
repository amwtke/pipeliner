#!/usr/bin/env bash
# Stops every pod started by run-pod.sh (SIGTERM, then SIGKILL after 10s).
HERE="$(cd "$(dirname "$0")" && pwd)"
LOGS="$(cd "$HERE/.." && pwd)/logs"
for f in "$LOGS"/*.pid; do
  [ -e "$f" ] || continue
  pid=$(cat "$f")
  if kill -0 "$pid" 2>/dev/null; then
    kill "$pid"
    for _ in $(seq 1 20); do kill -0 "$pid" 2>/dev/null || break; sleep 0.5; done
    kill -9 "$pid" 2>/dev/null || true
    echo "stopped $(basename "$f" .pid)"
  fi
  rm -f "$f"
done
