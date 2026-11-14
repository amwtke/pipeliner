#!/usr/bin/env bash
# Stops every pod started by run-pod.sh (SIGTERM, then SIGKILL after 10s) and the tick loop. A pid file is only trusted when the pid is
# still that pod's java process (or tick-loop.sh): a stale file whose pid was reused by something else is just removed.
HERE="$(cd "$(dirname "$0")" && pwd)"
. "$HERE/lib.sh"
for f in "$LOGS"/*.pid; do
  [ -e "$f" ] || continue
  name=$(basename "$f" .pid)
  pid=$(cat "$f" 2>/dev/null)
  if [ "$name" = tick-loop ]; then marker="tick-loop.sh"; else marker="--pipeliner.pod-id=$name "; fi
  if [ -n "$pid" ] && pid_runs "$pid" "$marker"; then
    kill "$pid" 2>/dev/null
    for _ in $(seq 1 20); do kill -0 "$pid" 2>/dev/null || break; sleep 0.5; done
    kill -9 "$pid" 2>/dev/null || true
    echo "stopped $name"
  elif [ -n "$pid" ]; then
    echo "ignoring stale pid file $f (pid $pid is not $name)" >&2
  fi
  rm -f "$f"
done
