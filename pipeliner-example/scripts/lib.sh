#!/usr/bin/env bash
# Shared helpers for the pod scripts (sourced, not executed). Expects HERE to be the scripts directory.
# LOGS honours PIPELINER_LOGS exactly like run-pod.sh / stop-pods.sh / tick-loop.sh.
LOGS="${PIPELINER_LOGS:-$(cd "$HERE/.." && pwd)/logs}"

# pid_runs <pid> <marker>: true when the process exists AND its command line contains <marker> (guards against a stale pid file whose pid
# was reused by an unrelated process). The command line is padded with a trailing space so "pod-id=pod1 " does not match "pod-id=pod10 ".
pid_runs() {
  local cmd
  cmd=$(ps -p "$1" -o command= 2>/dev/null) || return 1
  [[ "$cmd " == *"$2"* ]]
}

# pod_pid <name>: prints the pid of the running pod <name> (pid file + command-line check); non-zero when it is not running.
pod_pid() {
  local f="$LOGS/$1.pid" pid
  [ -f "$f" ] || return 1
  pid=$(cat "$f" 2>/dev/null)
  [ -n "$pid" ] || return 1
  pid_runs "$pid" "--pipeliner.pod-id=$1 " || return 1
  echo "$pid"
}

# kill_pod <name>: SIGKILL the running pod; non-zero (with a message on stderr) when there is no such pod or the kill fails.
kill_pod() {
  local pid
  pid=$(pod_pid "$1") || { echo "kill_pod: pod $1 is not running (pid file $LOGS/$1.pid missing, stale or not its java process)" >&2; return 1; }
  kill -9 "$pid" 2>/dev/null || { echo "kill_pod: kill -9 $pid ($1) failed" >&2; return 1; }
}
