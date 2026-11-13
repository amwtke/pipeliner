#!/usr/bin/env bash
# Simulates an xxl-job trigger (RANDOM route, FAILOVER on error): every INTERVAL seconds POST /pg/master/tick to a random pod; if the call fails
# (pod killed) the other pods are tried in the same round. Usage: tick-loop.sh <port>...   Env: PG_ADMIN_TOKEN, TICK_INTERVAL (1), LOGS
# Writes one line per result to $LOGS/tick-loop.log and its pid to $LOGS/tick-loop.pid (stop-pods.sh stops it).
HERE="$(cd "$(dirname "$0")" && pwd)"
LOGS="${PIPELINER_LOGS:-$(cd "$HERE/.." && pwd)/logs}"
mkdir -p "$LOGS"
echo $$ > "$LOGS/tick-loop.pid"
: > "$LOGS/tick-loop.log"
TOKEN="${PG_ADMIN_TOKEN:-dev-admin-token-change-me}"
PORTS=("$@")
while true; do
  order=($(printf '%s\n' "${PORTS[@]}" | sort -R))
  for p in "${order[@]}"; do
    out=$(curl -s -m 5 -f -X POST -H "Authorization: Bearer $TOKEN" "http://127.0.0.1:$p/pg/master/tick") && { echo "$p $out" >> "$LOGS/tick-loop.log"; break; }
    echo "$p FAILED" >> "$LOGS/tick-loop.log"
  done
  sleep "${TICK_INTERVAL:-1}"
done
