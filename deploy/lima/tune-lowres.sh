#!/usr/bin/env bash
# Low-resource tuning for the test services installed by setup-services.sh.
# Usage (on the host):  limactl shell <instance> -- sudo bash deploy/lima/tune-lowres.sh
# Only touches ClickHouse / MySQL config and restarts those two services; Redis is left as is (~20 MB).
set -euo pipefail

# ---------- ClickHouse: cap memory, shrink caches and merge pool, stop the always-on system logs ----------
cat >/etc/clickhouse-server/config.d/zz-lowres.xml <<'EOF'
<clickhouse>
  <max_server_memory_usage>1610612736</max_server_memory_usage>   <!-- 1.5 GiB -->
  <mark_cache_size>268435456</mark_cache_size>                    <!-- 256 MiB -->
  <uncompressed_cache_size>268435456</uncompressed_cache_size>    <!-- 256 MiB -->
  <!-- background_pool_size is left at its default: lowering it below 10 fails ClickHouse's
       startup check against number_of_free_entries_in_pool_to_execute_mutation (20) -->
  <!-- system log tables written continuously even when idle; query_log is kept for debugging -->
  <trace_log remove="1"/>
  <metric_log remove="1"/>
  <asynchronous_metric_log remove="1"/>
  <part_log remove="1"/>
  <text_log remove="1"/>
  <query_thread_log remove="1"/>
  <processors_profile_log remove="1"/>
</clickhouse>
EOF
chown clickhouse:clickhouse /etc/clickhouse-server/config.d/zz-lowres.xml || true

# ---------- MySQL: no performance_schema, no binlog, relaxed flushing (test data only) ----------
cat >/etc/mysql/mysql.conf.d/zz-lowres.cnf <<'EOF'
[mysqld]
performance_schema = OFF
innodb_buffer_pool_size = 128M
innodb_log_buffer_size = 8M
innodb_flush_log_at_trx_commit = 2
skip-log-bin
EOF

systemctl restart clickhouse-server mysql
for i in $(seq 1 30); do
  clickhouse-client --user pipeliner --password pipeliner -q "SELECT 1" >/dev/null 2>&1 && break
  sleep 1
done

echo "clickhouse max memory: $(clickhouse-client --user pipeliner --password pipeliner -q "SELECT formatReadableSize(toUInt64(value)) FROM system.server_settings WHERE name='max_server_memory_usage'")"
echo "mysql performance_schema: $(mysql -upipeliner -ppipeliner -N -e 'select @@performance_schema' 2>/dev/null)"
ps -eo rss,comm | awk '/clickhouse|mysqld|redis/ {printf "%6.0f MB  %s\n", $1/1024, $2}'
