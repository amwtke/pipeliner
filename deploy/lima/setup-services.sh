#!/usr/bin/env bash
# Install Redis, MySQL and ClickHouse natively inside a Lima Ubuntu VM for local testing.
# Usage (on the host):  limactl shell <instance> -- sudo bash deploy/lima/setup-services.sh
# Lima forwards the guest ports to the host's 127.0.0.1:
#   Redis 6380, MySQL 3306, ClickHouse HTTP 8123 / native 9000.
# Redis uses 6380 because Lima's forwarder can get stuck on 6379 after a restart.
set -euo pipefail
export DEBIAN_FRONTEND=noninteractive

PG_USER=pipeliner
PG_PASS=pipeliner
PG_DB=pipeliner

apt-get update -qq
apt-get install -y -qq apt-transport-https ca-certificates curl gnupg >/dev/null

# ---------- Redis ----------
apt-get install -y -qq redis-server >/dev/null
sed -i -E 's/^bind .*/bind 0.0.0.0/' /etc/redis/redis.conf
sed -i -E 's/^port .*/port 6380/' /etc/redis/redis.conf
sed -i -E 's/^protected-mode .*/protected-mode no/' /etc/redis/redis.conf
# AOF so that streams survive a restart (resume tests)
sed -i -E 's/^appendonly .*/appendonly yes/' /etc/redis/redis.conf
systemctl enable --now redis-server
systemctl restart redis-server

# ---------- MySQL ----------
apt-get install -y -qq mysql-server >/dev/null
cat >/etc/mysql/mysql.conf.d/zz-pipeliner.cnf <<'EOF'
[mysqld]
bind-address = 0.0.0.0
mysqlx-bind-address = 127.0.0.1
max_connections = 500
EOF
systemctl enable --now mysql
systemctl restart mysql
mysql -uroot <<EOF
CREATE DATABASE IF NOT EXISTS ${PG_DB} DEFAULT CHARACTER SET utf8mb4;
CREATE USER IF NOT EXISTS '${PG_USER}'@'%' IDENTIFIED BY '${PG_PASS}';
GRANT ALL PRIVILEGES ON *.* TO '${PG_USER}'@'%';
FLUSH PRIVILEGES;
EOF

# ---------- ClickHouse ----------
if ! command -v clickhouse-server >/dev/null 2>&1; then
  curl -fsSL 'https://packages.clickhouse.com/rpm/lts/repodata/repomd.xml.key' \
    | gpg --dearmor --yes -o /usr/share/keyrings/clickhouse-keyring.gpg
  ARCH=$(dpkg --print-architecture)
  echo "deb [signed-by=/usr/share/keyrings/clickhouse-keyring.gpg arch=${ARCH}] https://packages.clickhouse.com/deb stable main" \
    >/etc/apt/sources.list.d/clickhouse.list
  apt-get update -qq
  apt-get install -y -qq clickhouse-server clickhouse-client >/dev/null
fi
mkdir -p /etc/clickhouse-server/config.d /etc/clickhouse-server/users.d
cat >/etc/clickhouse-server/config.d/zz-listen.xml <<'EOF'
<clickhouse>
  <listen_host>0.0.0.0</listen_host>
</clickhouse>
EOF
cat >/etc/clickhouse-server/users.d/zz-pipeliner.xml <<EOF
<clickhouse>
  <users>
    <${PG_USER}>
      <password>${PG_PASS}</password>
      <networks><ip>::/0</ip></networks>
      <profile>default</profile>
      <quota>default</quota>
      <access_management>1</access_management>
    </${PG_USER}>
  </users>
</clickhouse>
EOF
chown -R clickhouse:clickhouse /etc/clickhouse-server/config.d /etc/clickhouse-server/users.d || true
systemctl enable clickhouse-server
systemctl restart clickhouse-server
for i in $(seq 1 30); do
  clickhouse-client --user "${PG_USER}" --password "${PG_PASS}" -q "SELECT 1" >/dev/null 2>&1 && break
  sleep 1
done
clickhouse-client --user "${PG_USER}" --password "${PG_PASS}" -q "CREATE DATABASE IF NOT EXISTS ${PG_DB}"

echo "redis:      $(redis-cli -p 6380 ping)"
echo "mysql:      $(mysql -u${PG_USER} -p${PG_PASS} -N -e 'select version()' 2>/dev/null)"
echo "clickhouse: $(clickhouse-client --user ${PG_USER} --password ${PG_PASS} -q 'select version()')"
