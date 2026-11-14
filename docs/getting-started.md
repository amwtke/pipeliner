# 快速开始

本页带你把 pipeliner 跑起来：先在本机启动 3 个 pod 的示例应用，提交一轮计算并观察它，再用脚本验证「pod 被 `kill -9` 后结果不变」，最后把 pipeliner 接进你自己的 Spring Boot 应用。读者是第一次接触 pipeliner 的开发者。概念（pipeline、run、gate、round、chunk）在[使用指南](./user-guide.md#核心概念)里解释，这里只在需要时点到。

- [准备环境](#准备环境)
- [构建](#构建)
- [启动 3 个 pod](#启动-3-个-pod)
- [造数据、提交一轮、观察](#造数据提交一轮观察)
- [用脚本验证容错](#用脚本验证容错)
- [在你自己的 Spring Boot 应用里使用](#在你自己的-spring-boot-应用里使用)
- [下一步](#下一步)

## 准备环境

| 需要 | 说明 |
|---|---|
| JDK 21 | 构建用。产物是 Java 8 字节码（`maven.compiler.release=8`），运行时 Java 8 及以上即可。 |
| Maven | 项目没有 Maven Wrapper，用你本机的 `mvn`。 |
| Redis | 数据面（Redis Streams）必需，只支持**单机** Redis（不支持 Redis Cluster）；要求 6.0 及以上（不依赖 6.2 才有的 `XAUTOCLAIM`、`XTRIM MINID` 和 `XRANGE` 开区间）。示例默认连 `127.0.0.1:6380`，注意是 **6380** 而不是 6379。 |
| MySQL | 示例的业务表在 MySQL；控制面选 `mysql` 时也用它。默认连 `127.0.0.1:3306`，用户 / 密码 `pipeliner` / `pipeliner`。 |
| ClickHouse | 示例的结果表在 ClickHouse。默认连 HTTP `127.0.0.1:8123`，用户 / 密码 `pipeliner` / `pipeliner`。 |

示例用到的 MySQL 和 ClickHouse 库表由 `POST /demo/seed` 自动创建，不需要提前建表。

仓库里提供了一个在 Lima 虚拟机里安装这三个服务的脚本，装好后端口转发到本机（Redis 6380、MySQL 3306、ClickHouse HTTP 8123）：

```bash
limactl shell <instance> -- sudo bash deploy/lima/setup-services.sh
```

> [!NOTE]
> Lima 不是必需的，任何地方的 Redis、MySQL、ClickHouse 都可以，改 `pipeliner-example/src/main/resources/application.yml` 里的地址，或者启动时用 `--pipeliner.redis.uri=...`、`--pipeliner.datasources.mysql.url=...` 覆盖（完整配置见[配置参考](./configuration.md#示例应用的配置文件)）。部分压测脚本例外，见[用脚本验证容错](#用脚本验证容错)。

ClickHouse 的 JDBC 连接串必须带 `compress=0`，例如 `jdbc:clickhouse:http://127.0.0.1:8123/default?compress=0`。

## 构建

在仓库根目录：

```bash
export JAVA_HOME=/opt/homebrew/opt/openjdk@21   # 按你本机的 JDK 21 路径修改
mvn -DskipTests install
```

`install` 会把所有模块装进本地 Maven 仓库（后面接入自己的应用时需要），同时在 `pipeliner-example/target/` 下生成可执行 jar（`pipeliner-example-*.jar`），`run-pod.sh` 启动 pod 用的就是它。

> [!TIP]
> 去掉 `-DskipTests` 会跑全部单元测试，测试需要上面的服务可用。

## 启动 3 个 pod

```bash
cd pipeliner-example
scripts/run-pod.sh pod1 8081
scripts/run-pod.sh pod2 8082
scripts/run-pod.sh pod3 8083
```

`run-pod.sh <name> <port> [额外的 Spring 参数...]` 在后台启动 fat jar，日志和 pid 在 `pipeliner-example/logs/<name>.log` 和 `<name>.pid`。停止所有 pod：

```bash
scripts/stop-pods.sh
```

默认用 Redis 做控制面。用 MySQL 做控制面（需要 MySQL 里有 `pipeliner` 库的访问权限，参见 `application-mysql-store.yml`）：

```bash
scripts/run-pod.sh pod1 8081 --spring.profiles.active=mysql-store
```

三个 pod 共用同一个 Redis，其中一个通过抢租约成为 leader（master），负责推进 round；其余 pod 是候补，leader 失效后接管。每个 pod 都会执行 chunk（worker）。

`run-pod.sh` 认的环境变量：

| 变量 | 含义 |
|---|---|
| `PIPELINER_JAR` | 指定要启动的 jar，默认取 `target/pipeliner-example-*.jar` |
| `PIPELINER_LOGS` | 日志和 pid 的目录，默认 `pipeliner-example/logs` |
| `JAVA_HOME` / `JAVA_OPTS` | 使用的 JDK 和 JVM 参数 |
| `MASTER_TICK=external` | pod 以 `external` 模式启动（不起 master 线程，需要外部触发 tick，见[运维手册](./operations.md#外部触发-master-推进)），配套 `TICK_INTERVAL_MS`（默认 1000）和 `LEADER_TTL_MS`（默认 3000） |

## 造数据、提交一轮、观察

下面的命令都对 pod1（8081）执行，对任意一个 pod 执行结果都一样。管理接口的变更类调用（POST）需要 ADMIN token，示例应用在没有设置环境变量时使用开发用 token：

```bash
export PG_ADMIN_TOKEN=dev-admin-token-change-me
```

### 1. 造数据

```bash
curl -X POST 'localhost:8081/demo/seed?materials=600&seed=42'
```

> [!WARNING]
> `/demo/seed` 会 **drop 并重建**示例用的全部 MySQL 表和 ClickHouse 表（ClickHouse 库默认 `pipeliner_demo`），然后重新生成主数据。重复调用会清掉之前所有轮次的结果。`/demo/*` 接口只用于演示，没有认证。

### 2. 提交一轮

```bash
curl -X POST 'localhost:8081/demo/rounds?planVersion=V1'     # 返回 {"roundId":"..."}
```

`planVersion` 同时被用作 `bizKey`：对同一个图重复提交同一个 `bizKey` 不会产生新的 round，而是返回已有的 `roundId`。

### 3. 观察

```bash
RID=<上一步返回的 roundId>
curl localhost:8081/pg/graphs/net-demand/plan                # 执行计划（文本）：分成哪些 run、哪些闸门、各自的执行模式
curl localhost:8081/pg/rounds/$RID                           # 状态：round / runs / gates / pipelines / chunk 计数
curl localhost:8081/pg/rounds/$RID/backpressure              # 通道长度、水位、瓶颈 pipeline
curl localhost:8081/pg/cluster                               # 当前 pod 是否 leader、本 pod 上已实例化的 pipeline
curl localhost:8081/pg/cluster/workers                       # 所有存活 pod、谁是 leader、各自持有的 pipeline 实例
```

也可以直接打开内置界面，它展示图的拓扑和这一轮的实时状态：

```text
http://localhost:8081/pg-ui/?round=<roundId>
```

如果你想看每一个 JSON 字段和管理接口的全部用法，见[运维手册](./operations.md#管理接口-rest-api)。

### 4. 打开人工闸门

示例图里的 `approve` 是人工闸门（`manual`）：上游 run 跑完后，闸门被「布防」（`armedAt` 有值，状态仍是 `CLOSED`），需要人工放行，下游的 `publish` 才会开始：

```bash
curl -X POST -H "Authorization: Bearer $PG_ADMIN_TOKEN" \
     "localhost:8081/pg/gates/$RID/approve/open"
```

> [!NOTE]
> 启用鉴权时（示例应用默认启用），`openGate` 的操作人取自 token 的名字（示例里是 `ops`），请求参数 `operator` 被忽略；只有鉴权关闭时才使用 `operator` 参数。

### 5. 校验结果

```bash
curl 'localhost:8081/demo/checksum?planVersion=V1'
```

返回按物料种类（`BIG` / `FINE`）统计的行数、`sumQty` 和 `hash`，格式为 `{"kinds":{"BIG":{"rows":...,"sumQty":"...","hash":"..."},"FINE":{...}}}`。对不同的 `planVersion` 各跑一轮，比较两次的 checksum，就能判断重跑的结果是否一致，这正是下面脚本做的事。

## 用脚本验证容错

脚本都在 `pipeliner-example/scripts/`，需要先装好并启动上面的三个服务。脚本会自己构建（`mvn -q -DskipTests -pl ... -am package`），设置 `SKIP_BUILD=1` 可以跳过。`e2e.sh` 和 `restart-resume.sh` 先用离线模式（`-o`），失败再联网重试；`chaos.sh`、`chaos-admin.sh`、`loadtest.sh` 只用离线模式，依赖需要已经在本地 Maven 仓库里（先成功构建一次即可）。脚本需要 `python3`。

> [!WARNING]
> 这些脚本会反复 `kill -9` 进程，并会 drop 并重建示例用的库表（`chaos.sh` 使用独立的 Redis DB 和库名，见下）。不要对有真实数据的 Redis / MySQL / ClickHouse 运行。

通用环境变量：

| 变量 | 默认 | 含义 |
|---|---|---|
| `MATERIALS` | `600`（`loadtest.sh` 为 `3000`） | 造数据的物料数 |
| `STORE_PROFILE` | 空 | 空 = Redis 控制面；`mysql-store` = MySQL 控制面 |
| `SKIP_BUILD` | 空 | 设为 `1` 跳过构建 |
| `TIMEOUT_SEC` | `600`（`chaos.sh` 为 `900`，`chaos-admin.sh` 为 `420`，`loadtest.sh` 为 `1800`） | 每一轮（chaos 是每次迭代）的超时 |
| `PG_ADMIN_TOKEN` | `dev-admin-token-change-me` | 脚本调用 `/pg` 时使用的 token，也会传给 pod |

| 脚本 | 验证了什么 |
|---|---|
| `e2e.sh` | 3 个 pod。先跑完 V1 作为基线；再跑 V2，并在 V2 的 `R2` 运行中 `kill -9` 当前 leader 和一个 worker，3 秒后重启它们；最后比较 V1 和 V2 的 checksum 必须一致。证明「leader 和 worker 同时被杀，结果不变」。 |
| `restart-resume.sh` | 2 个 pod。先跑完 V1；再跑 V2，在 `R2` 运行中 `kill -9` **全部** pod，等 5 秒后全部重启；V2 从控制面续跑完成，比较 checksum。证明「整个集群重启后可以断点续跑」。失败或超时返回非 0。 |
| `chaos.sh` | 多个 pod（默认 4 个）每轮迭代同时跑两个 round，期间随机 `kill -9` 任意 pod / leader，用 `SIGSTOP` 冻结 leader 或 worker（制造僵尸进程），并暂停 round。每个 round 与单线程 `INLINE` 基线逐项对比：checksum；MySQL 与 ClickHouse 结果表的行数、业务键无重复、内容 hash 一致；控制面无 `DEAD` chunk、源 chunk 全部 `DONE`；Redis 无残留 key；没有遗留的 pipeline 实例。任何一项不符退出码非 0。 |
| `chaos-admin.sh` | 管理操作与僵尸进程的组合场景（`SCENARIOS=A,B,C,D`）：A 在冻结旧 attempt 的 worker 时用变化的输入 `rerun`；B 注入故障 → 死信 → 关闭故障后 `retry`；C 取消 round，用同一个 `planVersion` 重新提交，并唤醒落后的进程；D `rerunFrom` 之后立刻冻结 leader。 |
| `loadtest.sh` | 单个场景的压测：起 pod、造数据、提交一轮、`approve` 布防后自动打开它、等待结束，每 2 秒采样 Redis 内存和通道长度，最后输出 `report.json`（含 Prometheus 指标、日志统计、checksum 和重复检查）。 |
| `run-pod.sh` / `stop-pods.sh` | 启动 / 停止 pod（同时停止 `tick-loop.sh`） |
| `tick-loop.sh` | 模拟 xxl-job：每秒向随机存活 pod 发一次 `POST /pg/master/tick`，失败时换下一个 pod；由 `e2e.sh` 在 `MASTER_TICK=external` 时启动 |

```bash
export JAVA_HOME=/opt/homebrew/opt/openjdk@21
pipeliner-example/scripts/e2e.sh
STORE_PROFILE=mysql-store pipeliner-example/scripts/e2e.sh        # MySQL 控制面
MASTER_TICK=external pipeliner-example/scripts/e2e.sh             # 外部触发 master 推进
MATERIALS=600 pipeliner-example/scripts/restart-resume.sh
```

`chaos.sh`、`chaos-admin.sh` 和 `loadtest.sh` 额外的参数：

| 脚本 | 参数 |
|---|---|
| `chaos.sh` | `PODS=4`、`ROUNDS=6`（迭代次数，每次 2 个 round）、`SEED`（随机种子）、`CHAOS_MAX_SEC=150`（超过后不再注入故障，等 round 收敛）、`BASE_PORT=9101`；隔离用的库：`REDIS_DB=5`、`MYSQL_DB=pipeliner_chaos`、`CP_DB=pipeliner_chaos_cp`、`CH_DB=pipeliner_chaos`；`OUT` 为日志和证据目录（默认 `pipeliner-example/chaos-out/...`） |
| `chaos-admin.sh` | `SCENARIOS=A,B,C,D`、`ITER=1`、`BG_CHAOS=1`（`0` 关闭后台的随机 kill / 冻结）、`FAULT_MOD=7`、`A_DELTA`、`A_BIG`、`A_FINE`，以及 `chaos.sh` 的全部参数 |
| `loadtest.sh` | `PODS=3`、`CAP_MODE=LOCAL`（`cap-alloc` 的执行模式，可选 `CLUSTER`）、`CAP_PARALLELISM=3`、`SEED=42`、`POD_XMX=4g`、`BASE_PORT=8081`、`OUT` |

> [!NOTE]
> `MASTER_TICK=external` 只被 `e2e.sh` 支持，`restart-resume.sh`、`chaos.sh`、`chaos-admin.sh` 遇到它会直接退出：没有人触发 tick，round 会一直停在 `CREATED`。
> `loadtest.sh` 通过 `limactl shell cg -- mysql` 查询 MySQL，并采样名为 `cg` 的 Lima 实例的负载，所以按原样运行需要这个实例；`chaos.sh` 只用它读取虚拟机负载作为参考信息。

## 在你自己的 Spring Boot 应用里使用

### 1. 安装并依赖 starter

pipeliner 还没有发布到 Maven Central，必须先在本地构建并安装（即上面的 `mvn -DskipTests install`），你的应用才能解析到它。坐标取自各模块的 `pom.xml`：

```xml
<dependency>
  <groupId>io.github.amwtke</groupId>
  <artifactId>pipeliner-spring-boot-starter</artifactId>
  <version>0.1.0-SNAPSHOT</version>
</dependency>
```

要求 Spring Boot 2.7.x（项目基于 2.7.18），Java 8 及以上。starter 已经传递依赖了 api、core、runtime、Redis / MySQL 两种控制面实现、Redis 数据面、JDBC 连接器、`spring-boot-starter-web`、`micrometer-core` 和 HikariCP。**不包含**的有：

- MySQL / ClickHouse 的 JDBC 驱动，需要自己依赖：

  ```xml
  <dependency><groupId>com.mysql</groupId><artifactId>mysql-connector-j</artifactId></dependency>
  <dependency>
    <groupId>com.clickhouse</groupId><artifactId>clickhouse-jdbc</artifactId>
    <version>0.7.2</version><classifier>all</classifier>
  </dependency>
  ```

- 暴露指标需要的 `spring-boot-starter-actuator` 和 `micrometer-registry-prometheus`，见[运维手册](./operations.md#指标与告警)。

> [!NOTE]
> 项目父 pom 里 ClickHouse 驱动的版本是 `0.7.2`（classifier `all`），`mysql-connector-j` 的版本由 Spring Boot 的依赖管理决定。

### 2. 最小配置

`application.yml`：

```yaml
pipeliner:
  redis:
    uri: redis://127.0.0.1:6379/0          # 数据面 Redis（单机）；框架默认是 127.0.0.1:6380
  datasources:                              # 名字就是图里引用的数据源名
    src:
      url: jdbc:mysql://127.0.0.1:3306/shop?useSSL=false&serverTimezone=UTC
      username: shop
      password: ${SHOP_DB_PASSWORD}
      driver-class-name: com.mysql.cj.jdbc.Driver
    dst:
      url: jdbc:mysql://127.0.0.1:3306/report?useSSL=false&serverTimezone=UTC
      username: report
      password: ${REPORT_DB_PASSWORD}
      driver-class-name: com.mysql.cj.jdbc.Driver
  admin:
    security:
      tokens:
        - {name: ops, token: "${PG_ADMIN_TOKEN}", role: ADMIN}
```

控制面默认存 Redis（`pipeliner.store.type: redis`），不用另外配置。要换成 MySQL 控制面，见[配置参考](./configuration.md#基础配置)。

> [!WARNING]
> 不配置任何 token 时，管理接口的所有变更操作返回 403（只读仍可用）。生产环境务必配置 token，并通过环境变量注入，不要写在仓库里。

### 3. 一个最小的图

假设要把 `shop.orders` 的金额翻倍后写到 `report.orders_out`。目标表需要有 `pg_version` 列，用于版本化写入（原理见[使用指南](./user-guide.md#sink-与版本化写入)）：

```sql
CREATE TABLE orders_out (
  id         BIGINT PRIMARY KEY,
  amount     DECIMAL(18,2) NOT NULL,
  pg_version BIGINT NOT NULL DEFAULT 0
);
```

runner 是一个普通类，pipeliner 在需要它的 pod 上通过 Spring 创建（所以可以直接注入 Bean 和 `@Value`，不必注册成 Bean）：

```java
import io.github.amwtke.pipeliner.api.Collector;
import io.github.amwtke.pipeliner.api.Row;
import io.github.amwtke.pipeliner.api.RowRunner;
import io.github.amwtke.pipeliner.api.RunContext;
import java.math.BigDecimal;

public class DoubleAmountRunner implements RowRunner {
    @Override
    public void process(Row row, Collector out, RunContext ctx) {
        out.emit(Row.of("id", row.getLong("id"),
                        "amount", row.getDecimal("amount").multiply(new BigDecimal("2"))));
    }
}
```

图声明成一个 `PipelineGraph` Bean，自动配置会把容器里所有的 `PipelineGraph` 注册进引擎：

```java
import io.github.amwtke.pipeliner.connectors.jdbc.JdbcSinks;
import io.github.amwtke.pipeliner.connectors.jdbc.JdbcSources;
import io.github.amwtke.pipeliner.core.graph.PipelineGraph;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class DoubleAmountGraphConfig {
    @Bean
    public PipelineGraph doubleAmountGraph() {
        return PipelineGraph.builder("double-amount")
            .pipeline("copy", p -> p
                .source(JdbcSources.mysql("src").table("orders").columns("id", "amount")
                        .hashSplit("id", 4).orderBy("id").build())
                .row(DoubleAmountRunner.class)
                .sink(JdbcSinks.mysql("dst").table("orders_out")
                        .upsertKeys("id").versionColumn("pg_version").build()))
            .build();
    }
}
```

`JdbcSources.mysql("src")` 里的 `"src"` 就是 `pipeliner.datasources.src`。

> [!IMPORTANT]
> 集群里**所有 pod 必须部署同一份图定义**。每个 round 在创建时记录图的 plan hash，只有 plan hash 一致的 pod 才会推进它、执行它的 LOCAL / INLINE run。

### 4. 通过 PipelinerEngine 提交一轮

引擎在应用启动完成（`ApplicationReadyEvent`）之后自动启动，在应用关闭时停止。注入 `PipelinerEngine` 就可以提交、查询和操作 round：

```java
import io.github.amwtke.pipeliner.core.engine.PipelinerEngine;
import io.github.amwtke.pipeliner.core.store.RoundStatus;
import java.util.Collections;
import org.springframework.stereotype.Service;

@Service
public class DoubleAmountService {
    private final PipelinerEngine engine;

    public DoubleAmountService(PipelinerEngine engine) { this.engine = engine; }

    /** 同一个 bizKey 重复提交，返回已有的 roundId，不会产生新的 round。 */
    public String submit(String day) {
        return engine.submit("double-amount", "day-" + day, Collections.singletonMap("day", day));
    }

    public RoundStatus waitFor(String roundId) throws InterruptedException {
        return engine.awaitRound(roundId, 10 * 60 * 1000L);   // 超时返回当前状态
    }
}
```

`submit(graphName, bizKey, params)` 的 `params` 会随 round 保存，runner 里用 `ctx.params()` / `ctx.param("day")` 读取，JDBC 源的 `.where("... = :day")` 里的 `:day` 也取自这里。提交之后可以用管理接口或界面观察：

```bash
curl localhost:8080/pg/graphs                      # ["double-amount"]
curl localhost:8080/pg/rounds/<roundId>
```

提交也可以走 REST，不写代码：

```bash
curl -H "Authorization: Bearer $PG_ADMIN_TOKEN" -H 'content-type: application/json' \
     -d '{"graph":"double-amount","bizKey":"day-2026-01-01","params":{"day":"2026-01-01"}}' \
     localhost:8080/pg/rounds
```

## 下一步

- 理解图是怎么描述和规划的：[使用指南](./user-guide.md)
- 配置项逐个说明：[配置参考](./configuration.md)
- 接入监控、处理故障：[运维手册](./operations.md)
