# pipeliner

面向有界批量计算的分布式流水线框架：把一次计算描述成 pipeline 图，由 Redis Streams 做数据通道和背压，MySQL 或 Redis 做控制面，多 pod 协同执行，任意 pod（包括 leader）被 kill -9 后可从控制面恢复并得到相同结果。

设计文档：[docs/pipelinegraph-design.md](docs/pipelinegraph-design.md)（设计稿，部分细节以代码为准）。

## 模块一览

| 模块 | 职责 |
|---|---|
| `pipeliner-api` | 用户实现的接口：`SplitSource`、`RowRunner`、`BatchRunner`、`Sink`、`GateCondition` 等 |
| `pipeliner-core` | 图模型（`PipelineGraph` builder、`Gates`）、`GraphYaml`、`Planner`/`ExecutionPlan`、`PipelinerEngine`、`RuntimeConfig`、控制面存储接口 |
| `pipeliner-store-mysql` | 控制面存储的 MySQL 实现 |
| `pipeliner-store-redis` | 控制面存储的 Redis 实现 |
| `pipeliner-dataplane-redis` | 数据面：Redis Streams 通道、Lua 脚本 |
| `pipeliner-connectors` | JDBC 源与 sink（`JdbcSources`、`JdbcSinks`，MySQL / ClickHouse） |
| `pipeliner-runtime` | 引擎实现：master（调度、闸门、状态归并）、worker、本地执行 |
| `pipeliner-spring-boot-starter` | 自动配置（`PipelinerProperties`，前缀 `pipeliner`）和管理 REST（`PipelinerAdminController`，`/pg`） |
| `pipeliner-example` | 净需求示例：`NetDemandGraphConfig`、`DemoController`、e2e 脚本 |

## 核心概念

- **pipeline** = puller / runner / pusher 三段：puller 对应 `SplitSource`（读库）或上游 stream；runner 是 `RowRunner`（逐行）或 `BatchRunner`（逐批）；pusher 对应 `Sink`（落表）或输出 stream 端口。
- **chunk** 是唯一的工作单位：传输、确认、重试、断点都以 chunk 为粒度。runner 不保留跨 chunk 状态。
- **闸门（gate）**：barrier、condition、external、manual、timer。闸门和落表把图切开。
- **层级**：round（一次提交）> run（图被闸门/落表切出的、可一口气跑完的一块）> pipeline > chunk。
- **三种执行模式**（`ExecMode`）：`INLINE`（一条直链、小数据，直接方法调用、无队列）、`LOCAL`（单 pod 内存队列）、`CLUSTER`（多 pod 经 Redis Stream 协同）。未指定 `.mode()` 时由 `PlanOptions`（`inlineMaxRows`、`localMaxRows`）按 `estimatedRows` 选择。

## 快速开始

### 环境

- 构建需要 JDK 21（`export JAVA_HOME=/opt/homebrew/opt/openjdk@21`），产物是 Java 8 字节码（`maven.compiler.release=8`）。
- 依赖服务（Redis、MySQL、ClickHouse）用 Lima 虚拟机安装：`limactl shell <instance> -- sudo bash deploy/lima/setup-services.sh`。转发到本机后：Redis **6380**（不是 6379）、MySQL 3306、ClickHouse HTTP 8123。
- ClickHouse JDBC 连接串必须带 `compress=0`（见 `application.yml` 中 `jdbc:clickhouse:http://127.0.0.1:8123/default?compress=0`）。

### 构建

```bash
mvn install
```

### 启动 3 个 pod

```bash
cd pipeliner-example
scripts/run-pod.sh pod1 8081
scripts/run-pod.sh pod2 8082
scripts/run-pod.sh pod3 8083
# 日志与 pid：pipeliner-example/logs/<name>.log|.pid；停止：scripts/stop-pods.sh
# 用 Redis 做控制面：run-pod.sh pod1 8081 --spring.profiles.active=redis-store
```

### 造数据、提交、观察

```bash
curl -X POST 'localhost:8081/demo/seed?materials=600&seed=42'
curl -X POST 'localhost:8081/demo/rounds?planVersion=V1'     # 返回 {"roundId":"..."}
curl localhost:8081/pg/graphs/net-demand/plan                 # 执行计划（文本）
curl localhost:8081/pg/rounds/<roundId>                       # 状态：round / runs / gates
curl localhost:8081/pg/rounds/<roundId>/backpressure          # 通道长度、水位、瓶颈 pipeline
curl localhost:8081/pg/cluster                                # 当前 pod 与是否 leader
```

示例图里的 `approve` 是人工闸门，上游 run 跑完后需手动打开：

```bash
curl -X POST 'localhost:8081/pg/gates/<roundId>/approve/open?operator=me'
```

结果校验：

```bash
curl 'localhost:8081/demo/checksum?planVersion=V1'
```

返回按 kind（BIG / FINE）的行数、`sumQty` 和 `hash`。比较不同 `planVersion` 的 checksum 即可判断重跑结果是否一致。

### 脚本

均在 `pipeliner-example/scripts/`，通用环境变量：`MATERIALS`（默认 600）、`STORE_PROFILE`（如 `redis-store`）、`SKIP_BUILD=1`、`TIMEOUT_SEC`（每轮超时，默认 600）。

| 脚本 | 内容 |
|---|---|
| `e2e.sh` | 3 个 pod；跑 V1；跑 V2 时 kill -9 一个 worker 和 leader 并重启；比较 checksum |
| `restart-resume.sh` | 2 个 pod；跑 V1；V2 的 R2 运行中 kill -9 全部 pod，等 5 秒后重启；V2 续跑完成后比较 checksum；失败或超时返回非 0 |
| `run-pod.sh` / `stop-pods.sh` | 启动 / 停止 pod |

```bash
export JAVA_HOME=/opt/homebrew/opt/openjdk@21
pipeliner-example/scripts/e2e.sh
STORE_PROFILE=redis-store MATERIALS=600 pipeliner-example/scripts/restart-resume.sh
```

## 定义一个图

Java：

```java
PipelineGraph g = PipelineGraph.builder("demo")
    .pipeline("load", p -> p
        .source(JdbcSources.mysql("mysql").table("src").columns("id", "k")
                .hashSplit("id", 8).orderBy("id").build())
        .batch(new MyBatchRunner()).ports("out")
        .sink(JdbcSinks.mysql("mysql").table("mid").upsertKeys("id")
                .dataset("mid").build())
        .mode(ExecMode.CLUSTER).chunkKey("id").chunkRows(800))
    .pipeline("post", p -> p
        .row(new MyRowRunner())
        .sink(JdbcSinks.clickhouse("ch").table("db.res").dataset("res").build()))
    .edge("load", "post")                        // stream 边：load 的输出端口直接喂给 post
    .gate("approve", Gates.manual(),             // 闸门：after 全部成功后才放行 before
          g -> g.after("post").before("publish"))
    .pipeline("publish", p -> p
        .source(JdbcSources.clickhouse("ch").table("db.res").dataset("res").useFinal()
                .noSplit().build())                  // table 与 query 二选一；useFinal 只用于 table 模式
        .batch(new PublishRunner())
        .sink(JdbcSinks.mysql("mysql").table("publish_log").upsertKeys("id").build()))
    .build();
```

等价的 YAML（`GraphYaml.load(InputStream, ComponentResolver)`，组件用 bean 名引用）：

```yaml
graph: demo
pipelines:
  - id: load
    source: srcBean
    runner: { type: batch, bean: myBatchRunner }
    ports: [out]
    sinks: { out: [midSink] }
    mode: cluster
    chunkKey: [id]
    chunkRows: 800
  - id: post
    runner: { type: row, bean: myRowRunner }
    sinks: { out: [resSink] }
  - id: publish
    source: resSource
    runner: { type: batch, bean: publishRunner }
    sinks: { out: [logSink] }
edges:
  - { from: load, to: post }          # 具名端口写 load.big
gates:
  - id: approve
    type: manual                      # barrier | manual | condition | external | timer
    after: [post]
    before: [publish]
    timeout: 2h                       # 可选
    onTimeout: fail
```

其他闸门类型的参数：`condition` 需 `condition: <bean>`；`external` 需 `expectFrom`（对应 `Gates.externalCountOf`）、`expectCount` 或 `condition`；`timer` 需 `at`（epoch 毫秒）或 `delay`（如 `30s`）。

**物化边与自动 barrier**：planner 比较各 pipeline 的 source 与 sink 的 `datasetName()`。某个 pipeline 读的 dataset 恰好被另一个 pipeline 写入时，二者之间是物化边，planner 自动生成 `barrier:<dataset>` 闸门并据此切 run；stream 边（`.edge`）则在同一个 run 内用通道连接。上例中 `publish` 读 `res`、`post` 写 `res`，所以两者之间有自动 barrier，不必手写。

**何时要显式 barrier**：依赖不通过 dataset 名体现时（例如 runner 里按键 lookup 另一张表、`ReadsDatasets` 之外的隐式读），planner 看不到依赖，需要用 `Gates.barrier()` 的 `after(...)/before(...)` 手动声明。示例中的 `refurb-ready` 就是这种用法：`big-final`、`fine-net` 在运行时 lookup `cap-alloc` 的结果。

## 配置项

前缀 `pipeliner`，由 `PipelinerProperties` 绑定；`runtime.*` 绑定 `RuntimeConfig`，`plan.*` 绑定 `PlanOptions`。下表为框架默认值，`pipeliner-example/application.yml` 对 `runtime.*` 中的若干项做了缩短（tick 500ms、leader-ttl 6000ms 等）。

| 属性 | 默认 | 说明 |
|---|---|---|
| `pipeliner.pod-id` | 空（hostname + 随机后缀） | pod 标识 |
| `pipeliner.master-enabled` / `worker-enabled` | `true` / `true` | 是否参与选主 / 执行 chunk |
| `pipeliner.redis.uri` | `redis://127.0.0.1:6380/0` | 数据面 Redis |
| `pipeliner.store.type` | `mysql` | 控制面：`mysql` 或 `redis` |
| `pipeliner.store.datasource` | `pgstore` | mysql 控制面所用数据源名 |
| `pipeliner.datasources.<name>.url/username/password/driver-class-name` | - | Hikari 数据源，供 `JdbcSources`/`JdbcSinks` 按名引用 |
| `pipeliner.datasources.<name>.maximum-pool-size` | `10` | 连接池大小 |
| `pipeliner.admin.enabled` | `true` | 是否暴露 `/pg` 管理接口 |
| `pipeliner.runtime.tick-millis` | `1000` | master 调度周期 |
| `pipeliner.runtime.leader-ttl-millis` | `15000` | leader 租约 |
| `pipeliner.runtime.heartbeat-millis` | `5000` | 心跳间隔 |
| `pipeliner.runtime.worker-dead-millis` | `20000` | 判定 worker 死亡 |
| `pipeliner.runtime.claim-idle-millis` | `60000` | chunk 空闲多久后被 autoclaim |
| `pipeliner.runtime.renew-interval-millis` | `20000` | chunk 续租间隔 |
| `pipeliner.runtime.auto-claim-interval-millis` | `10000` | autoclaim 扫描间隔 |
| `pipeliner.runtime.high-watermark` / `low-watermark` | `64` / `32` | 通道水位，达到高水位上游提交返回 BUSY |
| `pipeliner.runtime.max-attempts` | `3` | chunk 最大重试次数 |
| `pipeliner.runtime.max-deliveries` | `5` | autoclaim 投递超过此值直接进死信 |
| `pipeliner.runtime.autoscale` | `true` | 按积压自动扩并行度 |
| `pipeliner.runtime.max-parallelism` | `8` | 自动扩容上限 |
| `pipeliner.runtime.autoscale-cooldown-millis` | `15000` | 扩缩容冷却 |
| `pipeliner.runtime.local-queue-capacity` | `64` | LOCAL 模式队列容量 |
| `pipeliner.runtime.admin-timeout-millis` | `30000` | 管理命令等待 leader 执行的时间 |
| `pipeliner.runtime.redis-memory-limit-bytes` | `0` | Redis 内存上限（0 = 不限） |
| `pipeliner.plan.inline-max-rows` | `50000` | 不超过则 INLINE |
| `pipeliner.plan.local-max-rows` | `2000000` | 不超过则 LOCAL，否则 CLUSTER |
| `pipeliner.plan.default-parallelism` | `2` | 默认并行度 |
| `pipeliner.plan.force-mode` | 无 | 强制所有 run 的执行模式 |

示例应用另有 `demo.core-grade-delay-ms`（默认 0，e2e 脚本设为 3，用于拉长 core-grade 便于注入故障/观察背压）和 `demo.algo-delay-ms`。

## 管理接口

基础路径 `/pg`（`PipelinerAdminController`）。

| 方法 | 路径 | 说明 |
|---|---|---|
| GET | `/pg/graphs` | 已注册的图名 |
| GET | `/pg/graphs/{name}/plan` | 执行计划（text/plain） |
| POST | `/pg/rounds` | 提交，body：`{"graph","bizKey","params":{}}`，返回 `roundId` |
| GET | `/pg/rounds/{id}` | round / run / gate 状态 |
| GET | `/pg/rounds/{id}/chunks` | chunk 列表，参数 `pipeline`、`status`、`limit`（100）、`offset`（0） |
| GET | `/pg/rounds/{id}/channels` | 各通道当前长度 |
| GET | `/pg/rounds/{id}/backpressure` | 高/低水位、通道长度、瓶颈 pipeline |
| POST | `/pg/rounds/{id}/pause` · `/resume` · `/cancel` | 暂停 / 恢复 / 取消 |
| POST | `/pg/runs/{roundId}/{runId}/retry` | 重试 run |
| POST | `/pg/runs/{roundId}/{runId}/rerun` | 从该 run 起重跑 |
| POST | `/pg/runs/{roundId}/{runId}/skip` | 跳过死信 |
| POST | `/pg/gates/{roundId}/{gateId}/signal` | 向 external 闸门发信号，body：`{"key","payload"}` |
| POST | `/pg/gates/{roundId}/{gateId}/open` | 打开闸门，参数 `operator`（默认 `admin`） |
| GET | `/pg/cluster` | 当前 pod 的 `podId`、`isLeader` |

示例应用另有 `POST /demo/seed`、`POST /demo/rounds?planVersion=`、`GET /demo/checksum?planVersion=`。

## 验证结果（v1）

- 121 个单元 / 集成测试通过。
- e2e：R2 期间 kill -9 一个 worker 和 leader，在 MySQL 与 Redis 两种控制面上均通过，checksum 一致（600 个 material 时 BIG 19200 行 / FINE 28800 行）。
- 全集群 kill -9 后重启，续跑到相同 checksum（`restart-resume.sh`）。
- 背压：通道长度被限制在高水位（默认 64；设为 6/3 时为 6）；core-grade 自动扩容 2→4；观察到上游 BUSY 等待。
- 规模：3000 material / 3 pod 总计约 67 秒（R2 42.7 秒，R3 17.1 秒）。

## v1 已知限制

- 不支持 Redis Cluster，只支持单机 Redis（Lua 脚本访问了未在 KEYS 中声明的 key）。
- cap-alloc 这类 LOCAL 模式的 run 只在单个 pod 上执行。
- 管理命令被 leader 取出后、执行前 leader 宕机，命令会丢失（客户端超时，重试即可）。
- MySQL fencing 在读取 round epoch 的子查询与新 master 的 `fenceRound` 之间有极小窗口。
- ClickHouse sink 的幂等依赖 ReplacingMergeTree 与 `FINAL` 读取。
- 行数为 0 的 source split 不会触发外部回调（`externalCountOf` 闸门据此计数）。
- 尚无 Kafka source、HTTP sink。
- 未实现按 chunk 批量拉取（`pullBatch`）。
- 未实现停滞 chunk 的 XPENDING 告警（`stalledChunkAlertMillis` 仅有配置项）。
