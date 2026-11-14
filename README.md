# pipeliner

面向有界批量计算的分布式流水线框架：把一次计算描述成 pipeline 图，由 Redis Streams 做数据通道和背压，Redis（默认）或 MySQL 做控制面，多 pod 协同执行，任意 pod（包括 leader）被 kill -9 后可从控制面恢复并得到相同结果。

设计文档：[docs/pipelinegraph-design.md](docs/pipelinegraph-design.md)（描述已实现的 v1 的设计；运维以本文为准）。

### 相对 0.1.0-SNAPSHOT 4032200 的不兼容变更

升级前请逐条核对（细节见各自章节）：

- **接口**：`ControlPlaneStore.savePendingChunks(..., long epoch)` 与 `setRoundIncomplete(roundId, epoch)`（返回 `boolean`）带 epoch 并按 round 级 fence 校验；`DataPlane.deadLetter(...)` 返回 `boolean`（真正移入死信才为 true）；`PipelinerEngine.signalGate` 返回 `boolean`（过时回调返回 false）；`SinkContext.batchId(chunk)` 取代旧的两参 / 三参 `batchId` 重载（格式 `<roundId>:<pipelineId>:<chunkId>@<attempt>`）；`GateContextImpl` 的 8 参构造器和 `ChunkEvent.runAttempt` 已删除。新增的接口方法（`maxMasterEpoch`、`reserveMasterEpoch`、`maxRoundSeq`、`pendingPurges` / `clearPendingPurges`、`DataPlane.nextRoundSeq(floor)`、`groupExists` 等）都有默认实现，旧的自定义实现仍可编译。
- **存储结构**：MySQL 新增 `pg_master_epoch`、`pg_round_seq`、`pg_pending_purge` 三张表，`pg_gate_signal.signal_key` 变为 `VARCHAR(512)`，`round_id` / `gate_id` / `pipeline_id` 加宽（`init()` 自动 `ALTER`）；Redis 控制面新增 `pg:store:maxepoch`、`maxseq`、`pendingpurge`；数据面新增 `pg:{r}:keys`（key 注册表）。示例应用新增 MySQL 表 `ex_algo_request`（`FakeAlgoService` 的持久化请求）。
- **标识符与参数校验**：图名 ≤ 64、gate id / pipeline id ≤ 128（含自动 `barrier:<dataset>`），pipeline id 和输出口名不得含 `:`；`parallelism` ≥ 1；`max-attempts` ≥ 1；`RuntimeConfig` 各项在引擎启动时校验。run 的 attempt 必须 < 1000：master 在 `rerunFrom` / `retryRun` / 本地 run 重新分配时拒绝让 attempt 达到 1000。
- **行值**：`Row.set/of` 规范化为统一类型（整数 → `Long`，浮点 → `BigDecimal`，时间 → ISO 字符串），`byte[]`、`UUID` 等不支持的类型直接抛 `IllegalArgumentException`。
- **规划**：`pipeliner.plan.force-mode` 覆盖所有 `.mode()` 提示；类型形式（`row(Class)`）且读取别的 dataset 的 runner 必须声明 `public static Collection<String> readDatasets()`（不要再实现 `ReadsDatasets`，同一个类不可能同时有同签名的实例方法和静态方法），否则 plan 失败。
- **闸门与管理操作**：`externalCountOf` 按当前 attempt 去重计数，过时回调被拒；`openGate` 默认要求所有 after-run 已 SUCCEEDED，需 `force=true` 才能越过；`retry` / `skip` 在多种状态下被拒绝（见「管理操作规则」）；管理命令带 id 和截止时间，超时即撤回；leader 对 plan hash 不同的 round 拒绝操作。
- **认证**：`AdminSecurityFilter`（servlet 过滤器）改为 `AdminSecurityInterceptor`（Spring MVC 拦截器），包名不变；不再有过滤器 Bean。
- **运行时**：EXTERNAL 模式下 leader 由本地 `pg-lease-keeper` 线程保活（见「外部触发 master 推进」）；`RuntimeConfig.stopJoinMillis`（默认 10000）限制 `stop()` 等待 worker / 本地 run 线程的时间；master 能识别数据面丢失（含只丢了消费组）。

## 模块一览

| 模块 | 职责 |
|---|---|
| `pipeliner-api` | 用户实现的接口：`SplitSource`、`RowRunner`、`BatchRunner`、`Sink`、`GateCondition`，以及 `Lifecycle`、`PipelineContext`、`ComponentFactory` |
| `pipeliner-core` | 图模型（`PipelineGraph` builder、`Gates`）、`GraphYaml`、`Planner`/`ExecutionPlan`、`PipelinerEngine`、`RuntimeConfig`、控制面存储接口 |
| `pipeliner-store-mysql` | 控制面存储的 MySQL 实现 |
| `pipeliner-store-redis` | 控制面存储的 Redis 实现 |
| `pipeliner-dataplane-redis` | 数据面：Redis Streams 通道、Lua 脚本 |
| `pipeliner-connectors` | JDBC 源与 sink（`JdbcSources`、`JdbcSinks`，MySQL / ClickHouse） |
| `pipeliner-runtime` | 引擎实现：master（调度、闸门、状态归并）、worker、本地执行、pipeline 实例的按需创建与释放 |
| `pipeliner-spring-boot-starter` | 自动配置（`PipelinerProperties`，前缀 `pipeliner`）和管理 REST（`PipelinerAdminController`，`/pg`） |
| `pipeliner-example` | 净需求示例：`NetDemandGraphConfig`、`DemoController`、e2e 脚本 |

## 核心概念

- **pipeline** = puller / runner / pusher 三段：puller 对应 `SplitSource`（读库）或上游 stream；runner 是 `RowRunner`（逐行）或 `BatchRunner`（逐批）；pusher 对应 `Sink`（落表）或输出 stream 端口。
- **chunk** 是唯一的工作单位：传输、确认、重试、断点都以 chunk 为粒度。runner 不保留跨 chunk 状态。
- **闸门（gate）**：barrier、condition、external、manual、timer。闸门和落表把图切开。
- **层级**：round（一次提交）> run（图被闸门/落表切出的、可一口气跑完的一块）> pipeline > chunk。
- **三种执行模式**（`ExecMode`）：`INLINE`（一条直链、小数据，直接方法调用、无队列）、`LOCAL`（单 pod 内存队列）、`CLUSTER`（多 pod 经 Redis Stream 协同）。未指定 `.mode()` 时由 `PlanOptions`（`inlineMaxRows`、`localMaxRows`）按 `estimatedRows` 选择。`.mode()` 提示按 pipeline 生效，未提示的成员仍按自己的估算选择，一个 run 取其成员中最重的模式；`pipeliner.plan.force-mode` 优先级最高，覆盖所有提示。

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
# 默认用 Redis 做控制面
# 用 MySQL 做控制面：run-pod.sh pod1 8081 --spring.profiles.active=mysql-store
```

### 造数据、提交、观察

```bash
curl -X POST 'localhost:8081/demo/seed?materials=600&seed=42'
curl -X POST 'localhost:8081/demo/rounds?planVersion=V1'     # 返回 {"roundId":"..."}
curl localhost:8081/pg/graphs/net-demand/plan                 # 执行计划（文本）
curl localhost:8081/pg/rounds/<roundId>                       # 状态：round / runs / gates
curl localhost:8081/pg/rounds/<roundId>/backpressure          # 通道长度、水位、瓶颈 pipeline
curl localhost:8081/pg/cluster                                # 当前 pod、是否 leader、本 pod 上已实例化的 pipeline
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

均在 `pipeliner-example/scripts/`，通用环境变量：`MATERIALS`（默认 600）、`STORE_PROFILE`（默认空 = Redis 控制面；`mysql-store` = MySQL 控制面）、`SKIP_BUILD=1`、`TIMEOUT_SEC`（每轮超时，默认 600）。

| 脚本 | 内容 |
|---|---|
| `e2e.sh` | 3 个 pod；跑 V1；跑 V2 时 kill -9 一个 worker 和 leader 并重启；比较 checksum |
| `restart-resume.sh` | 2 个 pod；跑 V1；V2 的 R2 运行中 kill -9 全部 pod，等 5 秒后重启；V2 续跑完成后比较 checksum；失败或超时返回非 0 |
| `run-pod.sh` / `stop-pods.sh` | 启动 / 停止 pod |

```bash
export JAVA_HOME=/opt/homebrew/opt/openjdk@21
pipeliner-example/scripts/e2e.sh
STORE_PROFILE=mysql-store pipeliner-example/scripts/e2e.sh      # MySQL 控制面
MATERIALS=600 pipeliner-example/scripts/restart-resume.sh
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
                .versionColumn("pg_version")             // 版本化写入，见「多 pod 正确性模型」
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

**何时要显式 barrier**：依赖不通过 dataset 名体现时（例如 runner 里按键 lookup 另一张表、`ReadsDatasets` 之外的隐式读；`ReadsDatasets` 可由 source 或 runner 实例实现，类型声明（`row(Class)`）的 runner 不会被实例化，需提供 `public static Collection<String> readDatasets()`，否则 plan 失败），planner 看不到依赖，需要用 `Gates.barrier()` 的 `after(...)/before(...)` 手动声明。示例中的 `refurb-ready` 就是这种用法：`big-final`、`fine-net` 在运行时 lookup `cap-alloc` 的结果。

## Runner 的按需实例化与生命周期

runner 有两种声明方式：

- **实例形式**：`.row(new MyRowRunner())` / `.batch(new MyBatchRunner())`。图构建时就创建，是所有 round、所有线程共享的单例，必须线程安全。向后兼容。
- **类型形式（推荐）**：`.row(MyRowRunner.class)` / `.batch(MyBatchRunner.class)`；YAML 为 `runner: {type: batch, class: com.x.MyRunner}`。runner 对象在 pod 上第一个处理该 pipeline 的线程启动时才通过 `ComponentFactory` 创建，pipeline 结束后释放。加 `.runnerPerThread()`（YAML：`perThread: true`）则每个 worker 线程各自持有一个 runner，runner 不必线程安全。`perThread` 只能和类型形式一起用；planner 校验每个 pipeline 至多一个 runner。

```java
.pipeline("grade", p -> p.row(CoreGradeRunner.class).runnerPerThread().sink(...))
```

**`Lifecycle`**（runner、source、sink 均可选实现）：`open(PipelineContext)` 在 pipeline 于本 pod 激活时、第一个 chunk 之前调用；`close()` 在本 pod 上该 pipeline 的最后一个线程退出时调用。`PipelineContext` 提供 `roundId()`、`runId()`、`pipelineId()`、`podId()`、`params()`、`dataSources()`，适合在 `open` 里一次性预加载只读数据（示例的 `CapAllocRunner` 在 `open` 里加载全部站点产能）。`open` 抛异常按处理失败对待：CLUSTER 模式记录日志、退避、稍后重试；INLINE / LOCAL 的 run 对每个 pipeline 最多按 `max-attempts` 次退避重试 `open`，仍失败才让 run 失败；都不会拖垮 agent。`close` 抛异常只记日志，`open` 失败的组件不会被 `close`（用 `ComponentFactory` 创建的会被 `destroy`）。用户的 `open`/`close` 代码不在 pod 级锁内执行（同一 key 的并发获取者等同一次 `open`），慢的 `open` 不会阻塞心跳。注意：实例形式的 source、sink 和 runner 是共享单例，按 pod 引用计数：第一次被某个 pipeline 实例（任意 round、attempt）获取时 `open`，最后一个释放时 `close`，期间其他实例不再重复 `open`；有状态的 runner 请用类型形式（每个 pipeline 实例各有自己的对象）。INLINE / LOCAL 的每次 chunk 重试都用输入行的全新拷贝，同一行发到多个输出口时每个口各得一份拷贝，原地修改行的结果与 CLUSTER 一致。source/sink 仍是急切创建的配置对象（master 枚举 split 用的就是这个 source 实例），只应在 `read`/`write` 或自己的 `open`/`close` 里占用资源。

**`ComponentFactory`**：`create(Class)` 创建，`destroy(Object)` 在 `close()` 之后销毁。默认 `ReflectionComponentFactory`（public 无参构造）；Spring Boot 下自动配置的 `SpringComponentFactory` 用 `createBean`/`destroyBean`，类型形式的 runner 可直接用构造器/字段注入和 `@Value`，不必注册成 bean；自定义 `ComponentFactory` bean 可替换它。非 Spring 场景：`new DefaultPipelinerEngine(cfg, store, plane, dataSources, metrics, factory)`。

**按需生命周期**：master 启动某个 run 时写入 `pg:{round}:desired`，各 pod 的 WorkerAgent 才启动该 pipeline 的 worker 线程，同时在该 pod 上创建并 `open` 这个 pipeline 的实例（按 roundId + pipelineId 引用计数）；pipeline 结束（线程数降为 0、被 kill 或 round 不再活跃）后释放。INLINE/LOCAL run 在启动时实例化 run 内各 pipeline，结束、取消或失败时释放。因此一个 pod 只持有正在它上面运行的 pipeline 的 runner。`GET /pg/cluster` 的 `liveInstances` 就是这个视图：

```json
{"podId":"pod2","isLeader":false,"liveInstances":[
  {"roundId":"net-demand-20260101-120000-ab12","pipelineId":"gd-fix","threads":3,"perThread":false,"runner":"...GdFixRunner"}]}
```

## 配置项

前缀 `pipeliner`，由 `PipelinerProperties` 绑定；`runtime.*` 绑定 `RuntimeConfig`，`plan.*` 绑定 `PlanOptions`。`runtime.*` 里由专用属性接管的键（`runtime.pod-id`、`master-enabled`、`worker-enabled`、`plan-options`、`retention-*`）不能再用 `runtime.*` 设置：值与默认值不同时启动直接失败，并提示该用哪个属性（`pipeliner.pod-id`、`pipeliner.master-enabled`、`pipeliner.worker-enabled`、`pipeliner.plan.*`、`pipeliner.retention.*`）。错误响应：客户端错误（参数类型/缺失、坏 JSON、不支持的媒体类型）返回 400/415，未知 round / graph / run 返回 404，其余异常为 500 且只带简短消息。下表为框架默认值，`pipeliner-example/application.yml` 对 `runtime.*` 中的若干项做了缩短（tick 500ms、leader-ttl 6000ms 等）。

| 属性 | 默认 | 说明 |
|---|---|---|
| `pipeliner.pod-id` | 空（hostname + 随机后缀） | pod 标识 |
| `pipeliner.master-enabled` / `worker-enabled` | `true` / `true` | 是否参与选主 / 执行 chunk |
| `pipeliner.redis.uri` | `redis://127.0.0.1:6380/0` | 数据面 Redis |
| `pipeliner.store.type` | `redis` | 控制面：`redis`（默认）或 `mysql` |
| `pipeliner.store.datasource` | `pgstore` | 仅 `type: mysql` 时使用的数据源名（需在 `datasources` 中配置，示例见 `application-mysql-store.yml`） |
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
| `pipeliner.runtime.high-watermark` / `low-watermark` | `64` / `32` | 通道水位，达到高水位上游提交返回 BUSY；要求 `0 < low-watermark ≤ high-watermark`。引擎启动时校验 `RuntimeConfig`（上述水位、`max-attempts ≥ 1`、各类超时/间隔 > 0、并行度 > 0 等），不合法立即抛出带明确信息的 `PipelinerException`；`stop-join-millis`（默认 10000）：`engine.stop()` 最多等待 worker / 本地 run 线程这么久 |
| `pipeliner.runtime.master-tick-mode` · `external-tick-interval-millis` | `local` · `2000` | master 推进由本地线程或外部触发，见「外部触发 master 推进」 |
| `pipeliner.runtime.max-attempts` | `3` | 每个 chunk 的总尝试次数（含第一次，必须 ≥ 1；`1` 表示不重试）。三种模式用同一套重试逻辑：任何 `Throwable`（含 `StackOverflowError`）算一次失败尝试，`OutOfMemoryError` 直接抛出、不重试也不进死信 |
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

示例应用另有 `demo.core-grade-delay-ms`（默认 0，e2e 脚本设为 3，用于拉长 core-grade 便于注入故障/观察背压）和 `demo.algo-delay-ms`。`FakeAlgoService` 把每个请求先同步写入 MySQL 表 `ex_algo_request`（batch id 为主键，`INSERT IGNORE`），再由每个 pod 上的后台 poller 原子认领（条件 `UPDATE`；认领超过 `demo.algo-claim-timeout-ms`（默认 10000）视为放弃）、写 ClickHouse（ReplacingMergeTree，重放幂等）、`signalGate`（按 batch id 幂等）并标记完成；pod 被 kill -9 后其未完成的请求由其他 pod 接手，回调不会丢。

## 管理接口

基础路径 `/pg`（`PipelinerAdminController`）。

| 方法 | 路径 | 说明 |
|---|---|---|
| GET | `/pg/graphs` | 已注册的图名 |
| GET | `/pg/graphs/{name}/plan` | 执行计划（text/plain） |
| POST | `/pg/rounds` | 提交，body：`{"graph","bizKey","params":{}}`，返回 `roundId` |
| GET | `/pg/rounds/{id}` | round / run / gate 状态 |
| GET | `/pg/rounds/{id}/chunks` | chunk 列表，参数 `pipeline`、`status`、`limit`（默认 100，范围 1..1000）、`offset`（默认 0，>= 0）；非法值返回 400 |
| GET | `/pg/rounds/{id}/channels` | 各通道当前长度 |
| GET | `/pg/rounds/{id}/backpressure` | 高/低水位、通道长度、瓶颈 pipeline |
| POST | `/pg/rounds/{id}/pause` · `/resume` · `/cancel` | 暂停 / 恢复 / 取消 |
| POST | `/pg/runs/{roundId}/{runId}/retry` | 重试 run |
| POST | `/pg/runs/{roundId}/{runId}/rerun` | 从该 run 起重跑 |
| POST | `/pg/runs/{roundId}/{runId}/skip` | 跳过死信 |
| POST | `/pg/gates/{roundId}/{gateId}/signal` | 向 external 闸门发信号，body：`{"key","payload"}`；`key` 为 1..512 个字符，否则 400。key 带 `@<attempt>` 且低于对应 run 的当前 attempt 的是过时回调，被忽略（返回仍是 ok，只在 DEBUG 日志里留痕） |
| POST | `/pg/gates/{roundId}/{gateId}/open` | 打开 CLOSED 闸门；默认要求所有 after-run 已 SUCCEEDED，参数 `force=true` 跳过该检查；未知闸门、非 CLOSED（已 OPEN / TIMEOUT / REJECTED）的闸门返回错误；操作人取自认证身份（token 名），仅在认证关闭时才使用参数 `operator`（默认 `admin`） |
| GET | `/pg/graphs/{name}/plan.json` | 结构化执行计划（runs / pipelines / channels / gates），供界面使用 |
| GET | `/pg/rounds?graph=&limit=20` | 最近的 round：活跃的在前，其余按创建时间倒序 |
| GET | `/pg/cluster/workers` | 所有存活 pod：leader 标记、心跳年龄、各自持有的 pipeline 实例（来自心跳里的 `live` 字段） |
| POST | `/pg/master/tick` | 执行一次 master 推进（外部触发，ADMIN token），返回 `TickResult`，见「外部触发 master 推进」 |
| GET | `/pg/cluster` | 当前 pod 的 `podId`、`isLeader`、`liveInstances`（本 pod 上已实例化的 pipeline） |

### 管理操作规则

pause / resume / cancel / retry / rerun / skip / openGate 都写入 leader 的命令队列，由 leader 在 tick 里用自己的 epoch 执行（`PipelinerEngine` 上对应方法在 leader 执行完才返回，失败抛 `PipelinerException`，HTTP 返回错误消息）。规则：

- **命令 id、截止时间、撤回**：每条命令带随机 `id` 和 `deadline = 提交时间 + admin-timeout-millis`。leader 每次只取出一条命令，取出前先确认自己仍是 leader（被取代 / 租约丢失时剩下的命令留在队列里给下一任，不会丢）；已过截止时间的命令不执行，结果记为 `expired`（`ok=false`）。客户端等待超时后先把命令从队列里**撤回**（撤回成功即保证没有被执行，错误消息写明 `withdrawn (not executed)`）；撤回失败说明 leader 已取走，再宽限等待 2 秒，仍无结果时报「outcome is unknown」，需先查 round 状态再重试。命令结果保存 10 分钟。
- **未知 round** 是普通错误，不会让 leader 退位；leader 若发现自己已被更新的 epoch 取代（round 级 fence 失败），命令返回 `leader deposed ... retry` 并退位，队列里其余命令不受影响。
- **plan hash 检查**：round 创建时的 plan hash 与 leader 上注册的同名图不一致（滚动发布中途）时，所有会改状态的命令（`rerunFrom`、`retryRun`、`skipDeadLetters`、`cancel`、`openGate`）直接拒绝，错误为「was created under a different plan version」，待发布完成后再操作。
- **`retryRun` 的拒绝条件**：round 已 CANCELLED；round 已被压缩（用 `rerunFrom`）；run 的某个 in-gate 不是 OPEN（TIMEOUT / REJECTED 说明 run 是因闸门失败的，用 `rerunFrom` 重跑上游 run 或重新打开闸门）；run 不是 FAILED 时只记 WARN 并返回，不做任何事。CLUSTER run 先（带 fence）把当前 attempt 的死信放回通道，再做 store 迁移（FAILED→RUNNING 的 CAS，清掉 error 与 finishedAt、保留 startedAt）；中途失败时 run 仍是 FAILED，重新发出 retry 即可补完（空 DLQ 的 requeue 是空操作）。LOCAL / INLINE run 按新 attempt 整体重放。
- **`skipDeadLetters` 的拒绝条件**：除上面各项外，LOCAL / INLINE run 不支持（用 `retryRun` / `rerunFrom`）；run 没有任何死信（`DEAD` 记录和当前 DLQ 都为 0）时拒绝。执行顺序：先把 journal 排空（迟到的 DEAD 事件不会复活被跳过的 chunk），再在 store 里把 DEAD 记为 SKIPPED、round 标记 incomplete（带 epoch 的 `setRoundIncomplete`，被取代的 leader 写不进去），再恢复 run / pipeline，最后清 DLQ。
- **`rerunFrom`**：一次原子的 `resetRuns`；复位的 run 的 attempt 加 1，加 1 后达到 1000 的拒绝（先于任何修改），CANCELLED 的 round 拒绝。
- **`openGate`**：见上表；`force=true` 只跳过 after-run 检查，仍要求闸门为 CLOSED。

### 可视化界面

每个 pod 都在 `http://<pod>:<port>/pg-ui/` 提供内置界面（纯静态页面，无构建、无外部依赖，内网可用；不经过 `/pg/*` 的认证拦截器）。

- 展示：选定图的拓扑（stage 列 → run 框 → pipeline 节点，stream 边标通道长度、达到高水位变红，闸门为菱形，自动 barrier 标 `barrier:<dataset>`），以及所选 round 的实时状态：round 摘要（状态、当前阶段、用时、incomplete/compacted）、各 run / pipeline / 闸门状态、source 的 `done/total splits`、行数、DLQ、各 pod 持有的线程数。不选 round 时只显示拓扑。支持拖动平移、滚轮缩放、“适应”。
- 点击 pipeline / run / 闸门打开侧栏：详情、失败 chunk，以及 重试 / 从此处重跑 / 跳过死信 / 打开闸门、暂停 / 恢复 / 取消（执行前有确认框，失败时显示服务端返回的错误）。
- 自动刷新：所选 round 活跃时每 2 秒轮询一次。深链：`/pg-ui/?graph=<图>&round=<roundId>`，可加 `&theme=dark|light`。
- Token：右上角输入框（只存 `sessionStorage`），每个 `/pg` 请求都以 `X-PG-Token` 头发送（服务端始终接受该头，与 `header` 配置无关）。读取默认免 token；所有操作按钮需要 `ADMIN` token；服务端配了 `read-requires-token` 时读取也需要，界面会明确提示 401 / 403。
- 关闭：`pipeliner.admin.ui.enabled=false`（默认 `true`），此时 `/pg-ui/**` 返回 404。
- 注意：pod 持有的实例来自心跳（`heartbeat-millis`，默认 5 秒），略有延迟。

### 认证与授权

`/pg/*` 由 Spring MVC 拦截器 `AdminSecurityInterceptor` 保护（`io.github.amwtke.pipeliner.spring.security`，不依赖 Spring Security；旧版本的 servlet 过滤器 `AdminSecurityFilter` 已删除），默认开启。

- **角色**：`ADMIN` 可调用所有接口；`VIEWER` 只能读。
- **规则**：`GET`/`HEAD` 默认免 token；所有变更方法（POST/PUT/PATCH/DELETE）仅 `ADMIN`。`read-requires-token: true` 时读也需要 `VIEWER` 或 `ADMIN`。
- **失败响应**：缺失/未知 token → `401` + `WWW-Authenticate: Bearer`，body `{"error": "..."}`；`VIEWER` 做变更 → `403`。
- **安全默认**：开启但未配置任何 token 时，启动 WARN，所有变更接口返回 `403`（只读仍可用）。token 短于 16 字符会 WARN（仍可用）。token 用常量时间比较。
- **传递方式**：`Authorization: Bearer <token>`，或 `X-PG-Token: <token>`（`header` 可改默认的 `Authorization` 头名）。鉴权是 Spring MVC 拦截器，作用于 `PipelinerAdminController` 的所有处理器和 DispatcherServlet 内 `/pg` 下的路径，所以 `spring.mvc.servlet.path` / context path 改变后依然生效；`pipeliner.admin.ui.enabled=false` 的 `/pg-ui` 404 开关同理。配置的 token 会 trim（并对首尾空白或内部空白打 WARN）。

```yaml
pipeliner:
  admin:
    security:
      enabled: true            # 默认 true；false 则完全不注册拦截器（此时 open 的 operator 取自参数）
      read-requires-token: false
      header: Authorization
      tokens:
        - {name: ops,    token: "${PG_ADMIN_TOKEN}",  role: ADMIN}
        - {name: viewer, token: "${PG_VIEWER_TOKEN}", role: VIEWER}
```

```bash
curl -H "Authorization: Bearer $PG_ADMIN_TOKEN" -X POST localhost:8080/pg/rounds/$RID/pause
curl -H "X-PG-Token: $PG_ADMIN_TOKEN" -H 'content-type: application/json' \
     -d '{"graph":"net-demand","bizKey":"b1"}' localhost:8080/pg/rounds
```

**审计日志**：每次变更调用写一行 INFO，logger `pipeliner.audit`：
`principal=ops method=POST path=/pg/rounds/r1/pause round=r1 run/gate=- status=200`（不会记录 token）。

**注意**：示例应用的 dev token（`dev-admin-token-change-me`）仅供本地，务必用 `PG_ADMIN_TOKEN`/`PG_VIEWER_TOKEN` 覆盖；e2e/chaos 脚本读取 `PG_ADMIN_TOKEN` 并在每次调用里带上。示例的 `/demo/*` 接口仅用于演示，**不经过**此拦截器、无认证。Actuator 端点同样不在保护范围内，请用 `management.server.port` 单独监听内网端口或由平台层保护。

示例应用另有 `POST /demo/seed`、`POST /demo/rounds?planVersion=`、`GET /demo/checksum?planVersion=`。

## 外部触发 master 推进（xxl-job 等）

master 的一次推进（续租/抢主、执行管理命令、推进所有活跃 round、保留清理、内存限流）是一个显式、幂等的操作：`PipelinerEngine.tick()`，返回 `TickResult`（`leader`、`epoch`、`roundsTicked`、`commandsExecuted`、`durationMillis`、`skipped`、`skipReason`）。默认（`LOCAL`）由每个 pod 内部的 `pg-master` 线程每 `tick-millis` 调用一次；也可以改为 `EXTERNAL`，由外部调度器驱动，此时 pod 不启动 master 线程。

| 属性 | 默认 | 说明 |
|---|---|---|
| `pipeliner.runtime.master-tick-mode` | `local` | `local` = 内部线程；`external` = 不起线程，由外部调用 `engine.tick()` |
| `pipeliner.runtime.external-tick-interval-millis` | `2000` | 预期的外部触发间隔，仅用于校验与监控：`leader-ttl-millis < 3 ×` 该值时启动打 WARN（租约会在两次触发之间过期，leader 抖动）。运行期另有检查：相邻两次 tick 的间隔超过 `leader-ttl-millis / 2` 时打 WARN（每分钟最多一条），提示调大 `leader-ttl-millis` 或缩短触发间隔 |

语义：
- leader 语义不变：租约 + 每次（重新）获得 leadership 取新 epoch + round 级 fencing。非 leader 的 tick 在尝试抢租约后返回 `skipped`，`skipReason="not leader"`。同一 pod 上并发的 tick 由 `tryLock` 保护，后到者返回 `skipped "tick in progress"`；`master-enabled=false` 返回 `skipped "master disabled"`。
- worker 心跳/agent 在任何模式下都是本地线程：pod 的存活判定不能依赖调度器。leader 租约和全局限流标志（`pg:throttle`）同样不再依赖触发节奏：external 模式下每个 pod 有一个轻量的本地 `pg-lease-keeper` 线程，**仅当本 pod 是 leader** 时每约 `leader-ttl-millis / 3` 续一次租约（以及限流生效时刷新限流标志的 TTL），它不抢主、不推进 round、不执行命令。抢主（取新 epoch）仍由外部 tick 触发，所以 leader 一旦产生就不会因为触发间隔抖动、或随机路由落到别的 pod 而丢租约。路由建议不变：触发间隔 < `leader-ttl-millis` / 3，并让触发能到达存活 pod；只有 leader 宕机后，才需要一次落到其他 pod 的 tick 来接管（租约到期之后）。
- 管理命令（pause/cancel/open gate 等）在下一次 tick 才执行，延迟最长约一个触发间隔（`admin-timeout-millis` 需大于它）。round 的推进粒度同样变为触发间隔：间隔越大越省资源，但闸门/run 切换越慢；建议 1–5 秒。

xxl-job 处理器示例（仅示例，框架不依赖 xxl-job）：

```java
@XxlJob("pipelinerTick")
public void pipelinerTick() { TickResult r = engine.tick(); XxlJobHelper.log(r.toString()); }
```

路由建议：任何路由策略都可用——只有持有租约的 pod 会推进，其余 pod 返回 `not leader`（无副作用）。推荐 FAILOVER 或 RANDOM，触发间隔 1–5 秒，且触发间隔 < `leader-ttl-millis` / 3（即 `leader-ttl-millis` > 3 × 间隔）；不要用"广播"以外会长期固定到已死 pod 的策略。

HTTP 替代：调度器直接请求任意存活 pod：

```bash
curl -X POST -H "Authorization: Bearer $PG_ADMIN_TOKEN" http://<pod>:8080/pg/master/tick
# {"leader":true,"epoch":7,"roundsTicked":1,"commandsExecuted":0,"durationMillis":12,"skipped":false,"skipReason":null}
```

该接口是变更类接口，需要 ADMIN token；始终返回 200，是否推进看 `skipped`；Redis / 存储等基础设施故障时同样返回 200，`skipped=true` 且 `error` 字段带简短原因（外部调度器把非 2xx 当作任务失败，所以不用 5xx）。指标：`pipeliner_master_last_tick_timestamp_seconds`（每次完成的 leader tick 更新）、`pipeliner_master_tick_duration_seconds`。告警 `PipelinerMasterNotTicking`（`deploy/prometheus/pipeliner-alerts.yml`）：有活跃 round（`pipeliner_active_rounds > 0`）而 60 秒内没有 leader tick 并持续 1 分钟（`for: 1m`）时触发。阈值取 max(60s, 2 × scrape_interval + 5 × 预期 tick 间隔)：样本最多比真实 tick 旧一个抓取间隔，再留一个抓取间隔的余量；60 秒适用于抓取间隔 ≤ 15 秒且 tick 间隔 ≤ 6 秒，更大时按公式调大规则里的 60。

e2e：`MASTER_TICK=external pipeliner-example/scripts/e2e.sh`（pod 用 external 模式，脚本每秒向随机存活 pod 发一次 `POST /pg/master/tick`，leader-ttl 设为 3 秒）。

## 多 pod 正确性模型（fencing / attempt / 版本化 sink）

**Attempt**：`RunRecord.attempt` 是 run 的"代"。`rerunFrom` / `retryRun`（本地 run）通过一次原子的 `store.resetRuns`（MySQL 单事务 / Redis 单个 Lua）把 run、pipeline、chunk 记录、下游闸门（含其 signal）和 round 状态一起复位，attempt+1。数据面 key 带 attempt：`pg:{r}:ch:<edge>:<attempt>`、`split|splitidx|dlq:<pipeline>:<attempt>`；期望状态为 `并行度|attempt`。Worker 按 `(round, pipeline, attempt)` 管理线程和 pipeline 实例：attempt 变化后旧线程做完当前 chunk 即退出，新线程用全新的实例（`open()` 会重新执行）。journal 事件带 `attempt`（run 的代，不是 chunk 重试次数），attempt 不符的事件被 master 丢弃；`ChunkRecord.attempt` 同样是 run 的代。run 的 attempt 必须 < 1000（`writeVersion = seq × 1000 + attempt`，更大会撞到下一个 round 的版本区间）：达到 1000 的 run 不再执行，报明确错误，请取消该 round 并重新提交。INLINE / LOCAL run 的完成事件（DONE / FAILED，只会写其中一个）会带退避重试直到成功或 run 被取消；被取消的 executor 不再写 journal。数据面清理由 master 幂等重放：RunStarter 启动第 n 次 attempt 前先（带 fence）删除该 run 所有 <n 的 key；run 成功后删除当前 attempt 的 key。

**Epoch 下限与持久预留**：master 每次取新 epoch：`floor = store.maxMasterEpoch()`，`e = plane.nextEpoch(floor)`（Lua 里若 `pg:epoch` 小于下限先置为下限再 `INCR`，所以 `e > floor`），再 `store.reserveMasterEpoch(e)`：在控制面存储里原子地"当且仅当 `e` 高于已预留的最大值和所有 round 的 epoch 时写入"，返回 false 就**不当 leader**（本 tick 返回，下一 tick 重新检查租约并取新 epoch）。`maxMasterEpoch` 是已预留 epoch 与所有 round epoch 的最大值：MySQL 是单行表 `pg_master_epoch`（条件 `UPDATE`，影响行数为 1 即拥有）与 `MAX(pg_round.master_epoch)`；Redis 控制面是 `pg:store:maxepoch`（Lua compare-and-set，`fenceRound` 只在 round epoch 真正升高时才顺带抬高它，`init()` 从已有 round 回填）。两层作用：Redis 数据丢失（`pg:epoch` 被清）后新 epoch 仍高于已用过的所有 epoch；一个已取得 epoch 但在写任何 round 之前冻结的 master，它的 epoch 不会在计数器丢失后发给第二个 master。接口方法都有默认实现（`maxMasterEpoch()`→0，`reserveMasterEpoch`→true 即无持久预留，`nextEpoch(floor)`→`nextEpoch()`），自定义的旧实现仍可编译运行。master 取得租约后若存储调用失败，会先释放租约再抛出；`stop()` 在本 pod 持有租约时一律释放。master / worker 的循环线程捕获 `Throwable`，记录日志后退避重试，线程不会因为一个 Error 消失。

**Fencing**：master 每次 tick 先 `store.fenceRound` 再 `plane.fenceRound(roundId, epoch)`（`pg:{r}:fence` 只增不减），任一返回 false 即退位。master 对数据面的所有写（期望并行度、本地 run 分配、入队 split、删 key、trim journal、paused 标志、requeue/clear DLQ、建 group、移出 active）都在 Lua 里先校验 fence ≤ epoch，否则抛 `StaleEpochException` 且不产生任何变更，所以冻结后恢复的僵尸 master 什么也改不了。管理命令也在 master tick 内用同一 epoch 执行。`pg:{r}:fence` 没有 TTL，只随 round 数据一起被 `purgeRoundData` 删除；`pg:{r}:keys` 是该 round 的 key 注册表（Lua 在建 key 时登记），purge 按它删除而不扫描整个 keyspace。Worker 的 commit / deadLetter 不需要 fence，但 commit.lua / dead_letter.lua 会校验 pending 条目仍属于提交的 consumer（被别的 pod autoclaim 后原 consumer 得到 CONFLICT / 不入 DLQ），renew 同理只续自己持有的条目。每个 tick master 还按 round 状态校正 `paused` 标志；`pg:throttle` 带 TTL（`max(3×tick, 5s)`），超限期间每 tick 续期。

**版本化 sink**：每个 round 提交时拿到全局单调的 `RoundRecord.seq`（`plane.nextRoundSeq(store.maxRoundSeq())`：`pg:roundseq` 先被抬到存储里已有的最大 seq 再 `INCR`，所以 Redis 数据丢失后新 round 的版本仍高于 sink 表里已有的版本；`maxRoundSeq()` 返回「持久化的最大已签发 seq」与现存 round 最大 seq 中较大者：MySQL 是单行表 `pg_round_seq`（建 round 的同一事务里 `UPDATE ... WHERE max_seq < seq`，`init()` 从现有 round 回填）与 `MAX(pg_round.seq)`，Redis 控制面是 `pg:store:maxseq`（建 round 的脚本里抬高，`init()` 回填）；所以 retention 删光所有 round 后下限依然保留，新 round 的版本不会回落到 sink 表里已有的版本之下），`SinkContext.writeVersion() = seq × 1000 + attempt`（`RunContext` 同样提供 `attempt()` / `writeVersion()`）。`JdbcSinks.xxx(...).versionColumn("pg_version")` 会在每一行写入该列：MySQL upsert 变为条件更新（`c = IF(VALUES(ver) >= ver, VALUES(c), c)`，版本列最后赋值），旧 attempt / 旧 round 的僵尸写入覆盖不了新结果；ClickHouse 需把表声明为 `ReplacingMergeTree(pg_version)`，`FINAL` 读取保留最高版本。校验和只覆盖业务列。外部系统（如算法回调）请按 batch id 幂等，batch id 必须全局唯一，由框架签发：sink 里调用 `ctx.batchId(chunk)`（`SinkContext.batchId(Chunk)`），格式 `<roundId>:<pipelineId>:<chunkId>@<attempt>`（`ChunkIds.batchId(roundId, pipelineId, chunkId, attempt)`）。chunk id 每个 round、每个源 pipeline 都按同样规则推出（`S0003`），只用 `chunkId@attempt` 的话，按 batch id 去重的外部服务会把下个月的 round、或同一 round 里另一个 pipeline 的 chunk 当成重复请求，所以 round id 和 pipeline id 都在 batch id 里（旧的两参、三参 `batchId` 重载已删除；pipeline id 和输出口名不允许含 `:`，图构建时会报错）。signal 按 round + 闸门存储，key 最长 512 个字符：`engine.signalGate` 对更长（或空）的 key 抛 `IllegalArgumentException`，不会静默截断（MySQL 的 `pg_gate_signal.signal_key` 为 `VARCHAR(512)`，`init()` 会把旧的 `VARCHAR(190)` 自动 `ALTER` 加宽；主键 64+64+512 个字符 × 4 字节 = 2560 字节，低于 InnoDB 的 3072）。`Gates.externalCountOf(p)` 的条件：当前 attempt 里按 chunk id **去重**后的 signal 数 ≥ 该 pipeline「真正调用过 sink 的 DONE chunk 数」（`GateContext.sinkChunkCount`：sink 写出行数 > 0 的 chunk；输出为空的 chunk 不会调用 sink，也就不会回调）。signal key 无 `@` 的算任意 attempt，以 `@<当前 attempt>` 结尾的算当前，旧格式（`chunk@attempt`、`round:chunk@attempt`）与新格式对应同一 chunk 时只算一次；`GateContext.runAttemptOf(pipelineId)` 提供当前 attempt。

**使用方的约束（正确性依赖这些假设）**：
- Runner 与 `enumerate` 必须确定性（同样输入、同样输出，同样的 chunk 切分）。
- 行值类型在 `Row.set/of` 时规范化，所有模式一致（与 CLUSTER 经 JSON 往返后相同）：整数 -> `Long`，浮点 -> `BigDecimal`（NaN / Infinity 存为 `null`），`java.time` / `java.util.Date` -> ISO 字符串，不支持的类型直接报错。
- YAML 时长（`timeout`、`delay`）写成带单位的字符串（`1h30m`）；`1:30:00` 这类写法会被拒绝，不会被当作 60 进制整数。`parallelism` 必须 >= 1。
- Source 的 `orderBy` 必须是全序（hash / distinct 分片没有 orderBy 时会告警一次）。
- Runner 输出必须按 `chunkKey` 分组；同一个 key 在分组结束后再次出现会抛 `PipelinerException`（output of port X is not grouped by chunkKey）。
- Sink 的 key 必须唯一（MySQL `upsertKeys`、ClickHouse 排序键）。
- ClickHouse 凡是参与产出结果的读取都要加 `FINAL`（`useFinal()`）。
- 外部系统对全局唯一、由框架签发的 batch id（`ctx.batchId(chunk)` = `<roundId>:<pipelineId>:<chunkId>@<attempt>`）幂等。

## 数据保留

控制面数据（round / run / gate / pipeline / chunk 记录）默认存 Redis，不清理会一直增长。leader 在 master 循环里按 `interval-minutes` 周期执行一次保留任务（非 leader 不跑；每轮最多处理 `batch` 个 round，并有很短的时间预算，不拖长 tick）：

| 属性 | 默认 | 说明 |
|---|---|---|
| `pipeliner.retention.enabled` | `true` | 总开关 |
| `pipeliner.retention.chunk-days` | `3` | 终态 round 结束超过该天数后做**压缩** |
| `pipeliner.retention.round-days` | `30` | 终态 round 结束超过该天数后**整体删除** |
| `pipeliner.retention.interval-minutes` | `10` | 两次保留任务的最小间隔（0 = 每个 tick 都跑，仅用于测试） |
| `pipeliner.retention.batch` | `100` | 每轮压缩 / 删除的 round 上限 |

- **压缩**（`ControlPlaneStore.compactRound`）：只处理终态（SUCCEEDED / FAILED / CANCELLED）且 `finishedAt` 早于截止时间、尚未压缩的 round。先把每个 pipeline 的汇总（各状态 chunk 数、rowsIn、rowsOut）写到 pipeline 记录上，再删除该 round 全部 chunk 记录和计数器，并标记 round 已压缩。压缩后 `GET /pg/rounds/{id}` 的 chunk 统计仍来自汇总，`/chunks` 列表为空。
- **删除**（`purgeRounds`）：删除 round、run、gate、signal、pipeline、chunk、计数器、bizKey 索引和排序索引，之后同一个 bizKey 可以重新提交。数据面里残留的 key（失败 round 为了 retry 保留的通道 / DLQ）和 `pg:workers` 里超过 `10 × worker-dead-millis` 的心跳成员也一并清理。数据面 key 按 round 的 key 注册表 `pg:{r}:keys` 删除；`purgeRounds` 在删除控制面记录的**同一个原子步骤**里把 round id 写入存储侧的持久待清理集合（MySQL 表 `pg_pending_purge`，Redis `pg:store:pendingpurge`），保留任务每轮读出该集合、调用 `purgeRoundData`，成功后才 `clearPendingPurges`；所以数据面清理失败、leader 切换或重启都不会让 key 泄漏（接口方法 `pendingPurges(max)` / `clearPendingPurges(ids)`，默认空实现）。
- **注册表之前的旧 round**：没有 `pg:{r}:keys` 的 round（升级前创建的）按注册表删不到它的通道 / split / DLQ。`purgeRoundData` 发现某个 round 没有注册表时，把它记入 `pg:purge:legacy`，再做**有预算、可续跑**的增量 `SCAN cursor MATCH pg:{<round>}:*`：每次调用最多花约 50 ms（外加一次 `COUNT 500` 的 SCAN 步），游标保存在 `pg:purge:legacycur`（任何 pod、重启后都从上次位置继续），扫完才移出待办；保留任务每轮即使没有新删除的 round 也调用一次 `purgeRoundData`，让待办继续推进。整个 keyspace 上的扫描只发生在没有注册表的 round 上，不会拖长 tick。
- **与并发重跑的安全性**：终态判断与删除是同一个原子步骤——MySQL 是单事务内对 `pg_round` 行 `SELECT ... FOR UPDATE`，Redis 是单个 Lua（`UNLINK` 删除）。`rerunFrom` / `retryRun` 通过 `resetRuns` 把 round 改回 RUNNING 后，保留任务看到的就是非终态，不会删除；反过来先删除的话，重跑会得到"round 不存在"。压缩过的 round 被重跑时，被重置的 pipeline 的汇总同时清除，round 的压缩标记也清除，重新结束后会再次压缩。
- MySQL 会在 `init()` 里幂等地加列（`pg_round.compacted`、`pg_pipeline.sum_*`）和索引 `(status, finished_at)`；Redis 会为已有的终态 round 补建 `pg:store:finished` / `pg:store:uncompacted` 索引。
- **标识符长度与 round id**：round id 为 `<graph>-<yyyyMMdd-HHmmss>-<12 位 hex>`（48 位随机），图名最长 64、gate id（含自动 `barrier:<dataset>`）和 pipeline id 最长 128（`PipelineGraph.build()` / Planner 校验）。MySQL 列按此定长（`round_id` 96、`gate_id` / `pipeline_id` 128，`pg_gate_signal` 主键 736 字符 × 4 = 2944 字节 ≤ 3072），`init()` 会把旧表的窄列自动 `ALTER` 加宽。`createRound` 遇到已存在的 round id 抛 `DuplicateRoundIdException`（Redis 脚本和 MySQL 主键冲突都会检测），`submit()` 换新 id 重试。Redis 控制面的一次性回填（终态索引、按图索引 `pg:store:graph:<graph>`、chunk 索引、最大 epoch / seq）由 `pg:store:schema` 标记保证只跑一次；`init()` 由 `DefaultPipelinerEngine.start()` 调用一次。`loadRoundRecord` / `loadRoundState` 不带 `planJson`（只有 `loadRound` 带）。
- 每轮有变更时输出一行 INFO：`retention: purged N rounds, compacted M rounds (K chunk records), pruned W stale workers`。

## 监控与告警

每个 pod 通过 Micrometer 暴露指标；示例应用已加入 `micrometer-registry-prometheus`，并在 `management.endpoints.web.exposure.include` 开放 `health,info,metrics,prometheus`：

```bash
curl -s localhost:8081/actuator/prometheus | grep '^pipeliner_'
```

自己的应用需要依赖 `micrometer-registry-prometheus` 和 `spring-boot-starter-actuator`。基数约束：计数器 / 计时器只带 `graph`、`pipeline`（、`run`、`status`、`gate`）标签，**不带 round id**；带 `round` 标签的 gauge 在 round 结束、leader 退位、或 2 分钟没有刷新时从注册表移除。round id 到 graph 的映射在指标实现内部用有界 LRU 缓存解析。

| 指标 | 类型 | 标签 | 含义 |
|---|---|---|---|
| `pipeliner_chunks_processed_total` | counter | graph, pipeline, run | 处理完成的 chunk 数 |
| `pipeliner_chunk_duration_seconds` | timer | graph, pipeline, run | chunk 处理耗时 |
| `pipeliner_rows_in_total` / `pipeliner_rows_out_total` | counter | graph, pipeline, run | 输入 / 输出行数 |
| `pipeliner_chunk_retries_total` | counter | graph, pipeline | chunk 重试次数 |
| `pipeliner_dead_letters_total` | counter | graph, pipeline | 进入死信的 chunk 数 |
| `pipeliner_busy_wait_seconds_total` | counter | graph, pipeline | 上游因下游满（BUSY）等待的秒数 |
| `pipeliner_channel_length` | gauge | graph, round, edge | 通道长度 |
| `pipeliner_channel_high_watermark` | gauge | - | 配置的高水位 |
| `pipeliner_gate_waiting_seconds` | gauge | graph, round, gate | 闸门已等待时长 |
| `pipeliner_gate_timeouts_total` | counter | graph, gate, policy | 闸门超时次数 |
| `pipeliner_leader` | gauge | pod | 1 = 该 pod 是 leader |
| `pipeliner_leader_changes_total` | counter | - | 当选次数 |
| `pipeliner_run_duration_seconds` | timer | graph, run, status | run 耗时 |
| `pipeliner_rounds_finished_total` | counter | graph, status | 结束的 round 数 |
| `pipeliner_active_rounds` | gauge | - | 活跃 round 数（leader 上报） |
| `pipeliner_live_instances` | gauge | pod | 该 pod 上存活的 pipeline 实例数 |
| `pipeliner_oldest_pending_seconds` | gauge | graph, round, pipeline | 输入 stream 里最老待确认 chunk 的空闲时间 |
| `pipeliner_stalled_chunk_alerts_total` | counter | graph, pipeline | 停滞 chunk 告警次数 |
| `pipeliner_redis_used_memory_bytes` | gauge | - | Redis used_memory（leader 约每 10 秒采样） |
| `pipeliner_retention_purged_rounds_total` / `_compacted_rounds_total` | counter | - | 保留任务删除 / 压缩的 round 数 |
| `pipeliner_retention_last_run_timestamp_seconds` | gauge | - | 保留任务上次运行时间 |

**停滞 chunk 告警**：master 每隔几个 tick 对 RUNNING 的 CLUSTER pipeline 的输入 stream（source 为 split 队列，其余为输入通道）做一次 XPENDING（汇总 + 最老一条的扩展查询，开销很小），把最老待确认条目的空闲时间写入 `pipeliner_oldest_pending_seconds`。空闲超过 `pipeliner.runtime.stalled-chunk-alert-millis`（默认 300000）时，对每个 (round, pipeline, entry) 只输出一条 WARN（含 consumer、投递次数、待确认数），并让 `pipeliner_stalled_chunk_alerts_total` 加 1。注意：worker 正常存活时 claim 续租会不断重置空闲时间，所以这个告警抓的是"没人续租 / 没人接手"的条目（worker 卡死且续租线程也停了、没有存活 worker、autoclaim 被关闭等）；worker 线程卡在 runner 里但续租仍在运行的情况需另行通过 `pipeliner_chunk_duration_seconds` 或 run 耗时观察。

**告警规则**：[deploy/prometheus/pipeliner-alerts.yml](deploy/prometheus/pipeliner-alerts.yml)（在 Prometheus 配置的 `rule_files` 里引用），包含：出现死信、round 失败、闸门等待过久 / 超时、chunk 停滞、持续背压（通道 10 分钟都在高水位：`min_over_time[10m]`，无 `for:`）、1 分钟无 leader、多 leader、leader 频繁切换、Redis 内存过高、保留任务未运行。阈值是示例值，按环境调整。

## 验证结果（v1）

- 全量 `mvn -o verify` 通过，504 个测试。
- e2e：R2 期间 kill -9 一个 worker 和 leader，在 Redis（默认）与 MySQL（`STORE_PROFILE=mysql-store`）两种控制面上均通过，checksum 一致（600 个 material 时 BIG 19200 行 / FINE 28800 行）。
- 全集群 kill -9 后重启，续跑到相同 checksum（`restart-resume.sh`，默认 Redis 控制面）。
- 背压：通道长度被限制在高水位（默认 64；设为 6/3 时为 6）；core-grade 自动扩容 2→4；观察到上游 BUSY 等待。
- 规模：3000 material / 3 pod 总计约 67 秒（R2 42.7 秒，R3 17.1 秒）。

## v1 已知限制

- 不支持 Redis Cluster，只支持单机 Redis（Lua 脚本访问了未在 KEYS 中声明的 key）。
- cap-alloc 这类 LOCAL 模式的 run 只在单个 pod 上执行。
- 管理命令被 leader 取出后、执行中 leader 宕机，该条命令的结果未知（客户端等到宽限期后报「outcome is unknown」，先查 round 状态再重试）；leader 每次只取一条命令并在取出前确认仍是 leader，其余命令不会因退位而丢失。
- MySQL fencing 在读取 round epoch 的子查询与新 master 的 `fenceRound` 之间有极小窗口。
- ClickHouse sink 的幂等依赖 ReplacingMergeTree 与 `FINAL` 读取。
- `externalCountOf` 期望的回调数来自 chunk 记录里的 sink 行数（MySQL 列 `pg_chunk.sink_rows`、Redis 计数器 `_sink`）：行数为 0 的 split / chunk 不调用 sink，也不计入期望。升级前已落库的 chunk 记录没有该数值（按 0 计），跨升级进行中的 round 其 `externalCountOf` 闸门会少算，需要时用 `openGate` 或 `rerunFrom`。
- Redis 数据面丢失（如被 FLUSHALL）时，运行中的 CLUSTER 源 pipeline（split 队列和 split 索引都没了、chunk 记录仍为 PENDING）会被 master 判为失败，错误为「data plane lost ... rerunFrom required」并打 ERROR 日志；需用 `rerunFrom` 重跑。另外，RUNNING 的 CLUSTER pipeline 的任一输入 stream 缺少消费组（`DataPlane.groupExists`，Redis 用 `XINFO GROUPS`，stream 不存在或 `NOGROUP` 都算缺失）也会按同样的错误失败：组是在 pipeline 变为 RUNNING 之前由 master 建好的，之后缺失只可能是数据面被清或被人为销毁。
- 尚无 Kafka source、HTTP sink。
- 未实现按 chunk 批量拉取（`pullBatch`）。
