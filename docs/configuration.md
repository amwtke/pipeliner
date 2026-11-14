# 配置参考

本页列出 pipeliner Spring Boot starter 的全部配置项（前缀 `pipeliner.*`）、示例应用自己的 `demo.*` 配置项，以及启动时的校验规则。读者是要部署、调优 pipeliner 的开发和运维人员。每一项的默认值都取自代码（`PipelinerProperties`、`PipelinerSecurityProperties`、`RuntimeConfig`、`PlanOptions`），没有写默认值的项表示没有默认。

- [配置是怎么绑定的](#配置是怎么绑定的)
- [基础配置](#基础配置)：pod、Redis、控制面存储、数据源
- [运行时配置](#运行时配置)：`pipeliner.runtime.*`
- [规划配置](#规划配置)：`pipeliner.plan.*`
- [数据保留配置](#数据保留配置)：`pipeliner.retention.*`
- [管理接口与界面配置](#管理接口与界面配置)：`pipeliner.admin.*`
- [启动校验规则](#启动校验规则)
- [被专用属性接管的 runtime 键](#被专用属性接管的-runtime-键)
- [可替换的 Bean](#可替换的-bean)
- [示例应用的 demo 配置](#示例应用的-demo-配置)
- [示例应用的配置文件](#示例应用的配置文件)

## 配置是怎么绑定的

`PipelinerProperties`（前缀 `pipeliner`）绑定所有配置。其中：

- `pipeliner.runtime.*` 逐字段绑定到 `RuntimeConfig`（引擎的运行时参数）。
- `pipeliner.plan.*` 绑定到 `PlanOptions`（规划器的阈值）。
- `pipeliner.admin.security.*` 由单独的 `PipelinerSecurityProperties` 绑定。

属性名使用 Spring Boot 的宽松绑定：`pipeliner.runtime.tick-millis`、`pipeliner.runtime.tickMillis` 和环境变量 `PIPELINER_RUNTIME_TICK_MILLIS` 等价。下面的表统一写 kebab-case。

> [!NOTE]
> 时间类的配置项没有使用 `Duration`，单位直接写在属性名里（`-millis`、`-minutes`、`-days`），值是整数。

## 基础配置

| 属性 | 类型 | 默认 | 含义 |
|---|---|---|---|
| `pipeliner.pod-id` | String | 空 | pod 标识，要求集群内唯一。留空时用 `主机名-4 位随机十六进制`。 |
| `pipeliner.master-enabled` | boolean | `true` | 本 pod 是否参与选主、推进 round。关闭后本 pod 的 `engine.tick()` 返回 `skipped`（`master disabled`）。 |
| `pipeliner.worker-enabled` | boolean | `true` | 本 pod 是否执行 chunk（启动心跳和 worker 线程）。 |
| `pipeliner.redis.uri` | String | `redis://127.0.0.1:6380/0` | Lettuce 的 Redis URI。数据面（Redis Streams）必用；控制面选 `redis` 时也用它。只支持单机 Redis。 |
| `pipeliner.store.type` | String | `redis` | 控制面存储：`redis` 或 `mysql`（不区分大小写），其他值启动失败。 |
| `pipeliner.store.datasource` | String | `pgstore` | 仅 `type: mysql` 时使用：控制面用的数据源名。可以是 `pipeliner.datasources` 里的名字，也可以是应用里某个 `DataSource` Bean 的名字。 |
| `pipeliner.datasources.<name>.url` | String | 无 | JDBC URL。`<name>` 就是 `JdbcSources.mysql("<name>")` / `JdbcSinks.clickhouse("<name>")` 里引用的数据源名。 |
| `pipeliner.datasources.<name>.username` | String | 无 | 用户名。 |
| `pipeliner.datasources.<name>.password` | String | 无 | 密码。 |
| `pipeliner.datasources.<name>.driver-class-name` | String | 无 | JDBC 驱动类；留空则由 JDBC URL 自动选择。 |
| `pipeliner.datasources.<name>.maximum-pool-size` | int | `10` | HikariCP 连接池大小。 |

数据源的说明：

- `pipeliner.datasources.*` 里配置的每一项会建一个 Hikari 池（池名 `pg-<name>`）；应用里已有的 `DataSource` Bean 也会按 Bean 名注册进同一个注册表，所以 `JdbcSources`、`ctx.lookup(...)` 可以直接引用它们。两者同名时，`pipeliner.datasources` 里的配置优先。
- 数据源在 `initializationFailTimeout=-1` 下创建，数据库暂时不可用不会让应用启动失败。
- starter 不带 MySQL 和 ClickHouse 的 JDBC 驱动，需要自己在应用里依赖（见[快速开始](./getting-started.md#在你自己的-spring-boot-应用里使用)）。
- ClickHouse 的连接串必须带 `compress=0`，例如 `jdbc:clickhouse:http://127.0.0.1:8123/default?compress=0`。

## 运行时配置

下表的默认值是框架默认值（`RuntimeConfig`）。示例应用的 `application.yml` 把其中几项调小了，见[后文](#示例应用的配置文件)。

### 选主与心跳

| 属性 | 类型 | 默认 | 含义 |
|---|---|---|---|
| `pipeliner.runtime.tick-millis` | long | `1000` | `local` 模式下 master 线程的推进周期；也用于推导全局限流标志的 TTL（`max(3 × tick, 5s)`）和停滞 chunk 的采样间隔（每 4 个 tick 一次）。 |
| `pipeliner.runtime.master-tick-mode` | `local` / `external` | `local` | `local`：每个 pod 有一个 `pg-master` 线程；`external`：不起线程，由外部调度器调用 `engine.tick()` 或 `POST /pg/master/tick`，见[运维手册](./operations.md#外部触发-master-推进)。 |
| `pipeliner.runtime.external-tick-interval-millis` | long | `2000` | `external` 模式下外部触发的预期间隔，只用于校验和告警，不会改变行为。`leader-ttl-millis < 3 ×` 该值时启动打 WARN。 |
| `pipeliner.runtime.leader-ttl-millis` | long | `15000` | leader 租约有效期。 |
| `pipeliner.runtime.heartbeat-millis` | long | `5000` | worker 心跳间隔（心跳里带本 pod 持有的 pipeline 实例和可执行的图的 plan hash）。 |
| `pipeliner.runtime.worker-dead-millis` | long | `20000` | 超过这个时间没有心跳，判定 worker 死亡（不再给它分配 LOCAL / INLINE run，`/pg/cluster/workers` 也不再列出）。 |
| `pipeliner.runtime.stop-join-millis` | long | `10000` | `engine.stop()` 最多等待 worker / 本地 run 线程这么久。 |

### chunk 投递、重试与死信

| 属性 | 类型 | 默认 | 含义 |
|---|---|---|---|
| `pipeliner.runtime.claim-idle-millis` | long | `60000` | 一个已投递但未确认的 chunk 空闲超过这个时间，会被别的 worker 用 `XPENDING` + `XCLAIM` 接手。 |
| `pipeliner.runtime.renew-interval-millis` | long | `20000` | 正在处理的 chunk 的续租间隔。应明显小于 `claim-idle-millis`，否则正常处理中的 chunk 会被别人抢走。 |
| `pipeliner.runtime.auto-claim-interval-millis` | long | `10000` | worker 扫描「空闲 chunk」的间隔。 |
| `pipeliner.runtime.read-block-millis` | long | `1000` | worker 读输入 stream 时的阻塞等待时间。 |
| `pipeliner.runtime.max-attempts` | int | `3` | 每个 chunk 的**总**尝试次数（含第一次，`1` 表示不重试）。三种执行模式共用同一套重试逻辑：任何 `Throwable`（含 `StackOverflowError`）算一次失败；`OutOfMemoryError` 直接抛出，不重试也不进死信。INLINE / LOCAL 的 run 对每个 pipeline 的 `open()` 也按这个次数重试。 |
| `pipeliner.runtime.retry-backoff-millis` | long | `1000` | 重试退避：第 n 次失败后等待 `n × retry-backoff-millis`；`open()` 失败后 CLUSTER worker 等待 `2 × retry-backoff-millis`。可以为 0。 |
| `pipeliner.runtime.max-deliveries` | int | `5` | 一个 chunk 被投递（含 autoclaim 重投）超过这个次数，直接进死信，不再处理。 |

### 背压与扩容

| 属性 | 类型 | 默认 | 含义 |
|---|---|---|---|
| `pipeliner.runtime.high-watermark` | int | `64` | 通道（stream）长度达到该值时，上游提交返回 `BUSY`；也是自动扩容的触发线。 |
| `pipeliner.runtime.low-watermark` | int | `32` | 上游遇到 `BUSY` 后等待，直到所有输出通道都低于该值再重试提交。 |
| `pipeliner.runtime.autoscale` | boolean | `true` | 是否自动扩并行度。 |
| `pipeliner.runtime.max-parallelism` | int | `8` | 自动扩容的并行度上限。 |
| `pipeliner.runtime.autoscale-cooldown-millis` | long | `15000` | 同一个 pipeline 两次扩容之间的最短间隔。 |
| `pipeliner.runtime.local-queue-capacity` | int | `64` | LOCAL 模式下 pipeline 之间内存队列的容量。 |
| `pipeliner.runtime.redis-memory-limit-bytes` | long | `0` | Redis 内存上限，`0` 表示不限。达到上限时全局限流所有源 pipeline，降到上限的 80% 以下才恢复。 |
| `pipeliner.runtime.stalled-chunk-alert-millis` | long | `300000` | 输入 stream 里最老的待确认 chunk 空闲超过该时间，打一条 WARN 并让 `pipeliner_stalled_chunk_alerts_total` 加 1。 |

> [!NOTE]
> 自动扩容只会把并行度每次加 1，直到 `max-parallelism`，不会自动缩容。并行度是**每个 worker pod 上**该 pipeline 的线程数（见[使用指南](./user-guide.md#并行度与自动扩容)）。

### master 内部

| 属性 | 类型 | 默认 | 含义 |
|---|---|---|---|
| `pipeliner.runtime.journal-batch` | int | `500` | master 每次从 journal（chunk 事件流）读取的事件数上限。 |
| `pipeliner.runtime.admin-timeout-millis` | long | `30000` | 管理命令等待 leader 执行的时间，也是命令的截止时间。`external` 模式下必须大于外部触发间隔。 |

## 规划配置

绑定到 `PlanOptions`，影响每个 run 的执行模式和默认并行度。选择规则见[使用指南](./user-guide.md#执行模式与规划)。

| 属性 | 类型 | 默认 | 含义 |
|---|---|---|---|
| `pipeliner.plan.inline-max-rows` | long | `50000` | 估算行数之和不超过该值、且是一条直链时，选 `INLINE`。 |
| `pipeliner.plan.local-max-rows` | long | `2000000` | 估算行数之和不超过该值时选 `LOCAL`，否则 `CLUSTER`。 |
| `pipeliner.plan.default-parallelism` | int | `2` | 没有调用 `.parallelism(n)` 的 pipeline 使用的并行度（只对 CLUSTER 有意义）。 |
| `pipeliner.plan.force-mode` | `INLINE` / `LOCAL` / `CLUSTER` | 无 | 强制所有 run 使用该模式，覆盖图里所有的 `.mode()` 提示。常用于测试基线（例如用 `INLINE` 得到单线程的参考结果）。 |

> [!WARNING]
> `pipeliner.plan.*` 决定每个 run 的执行模式和默认并行度，这些都是 plan hash 的组成部分。各 pod 的图定义和 `pipeliner.plan.*` 必须一致：master 只推进 plan hash 与本机注册的图一致的 round，LOCAL / INLINE run 也只会分配给 plan hash 一致的 pod。

## 数据保留配置

由 leader 在 master 循环里按间隔执行；每一遍最多处理 `batch` 个 round，并有很短的时间预算（`max(200ms, min(2s, leader-ttl-millis / 4))`），不会拖长 tick。保留任务的行为见[运维手册](./operations.md#数据保留)。

| 属性 | 类型 | 默认 | 含义 |
|---|---|---|---|
| `pipeliner.retention.enabled` | boolean | `true` | 总开关。关闭后控制面数据不会被清理，会一直增长。 |
| `pipeliner.retention.chunk-days` | long | `3` | 终态 round 结束超过该天数后做**压缩**（删除 chunk 记录，保留每个 pipeline 的汇总）。 |
| `pipeliner.retention.round-days` | long | `30` | 终态 round 结束超过该天数后**整体删除**。 |
| `pipeliner.retention.interval-minutes` | long | `10` | 两次保留任务之间的最小间隔，必须 ≥ 1。 |
| `pipeliner.retention.batch` | int | `100` | 每一遍压缩 / 删除的 round 数上限，必须 > 0。 |

> [!NOTE]
> `chunk-days` 和 `round-days` 没有做范围或相互大小的校验，请自行保证 `chunk-days ≤ round-days`。

## 管理接口与界面配置

| 属性 | 类型 | 默认 | 含义 |
|---|---|---|---|
| `pipeliner.admin.enabled` | boolean | `true` | 是否注册管理 REST（`PipelinerAdminController`，`/pg/**`）。只在 Web 应用里生效。 |
| `pipeliner.admin.ui.enabled` | boolean | `true` | 是否提供 `/pg-ui/` 界面。为 `false` 时 `/pg-ui` 和 `/pg-ui/**` 返回 404。界面本身调用 `/pg/**`，关闭管理接口后界面没有数据可看。 |
| `pipeliner.admin.security.enabled` | boolean | `true` | 是否注册 token 拦截器。为 `false` 时完全不鉴权，此时 `openGate` 的操作人取自请求参数 `operator`。 |
| `pipeliner.admin.security.read-requires-token` | boolean | `false` | 为 `true` 时 GET / HEAD 也需要 `VIEWER` 或 `ADMIN` token。 |
| `pipeliner.admin.security.header` | String | `Authorization` | 携带 token 的请求头名。`Authorization` 时要求 `Bearer <token>` 格式；其他名字时值可以是裸 token 或 `Bearer <token>`。无论怎么配置，`X-PG-Token` 头始终被接受。 |
| `pipeliner.admin.security.tokens[n].name` | String | `token` | token 的名字，用于审计日志里的 `principal` 和 `openGate` 的操作人。 |
| `pipeliner.admin.security.tokens[n].token` | String | 无 | token 值。首尾空白会被去掉（并打 WARN）；空值被忽略；短于 16 个字符会打 WARN。 |
| `pipeliner.admin.security.tokens[n].role` | `ADMIN` / `VIEWER` | `VIEWER` | `ADMIN` 可以调用所有接口，`VIEWER` 只读。 |

认证规则、响应码和审计日志见[运维手册](./operations.md#认证与授权)。

## 启动校验规则

引擎启动时（`ApplicationReadyEvent` 之后调用 `engine.start()`）先执行 `RuntimeConfig.validate()`，不合法立即抛出 `PipelinerException: invalid RuntimeConfig: ...`，列出所有不合法的项。规则如下：

| 项 | 规则 |
|---|---|
| `pod-id`（解析后） | 非空（留空时自动生成，所以实际上总是满足） |
| `max-attempts` | ≥ 1 |
| `low-watermark` | > 0 |
| `high-watermark` | ≥ `low-watermark` |
| `tick-millis`、`external-tick-interval-millis`、`leader-ttl-millis`、`heartbeat-millis`、`worker-dead-millis`、`claim-idle-millis`、`renew-interval-millis`、`auto-claim-interval-millis`、`read-block-millis`、`admin-timeout-millis`，以及 `retention.interval-minutes` 换算后的毫秒数 | > 0 |
| `retry-backoff-millis`、`stop-join-millis` | ≥ 0 |
| `journal-batch`、`max-parallelism`、`local-queue-capacity`、`retention.batch` | > 0 |
| `max-deliveries` | ≥ 1 |

其他会在启动或构图时失败的情况：

- `pipeliner.store.type` 不是 `redis` 或 `mysql`。
- `pipeliner.runtime.*` 里设置了被专用属性接管的键（见下节）。
- 图定义不合法（图名、pipeline id、gate id 的长度和字符限制，`parallelism ≥ 1` 等），见[使用指南](./user-guide.md#标识符规则与限制)。

> [!WARNING]
> 以下关系是推荐值，**不会被校验**，违反了也能启动，但会出现难以排查的现象：`renew-interval-millis` 应明显小于 `claim-idle-millis`；`tick-millis` 应小于 `leader-ttl-millis`；`heartbeat-millis` 应小于 `worker-dead-millis`；`external` 模式下 `leader-ttl-millis` 应不小于 `3 × external-tick-interval-millis`（违反时只打 WARN）。

## 被专用属性接管的 runtime 键

`pipeliner.runtime.*` 绑定的是整个 `RuntimeConfig`，但下面这些字段由专用属性负责，会在构造 `RuntimeConfig` 时被专用属性的值覆盖。为了避免「配了却不生效」，只要这些 `runtime.*` 键的值与 `RuntimeConfig` 默认值不同，应用启动就直接失败，错误信息里会指出该改用哪个属性：

| 被拒绝的键 | 请改用 |
|---|---|
| `pipeliner.runtime.pod-id` | `pipeliner.pod-id` |
| `pipeliner.runtime.master-enabled` | `pipeliner.master-enabled` |
| `pipeliner.runtime.worker-enabled` | `pipeliner.worker-enabled` |
| `pipeliner.runtime.plan-options.*`（只要绑定出非空对象） | `pipeliner.plan.*` |
| `pipeliner.runtime.retention-enabled` | `pipeliner.retention.enabled` |
| `pipeliner.runtime.chunk-retention-millis` | `pipeliner.retention.chunk-days` |
| `pipeliner.runtime.round-retention-millis` | `pipeliner.retention.round-days` |
| `pipeliner.runtime.retention-interval-millis` | `pipeliner.retention.interval-minutes` |
| `pipeliner.runtime.retention-batch` | `pipeliner.retention.batch` |

## 可替换的 Bean

自动配置里的下列 Bean 都带 `@ConditionalOnMissingBean`，应用里自己声明同类型的 Bean 就会替换默认实现：

| 类型 | 默认实现 | 说明 |
|---|---|---|
| `RedisClient` | `RedisClient.create(pipeliner.redis.uri)` | 数据面（以及 Redis 控制面）使用 |
| `DataSourceRegistry` | `PipelinerDataSources` | `pipeliner.datasources` 的 Hikari 池加应用里的 `DataSource` Bean |
| `ControlPlaneStore` | 按 `pipeliner.store.type` 选 Redis 或 MySQL 实现 | |
| `DataPlane` | `RedisDataPlane` | |
| `PipelinerMetrics` | 有 `MeterRegistry` 时用 `MicrometerPipelinerMetrics`，否则 `NOOP` | |
| `RuntimeConfig` | `PipelinerProperties.toRuntimeConfig()` | 自己提供时 `pipeliner.runtime.*` 等配置不再生效 |
| `ComponentFactory` | `SpringComponentFactory` | 创建类型形式的 runner |
| `PipelinerEngine` | `DefaultPipelinerEngine`，容器里所有 `PipelineGraph` Bean 会自动注册 | |
| `PipelinerAdminController` | 默认实现 | |

## 示例应用的 demo 配置

这些属性只属于 `pipeliner-example`，用于造故障、拉长处理时间，不属于框架。

| 属性 | 类型 | 默认 | 含义 |
|---|---|---|---|
| `demo.core-grade-delay-ms` | long | `0` | `core-grade` runner 每行的人为延迟（毫秒），用来拉长处理时间，方便注入故障或观察背压。`e2e.sh`、`restart-resume.sh` 和 `chaos` profile 设为 `3`。 |
| `demo.algo-delay-ms` | long | `200` | 模拟算法服务处理一个请求的耗时。 |
| `demo.algo-claim-timeout-ms` | long | `10000` | `FakeAlgoService` 的后台 poller 认领请求后超过该时间未完成，视为放弃，其他 pod 可以重新认领。 |
| `demo.chunk-rows` | int | `0` | 大于 0 时覆盖示例图里所有 pipeline 的 `chunkRows`（chunk 更小，更容易触发重投）。`chaos` profile 设为 `400`。 |
| `demo.cap-alloc-mode` | `INLINE` / `LOCAL` / `CLUSTER` | `LOCAL` | `cap-alloc` pipeline 的执行模式。 |
| `demo.cap-alloc-parallelism` | int | `3` | `cap-alloc` 的并行度，仅在 `demo.cap-alloc-mode=CLUSTER` 时生效。 |
| `demo.algo-callback-timeout` | `Duration` | `10m` | `algo-callback` 闸门的超时（超时策略 `FAIL`），用 Spring 的 Duration 写法，例如 `10m`、`30s`。 |

另有一个不是 Spring 属性的 JVM 系统属性：`-Ddemo.ch-db=<库名>`（默认 `pipeliner_demo`）指定示例使用的 ClickHouse 数据库。示例的单元测试用它指向 `pipeliner_demo_test`，避免动到演示数据。

## 示例应用的配置文件

`pipeliner-example/src/main/resources/` 下有三个配置文件。

**`application.yml`**（默认）：

| 配置 | 值 | 说明 |
|---|---|---|
| `server.port` | `8080` | 脚本用 `--server.port` 覆盖 |
| `management.endpoints.web.exposure.include` | `health,info,metrics,prometheus` | 暴露 `/actuator/prometheus` |
| `pipeliner.pod-id` | `${POD_ID:}` | 默认留空 |
| `pipeliner.redis.uri` | `redis://127.0.0.1:6380/0` | Redis 端口是 **6380** |
| `pipeliner.store.type` | `redis` | 用 `mysql-store` profile 切到 MySQL |
| `pipeliner.datasources.mysql` | `jdbc:mysql://127.0.0.1:3306/pipeliner_demo?...` | 用户 / 密码 `pipeliner` / `pipeliner` |
| `pipeliner.datasources.ch` | `jdbc:clickhouse:http://127.0.0.1:8123/default?compress=0` | 用户 / 密码 `pipeliner` / `pipeliner` |
| `pipeliner.runtime.tick-millis` | `500` | 缩短，默认 1000 |
| `pipeliner.runtime.leader-ttl-millis` | `6000` | 默认 15000 |
| `pipeliner.runtime.heartbeat-millis` | `2000` | 默认 5000 |
| `pipeliner.runtime.worker-dead-millis` | `8000` | 默认 20000 |
| `pipeliner.runtime.claim-idle-millis` | `15000` | 默认 60000 |
| `pipeliner.runtime.renew-interval-millis` | `4000` | 默认 20000 |
| `pipeliner.runtime.auto-claim-interval-millis` | `3000` | 默认 10000 |
| `pipeliner.admin.security.tokens` | `ops`（ADMIN）、`viewer`（VIEWER） | token 取自 `PG_ADMIN_TOKEN` / `PG_VIEWER_TOKEN`，缺省值是 `dev-admin-token-change-me` / `dev-viewer-token-change-me` |
| `demo.core-grade-delay-ms` / `demo.algo-delay-ms` | `0` / `200` | |

> [!WARNING]
> 默认的 dev token 写在仓库里，只能用于本地。任何共享环境都要用 `PG_ADMIN_TOKEN` 和 `PG_VIEWER_TOKEN` 覆盖。

**`application-mysql-store.yml`**（profile `mysql-store`）：`pipeliner.store.type: mysql`，`pipeliner.store.datasource: pgstore`，并配置数据源 `pgstore`（`jdbc:mysql://127.0.0.1:3306/pipeliner?...`，用户 / 密码 `pipeliner`）。启用方式：`scripts/run-pod.sh pod1 8081 --spring.profiles.active=mysql-store`。

**`application-chaos.yml`**（profile `chaos`，由混沌测试脚本使用）：把时间参数调得更紧、队列更小，放大竞态。文件注释要求保持 `renew < claim-idle`、`tick < leader-ttl`、`heartbeat < worker-dead`。

| 配置 | 值 |
|---|---|
| `pipeliner.runtime.tick-millis` / `leader-ttl-millis` | `300` / `3000` |
| `pipeliner.runtime.heartbeat-millis` / `worker-dead-millis` | `1000` / `4000` |
| `pipeliner.runtime.claim-idle-millis` / `renew-interval-millis` / `auto-claim-interval-millis` | `3000` / `1000` / `1000` |
| `pipeliner.runtime.high-watermark` / `low-watermark` | `8` / `4` |
| `pipeliner.runtime.autoscale` / `autoscale-cooldown-millis` | `true` / `3000` |
| `pipeliner.runtime.max-attempts` / `retry-backoff-millis` / `max-deliveries` | `3` / `500` / `5` |
| `demo.core-grade-delay-ms` / `demo.chunk-rows` | `3` / `400` |

## 下一步

- 管理接口、认证、指标和故障处理：[运维手册](./operations.md)
- 配置项对应的概念（执行模式、闸门、chunk）：[使用指南](./user-guide.md)
- 先把示例跑起来：[快速开始](./getting-started.md)
