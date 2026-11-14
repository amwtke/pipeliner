# PipelineGraph：流式计算框架设计

适用：Java 8（字节码目标）、Spring Boot 2.7.18、Redis ≥ 6.2（Streams、`XAUTOCLAIM`，仅单机，不支持 Redis Cluster）、MySQL 8、ClickHouse。

对应《售后预测计划净需求计算模型（参考版）》的 7 个阶段、52 个步骤。

## 版本说明

本文描述的是**已实现的 v1**，已按代码修订；运维、配置、接口清单以 [README](../README.md) 为准，这里只讲设计。阅读时注意：

- 文中的 Java 接口、Lua 脚本、DDL 是说明设计用的骨架，名字和签名与代码不完全一致（例如 puller / pusher 在代码里是 `SplitSource` / 上游通道 与 `Sink` / 输出口；实际 Builder 与 YAML 语法见 README「定义一个图」）。
- 没有实现的内容标为"未来工作"：`KafkaPuller`、`HttpPusher`、按 chunk 批量拉取（`pullBatch`）、ClickHouse 最小批量写入。
- 与原始设计稿相比，v1 在正确性上做了加强，集中在 8.1（epoch 与 fencing）、10.2（journal 位点）、11.1（版本化写入与外部批次号）、11.3（原子重跑）。

## 1. 先记住这八句话

1. 一次计算是一个**有界**的数据流：数据来自版本快照，量有限，跑完就结束。框架用流的方式传输和限速，但不做无界实时计算。
2. 一个 pipeline 由 puller、runner、pusher 三段组成。pipeline 之间用通道连接，分布式模式下通道是 Redis Stream。
3. **chunk 是唯一的工作单位**：puller 一次拉一个 chunk，runner 处理它，pusher 抛出结果，三件事一起提交。传输、确认、重试、断点、监控都以 chunk 为粒度。
4. **runner 不保留跨 chunk 的状态**。需要全量数据才能算的步骤，先落表，过一道闸门，再由下一个 run 重新拉取。
5. 闸门和落表把图切开，切出来的每一块能一口气跑完，这一块就是一个 run。`plan()` 的工作就是找出这些 run 并排好顺序。
6. master 只做推进和记账，不碰业务数据；slave 只做拉取、计算、抛出。master 没有内存状态，换一个 pod 接手可以从控制面完整恢复。
7. 背压靠"下游积压到高水位，上游的提交就被拒绝"实现，一跳一跳传回源头，最终让源 puller 停止读库。
8. 控制面四层：round、run、pipeline、chunk。一次提交的事实先原子地写进 Redis，再由 master 归并到 MySQL 或 Redis 的控制面存储。

## 2. 总体架构

```mermaid
flowchart LR
    subgraph CP[控制面]
        M[Master<br/>调度 / 闸门 / split 枚举 / 状态归并]
        S[(ControlPlaneStore<br/>MySQL 或 Redis)]
        A[Admin API / 指标]
    end
    subgraph DP[数据面]
        W1[Slave pod 1]
        W2[Slave pod 2]
        R[(Redis Streams<br/>数据通道 / split 队列 / journal)]
    end
    SRC[(MySQL / ClickHouse)]
    SNK[(MySQL / ClickHouse)]
    M -->|期望状态、split| R
    R -->|journal 事件| M
    M <--> S
    A --> S
    SRC --> W1
    SRC --> W2
    W1 <--> R
    W2 <--> R
    W1 --> SNK
    W2 --> SNK
```

Redis 是硬依赖，因为分布式通道、split 队列、提交事件都在 Redis Stream 上。MySQL 是可选依赖，只在控制面存储选 MySQL 时需要。

## 3. 核心概念

| 概念 | 含义 |
| --- | --- |
| PipelineGraph | 用户输入的数据流图：pipeline 是节点，流边、数据集依赖、闸门是边 |
| Pipeline | puller + runner + pusher |
| Channel | 一条流边的载体。每条边一个通道、一个消费组 |
| Dataset | 一张有名字的表（MySQL 或 ClickHouse）。pusher 写它，后面的 puller 读它 |
| Chunk | puller 一次拉到的一批数据，带 chunkId |
| Gate | 闸门：等一个条件成立再放行下游 |
| Run | 一组能一次跑到底的 pipeline，它们的准入条件相同 |
| ExecutionPlan | `plan()` 的产物：run 列表、闸门、阶段顺序、每个 run 的执行模式。不可变，带 hash |
| Round | ExecutionPlan 的一次执行，对应业务上的一个计划版本 |

四层标识的写法：

| 层 | 示例 | 生成方式 |
| --- | --- | --- |
| roundId | `net-demand-20261113-01` | 提交时生成；`(graph, bizKey)` 唯一（Redis 与 MySQL 两种控制面都保证），同一计划版本重复提交返回已有 round；round 被保留任务删除后才能用同一 bizKey 重新提交 |
| runId | `R3` | `plan()` 按拓扑顺序编号 |
| pipelineId | `big-net` | 用户在图里起的名字，图内唯一 |
| chunkId | `S0012`、`S0012>2.0` | 源 chunk 是 split 编号 `S0012`；下游 chunk 是"上游 chunkId + `>` + 边序号 + `.` + 输出序号" |

chunkId 由血缘推出，所以同一个输入重放时，产出的 chunkId 不变。这一点是幂等写库的基础。注意同一个图的每个 round 会推出**相同**的 chunkId，所以 chunkId 只在 round 内唯一；给外部系统的批次号必须再带上 roundId（见 11.1）。

## 4. Pipeline 模型

### 4.1 三段各自的种类

| 段 | 种类 | 行为 |
| --- | --- | --- |
| Puller | `SplitPuller`（MySQL / ClickHouse） | master 把表切成 split 放进 split 队列，slave 领一个 split，按范围读库 |
| | `StreamPuller` | 用消费组读上游通道，一条 Stream 记录是一个 chunk |
| | `KafkaPuller` | **未来工作，v1 未实现。** 设想：round 开始时锁定每个分区的起止 offset，一段 offset 区间是一个 split |
| Runner | `RowRunner` | 逐行处理：映射、过滤、展开 |
| | `BatchRunner` | 整个 chunk 一起处理：批量查库、滚动净算、组装调用入参 |
| Pusher | `StreamPusher` | 把结果切成 chunk 写入下游通道 |
| | `TablePusher` | 幂等写 MySQL 或 ClickHouse |
| | `HttpPusher` | **未来工作，v1 未实现。** 调外部接口需自己实现 `Sink`（示例的 `FakeAlgoSink`），批次号用 `<roundId>:<chunkId>@<attempt>`（见 11.1） |
| | `Route` / `Tee` | 在 v1 里是 runner 的命名输出口（`ports`），每个输出口可挂多个 sink 或一条流边 |

非流式的库通过 `SplitPuller` + `StreamPusher` 流化：读出来的行攒成 chunk 写进 Redis Stream，下游用 `StreamPuller` 接着处理。

所有 puller 最终都是从一条 Redis Stream 领活：`StreamPuller` 领的是数据，`SplitPuller` 领的是 split 描述。因此确认、超时回收、多 pod 分摊、完成判定只需要实现一套。

### 4.2 接口骨架

```java
/** 一批数据，也是提交单位。 */
public final class Chunk<T> {
    private final String chunkId;
    private final List<T> rows;
    private final Map<String, String> headers;   // roundId、runId、pipelineId、输出口、行数
    // 构造与 getter 略
}

/** master 侧：把有界数据源切成 split，每个 split 是一个源 chunk。 */
public interface SplitEnumerator {
    List<Split> enumerate(EnumContext ctx);       // 结果必须确定：同一快照两次枚举得到同样的 split
}

/** slave 侧：拉一个 chunk；没有数据时返回 null。 */
public interface Puller<I> extends AutoCloseable {
    void open(PullContext ctx);
    Chunk<I> pull(long timeoutMs) throws InterruptedException;
}

public interface RowRunner<I, O> {
    void process(I row, Collector<O> out, RunContext ctx) throws Exception;
}

public interface BatchRunner<I, O> {
    void process(List<I> rows, Collector<O> out, RunContext ctx) throws Exception;
}

/** Collector 支持命名输出口，用于分叉。 */
public interface Collector<O> {
    void emit(O row);
    void emit(String port, O row);
}

public interface Pusher<O> extends AutoCloseable {
    void open(PushContext ctx);
    /** 写外部系统的 pusher 在这里完成幂等写；流通道的写入由框架在提交脚本里完成。 */
    void push(Chunk<O> chunk, PushContext ctx) throws Exception;
}
```

`RunContext` 提供三样东西：round 参数（计划版本、月桶）、`lookup(dataset, keys)` 批量查快照表、`sideInput(name)` 读小维表缓存。

### 4.3 slave 上的一轮循环

```text
loop:
    等本地额度            // 在处理中的 chunk 数低于上限才继续，上限见 9.1
    chunk = puller.pull()  // XREADGROUP，或用 XAUTOCLAIM 接手别人超时的 chunk
    outs  = runner.run(chunk)   // 输出按 chunkKey 分组后切成 chunk
    外部 pusher 写库       // 幂等（版本化写入，见 11.1），先于提交
    r = commit.lua(...)    // 原子：确认输入 + 写下游通道 + 写 journal
    r == BUSY     → 下游积压，保持 chunk 不放，退避后重试提交，期间续租
    r == CONFLICT → 这个 chunk 已被别人提交，丢弃本次结果
```

处理时间长的 chunk，slave 每 20 秒执行一次 `XCLAIM ... JUSTID` 续租；超过 60 秒没有续租的 chunk 会被其他 slave 接手。投递次数超过上限（默认 5）的 chunk 直接转入死信流，不再处理，状态记为 `DEAD`。线程按 `(round, pipeline, run attempt)` 管理（见 8.4），attempt 变化后旧线程做完当前 chunk 即退出。

### 4.4 chunk 对业务键封闭

净需求计算的粒度是料号 × 节点 × 月，逐月滚动（步骤 21）和全网平衡（步骤 22）都要求一个料号的全部节点、全部月份同时在场。框架的约定是：**一个替代组的所有行必须落在同一个 chunk 里**。

- 源头切 split 时按替代组的键范围或哈希桶切，由 `SplitSource` 保证同键同 split。
- 流 pipeline 的输出口按 `chunkKey` 切 chunk：先按 `chunkRows` / `chunkBytes` 切，但 chunk 边界只落在 key 组之间，**一个 key 组永远不会被切开**；超过上限的大组整组保留成一个 chunk，不拆。切分是确定的，重放得到同样的 chunk。
- runner 的输出必须按 `chunkKey` 分组。同一个 key 在组结束后再次出现，框架抛 `PipelinerException`（output is not grouped by chunkKey），而不是悄悄切开。
- runner 需要的其他数据（库存、在途、安全库存）用 `ctx.lookup()` 按本 chunk 的料号批量查快照表，不做流与流的 join。
- 逐 chunk 处理不会打散分组，所以封闭性沿着流一路保持。

跨料号的计算不满足封闭性，例如翻新产能是所有料号共用的（步骤 28）。这类步骤的做法见第 6 节的"物化边"。

"攒批"在 v1 里只有一种写法：源 pipeline 把一个 split 读出的行按行数或字节数切成多个 chunk（仍然遵守 `chunkKey` 不拆组）。流 pipeline 一次只拉一个 chunk；"一次拉 N 个 chunk 合并处理"（`pullBatch`）**未实现，列为未来工作**。

## 5. 输入数据流图与 Builder

图可以用 YAML 描述，也可以直接用 Java 构建。下面的 YAML 和 Java 是设计稿的写法，用来说明概念；v1 的实际语法（`source` / `runner` / `sinks`、`chunkKey`、类型形式的 runner）见 README「定义一个图」。

```yaml
graph: net-demand
pipelines:
  - id: gd-fix
    puller: { type: clickhouse, dataset: algo_gross_demand, splitBy: material_group, splits: 64 }
    runner: { type: batch, bean: grossDemandFixRunner }
    pusher: { type: route, ports: { big: stream, fine: stream } }
  - id: big-net
    puller: { type: stream }
    runner: { type: batch, bean: bigPartNetRunner }
    pusher:
      type: tee
      outputs:
        - { type: clickhouse, dataset: nd1_big }
        - { type: stream }
edges:
  - { from: gd-fix.big, to: big-net }
gates:
  - id: algo-callback
    type: external
    after: [gd-submit]
    before: [gd-fix]
    timeout: 2h
    onTimeout: OPEN_WITH_FALLBACK
```

```java
PipelineGraph graph = PipelineGraph.builder("net-demand")
    .pipeline("gd-fix", p -> p
        .pull(Pullers.clickhouse("algo_gross_demand").splitBy("material_group", 64))
        .run(Runners.batch(grossDemandFixRunner))
        .push(Pushers.route("big", "fine")))
    .pipeline("big-net", p -> p
        .pull(Pullers.stream())
        .run(Runners.batch(bigPartNetRunner))
        .push(Pushers.tee(Pushers.clickhouse("nd1_big"), Pushers.stream())))
    .edge("gd-fix.big", "big-net")
    .gate("algo-callback", Gates.external().timeout(Duration.ofHours(2)).onTimeout(OPEN_WITH_FALLBACK))
        .after("gd-submit").before("gd-fix")
    .build();                                  // 或 PipelineGraph.builder().from(yamlSpec).build()

ExecutionPlan plan = graph.plan(PlanOptions.defaults());
System.out.println(plan.explain());
RoundHandle round = engine.submit(plan, RoundParams.of("bizKey", "PLAN-2026-11"));
```

图里有三种边：

| 边 | 怎么声明 | 性质 |
| --- | --- | --- |
| 流边 | `edge(from, to)` | 上下游同时运行，数据边产边消 |
| 物化边 | 不用声明。puller 读的 dataset 正好是图里某个 pusher 写的 dataset，框架自动识别 | 上游写完，下游才能开始读 |
| 闸门边 | `gate(...).after(...).before(...)` | 条件成立，下游才能开始 |

## 6. plan()：把图切成 run

### 6.1 规划步骤

1. **校验**。图必须无环；每个流输出口有且只有一个下游；上下游行类型一致；每个 `SplitPuller` 的切分键存在。
2. **给边分类**。流边、物化边、闸门边。每条物化边自动生成一个"上游完成"闸门。
3. **求区域**。去掉物化边和闸门边，只看流边，求连通分量。一个连通分量叫一个区域，区域内的 pipeline 必须同时运行。
4. **合并成 run**。计算每个区域的入闸集合（做传递约简，只保留最靠近它的那些闸门）。入闸集合相同的区域合并为一个 run，因为它们会在同一时刻获得准入。
5. **死锁检查**。闸门的两端不能落在同一个 run 里。如果 A 到 B 既有流边、又有一道等 A 完成的闸门，B 不启动就不消费，A 被背压堵住永远完不成。遇到这种情况 `plan()` 报错，并提示把那条流边改成物化边。
6. **排阶段**。run 之间的依赖是一张无环图，按拓扑分层得到阶段，同一阶段的 run 可以并行。
7. **选执行模式**。按拓扑形状和数据量提示为每个 run 选模式（见 6.2），并确定每条通道的实现、每个 pipeline 的并行度和水位。
8. **冻结**。产出不可变的 `ExecutionPlan`，计算 hash，随 round 一起存进控制面。断点续传时比对 hash，图变了就拒绝续跑。

需求文档提到翻新量与细料需求之间有循环依赖，建议按固定轮数迭代。图本身不允许有环，固定轮数的迭代由 builder 展开成 N 段首尾相接的 run。

### 6.2 三种执行模式

| 模式 | 适用的 run | 通道 | 线程与 pod | 断点粒度 |
| --- | --- | --- | --- | --- |
| `INLINE` | 一条直链，数据量小 | 直接方法调用，没有队列 | 单 pod 单线程 | 源 chunk |
| `LOCAL` | 有分叉或合流，数据量中等 | JVM 内有界队列 | 单 pod，每个 pipeline 一个线程 | 整个 run 重跑，依赖 pusher 幂等 |
| `CLUSTER` | 数据量大，或用户指定并行度大于 1 | Redis Stream | 多 pod，每个 pipeline 按并行度起线程 | 每个 pipeline 的 chunk |

默认规则：用户给了 `mode` 就用用户的；否则预估行数低于阈值时，直链选 `INLINE`、其他选 `LOCAL`；预估行数高于阈值选 `CLUSTER`。`LOCAL` 的断点粒度粗，是因为内存队列里的数据会随进程一起丢失，所以只把它用在重跑成本低的 run 上。

### 6.3 explain 输出示例

```text
ExecutionPlan net-demand  hash=3f9a...  7 runs / 6 gates / 22 pipelines
Stage 1  R1  CLUSTER  mat-map, mat-attr, net-topo, bom, wo-clean, ib-load, param-load
         G1  barrier   R1 写出的全部快照 dataset
Stage 2  R2  INLINE   gd-submit
         G2  external  毛需求算法回调收齐，超时 2h 后走兜底
Stage 3  R3  CLUSTER  gd-fix → big-net → core-grade ; gd-fix → fine-gd-sink
         G3  barrier   dataset grade_result
...
```

## 7. 闸门

闸门是 run 之间的等待点，只由 master 判定，状态存在控制面，所以 master 切换或集群重启后不会丢。

| 类型 | 放行条件 | 用在哪里 |
| --- | --- | --- |
| `barrier` | 指定 dataset 的所有写入方 pipeline 都已成功 | 物化边自动生成 |
| `condition` | 用户提供的判定函数返回放行，master 定时轮询 | 校验通过、行数达到预期 |
| `external` | 外部系统回调 `POST /gates/{id}/signal`，且关联的批次号全部收齐 | 异步调用外部算法 |
| `manual` | 有人在管理接口上点了放行 | 发布前审批 |
| `timer` | 到达指定时间 | 等业务库日切 |

```java
public interface GateCondition {
    GateDecision evaluate(GateContext ctx);     // OPEN / WAIT / REJECT
}
```

外部信号先写进控制面再判定，判定是按当前状态算的，不是按事件触发的，所以信号早到、重复到、master 当时不在线都不影响结果。

每个闸门有超时策略：`FAIL`（闸门置 `TIMEOUT`）、`OPEN_WITH_FALLBACK`（放行并在闸门上下文里打 `fallback` 标，下游 runner 通过 `RunContext.gateFallback(gateId)` 读到它，走兜底逻辑）、`ALERT_AND_WAIT`（告警后继续等）。

条件返回 `REJECT`、或超时策略为 `FAIL` 时，闸门置 `REJECTED` / `TIMEOUT`，它的 before-runs 立即置为失败；之后没有能启动的 run 时 round 失败。条件函数抛异常按 `WAIT` 处理。

`external` 闸门的信号按 round + gate 存储，键是外部批次号。`Gates.externalCountOf(pipelineId)` 只统计无 `@` 的键，或以 `@<上游 run 当前 attempt>` 结尾的键，所以重跑之前的旧 attempt 的回调不会放行闸门；重跑时闸门连同它的信号一起被清掉（见 11.3）。

## 8. 多 pod 执行：master 与 slave

### 8.1 选主

所有 pod 启动后都尝试 `SET pg:leader <podId> NX PX 15000`，成功者成为 master，每个心跳周期续期。每次（重新）获得 leadership，或发现全局 epoch 已被别人推进时，master 取新的任期号：`plane.nextEpoch(store.maxMasterEpoch())`。

**epoch 下限**。数据面用 Lua 原子地执行"若 `pg:epoch` 小于下限则置为下限，再 `INCR`"，所以新 epoch 一定大于控制面里已记录的最大 round epoch。控制面存储提供 `maxMasterEpoch()`（MySQL：`MAX(master_epoch)`，有索引；Redis：`fenceRound` 里同步维护的 `pg:store:maxepoch`）。这是为了 Redis 数据丢失（MySQL 控制面支持的恢复场景）：`pg:epoch` 从 1 重新计数，而存储里的 round epoch 更高，所有 `fenceRound` 都会被拒绝，master 一直退位、所有活跃 round 停摆。有下限之后新 master 的 epoch 总是高于存储里的值。

**round 级 fencing**。master 对每个 round 的 tick 先执行 `store.fenceRound(roundId, epoch)`（MySQL：`UPDATE ... SET master_epoch=? WHERE master_epoch<=?`；Redis：Lua），再执行 `plane.fenceRound(roundId, epoch)`（`pg:{r}:fence`，只增不减），任一返回 false 即退位。此后：

- 控制面的每次 master 写入都带 epoch，并在同一个事务 / 脚本里校验 round 的 epoch 不高于它（MySQL 用对 round 行的共享锁，与 `fenceRound` 串行）。
- 数据面的每次 master 写入（期望并行度、本地 run 分配、入队 split、删 key、trim journal、paused 标志、requeue / clear 死信、建消费组、移出活跃集合）都在 Lua 里先校验 fence 不高于 epoch，否则抛 `StaleEpochException` 且不产生任何变更。所以冻结后恢复的旧 master 什么也改不了。
- 管理命令在 master tick 内用同一 epoch 执行。slave 的 commit / deadLetter 不需要 fence，`XACK` 守卫保证只有持有 pending 的一方能提交。

选主做成接口，控制面选 MySQL 时也可以换成 MySQL 租约行实现（v1 未做，租约在 Redis）。master 所在的 pod 默认同时承担 slave 的工作，可以用配置关掉。

### 8.1.1 外部触发 master 推进

master 的一次推进是显式、幂等的 `engine.tick()`（续租 / 抢主、执行管理命令、推进活跃 round、保留清理、内存限流）。默认由每个 pod 内部线程按 tick 周期调用；也可以配置为 `external`，由 xxl-job 之类的外部调度器触发（或 `POST /pg/master/tick`），此时不起 master 线程，leader 语义（租约、epoch、fencing）不变，非 leader 的 tick 返回 `skipped`。worker 心跳始终是本地线程。细节见 README。

### 8.2 master 的推进循环

master 每秒跑一遍下面的循环。每一步都是"读当前状态，条件更新"，重复执行没有副作用。

```text
for round in 运行中的 round:
    fenceRound（控制面 + 数据面），失败即退位
    执行管理命令队列里的命令（暂停、重跑、放行……）
    消费 journal，把 chunk 事件连同位点批量归并进控制面存储，然后 trim journal
    对每个 CLOSED 的闸门：上游条件满足 → OPEN
    对每个 PENDING 的 run：入闸全部 OPEN → 建通道和消费组、枚举 split、发布期望状态 → RUNNING
    对每个 RUNNING 的 CLUSTER pipeline：见下面的完成判定 → SUCCEEDED / FAILED
    对每个 RUNNING 的 run：pipeline 全部成功且行数对账通过 → SUCCEEDED，清理通道
    采样背压指标，检查 slave 心跳；周期性执行保留清理
```

两个细节：

- **CLUSTER pipeline 的完成判定**。提交脚本在确认输入的同时把它从流里删掉，所以输入流的长度就等于"未投递 + 处理中"的 chunk 数，不需要结束标记。pipeline 完成要同时满足：上游全部成功（源 pipeline 则是 split 已全部入队）、输入流 `XLEN = 0`；然后先把 journal 完整归并一遍，再判定。对源 pipeline 还要求**每个 split 的 chunk 记录都已离开 `PENDING`**（DONE，或 DEAD / SKIPPED）：只看队列为空不够，队列空时可能还有 split 处于投递途中。最后看 `DEAD` 记录数决定 `SUCCEEDED` 还是 `FAILED`。
- **`INLINE` / `LOCAL` run 不靠 XLEN**。这两种 run 没有 Redis 通道，被指派的 pod 执行完后向 journal 写 `LOCAL_RUN_DONE`（或 `LOCAL_RUN_FAILED`）事件，事件带 run 的 attempt；master 归并时只接受 attempt 与当前 run 一致、且 run 仍为 `RUNNING` 的事件，其余丢弃（僵尸执行者、被重跑作废的旧 attempt）。收到 DONE 后 pipeline 置成功，run 的收尾与 CLUSTER 一样要过行数对账。
- **行数对账**。每条流边上，上游提交事件里的输出行数之和必须等于下游提交事件里的输入行数之和。对不上时 run 置为失败并告警。

### 8.3 slave 与 puller 分压

master 把"哪些 pipeline 处于运行中、各自的并行度"写成期望状态。slave 每秒读一次，在本地增减 pipeline 线程，每个线程以 `<podId>#<序号>` 作为消费者名加入该 pipeline 的消费组。

分压不需要 master 分配：所有 pod 的线程在同一个消费组里竞争拉取，谁有空余额度谁拉。处理慢的 pod 自然拉得少。源 pipeline 的 split 队列也是同样的机制。

slave 每 5 秒上报心跳。某个 pod 挂掉后，它名下没提交的 chunk 超过 60 秒无人续租，被其他 slave 用 `XAUTOCLAIM` 接手。

### 8.4 run attempt 与 attempt 作用域的 key

`RunRecord.attempt` 是 run 的"代"。重跑（见 11.3）让 attempt 加 1。数据面 key 带 attempt：`pg:{r}:ch:<edge>:<attempt>`、`split|splitidx|dlq:<pipeline>:<attempt>`；期望状态写成 `并行度|attempt`。slave 按 `(round, pipeline, attempt)` 管理线程和 pipeline 实例，attempt 变化后旧线程做完当前 chunk 即退出，新线程用全新的实例；journal 事件带 `runAttempt`，与当前 attempt 不符的事件被 master 丢弃。数据面清理由 master 幂等重放：启动第 n 次 attempt 前先（带 fence）删除该 run 所有小于 n 的 key；run 成功后删除当前 attempt 的 key。

### 8.5 管理操作走 leader 命令队列

pause / resume / cancel / retry / rerun / skip / openGate 不由接收请求的 pod 直接执行，而是写入命令队列，由 leader 在 tick 里用自己的 epoch 取出执行并回写结果，请求方等待结果（`admin-timeout-millis`）。这样所有对控制面的变更都出自持有 fence 的 master。命令被取出后、执行前 leader 宕机会丢命令，客户端超时后重试即可。管理接口受 token 认证保护（`ADMIN` 可变更，`VIEWER` 只读，变更写审计日志），每个 pod 还提供静态的 Web UI（`/pg-ui/`）展示拓扑与 round 状态。细节见 README。

## 9. 背压

puller 是主动拉取的，没拉走的数据留在 Redis 里，所以单看 puller 自身不会被撑爆。真正的风险有两处：slave 进程里同时持有的 chunk 太多；Redis 里的通道积压无限增长。背压分三层处理。

### 9.1 进程内：额度

每个 pipeline 线程同一时刻只持有一个输入 chunk（v1 没有 `pullBatch`），上一个提交之前不拉下一个。单 pod 的内存上限可以直接算出来：

```text
内存上限 ≈ Σ(各 pipeline 线程数 × maxChunkBytes × 反序列化膨胀系数)
```

输出切 chunk 时同时受行数和字节数两个上限约束（`chunkRows` / `chunkBytes`），但 `chunkKey` 不拆组：遇到超大的 key 组，chunk 会超过上限，内存上限要按最大 key 组估算。

### 9.2 通道：高低水位

每条通道有高水位和低水位（默认 64 和 32 个 chunk）。提交脚本在写下游之前先检查下游通道长度，达到高水位就整体拒绝，什么都不改：

```lua
-- commit.lua（骨架）
-- KEYS[1] 输入流  KEYS[2] journal  KEYS[3..] 输出流
-- ARGV[1] 消费组  ARGV[2] 输入记录 id  ARGV[3] 高水位  ARGV[4] 事件 JSON
-- ARGV[5..] 成对出现：输出流在 KEYS 中的下标、chunk 内容
for i = 3, #KEYS do
  if redis.call('XLEN', KEYS[i]) >= tonumber(ARGV[3]) then
    return -1                                   -- BUSY：下游积压
  end
end
if redis.call('XACK', KEYS[1], ARGV[1], ARGV[2]) == 0 then
  return 0                                      -- CONFLICT：已被其他 slave 提交
end
redis.call('XDEL', KEYS[1], ARGV[2])
for i = 5, #ARGV, 2 do
  redis.call('XADD', KEYS[tonumber(ARGV[i])], '*', 'c', ARGV[i + 1])
end
redis.call('XADD', KEYS[2], '*', 'e', ARGV[4])
return 1
```

收到 `BUSY` 的线程带着已算好的结果等待，等下游长度降到低水位以下再重试。它等待期间不拉新 chunk，于是它自己的输入通道开始积压，它的上游也会收到 `BUSY`。压力这样逐跳回传，直到源 puller 停止读库。

高水位是软上限：一个输入 chunk 展开出多个输出 chunk 时，单次提交可能超出一点，超出量不超过一次提交的输出数。

写库的 sink 是同步调用，库慢了线程自然阻塞，效果相同。ClickHouse 的最小批量写入（避免产生过多小分片）**未实现，列为未来工作**；目前每个 chunk 一次写入。

### 9.3 全局：master 监测

master 每秒采样各通道的长度、流入流出速率、最老 chunk 的等待时间，以及各 pipeline 线程处于 `BUSY` 等待的时间占比。

| 检测到的情况 | 判断 | 动作 |
| --- | --- | --- |
| 某 pipeline 输入通道满、输出通道不满 | 它是瓶颈 | 在并行度上限内调高它的期望并行度，slave 自动加线程 |
| 某 pipeline 长时间处于 `BUSY` | 它被下游拖住 | 只记录指标，不处理，根因在瓶颈处 |
| Redis `used_memory` 超过阈值 | 全局风险 | 置全局限流标志（带 TTL，超限期间每 tick 续期，所以退位的 master 留不下残留标志），所有源 puller 暂停，告警 |
| 某 chunk 等待时间超过阈值 | 卡死或毒数据 | 告警，附 chunkId 和持有它的 pod |

## 10. 控制面

### 10.1 四层状态

| 层 | 状态 | 谁改 |
| --- | --- | --- |
| round | `CREATED → RUNNING → SUCCEEDED / FAILED / CANCELLED`，可 `PAUSED` | master |
| run | `PENDING → RUNNING → SUCCEEDED / FAILED`，可 `PAUSED` | master |
| pipeline | `PENDING → RUNNING → SUCCEEDED / FAILED` | master |
| chunk | `PENDING → RUNNING → DONE / DEAD` | slave 产生事件，master 归并 |
| gate | `CLOSED → OPEN / REJECTED / TIMEOUT` | master |

失败向上传播：chunk 进死信，所属 pipeline 失败，所属 run 失败，round 失败。与失败 run 无依赖关系的其他 run 会跑完，不会被中断。闸门 `REJECTED` / `TIMEOUT` 同样让它的 before-runs 失败。

### 10.2 提交事实与控制面存储的关系

chunk 的提交必须和"确认输入、写下游"是同一个原子操作，而 Redis 与 MySQL 之间做不到原子。所以分成两步：

1. slave 在 `commit.lua` 里把提交事件写进 `journal` 流。这一步和数据提交是原子的，是事实的唯一来源。
2. master 消费 `journal`，把事件批量、幂等地写进 `ControlPlaneStore`。存储里的 chunk 状态是 journal 的投影，会晚几百毫秒。

**journal 位点存在控制面存储里，不用 Redis 消费组**：master 每读一批，就用 `applyChunkEvents(roundId, events, journalOffset, epoch)` 把这批事件的效果和新的位点（`RoundRecord.journalOffset`）在同一个事务 / Lua 里写入，并带 round 级 fence。所以"事件已应用"和"位点已前进"不会分离：master 在两者之间崩溃或被 fence，新 master 从存储里的位点继续，既不丢也不重复应用。应用成功后 master 再（带 fence）`trimJournal` 删掉已处理的部分，round 成功收尾时整条 journal 被删除，Redis 里的 journal 因此不会无限增长。位点落后只会让 trim 晚一些，不影响正确性。

这样两种存储的实现都很薄，slave 不直接写 MySQL，MySQL 的写入量由 master 攒批控制。master 判定 pipeline 完成时，除了输入流为空，还要求 journal 已经消费到末尾（见 8.2）。

```java
public interface ControlPlaneStore {
    void createRound(RoundRecord round, List<RunRecord> runs,
                     List<GateRecord> gates, List<PipelineRecord> pipelines);
    boolean fenceRound(String roundId, long epoch);        // master_epoch <= epoch 才成功
    long maxMasterEpoch();                                 // 新 epoch 的下限
    boolean casRoundStatus(String roundId, RoundStatus expect, RoundStatus target, String error, long epoch);
    boolean casRunStatus(String roundId, String runId, RunStatus expect, RunStatus target, String error, long epoch);
    boolean casPipelineStatus(String roundId, String pipelineId, PipelineStatus expect, PipelineStatus target, String error, long epoch);
    boolean casGateStatus(String roundId, String gateId, GateStatus expect, GateStatus target, String openedBy, Map<String, String> context, long epoch);
    void saveGateSignal(String roundId, String gateId, String signalKey, String payload);
    void applyChunkEvents(String roundId, List<ChunkEvent> events, String journalOffset, long epoch);   // 批量、幂等，位点同事务
    void resetRuns(String roundId, List<String> runIds, List<String> gateIds, long epoch);          // 原子重跑复位
    RoundView loadRound(String roundId);                                          // 四层树，恢复和监控共用
    List<String> findActiveRounds();
    List<ChunkRecord> queryChunks(ChunkQuery query);
}
```

### 10.3 两种存储怎么选

| | MySQL | Redis |
| --- | --- | --- |
| 适合 | 生产环境 | 开发、测试、轻量部署 |
| 历史查询 | SQL 任意查，可长期保留 | 只能按 key 查，round 结束后按 TTL 过期 |
| 条件更新 | `UPDATE ... WHERE status = ? AND master_epoch <= ?` | Lua 脚本内比较后写入 |
| 额外依赖 | 多一个 MySQL | 无 |
| Redis 数据全丢时 | round / run / gate 状态与已归并的 chunk 记录还在，已成功的 run 不动；master 重新登记活跃 round。epoch 计数器丢失由 epoch 下限兜底（8.1）。v1 不自动检测通道数据的丢失，未完成的 run 用 `rerunFrom` 重跑 | 控制面一起丢，只能整轮重跑 |

### 10.4 MySQL 表结构

```sql
CREATE TABLE pg_round (
  round_id      VARCHAR(64)  NOT NULL PRIMARY KEY,
  graph_name    VARCHAR(64)  NOT NULL,
  biz_key       VARCHAR(128) NOT NULL,          -- 业务幂等键，如计划版本号
  plan_hash     CHAR(32)     NOT NULL,
  plan_json     MEDIUMTEXT   NOT NULL,          -- 冻结的 ExecutionPlan
  params_json   TEXT,
  status        VARCHAR(16)  NOT NULL,
  master_epoch  BIGINT       NOT NULL,
  journal_offset VARCHAR(64) NOT NULL DEFAULT '0-0',  -- journal 位点，与 chunk 事件同事务写入
  seq           BIGINT       NOT NULL DEFAULT 0,      -- 全局单调的提交序号，版本化写入用（11.1）
  created_at    DATETIME(3)  NOT NULL,
  started_at    DATETIME(3),
  finished_at   DATETIME(3),
  UNIQUE KEY uk_biz (graph_name, biz_key),
  KEY idx_master_epoch (master_epoch)                 -- MAX(master_epoch) 走索引
);

CREATE TABLE pg_run (
  round_id      VARCHAR(64) NOT NULL,
  run_id        VARCHAR(16) NOT NULL,
  stage         INT         NOT NULL,
  exec_mode     VARCHAR(12) NOT NULL,           -- INLINE / LOCAL / CLUSTER
  status        VARCHAR(16) NOT NULL,
  attempt       INT         NOT NULL DEFAULT 0,
  master_epoch  BIGINT      NOT NULL,
  started_at    DATETIME(3),
  finished_at   DATETIME(3),
  PRIMARY KEY (round_id, run_id)
);

CREATE TABLE pg_gate (
  round_id      VARCHAR(64) NOT NULL,
  gate_id       VARCHAR(64) NOT NULL,
  gate_type     VARCHAR(16) NOT NULL,
  before_run    VARCHAR(16) NOT NULL,
  status        VARCHAR(16) NOT NULL,
  deadline_at   DATETIME(3),
  opened_by     VARCHAR(64),                    -- master / 回调方 / 操作人
  opened_at     DATETIME(3),
  context_json  TEXT,                           -- 兜底标记、已收到的信号摘要
  master_epoch  BIGINT      NOT NULL,
  PRIMARY KEY (round_id, gate_id)
);

CREATE TABLE pg_gate_signal (
  round_id      VARCHAR(64)  NOT NULL,
  gate_id       VARCHAR(64)  NOT NULL,
  signal_key    VARCHAR(128) NOT NULL,          -- 如外部调用的批次号
  payload       TEXT,
  received_at   DATETIME(3)  NOT NULL,
  PRIMARY KEY (round_id, gate_id, signal_key)
);

CREATE TABLE pg_pipeline (
  round_id      VARCHAR(64) NOT NULL,
  pipeline_id   VARCHAR(64) NOT NULL,
  run_id        VARCHAR(16) NOT NULL,
  status        VARCHAR(16) NOT NULL,
  parallelism   INT         NOT NULL,
  enum_done     TINYINT     NOT NULL DEFAULT 0, -- 源 pipeline：split 是否已全部入队
  chunks_total  BIGINT      NOT NULL DEFAULT 0,
  chunks_done   BIGINT      NOT NULL DEFAULT 0,
  rows_in       BIGINT      NOT NULL DEFAULT 0,
  rows_out      BIGINT      NOT NULL DEFAULT 0,
  master_epoch  BIGINT      NOT NULL,
  started_at    DATETIME(3),
  finished_at   DATETIME(3),
  PRIMARY KEY (round_id, pipeline_id),
  KEY idx_run (round_id, run_id)
);

CREATE TABLE pg_chunk (
  round_id      VARCHAR(64)  NOT NULL,
  pipeline_id   VARCHAR(64)  NOT NULL,
  chunk_id      VARCHAR(128) NOT NULL,
  run_id        VARCHAR(16)  NOT NULL,
  status        VARCHAR(12)  NOT NULL,
  attempt       INT          NOT NULL DEFAULT 0,
  owner         VARCHAR(80),                    -- podId#线程序号
  split_spec    VARCHAR(1024),                  -- 仅源 chunk：键范围 / offset 区间
  rows_in       BIGINT,
  rows_out      BIGINT,
  cost_ms       INT,
  error         VARCHAR(2000),
  updated_at    DATETIME(3)  NOT NULL,
  PRIMARY KEY (round_id, pipeline_id, chunk_id),
  KEY idx_status (round_id, run_id, status)
);
```

`pg_chunk` 是写入量最大的表，一轮可能有几十万行。master 攒批写入（单批事件数有上限）。保留策略见 10.6。上面的 DDL 是简化后的示意，实际表还有 `compacted`、汇总列和若干索引，以 `MySqlControlPlaneStore.init()` 为准。

### 10.5 Redis key 布局

key 用 `{roundId}` 做哈希标签，让同一个 round 的 key 在命名上聚在一起；但 **v1 不支持 Redis Cluster，只支持单机 Redis**：Lua 脚本里访问了没有在 `KEYS` 里声明的 key（全局 key、跨 round 的索引），在 Cluster 下会失败。

```text
pg:leader                              STRING  当前 master 的 podId
pg:epoch                               STRING  任期号（nextEpoch 带下限，见 8.1）
pg:roundseq                            STRING  全局单调的 round 提交序号
pg:workers                             ZSET    podId → 最近心跳时间
pg:rounds:active                       SET     运行中的 roundId
pg:{<roundId>}:fence                   STRING  数据面 round 级 fence，只增不减
pg:{<roundId>}:desired                 HASH    pipelineId → 并行度|attempt（master 写，slave 读）
pg:{<roundId>}:ch:<edgeId>:<attempt>   STREAM  数据通道
pg:{<roundId>}:split:<pipelineId>:<attempt>     STREAM  源 pipeline 的 split 队列
pg:{<roundId>}:splitidx:<pipelineId>:<attempt>  SET     已入队的 splitId，保证枚举幂等
pg:{<roundId>}:dlq:<pipelineId>:<attempt>       STREAM  死信
pg:{<roundId>}:journal                 STREAM  chunk 提交事件（归并后被 trim，round 成功后删除）

# 以下仅在控制面存储选 Redis 时使用
pg:{<roundId>}:round                   HASH
pg:{<roundId>}:run:<runId>             HASH
pg:{<roundId>}:gate:<gateId>           HASH
pg:{<roundId>}:pl:<pipelineId>         HASH
pg:{<roundId>}:chunk:<pipelineId>      HASH    chunkId → 状态 JSON
pg:store:rounds / biz / active         全局索引：round 列表、(graph,bizKey) → roundId、活跃集合
pg:store:finished / uncompacted        保留任务用的终态 / 待压缩索引
pg:store:maxepoch                      STRING  所有 round 的最大 master epoch（8.1 的下限）
```

### 10.6 数据保留

控制面数据默认存 Redis，不清理会一直增长。leader 在 master 循环里周期性执行保留任务（有数量与时间预算，不拖长 tick）：终态 round 结束超过 `chunk-days` 后**压缩**（把每个 pipeline 的汇总写到 pipeline 记录上，删除全部 chunk 记录，round 的统计仍可读）；超过 `round-days` 后**整体删除**（round、run、gate、signal、pipeline、chunk、bizKey 索引及数据面残留 key，此后同一个 bizKey 可重新提交）。终态判断与删除是同一个原子步骤，所以与并发的 `rerunFrom` 不会互相踩：重跑把 round 改回 `RUNNING` 后保留任务就不会再动它。参数见 README「数据保留」。

## 11. 断点续传与一致性

### 11.1 保证的语义

- **流到流**：恰好一次。确认输入和写出结果在同一个 Lua 脚本里，要么都发生，要么都不发生。两个 slave 同时处理同一个 chunk 时，`XACK` 的返回值保证只有一个提交成功。
- **流到外部库**：至少一次投递，加上幂等写入，结果等同于恰好一次。写库发生在提交之前，提交前崩溃会重放这个 chunk，再写一次同样的数据。

幂等写入的做法是**版本化写入**。每个 round 提交时拿到全局单调的 `seq`，每次写入带版本 `pg_version = roundSeq * 1000 + attempt`（`attempt` 是 run 的 attempt，见 8.4）。版本越新的数据越能覆盖旧的，所以旧 attempt、旧 round 的僵尸写入（被 fence 的 master 之前派出的 slave、重跑前慢下来的线程）覆盖不了新结果：

| 目标 | 做法 |
| --- | --- |
| MySQL | 条件 upsert：`INSERT ... ON DUPLICATE KEY UPDATE c = IF(VALUES(pg_version) >= pg_version, VALUES(c), c)`，版本列放在最后赋值；唯一键为业务键 |
| ClickHouse | 表声明为 `ReplacingMergeTree(pg_version)`，排序键为业务键；**每一个会影响结果的读取都必须带 `FINAL`**（`useFinal()`），否则可能读到合并前的旧版本行 |
| 外部接口 | 批次号 `<roundId>:<chunkId>@<attempt>`，要求对方按批次号幂等 |

校验和只覆盖业务列，不含 `pg_version`。

**外部批次号为什么带 roundId**。chunkId 在每个 round 里推出的值相同（`S0003` 每个月都是 `S0003`），如果批次号只是 chunkId，按批次号幂等的外部服务会把下个月的 round 当成重复请求，返回上个月的旧结果。所以批次号是全局唯一的 `<roundId>:<chunkId>@<attempt>`（`ChunkIds.batchId(roundId, chunkId, attempt)`）。末尾的 `@<attempt>` 是 `externalCountOf` 闸门识别当前 attempt 的依据（见 7）；重跑后 attempt 变了，批次号也变，对方会把它当新请求重新计算。

幂等成立的前提是 runner 对同一个输入 chunk 产出同样的结果，且 sink 的 key 唯一。runner 读的是锁定版本的快照，这一点由需求文档里"每个阶段落快照、后一阶段只读快照"保证。

### 11.2 各种故障怎么恢复

| 故障 | 恢复方式 | 重做范围 |
| --- | --- | --- |
| slave 线程异常 | chunk 留在待确认列表，超时后重新投递 | 一个 chunk |
| slave pod 挂掉 | 其他 slave 用 `XAUTOCLAIM` 接手 | 该 pod 未提交的 chunk |
| 毒数据反复失败 | 超过重试上限进死信，run 失败；修复后调用 `retry(runId)` 把死信放回通道 | 死信里的 chunk |
| master 挂掉 | 新 master 当选（epoch 高于存储里所有 round 的 epoch），从控制面加载运行中的 round，从存储里的 journal 位点继续归并 | 无。slave 在此期间照常处理 |
| master 在枚举 split 中途挂掉 | 新 master 重新枚举，已入队的 splitId 被跳过 | 无 |
| 集群整体重启 | 通道和 journal 靠 Redis 持久化保留，控制面在存储里，启动后继续 | 未提交的 chunk |
| Redis 主从切换，丢失约 1 秒写入 | 通道和 journal 一起回退到同一个时间点，被回退的 chunk 重新处理；收尾时行数对账兜底 | 最后约 1 秒提交的 chunk |
| Redis 数据全丢（控制面在 MySQL） | 已成功的 run 不动；运行中的 run 整体重置，从它的源快照表重跑 | 未完成的 run |
| `INLINE` / `LOCAL` 的 run 所在 pod 挂掉 | master 把 run 派给另一个 pod；这类 run 靠 `LOCAL_RUN_DONE` 事件完成（8.2） | 见 6.2 的断点粒度 |
| 旧 master 冻结后恢复（僵尸） | round 级 fence 使它对控制面和数据面的写都失败，它退位 | 无 |

run 的边界都是落了表的 dataset，所以 run 是最后的重放单位：只要上游快照表还在，任何一个 run 都可以单独重跑。

### 11.3 人工操作

| 操作 | 效果 |
| --- | --- |
| `pause(roundId)` / `resume(roundId)` | 暂停后 slave 做完手上的 chunk 就停止拉取；恢复后从通道里剩下的继续 |
| `retry(runId)` | 失败的 run 把死信放回通道，继续跑 |
| `rerunFrom(runId)` | 一次原子的 `store.resetRuns`（MySQL 单事务 / Redis 单个 Lua）：该 run 及所有下游 run、它们的 pipeline、chunk 记录、相关闸门及其 signal 一起复位，run 的 attempt 加 1，round 回到 `RUNNING`；数据面旧 attempt 的 key 由 master 幂等清理 |
| `skipChunk(chunkId)` | 把死信里的 chunk 标记为跳过，run 可以继续，round 结果打上"不完整"标记 |
| `openGate(gateId)` | 人工放行 |

以上操作都经 leader 的命令队列执行（8.5）。

## 12. 监控

指标通过 Micrometer 暴露，标签为 `graph`、`round`、`run`、`pipeline`。

| 层 | 指标 |
| --- | --- |
| 集群 | 存活 slave 数、master 任期号与切换次数、Redis 内存、运行中 round 数 |
| round | 当前阶段、已完成 run 数、各闸门等待时长、总耗时 |
| run | 状态、耗时、失败次数 |
| pipeline | 输入输出行速率、chunk 处理耗时分位数、输入通道长度、`BUSY` 等待时间占比、重投次数、死信数、当前并行度 |
| chunk | 最老未提交 chunk 的等待时间、重试次数分布 |

源 pipeline 的进度是"已完成 chunk 数 / split 总数"。流 pipeline 在上游完成之前不知道总量，显示已完成数和当前积压。

另有停滞 chunk 告警（输入流里最老的待确认条目长时间无人续租 / 接手）。每个 pod 内置 Web UI（`/pg-ui/`），展示拓扑和 round 实时状态。管理接口受 token 认证（`ADMIN` / `VIEWER`），变更写审计日志。完整指标与告警规则见 README。

管理接口（节选）：

```text
GET  /pg/rounds/{roundId}                 四层树：run、闸门、pipeline、chunk 汇总
GET  /pg/rounds/{roundId}/chunks?status=  chunk 明细
GET  /pg/rounds/{roundId}/backpressure    各通道长度与瓶颈 pipeline
POST /pg/rounds/{roundId}/pause | resume | cancel
POST /pg/runs/{roundId}/{runId}/retry | rerun
POST /pg/gates/{roundId}/{gateId}/signal | open
```

建议的告警：闸门等待超时、出现死信、某 pipeline `BUSY` 占比持续高于 80%、最老 chunk 等待超过阈值、master 切换、Redis 内存超过阈值、行数对账失败。

## 13. 套到净需求计算模型上

按第 6 节的规则，52 个步骤切成 7 个 run、6 道闸门。

```mermaid
flowchart TD
    R1[R1 主数据装配<br/>步骤 2–8] --> G1{{G1 快照齐备}}
    G1 --> R2[R2 提交毛需求算法<br/>步骤 9–10]
    R2 --> G2{{G2 算法回调收齐}}
    G2 --> R3[R3 毛需求修正、大部件净算、分级<br/>步骤 11–27]
    R3 --> G3{{G3 分级结果落表}}
    G3 --> R4[R4 翻新计划<br/>步骤 28–33，并提交步骤 34]
    R4 --> G4{{G4 翻新细料预测收齐}}
    G4 --> R5[R5 细料净算与利旧<br/>步骤 35–46]
    R5 --> G5{{G5 两条线结果齐备}}
    G5 --> R6[R6 汇总与校验<br/>步骤 47–51]
    R6 --> G6{{G6 校验通过并人工确认}}
    G6 --> R7[R7 发布<br/>步骤 52]
```

| run | pipeline | 模式 | 为什么在这里切开 |
| --- | --- | --- | --- |
| R1 | 7 条互相独立的"读业务库 → 清洗 → 写快照"，对应步骤 2–8 | `CLUSTER` | 后面所有 run 都只读这些快照，自动生成 `barrier` |
| R2 | `gd-submit`：读步骤 6、7 的底表，组装入参，调用毛需求算法 | `INLINE` | 算法是异步的，要等回调，`external` 闸门，超时走兜底预测并打标 |
| R3 | `gd-fix`（步骤 11–14）分流到 `big-net`（16–24）和 `fine-gd-sink`（15）；`big-net` 接 `core-grade`（25–27） | `CLUSTER` | 步骤 28 的产能是全体料号共用的，不能逐 chunk 算，分级结果必须先落表 |
| R4 | `cap-alloc`（28–29）按翻新点切 split，按月份和优先级顺序读；接 `refurb-out`（30–32）和 `refurb-req`（33，并提交 34） | `LOCAL` | 再次等待外部回调 |
| R5 | `fine-merge`（35–40）接 `harvest`（41–46） | `CLUSTER` | 汇总需要两条线的最终结果都落表 |
| R6 | `sum-round`（47–48）、`conserve`（49）、`diff`（50）、`trace`（51）四条并行 | `LOCAL` | 发布前要校验通过，`condition` 加 `manual` |
| R7 | `publish`（52） | `INLINE` | 终点 |

步骤 1"创建计划版本"不是 pipeline，它就是创建 round：计划版本号作为 `bizKey`，数据截止时间和 16 个月的月桶作为 round 参数。

几个步骤的落法值得单独说明：

- **步骤 21–22，滚动净算与全网平衡**。只涉及单个替代组的所有节点和月份，满足 chunk 封闭，在 `big-net` 的 `BatchRunner` 里一次算完。库存、在途、安全库存用 `ctx.lookup()` 按本 chunk 的料号批量查。
- **步骤 28，产能约束**。产能在料号之间按优先级分配，是全局约束。R3 把分级结果落表，R4 的 `cap-alloc` 以翻新点为 split，每个 split 内按月份和优先级有序读出、顺序扣减产能。一个翻新点的数据在一个 split 里，所以这里的 chunk 对"翻新点"封闭。
- **步骤 42，拆解 BOM 展开**。报废件按大部件料号组织，利旧量按细料料号组织，键变了。不做流内重分区：报废件量在 R4 已经落表，R5 的 `harvest` 以细料料号为驱动，用 `lookup` 让 ClickHouse 按细料聚合"报废件 × 拆解 BOM"。
- **步骤 10、34，外部调用**。提交和收结果分在闸门两侧。外部调用的 sink（v1 没有内置 `HttpPusher`，示例里是 `FakeAlgoSink`）用 `<roundId>:<chunkId>@<attempt>` 当批次号，重放不会重复计费或重复计算，下个月的 round 也不会与本月撞号；闸门按批次号清点回调（`externalCountOf`），收齐当前 attempt 的才放行。
- **两条线的两处衔接**（翻新计划量、报废件量）都经过落表的 dataset，不是流边，所以大部件线和细料线之间没有同时运行的耦合。

## 14. 模块划分与落地顺序

下面是设计稿的模块名和里程碑；v1 的实际模块（`pipeliner-*`）见 README「模块一览」，里程碑 M1–M4 均已完成。

```text
pipelinegraph-api             Chunk、Puller、Runner、Pusher、GateCondition、SplitEnumerator
pipelinegraph-core            PipelineGraph、Builder、YAML 解析、Planner、ExecutionPlan
pipelinegraph-runtime         Master（调度、闸门、枚举、归并、背压监测）、Slave（pipeline 线程、续租、回收）
pipelinegraph-channel-redis   Redis Stream 通道、Lua 脚本
pipelinegraph-store-mysql     ControlPlaneStore 的 MySQL 实现
pipelinegraph-store-redis     ControlPlaneStore 的 Redis 实现
pipelinegraph-connectors      MySQL、ClickHouse 的 puller 与 pusher（Kafka、HTTP 为未来工作）
pipelinegraph-admin           管理接口、指标
pipelinegraph-spring-boot-starter
```

| 里程碑 | 内容 | 验收 |
| --- | --- | --- |
| M1 | api、core、`INLINE` 与 `LOCAL` 模式、Redis 控制面 | 单进程跑通 R1–R3 的样例图，`plan().explain()` 输出符合预期 |
| M2 | Redis Stream 通道、`commit.lua`、master 与 slave、背压 | 多 pod 跑 `CLUSTER`；随机杀 slave，结果与不杀时逐行一致 |
| M3 | 五种闸门、MySQL 控制面、人工操作 | 杀 master、重启集群、清空 Redis 三种演练都能续跑 |
| M4 | 监控指标、管理接口、告警 | 压测时能从指标直接定位瓶颈 pipeline |

## 15. 我做的假设

以下几条是设计时默认的选择，与实际情况不符时对应章节需要调整。

- 技术栈沿用现有工程的 Java 8（字节码目标）和 Spring Boot（v1 为 2.7.18）。
- Redis 版本不低于 6.2，单机部署。**Redis Cluster 不支持**（Lua 脚本访问未在 KEYS 中声明的 key）。
- 单个 chunk 序列化后不超过约 1 MB，直接放在 Stream 记录里。如果有更大的行，需要把内容放到外部存储，Stream 里只放引用，本文没有展开。
- 一轮计算的 chunk 数量在百万以内，`pg_chunk` 用单表可以承受。
- 所有落库的结果表都能按"计划版本 + 业务键"做幂等写。
- 外部算法服务支持按批次号幂等，并能回调；批次号是全局唯一的 `<roundId>:<chunkId>@<attempt>`。
- 一个 key 组（`chunkKey` 相同的行）能装进一个 chunk（内存）；超大组不会被拆开。
- 第 13 节的 run 划分基于参考版模型的步骤描述，各 pipeline 的数据量和模式选择是估计的，要用真实数据量校准。
