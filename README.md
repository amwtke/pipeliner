<div align="center">

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="docs/assets/banner-dark.svg">
  <img src="docs/assets/banner-light.svg" alt="pipeliner：把批量计算画成一张图，让它在多个 pod 上流动起来" width="900">
</picture>

<br>

**嵌入 Spring Boot 的分布式流式批量计算框架**

把一次大批量计算描述成一张 pipeline 图，框架把它切成可以断点续跑的小段，<br>
在多个 pod 上并行执行，自带背压、闸门和管理界面。只需要一个 Redis。

<br>

![Java 8+](https://img.shields.io/badge/Java-8%2B-437291?logo=openjdk&logoColor=white)
![Spring Boot 2.7](https://img.shields.io/badge/Spring%20Boot-2.7-6DB33F?logo=springboot&logoColor=white)
![Redis Streams](https://img.shields.io/badge/Redis%20Streams-%E2%89%A5%206.2-DC382D?logo=redis&logoColor=white)
![MySQL](https://img.shields.io/badge/MySQL-8-4479A1?logo=mysql&logoColor=white)
![ClickHouse](https://img.shields.io/badge/ClickHouse-supported-FFCC01?logo=clickhouse&logoColor=black)
![Version](https://img.shields.io/badge/version-0.1.0--SNAPSHOT-8b5cf6)

[快速开始](#-快速开始) ·
[技术原理](docs/architecture.md) ·
[使用指南](docs/user-guide.md) ·
[运维手册](docs/operations.md) ·
[压测报告](docs/benchmark.md)

</div>

<br>

<p align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="docs/assets/pg-ui-demo-dark.gif">
    <img src="docs/assets/pg-ui-demo.gif" alt="内置管理界面：一轮计算在拓扑图上逐段推进，停在人工审批闸门，放行后完成" width="960">
  </picture>
</p>
<p align="center"><sub>内置管理界面的实录：一轮净需求计算在 3 个 pod 上逐段推进，停在人工审批闸门，放行后完成。</sub></p>

## 💡 它解决什么问题

有一类计算既不是实时流，也不是一条 SQL 能做完的：每月或每天跑一次，输入是几百万到上千万行的快照数据，要经过几十个步骤，中间还要等外部算法回调、等人审批。这个项目起源于其中一个：售后预测计划的净需求计算，7 个阶段、52 个步骤。

这类计算常见的写法，各有各的麻烦：

| 常见写法 | 遇到的麻烦 |
| --- | --- |
| 定时任务里写一个大循环 | 单机单线程，数据一大就 OOM；跑到一半挂了只能从头再来 |
| 拆成多个定时任务，用中间表衔接 | 任务之间靠约好的时间衔接；失败后要人工判断从哪一步补；没有统一的进度视图 |
| 引入专门的大数据计算集群 | 要多部署、多运维一套系统；对只有 Redis 和 MySQL 的 Spring Boot 团队来说偏重 |

pipeliner 是一个**库**，不是一套集群。把它的 starter 加进现有的 Spring Boot 应用，应用的每个 pod 就都成了计算节点：

- 你只写业务：数据从哪读（source）、怎么算（runner）、写到哪（sink），再用一张图把它们连起来。
- 框架负责其余的事：切分数据、多 pod 并行、限速、失败重试、断点续跑、等待外部条件、展示进度。

## ✨ 核心能力

<table>
<tr>
<td width="50%" valign="top">

**🧩 用图描述计算**<br>
pipeline = source → runner → sink。用 Java DSL 或 YAML 把 pipeline 连成图，支持分叉、合流和命名输出口。

</td>
<td width="50%" valign="top">

**✂️ 自动规划**<br>
`plan()` 沿着闸门和落表把图切成若干个 run，排好先后阶段，并按数据量为每个 run 选择单线程、单 pod 多线程或多 pod 流式执行。

</td>
</tr>
<tr>
<td valign="top">

**🚦 五种闸门**<br>
等上游落表（barrier）、等校验通过（condition）、等外部回调收齐（external）、等人工审批（manual）、等到指定时间（timer），都带超时策略。

</td>
<td valign="top">

**🌊 逐跳背压**<br>
下游积压到高水位，上游的提交就被拒绝，压力一跳一跳传回源头，直到停止读库。Redis 内存不会被撑爆。

</td>
</tr>
<tr>
<td valign="top">

**🔁 chunk 级断点续跑**<br>
任意 pod 被 `kill -9`，包括 master，别的 pod 自动接手没做完的 chunk。整个集群重启后从断点继续，结果与不出故障时逐行一致。

</td>
<td valign="top">

**🎯 结果不重不漏**<br>
流到流靠一个 Lua 脚本原子提交，恰好一次。写库靠带版本号的幂等写入，重放和僵尸进程的旧写入都覆盖不了新结果。

</td>
</tr>
<tr>
<td valign="top">

**⚖️ 多 pod 自动分压**<br>
所有 pod 在同一个消费组里竞争拉取，谁有空谁干活，不需要 master 分配。瓶颈 pipeline 会被自动调高并行度。

</td>
<td valign="top">

**👑 无状态 master**<br>
master 由选主产生，只做推进和记账，状态全在控制面里。换一个 pod 接手即可完整恢复，冻结后醒来的旧 master 什么也改不了。

</td>
</tr>
<tr>
<td valign="top">

**🖥️ 管理界面与 API**<br>
每个 pod 内置拓扑图界面，实时显示每个 pipeline 的状态和当前跑到哪里。支持暂停、重试、从某一步重跑、跳过坏数据、放行闸门，带 token 认证和审计日志。

</td>
<td valign="top">

**📈 可观测**<br>
Micrometer 指标加 13 条现成的 Prometheus 告警规则：死信、背压持续、chunk 停滞、master 停摆、Redis 内存等。

</td>
</tr>
</table>

另外还有：控制面可以放 Redis（默认）或 MySQL；master 的推进可以由本地线程驱动，也可以交给 xxl-job 这类外部调度器；历史数据按保留策略自动压缩和清理。

## 🔍 一个例子

下面是[示例应用](pipeliner-example)里净需求计算图的节选。完整的图有 8 个 pipeline、4 道手写的闸门，见 [`NetDemandGraphConfig`](pipeliner-example/src/main/java/io/github/amwtke/pipeliner/example/NetDemandGraphConfig.java)。

```java
@Bean
public PipelineGraph netDemandGraph(FakeAlgoSink algoSink) {
    return PipelineGraph.builder("net-demand")
        // 从 ClickHouse 按哈希切成 16 份读出，修正毛需求，分 big / fine 两路输出
        .pipeline("gd-fix", p -> p
            .source(JdbcSources.clickhouse("ch").table("pipeliner_demo.ex_algo_gd").dataset("algo_gd").useFinal()
                    .where("plan_version = :planVersion").hashSplit("group_id", 16).build())
            .batch(GdFixRunner.class).ports("big", "fine")
            .sink("fine", JdbcSinks.clickhouse("ch").table("pipeliner_demo.ex_fine_gd")
                    .versionColumn("pg_version").dataset("fine_gd").build())
            .mode(ExecMode.CLUSTER).parallelism(3).chunkKey("material_id"))

        // 没有 source：直接消费上游 big 输出口流过来的数据，算完写 MySQL
        .pipeline("big-net", p -> p
            .batch(BigNetRunner.class)
            .sink(JdbcSinks.mysql("mysql").table("ex_nd1_big")
                    .upsertKeys("plan_version", "material_id", "node", "month")
                    .versionColumn("pg_version").dataset("nd1_big").build())
            .chunkKey("material_id"))
        .edge("gd-fix.big", "big-net")

        // ……其余 pipeline 略……

        // 外部算法的回调收齐才往下走，10 分钟收不齐就失败
        .gate("algo-callback", Gates.externalCountOf("algo-submit"),
              g -> g.after("algo-submit").before("gd-fix").timeout(Duration.ofMinutes(10), TimeoutPolicy.FAIL))
        // 发布前要人工审批
        .gate("approve", Gates.manual(), g -> g.after("big-final", "fine-net").before("publish"))
        .build();
}
```

业务逻辑写在 runner 里。runner 一次拿到一个 chunk，不保留跨 chunk 的状态：

```java
public class BigNetRunner implements BatchRunner {
    @Override
    public void process(List<Row> input, Collector out, RunContext ctx) {
        // 这个 chunk 里是若干个料号的全部节点和月份；需要的库存用 ctx.lookup(...) 按料号批量查
        // 逐月滚动净算……
        out.emit(Row.of("plan_version", ctx.param("planVersion"), "material_id", id, "node", node, "month", m, "nd1", nd1));
    }
}
```

提交一轮计算：

```java
String roundId = engine.submit("net-demand", "PLAN-2026-11", params);   // 同一个业务键重复提交，返回已有的那一轮
```

框架把这张图规划成 6 个 run、8 道闸门（其中 4 道是根据"谁写的表被谁读"自动加的）：

```text
ExecutionPlan net-demand  hash=d6c39860...  6 runs / 8 gates / 8 pipelines
Stage 1  R1  INLINE   algo-submit
         algo-callback        external  after [algo-submit] -> before [gd-fix]; timeout 600000ms FAIL
Stage 2  R2  CLUSTER  gd-fix → big-net → core-grade
         barrier:fine_gd      barrier   dataset fine_gd; after [gd-fix] -> before [fine-net]
         barrier:grade_result barrier   dataset grade_result; after [core-grade] -> before [cap-alloc]
         barrier:nd1_big      barrier   dataset nd1_big; after [big-net] -> before [big-final]
Stage 3  R3  LOCAL    cap-alloc
         refurb-ready         barrier   after [cap-alloc] -> before [big-final, fine-net]
Stage 4  R4  CLUSTER  big-final
         R5  CLUSTER  fine-net
         conserve-check       condition after [big-final, fine-net] -> before [publish]
         approve              manual    after [big-final, fine-net] -> before [publish]
         barrier:nd_final     barrier   dataset nd_final; after [big-final, fine-net] -> before [publish]
Stage 5  R6  INLINE   publish
```

```mermaid
flowchart LR
    R1["R1 algo-submit<br/>INLINE"] --> G1{{"algo-callback<br/>等外部回调收齐"}}
    G1 --> R2["R2 gd-fix → big-net → core-grade<br/>CLUSTER，多 pod 流式"]
    R2 --> G2{{"barrier<br/>等结果落表"}}
    G2 --> R3["R3 cap-alloc<br/>LOCAL"]
    R3 --> G3{{"refurb-ready"}}
    G3 --> R4["R4 big-final<br/>CLUSTER"]
    G3 --> R5["R5 fine-net<br/>CLUSTER"]
    R4 --> G4{{"conserve-check + approve<br/>校验通过 + 人工审批"}}
    R5 --> G4
    G4 --> R6["R6 publish<br/>INLINE"]
```

## 🚀 快速开始

**准备**：JDK 21（构建用，产物是 Java 8 字节码）、Redis ≥ 6.2、MySQL 8、ClickHouse。示例配置里 Redis 的端口是 `6380`。在一台 Ubuntu 虚拟机上可以用 [`deploy/lima/setup-services.sh`](deploy/lima/setup-services.sh) 一次装好这三个服务。

```bash
git clone https://github.com/amwtke/pipeliner.git && cd pipeliner
mvn -q -DskipTests package

# 启动 3 个 pod（同一个示例应用，端口不同）
cd pipeliner-example
scripts/run-pod.sh pod1 8081
scripts/run-pod.sh pod2 8082
scripts/run-pod.sh pod3 8083

# 造数据，提交一轮计算
api() { curl -s -H "Authorization: Bearer dev-admin-token-change-me" "$@"; }
api -X POST 'localhost:8081/demo/seed?materials=600&seed=42'
api -X POST 'localhost:8081/demo/rounds?planVersion=V1'        # 返回 {"roundId":"net-demand-..."}
```

然后打开 <http://localhost:8081/pg-ui/> 看它跑。这一轮会停在人工闸门 `approve`：在页面右上角填入 token `dev-admin-token-change-me`，点开闸门放行，它就会跑完。

想看它抗不抗揍，跑端到端脚本：它会在计算中途 `kill -9` 一个 worker 和 master，再比较结果的校验和。

```bash
scripts/e2e.sh                                # 默认：Redis 控制面
STORE_PROFILE=mysql-store scripts/e2e.sh      # MySQL 控制面
```

接入自己的应用、各脚本的用途，见 [快速开始](docs/getting-started.md)。

## 🔧 它是怎么工作的

```mermaid
flowchart LR
    subgraph APP["你的 Spring Boot 应用，N 个 pod"]
        direction TB
        M["Master：选主产生，无状态<br/>规划、闸门、状态归并、背压监测"]
        W["Worker：每个 pod 都是<br/>拉取 chunk → runner → 写出 → 原子提交"]
    end
    R[("Redis Streams<br/>数据通道、split 队列、提交日志")]
    S[("控制面存储<br/>Redis 或 MySQL")]
    SRC[("源库<br/>MySQL / ClickHouse")]
    SNK[("结果库<br/>MySQL / ClickHouse")]
    UI["管理界面、REST API、Prometheus"]

    M -- "期望状态、split" --> R
    R -- "提交事件" --> M
    M <--> S
    W <--> R
    SRC --> W
    W --> SNK
    UI --> M
```

五句话讲完原理：

1. **chunk 是唯一的工作单位。** 一次拉取一个 chunk，处理完，把"确认输入、写出结果、记录事件"在一个 Lua 脚本里原子提交。传输、重试、断点、监控都以 chunk 为粒度。
2. **闸门和落表把图切开。** 需要全量数据才能算的步骤，先落表，过一道闸门，再由下一段重新读取。切出来的每一段叫一个 run，是最小的重跑单位。
3. **master 只推进，不碰数据。** 它每秒把状态往前推一步，每一步都是"读当前状态，条件更新"，重复执行没有副作用，所以随时可以换人。
4. **背压靠拒绝提交。** 下游通道到了高水位，上游的提交被整体拒绝；上游带着算好的结果等待，不再拉新数据，压力自然向源头传。
5. **任期号加版本号防僵尸。** master 的每次写入都带任期号，旧 master 的写入被拒绝；每次写库都带版本号，旧的重试覆盖不了新结果。

每一条的细节：

| 想了解 | 看这里 |
| --- | --- |
| 整体架构、规划算法、master 与 worker 的循环 | [技术原理](docs/architecture.md) |
| 为什么杀掉任何 pod 结果都对 | [一致性与容错](docs/consistency.md) |
| 上游快、下游慢时会发生什么 | [背压与流控](docs/backpressure.md) |
| 设计时的完整推演 | [设计文档](docs/pipelinegraph-design.md) |

## 📊 性能

示例的净需求计算，在一台笔记本上测得（数据库跑在 4 核虚拟机里，且是低资源配置）：

| 物料数 | 每阶段行数 | pod 数 | 整轮耗时 | Redis 内存峰值 |
| ---: | ---: | :---: | ---: | ---: |
| 3,000 | 24 万 | 3 | 25.6 秒 | 18 MB |
| 12,000 | 96 万 | 6 | 85.1 秒 | 32 MB |
| 40,000 | 320 万 | 6 | 226.5 秒 | 59 MB |

同样 6 个 pod，数据量涨到 3.3 倍，耗时涨到 2.7 倍。Redis 里始终只有几十 MB，因为背压把积压限制在水位线附近。7 个场景的完整数据、每个 pipeline 的吞吐和瓶颈分析见 [压测报告](docs/benchmark.md)。

## 🧪 可靠性验证

这些测试都在仓库里，可以自己跑：

| 测试 | 做了什么 | 判定 |
| --- | --- | --- |
| 504 个单元与集成测试 | 存储契约测试同时跑在 Redis、MySQL 和内存实现上 | 全部通过 |
| `e2e.sh` | 计算中途 `kill -9` 一个 worker 和 master | 校验和与无故障时一致 |
| `restart-resume.sh` | 计算中途 `kill -9` 全部 pod，再重启 | 从断点续跑，校验和一致 |
| `chaos.sh` | 多轮并发，随机强杀、冻结（制造僵尸 master 和僵尸 worker）、暂停 | 每轮结果的校验和与单线程基线一致 |
| `chaos-admin.sh` | 重跑、重试、取消等管理操作与故障同时发生 | 同上，且不留残余数据 |

后四项都在 Redis 和 MySQL 两种控制面上各跑一遍，全部通过。

## 📚 文档

| 文档 | 内容 |
| --- | --- |
| [快速开始](docs/getting-started.md) | 跑通示例，接入自己的 Spring Boot 应用 |
| [使用指南](docs/user-guide.md) | 定义图、写 runner、source 与 sink、闸门、执行模式 |
| [技术原理](docs/architecture.md) | 架构、规划算法、master 与 worker 如何协作 |
| [一致性与容错](docs/consistency.md) | 原子提交、任期号、版本化写入、各种故障如何恢复 |
| [背压与流控](docs/backpressure.md) | 三层背压、水位、自动扩并行度、内存限流 |
| [运维手册](docs/operations.md) | 管理 API、界面、认证、指标与告警、故障处理 |
| [配置参考](docs/configuration.md) | 全部配置项与默认值 |
| [压测报告](docs/benchmark.md) | 测试方法、数据、瓶颈分析 |
| [设计文档](docs/pipelinegraph-design.md) | 完整的设计推演，以及如何套到 52 步的净需求模型上 |
| [变更记录](docs/changelog.md) | 不兼容变更 |

## 🧱 模块

| 模块 | 职责 |
| --- | --- |
| `pipeliner-api` | 用户实现的接口：`SplitSource`、`RowRunner`、`BatchRunner`、`Sink`、`GateCondition`、`Lifecycle` |
| `pipeliner-core` | 图模型与 builder、YAML 解析、规划器、引擎接口、控制面存储接口 |
| `pipeliner-runtime` | 引擎实现：master、worker、本地执行、pipeline 实例的按需创建与释放 |
| `pipeliner-dataplane-redis` | 数据面：Redis Streams 通道与 Lua 脚本 |
| `pipeliner-store-redis` · `pipeliner-store-mysql` | 控制面存储的两种实现 |
| `pipeliner-connectors` | MySQL 与 ClickHouse 的 JDBC source 和 sink |
| `pipeliner-spring-boot-starter` | 自动配置、管理 API、认证、指标、内置界面 |
| `pipeliner-example` | 净需求计算示例，以及 e2e、混沌、压测脚本 |

## 🧭 适用范围

**适合**：有界的批量计算（数据来自快照，跑完就结束）；步骤多、有先后依赖；中间要等外部系统或人工；希望复用现有的 Spring Boot 应用和 Redis，不想多运维一套集群。

**不适合**：无界的实时流计算；需要流与流 join、窗口、乱序处理的场景；不能接受"至少一次投递加幂等写入"的写库语义。

当前版本的限制：

- 只支持单机 Redis，不支持 Redis Cluster。
- `LOCAL` 模式的 run 只在一个 pod 上执行。
- 一个 split 会被整个读进内存，过大的 split 需要换更细的切分键。
- 内置的 source 和 sink 只有 MySQL 与 ClickHouse；调用外部接口需要自己实现 `Sink`。

完整列表见 [运维手册的已知限制](docs/operations.md#已知限制)。

## 🔭 路线图

- [ ] 大 split 分页拉取，不必整个读进内存
- [ ] 按多列组合切分数据源
- [ ] 条件闸门移出 master 的推进线程，并加超时
- [ ] Kafka source、HTTP sink
- [ ] Redis Cluster 支持
- [ ] 自动扩缩容的指标

## 🤝 参与

发现问题或有想法，欢迎提 [issue](https://github.com/amwtke/pipeliner/issues)。提交代码前请先跑通 `mvn verify` 和 `pipeliner-example/scripts/e2e.sh`。
