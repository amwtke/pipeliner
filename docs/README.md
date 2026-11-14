# pipeliner 文档

按你想做的事来找：

## 我想用起来

| 文档 | 内容 |
| --- | --- |
| [快速开始](./getting-started.md) | 跑通示例，把 starter 接进自己的 Spring Boot 应用 |
| [使用指南](./user-guide.md) | 定义图、写 runner、source 与 sink、闸门、执行模式 |
| [配置参考](./configuration.md) | 全部配置项、默认值、校验规则 |

## 我想弄懂它

| 文档 | 内容 |
| --- | --- |
| [技术原理](./architecture.md) | 核心模型、架构、从图到执行计划、master 与 worker 如何协作 |
| [一致性与容错](./consistency.md) | 为什么杀掉任何 pod 结果都对：原子提交、任期号、版本化写入、故障恢复 |
| [背压与流控](./backpressure.md) | 上游快、下游慢时发生什么：三层背压、水位、自动扩并行度、内存限流 |
| [设计文档](./pipelinegraph-design.md) | 完整的设计推演，以及如何套到 52 步的净需求计算模型上 |

建议的阅读顺序：技术原理 → 一致性与容错 → 背压与流控。设计文档是最完整的一份，适合想了解每个决定来由的人。

## 我要运维它

| 文档 | 内容 |
| --- | --- |
| [运维手册](./operations.md) | 管理 API、管理界面、认证、外部调度、数据保留、指标与告警、故障处理、已知限制 |
| [压测报告](./benchmark.md) | 三档数据量的实测结果与瓶颈分析，以及如何复现 |
| [变更记录](./changelog.md) | 不兼容变更 |

## 其他

- 告警规则：[`deploy/prometheus/pipeliner-alerts.yml`](../deploy/prometheus/pipeliner-alerts.yml)
- 测试用的依赖服务安装脚本：[`deploy/lima/`](../deploy/lima)
- 示例应用：[`pipeliner-example/`](../pipeliner-example)

回到 [项目首页](../README.md)。
