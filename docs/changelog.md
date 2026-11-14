# 变更记录

这里记录不兼容的变更。升级前请逐条核对。

## 相对 0.1.0-SNAPSHOT `4032200` 的不兼容变更

- **接口**：`ControlPlaneStore.savePendingChunks(..., long epoch)` 与 `setRoundIncomplete(roundId, epoch)`（返回 `boolean`）带 epoch 并按 round 级 fence 校验；`DataPlane.deadLetter(...)` 返回 `boolean`（真正移入死信才为 true）；`PipelinerEngine.signalGate` 返回 `boolean`（过时回调返回 false）；`SinkContext.batchId(chunk)` 取代旧的两参 / 三参 `batchId` 重载（格式 `<roundId>:<pipelineId>:<chunkId>@<attempt>`）；`GateContextImpl` 的 8 参构造器和 `ChunkEvent.runAttempt` 已删除。新增的接口方法（`maxMasterEpoch`、`reserveMasterEpoch`、`maxRoundSeq`、`pendingPurges` / `clearPendingPurges`、`DataPlane.nextRoundSeq(floor)`、`groupExists` 等）都有默认实现，旧的自定义实现仍可编译。
- **存储结构**：MySQL 新增 `pg_master_epoch`、`pg_round_seq`、`pg_pending_purge` 三张表，`pg_gate_signal.signal_key` 变为 `VARCHAR(512)`，`round_id` / `gate_id` / `pipeline_id` 加宽（`init()` 自动 `ALTER`）；Redis 控制面新增 `pg:store:maxepoch`、`maxseq`、`pendingpurge`；数据面新增 `pg:{r}:keys`（key 注册表）。示例应用新增 MySQL 表 `ex_algo_request`（`FakeAlgoService` 的持久化请求）。
- **标识符与参数校验**：图名 ≤ 64、gate id / pipeline id ≤ 128（含自动 `barrier:<dataset>`），pipeline id 和输出口名不得含 `:`；`parallelism` ≥ 1；`max-attempts` ≥ 1；`RuntimeConfig` 各项在引擎启动时校验。run 的 attempt 必须 < 1000：master 在 `rerunFrom` / `retryRun` / 本地 run 重新分配时拒绝让 attempt 达到 1000。
- **行值**：`Row.set/of` 规范化为统一类型（整数 → `Long`，浮点 → `BigDecimal`，时间 → ISO 字符串），`byte[]`、`UUID` 等不支持的类型直接抛 `IllegalArgumentException`。
- **规划**：`pipeliner.plan.force-mode` 覆盖所有 `.mode()` 提示；类型形式（`row(Class)`）且读取别的 dataset 的 runner 必须声明 `public static Collection<String> readDatasets()`（不要再实现 `ReadsDatasets`，同一个类不可能同时有同签名的实例方法和静态方法），否则 plan 失败。
- **闸门与管理操作**：`externalCountOf` 按当前 attempt 去重计数，过时回调被拒；`openGate` 默认要求所有 after-run 已 SUCCEEDED，需 `force=true` 才能越过；`retry` / `skip` 在多种状态下被拒绝（见[运维手册](./operations.md#管理操作规则)）；管理命令带 id 和截止时间，超时即撤回；leader 对 plan hash 不同的 round 拒绝操作。
- **认证**：`AdminSecurityFilter`（servlet 过滤器）改为 `AdminSecurityInterceptor`（Spring MVC 拦截器），包名不变；不再有过滤器 Bean。
- **运行时**：EXTERNAL 模式下 leader 由本地 `pg-lease-keeper` 线程保活（见[运维手册](./operations.md#外部触发-master-推进)）；`RuntimeConfig.stopJoinMillis`（默认 10000）限制 `stop()` 等待 worker / 本地 run 线程的时间；master 能识别数据面丢失（含只丢了消费组）。
