# m2s

MySQL → StarRocks 实时同步与受限增量计算项目。  
**当前状态：受限 v1 的核心功能已经形成；strict-small 已有通过候选，但正式规模与生产认证尚未完成。**  
更新：2026-10-04。

运行与恢复说明见 [OPERATIONS.md](OPERATIONS.md)；详细实验、提交 SHA、失败证据和历史过程见 [PROGRESS.md](PROGRESS.md)。  
精简前 README 保留在历史提交中，需要追溯设计过程时再查看。

## 1. 最终需求

### 1.1 同步范围

- 单 MySQL 实例 → StarRocks **4.1.1**。
- 默认 **全量回填与 CDC 并行**，不要求用户额外开启 CDC 模式。
- 支持 GTID / binlog 文件位置恢复。
- 全量和增量均保证：
  - 更新不能被旧快照覆盖；
  - DELETE 不能被旧 UPSERT 复活；
  - 本地 durable state、cursor、outbox、watermark 有明确原子边界；
  - HTTP 接受不等于成功，必须确认 StarRocks **VISIBLE**。
- 输出采用 **两阶段事务 + Merge Commit Async**，未知远端结果不得盲目重发。

### 1.2 受限 v1 增量计算

当前 v1 目标支持：

- projection / filter；
- COUNT / SUM / AVG；
- 受限双源 INNER equi-join；
- 动态新增、删除、重建任务；
- compatible source/operator state 共享；
- fixed-watermark W 构建、追赶、发布和安全 GC。

LEFT JOIN、MIN/MAX、DISTINCT、窗口、JOIN+聚合、更通用 SQL 和第二阶段集群化属于后续扩展，不计入当前 v1 完成条件。

### 1.3 正式验收目标

固定资源下完成：

- **5000 万初始行**；
- 持续增量 **50 行/秒**；
- 连续运行 **72 小时**；
- 健康状态下，已 ready 任务从 MySQL commit 到 StarRocks queryable：
  - **P95 ≤ 5s**
  - **P99 ≤ 10s**
- 故障恢复、动态任务 time_to_ready、CPU/RSS、磁盘、WAL、rowset/version debt、积压必须单独记录。
- 完整 event / aggregate / JOIN oracle 必须一致，最终 drain 后不得残留未处理 delivery。

## 2. 已完成

以下指**主线已有能力**，不等于已经完成生产认证。

- native row-event → Arrow 通路及 Python fallback。
- durable cursor / source state / journal / replay。
- capture 与 base apply 解耦，全量与 CDC 并行。
- fixed-W、versioned source、pin / consumer、状态引用与 GC。
- aggregate / JOIN 的增量撤回语义、bootstrap、generation、outbox。
- 动态任务 hot-add / drop / rebuild、shadow rebuild、fence / swap 和重启恢复。
- compatible state sharing、owner/follower、准入与资源等待。
- durable delivery assignment / TxnId / VISIBLE 查询与恢复。
- baseline、native、source-state、真实 daemon E2E、smoke、strict-small、完整 oracle、故障注入和 artifact 工具。
- SQLite online backup、状态检查、磁盘与版本债务观测。

### 当前最强候选：PR #58

[PR #58](https://github.com/justgo4/m2s/pull/58) 组合了：

- frozen-W follower / owner promotion 分块；
- FIFO SQLite writer admission；
- `CDC_MERGE_COMMIT_INTERVAL_MS=500`。

该候选在 exact-head strict-small 中：

- healthy P95 / P99：**4.356s / 7.381s**
- recovery P95 / P99：**2.250s / 2.821s**
- recovery max：**3.800s**
- 115,000 行 event / aggregate / JOIN digest：**全部一致**
- 4 个动态任务：**全部 ready**
- 最终 `pending=0`、`deliveries=0`
- max rowset：**59**
- 最大 SQLite writer acquire：约 **0.354s**
- 最大 writer hold：约 **0.293s**
- version recovery：未触发

**重要：PR #58 当前仍是 open/draft，未合并到 main，因此只能称为“已验证候选”，不能算主线已交付。**

已确认的参数结论：

- `CDC_BATCH_MS=1000→200`：没有稳定收益，不采用。
- `CDC_MERGE_COMMIT_INTERVAL_MS=1000→500`：同机 A/B/B/A 中两轮 500ms 均通过 5s/10s 门禁，两轮 1000ms 均失败；500ms 进入最终候选。
- FIFO writer 能显著降低 SQLite writer acquire 长尾，但必须与 500ms merge interval 组合，单独使用时 recovery 会恶化。

## 3. 还没有完成

### P0：收口最终代码

- 将 PR #58 重新基于最新 main 核对差异。
- 确认 draft/mergeability 状态和冲突。
- 冻结**唯一最终 SHA**。
- 在该 SHA 上重新跑：
  - baseline
  - native contracts
  - source-state
  - actual daemon E2E
  - smoke
  - strict-small
  - 完整 oracle / crash / drain

只有最终 SHA 全部重新通过，才能进入规模认证。

### P1：规模与压力验证

仍需逐级完成：

1. million / 中型数据集；
2. 高扇出、skew JOIN、大事务；
3. capture / apply / GC / snapshot 并发争用；
4. backlog、低磁盘、rowset/version debt；
5. 长时间吞吐与资源预算。

### P2：恢复与运维

仍需完成：

- state / catalog / StarRocks 目标的联合备份恢复；
- 真实跨版本升级、回滚演练；
- 低磁盘和版本债务处置；
- 告警与操作手册最终收口；
- Merge Commit 未知结果的自动对账 / 安全解隔离。

目前未知远端结果仍采用**隔离 + 人工恢复边界**，不能宣称所有异常都能自动修复。

### P3：正式生产认证

最终必须在持久、隔离测试机上跑原正式 profile：

- 5000 万初始行；
- 50 行/秒；
- 72 小时；
- 动态任务；
- JOIN 右侧更新；
- 故障注入；
- 完整 oracle；
- drain；
- CPU/RSS/磁盘/WAL/rowset/version debt 全部门禁。

hosted runner 的 strict-small PASS **不能替代**这一步。

## 4. 当前主要问题

### 4.1 PR #58 还不是主线

PR #58 虽然 strict-small 全绿，但目前仍是 draft/open，尚未合并。  
因此当前最重要的工程动作不是继续叠加新优化，而是先把它变成一个可重复验证、可合并的最终候选。

### 4.2 strict-small 通过不代表生产通过

当前最好的 4.356s / 7.381s 只是 hosted strict-small 结果。  
尚未证明在 50M、50 rows/s、72h 下仍能维持相同延迟和资源水平。

### 4.3 规模放大后的风险仍未知

需要重点观察：

- JOIN follower / owner promotion 在更大状态下是否重新出现长事务；
- FIFO writer 在高积压恢复时是否保持公平且不造成吞吐下降；
- 500ms merge interval 的 CPU、写放大、rowset/version debt 是否长期可接受；
- source→selected 延迟在百万级和持续积压时是否再次上升；
- WAL checkpoint / SQLite 文件增长 / GC 是否出现新的尾延迟。

### 4.4 正式长跑缺持久测试环境

临时工作区和 hosted runner 不能保证连续 72h，也无法代表最终机器的磁盘和服务竞争。  
正式认证前必须准备持久隔离主机，并固定 MySQL、StarRocks、m2s 的资源与配置指纹。

## 5. 下一步

按以下顺序继续，不再同时扩散新路线：

1. **收口 PR #58 → 最新 main → 唯一最终 SHA。**
2. 在最终 SHA 上重新跑完整 strict-small 合同。
3. 通过后升级到 million / 中型 / 压力测试。
4. 完成 upgrade / backup / restore / low-disk / backlog 演练。
5. 最后运行 **50M + 50 rows/s + 72h** 正式认证。
6. 全部门禁通过后，才标记受限 v1 可生产上线。

原则不变：**不降低 P95≤5s / P99≤10s 门槛，不删慢样本，不改变计时起点，不把 HTTP 接受当作 VISIBLE，也不把扩大资源后的结果混入同一对照。**
