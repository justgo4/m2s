# m2s

MySQL → StarRocks 的实时同步与增量计算项目。**当前受限 v1 的功能与恢复基线已形成，严格延迟和生产交付验收尚未完成。** 更新：2026-10-04。

运行与恢复见 [OPERATIONS.md](OPERATIONS.md)；提交、测试 SHA、失败证据及续接任务见 [PROGRESS.md](PROGRESS.md)。[精简前 README](https://github.com/justgo4/m2s/blob/51509f57001186202df49f92506c03ac9cac00a4/README.md)保留完整设计、历史评审、运行命令和研究路线。

## 1. 最终要实现的需求

| 范围 | 验收要求 |
|---|---|
| 第一阶段：单机受限 v1 | 单 MySQL 实例 → StarRocks **4.1.1**，稳定主键、ROW/FULL binlog；默认全量回填与 CDC 并行。支持 GTID/文件位置恢复，不改 StarRocks 默认服务端参数 |
| 同步与计算 | 已登记表/列上的投影、过滤、COUNT/SUM/AVG、受限双源 INNER equi-join；运行时新增、删除、重建任务，共享兼容源/算子状态 |
| 延迟与规模 | **5000 万初始行＋50 行/秒＋连续 72 小时**；健康运行中已就绪任务 MySQL commit → StarRocks queryable **P95≤5s、P99≤10s**；新增任务另报排队和 time_to_ready，故障恢复单独报告 |
| 正确性与恢复 | 全量不能覆盖更新或复活删除；完整源事务、计算状态/outbox/水位保持各自原子边界；固定水位 W 构建、追赶、可见后发布；强退、重连和重启可恢复 |
| 输出协议 | 支持两阶段事务及 Merge Commit Async 输出路径；全量/增量均遵守 durable journal、顺序与可见性确认。HTTP 接受不能代替 VISIBLE，未知远端结果禁止盲目重发 |
| 资源与运维 | 固定资源下验证 CPU/RSS、状态/目标空间、WAL、版本债务、积压、低磁盘、备份恢复及升级回滚；正式 profile 的 m2s 8GiB 配置预算不等于整机预算 |
| 后续扩展 | 更多 SQL、跨算子增量编译与共享状态、成本规划、公平对标、权限/MCP 控制面和第二阶段集群化，分项设计与验收；不作为当前受限 v1 已完成能力 |

不支持的 SQL、破坏性 DDL、日志缺口及不可读历史明确拒绝或重建。保存当前关系与必要的有界变化历史，不承诺从未捕获的列或已回收历史重建任意查询；多目标对外原子可见也不作未经证明的承诺。

## 2. 已完成事项

“已完成”指代码与相应合同已落地主线；不等于正式规模、延迟或生产认证。

| 主线能力 | 已有实现与证据 |
|---|---|
| 持久同步与源状态 | native 解码/Arrow 通路、持久日志/cursor、capture 与 base apply 解耦、磁盘 scratch/set-wise DML、背压与重放一致性检查 |
| 固定 W 与回收 | versioned source、pin、consumer、物理状态 identity/ref/health；按可读范围和保留者约束 GC |
| 受限增量计算 | aggregate/JOIN 的撤回语义、bootstrap、状态/consumer/outbox 提交、generation 与 writer bridge；完整行及随机化 oracle |
| 动态任务生命周期 | catalog/daemon 部署，hot-add/drop，语义变更的 shadow rebuild、fence/swap、cohort 与重启恢复 |
| 状态共享与准入基线 | exact/subview follower、owner promotion、依赖引用；compatible/off/adaptive 规则、共享偏好及 durable 资源准入/等待/退避。已有 100-task 合同，尚非生产规模证明 |
| 输出恢复与隔离 | durable assignment/TxnId、明确 VISIBLE 后的本地登记重试、重启续查；未知请求隔离目标，保留 journal |
| 验证与运维工具 | baseline/native/state/真实 daemon E2E、监督 workload/gate、完整行校验与失败 artifact；状态/磁盘检查及 SQLite online backup |

**候选代码已实现，但未合并、未达标：** 有界事件追踪、同机 A/B/B/A 驱动、已知 TxnId 等待流水线、只读准入预检、CDC 合批和冷历史构建准入（[PR #46–52](https://github.com/justgo4/m2s/pulls)）。它们不能计为主线已交付的性能收益。

最新候选 [PR #52](https://github.com/justgo4/m2s/pull/52) 的基线/native/state/8 格 E2E/smoke 全通过，100k 严格 small 的完整 oracle、4 个动态任务、强退恢复和排空也通过；**P95/P99=23.33/29.37s，延迟失败**。[同机对照 PR #53](https://github.com/justgo4/m2s/pull/53)已结束，工作流门禁失败，逐轮分析与方案收口仍待完成。不同 runner 的数字不能直接证明收益或回退。

## 3. 未完成事项

| 优先级 / 范围 | 待完成工作 |
|---|---|
| 1 / 受限 v1 | 收口健康长尾来源；缩短可变 leader 的 follower 初始化与 owner promotion 大事务，实现安全的可恢复分块；完善冷工作预算、依赖感知调度与必要的延迟反馈 |
| 2 / 受限 v1 | 分析同机对照，区分准入、批次大小、并发的收益与代价；选择有效候选并在**同一最终 SHA**重新验证正确性、恢复、任务就绪时间、资源和原严格延迟门槛 |
| 3 / 受限 v1 | 大事务、高扇出/skew JOIN、capture/apply/GC 争用、积压和低磁盘的压力测试、资源预算校准与处置验证 |
| 4 / 受限 v1 | state、catalog、远端目标的联合备份恢复，以及真实跨版本升级/回滚演练；收口操作与告警流程 |
| 5 / 正式认证 | 配置持久隔离测试机，逐级完成中型/规模/持续压力筛查，再运行原 `p11-50m-50rps-72h-v4`：10 个动态任务、故障及 JOIN 右侧更新、完整 oracle、drain 和资源/债务门禁 |
| 后续扩展 | LEFT JOIN、MIN/MAX、DISTINCT、窗口、JOIN＋聚合、更通用 SQL/共享 arrangement/成本优化器、权限/MCP 与集群化 |
| 当前功能边界 | Merge Commit 未知结果的自动对账/安全解隔离尚未实现；目前采用隔离与人工恢复边界，不能承诺自动处理所有故障 |

smoke 只证明开发范围内的合同，不能替代严格 small；5000 万行短跑也不能替代连续 72h。当前 small 的 sentinel 测 raw events 可见延迟，仍需完善各 stateful target 的对应 SLO 证据。


### 接下来执行的工作（2026-10-04）

| 顺序 | 具体动作 | 通过条件与边界 |
|---|---|---|
| 1 | 分析已结束的 PR53 四轮同机对照，逐轮记录健康/恢复延迟、task ready、CPU/RSS、写入量和精确性；确定下一轮的固定基线 SHA | 保留全部失败，区分冷准入与256行批次的组合效果；不把不同runner成绩作因果结论 |
| 2 | 对同一固定 SHA 分别做配置对照：先 `CDC_BATCH_MS` 1000→200，再独立比较 `CDC_MERGE_COMMIT_INTERVAL_MS` 1000→500；必要时再测100/300 | 每次只改一个参数，同机A/B/B/A、相同2CPU/4GiB开发预算与服务指纹、新隔离数据；同时检查小事务/rowset/版本债务、吞吐和CPU，不预设能回收1.5秒 |
| 3 | 同期推进共享JOIN follower初始化/owner promotion的可恢复分块：先复核已有PR40方案，再与选定基线整合 | fixed-W可读、pin/consumer/GC、generation、状态/outbox与发布边界不变；覆盖每个持久边界强退、重启、leader变化及删除；测持锁max和总构建成本，不直接合并旧失败候选 |
| 4 | 按trace检查prepare两次写事务的必要性、可见性查询/连接成本和WAL checkpoint进度；仅对有实测收益的部分做独立候选 | HTTP前持久请求意图与响应后TxnId登记继续分开；5秒兜底须先取命中证据；fixed-W逻辑pin不能当成长SQLite读事务证据 |
| 5 | 大事务缩短后，如仍有明显等锁长尾，再试覆盖各模块的公平/优先级写闸门 | 验证锁顺序、取消/错误释放、ready任务进展及GC/构建不饥饿；不能把闸门当作长事务抢占器 |
| 6 | 选择有效组合，冻结最终SHA，重跑完整合同与原严格small；通过后逐级升至million/中型/压力、升级恢复，再做正式50M/72h | 原P95≤5s/P99≤10s、完整oracle、故障、drain及资源门禁不变；各stateful target的SLO证据也要补齐；正式长跑需先配置持久隔离主机 |

**同lane并发暂不实施。** 先证明DELETE/tombstone、同键版本顺序和generation隔离；只有 `_cdc_seq` 列不足以防止旧UPSERT在新DELETE后复活数据。拆库/替换状态引擎继续以后续测量为依据。以上均是待执行工作，不是已实现或已达标声明。

## 4. 遇到的困难与解决原则

- **写锁与历史输出仍有长事务。** PR #52 的 JOIN follower 初始化单次持锁约 **11.01s**；此前 million 主线观测到约 **155.77s**。串行准入、行数限制都不能抢占已开始的事务，需要在固定 W、pin/consumer、outbox 和发布边界下实现可恢复分块。
- **小批次会增加提交和 CPU 成本。** 本地 1 万行 aggregate 诊断中，4096→256 行使构建步数 3→40，CPU 约 0.29→0.97s，输出一致；这不是端到端结论。限速必须同时核对 time_to_ready、积压、磁盘和总体吞吐。
- **源龄与竞争原因尚未全部关联。** 采样 trace 有丢失和覆盖边界；不能把全部长尾都归给某个锁。当前健康/恢复样本已分开；重启约 0.05s 与持续造数下几十至上百秒的追赶是不同指标。
- **简单隔离可能破坏一致性或造成阻塞。** 独立进程仍竞争同一 SQLite writer；拆库须设计跨库持久协调。暂停所有 snapshot 可能挡住同 lane 的 CDC，影子目标也不会自动消除本地锁竞争。
- **正式长跑缺持久资源。** 未配置持久测试机/云身份；临时工作区与 hosted runner 不能保证单机连续 72h。测试机还需容纳 MySQL、StarRocks、多目标数据、WAL 和完整扫描，不能只按 m2s 内存预算估算。
- **证据必须保持可比。** 同机 A/B/B/A、固定 SHA、镜像/资源指纹和新隔离数据；保留失败。禁止删慢样本、改计时起点、以 HTTP 接受冒充可见、扩大资源后混报，或降低原 5/10s 门槛。状态引擎/native 替换须由测量支持，不能先验视为必做。
