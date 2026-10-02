# m2s

单机 MySQL → StarRocks 实时同步与动态 SQL 计算项目。

目标：一次捕获源数据，已有任务持续更新；运行期间新增 SQL 和下游表，完成历史构建后持续增量维护。共享源镜像完整后，新任务正常情况下不再扫描 MySQL，也不默认复制整份基础数据。未来 MCP 自然语言入口复用同一套 SQL 校验与部署协议。

**当前已是可运行的 CDC + shared fixed-W 动态 SQL 基线；COUNT/SUM/AVG 与受限双源 INNER equi-join 已接入用户 catalog/daemon，支持无需重启的 stateful hot-add/drop，并持续用真实 MySQL→StarRocks 合同验证。** shared 模式已经接入 authoritative source log/base、generation 生命周期、drop/drain，以及 correctness-first 的跨任务状态复用：相同 aggregate/JOIN 可只维护一个 compute state；aggregate 的聚合输出子集与 JOIN 的投影子集可复用 superset state，保留独立 target/outbox/frontier；owner 退役时 follower 可在固定 frontier 提升为私有投影状态。当前还加入 durable sharing decision/telemetry、compatible/off/adaptive 准入和确定性的整图 leader preference。legacy 模式仍保留 MySQL snapshot 路径。本文区分已有证据、待实现协议和研究候选；不宣称已达到物理极限、生产就绪或全面超过其他引擎。

## 1. 场景与待验证假说

第一期限定单 MySQL 实例、单机、稳定主键、ROW + FULL row image，支持 GTID/文件位置恢复；显式登记源表及允许列。StarRocks 固定 **4.1.1 主键表、默认服务端参数**。不支持 SQL、破坏性 DDL、日志缺口必须拒绝或显式重建。

核心负载：**5000 万历史行 + 50 rows/s**。初始回填与 CDC 并行，旧回填不得覆盖新版本或复活删除。健康运行时已就绪任务目标为 MySQL commit → StarRocks queryable **P95 ≤ 5 秒、P99 ≤ 10 秒**；新增任务另报 `time_to_ready` 与完整性。

默认保存当前关系和有界变化历史，不永久保存数据库全部历史。未来任意新增查询仅指届时支持的、确定性的 SQL 范围；未捕获的列和已丢弃的历史无法凭空重建。**系统某处必须持久保存足以精确重建当前关系的信息**，但不要求存在一份字面上的逐行完整副本；压缩列式、factorized state、base+delta 等表示只要满足同一语义与恢复合同都可接受。

待验证的价值是：**在既有任务 SLO 和资源预算内，结合可恢复的在线构建、共享状态与状态放置，降低新增任务的构建时间、重复存储和持续维护成本。** 增量计算、共享索引和代价规划均有先例；这里的贡献需要联合协议、实验和反例检验来证明。

## 2. 当前代码与证据

- [j4.py](j4.py)、[cdc_catalog.py](cdc_catalog.py)：daemon、SQL catalog、动态部署。用户 catalog 已暴露确定性的单源投影/过滤/宏/模型视图，以及 v1 的 COUNT/SUM/AVG 与受限双源 INNER equi-join；窗口、JOIN+聚合、子查询等更广 stateful SQL 继续 fail-closed。`CDC_SHARED_SOURCE_STATE=1` 是 stateful catalog task 的硬前提；legacy 模式仍保留 MySQL `snapshot_worker`。
- hot-add / drop 已有持久 generation 生命周期：`building → history_staged → ready/retired`；drop 会先 drain 旧 durable jobs，再退出 worker。同名 sink re-add 仍 fail-closed；retained sink 的 SQL/filter/macro/UDF 语义变化已走在线 new-generation/shadow rebuild，并带远端 marker/fence/swap、强退续建与多 sink 协调。
- 当前组件包括 Python 控制/恢复、C 解码/批处理、Arrow、DuckDB 和 SQLite。native 路径仍含 Python 网络/协议与 IPC 成本，不等于完整 C replication client 或零拷贝；是否替换组件由 profile 决定。
- [incremental_contract.py](incremental_contract.py)：state identity / retention / Pareto 合同；[physical_state_catalog.py](physical_state_catalog.py) 持久化 semantic/backend/format/generation/readable-range/health/refs/pins，fixed-W pin 已按 state+owner 做重启幂等。
- [source_state.py](source_state.py)：P6A/P6B correctness-first SQLite authoritative source state，已接入 daemon，显式区分 log durable 与 base applied，提供 fixed-W pin/read、durable consumer，以及基于历史索引的限额批次 GC；daemon 以小批次持续回收，避免一次性大 DELETE 放大 WAL/写锁尾延迟。当前 point/update/snapshot 路径固定走主键索引、历史回收固定走 `source_versions_gc`，旧的宽 `source_versions_visible` 索引已在安装/升级时移除以减少版本 churn 写放大。底层 changelog/snapshot 读取同时按 min_readable_seq fail-closed，旧水位不会静默读取截断后缀。
- [relational_ir.py](relational_ir.py)、[incremental_ir.py](incremental_ir.py)：当前 stateless SQL 的 canonical relational IR 和 delete/upsert retract IR；执行器仍沿用现有路径。
- [aggregate_ir.py](aggregate_ir.py)、[aggregate_state.py](aggregate_state.py)、[aggregate_generation.py](aggregate_generation.py)、[aggregate_runtime.py](aggregate_runtime.py)、[aggregate_task_catalog.py](aggregate_task_catalog.py)、[aggregate_task_runner.py](aggregate_task_runner.py)：COUNT/SUM/AVG 候选闭环，包含 fixed-W、撤回、crash recovery、durable outbox/descriptor 和两种 StarRocks 输出协议的真实合同。
- [join_ir.py](join_ir.py)、[join_state.py](join_state.py)、[join_generation.py](join_generation.py)、[join_runtime.py](join_runtime.py)、[join_task_catalog.py](join_task_catalog.py)、[join_task_runner.py](join_task_runner.py)：受限双源 INNER equi-join 已形成 catalog/daemon 闭环；用稳定 source-PK pair identity 保留 SQL bag 语义，支持双表共同 W、同事务净差分、NULL 不匹配、fan-out、retract、durable outbox/descriptor。真实 StarRocks transaction/merge_async 合同已通过 E2E，并有 randomized/full-state oracle。
- [aggregate_shared_runtime.py](aggregate_shared_runtime.py)、[join_shared_runtime.py](join_shared_runtime.py)、[stateful_share_policy.py](stateful_share_policy.py)：共享 stateful compute 的 correctness/admission 层。exact sharing 与严格 subview sharing 均不建立 follower chain；follower 保有独立 durable journal/target frontier，leader retirement 会先冻结 frontier、投影/克隆必要 state，再解除依赖。scale contracts 覆盖 100 个 exact aggregate、100 个 aggregate subview、100 个 exact JOIN、100 个 JOIN projection subview，以及 100 个退休 follower 的 retention/GC 回收。
- [native/](native/)、[tools/](tools/)、[cdc_selftest.py](cdc_selftest.py)：解码差分、恢复、协议、故障注入、randomized oracle 和候选性能测量。

| 已验证范围 | 可核查证据 | 证据边界 |
|---|---|---|
| daemon + MySQL 8.4.6 → StarRocks 4.1.1；GTID ON/OFF、两输出协议、动态第二下游、断线/强退恢复、逐字段 oracle | [E2E](https://github.com/justgo4/m2s/actions/runs/36787425832)、[样本](reports/e2e-20261001.json)、[回填强退](https://github.com/justgo4/m2s/actions/runs/36787945824) | 短测，非 50M/72h |
| Python/C 解码差分、真实 MySQL、多类型、durable cursor、sanitizer | [integration](https://github.com/justgo4/m2s/actions/runs/36795348569)、[baseline](https://github.com/justgo4/m2s/actions/runs/36795348655) | 正确性/恢复，不是性能结论 |
| merge 已接受但响应丢失；未知请求持久化并隔离目标，独立目标继续 | [网络故障 CI](https://github.com/justgo4/m2s/actions/runs/36795348744)、[报告](reports/merge-quarantine-20261001.json) | 尚无自动远端对账/解隔离 |
| SQLite / DuckDB / RocksDB 状态候选；fixed-W 与 crash recovery 原型 | [layout](reports/state-layout-20261001.json)、[RocksDB](reports/state-rocks-20261001.json) | 小规模、未在线 pin/GC、未接 daemon |

2PC 与 merge_commit async 是已独立验收的两条输出协议。默认 `merge_async`，可选 `transaction`；不能因同时设置 header 就声称双机制已叠加。已知事务 ID 的恢复与“是否被接收也未知”的请求必须分开处理，后者不得盲目重放。

## 3. 必须先成立的运行协议

### 3.1 事务、水位与完整性

目标以源为中心：单一事务流只向共享基础状态和 changelog 持久化一次，再由任务消费；新增任务不新增 replication reader。基础状态保存可重建当前关系的信息，共享 arrangement/subview 保存可复用索引或派生结果，task generation 保存私有算子状态、构建进度和 outbox。

必须分别持久记录：

| 水位 | 含义与原子边界 |
|---|---|
| `source_durable` | 完整源事务的 base 变更、changelog、源 cursor 形成同一 durable commit boundary；同引擎可原子提交，跨引擎必须用显式可重放协调协议，不能用两个独立 commit 冒充原子性 |
| `task_compute` | 某个 generation 的算子状态、输出 delta/outbox、计算进度原子提交 |
| `target_visible` | **每个 target / ordered domain** 各自维护可查询的连续完成 frontier；并发完成的最大序号不能越过空洞。任务级发布 frontier 只能由所有必需输出 frontier 的共同前缀推导 |

提交序号须与 source UUID/epoch、GTID 或文件位置持久关联。初始跨表镜像也须证明共同切面；每张表分别扫描完成不足以证明全局一致。跨表 JOIN 在同一个源事务切面计算。HTTP 接收不等于可见；本地 commit boundary 也不等于多个 StarRocks 目标跨表原子可见，后者只有独立协议证明后才能承诺。

`complete(W)` 与 watermark 分开记录：完整扫描前，进度领先不代表关系完整。单源同步可明确提供未完整的实时预览；JOIN/聚合未完成初始化时不能发布为完整结果。

### 3.2 Fixed-W 构建与回收

新任务流程为：**原子取得 W 与保留句柄 → 读取一致 S(W) → 构建 generation → 重放 Δ(W, now] → 追赶 → 输出可见 → 发布**。源 CDC 在构建期间继续推进。

不能扫描变化中的 latest 再重放旧 W，也不能只 pin changelog；必须同时保留 S(W) 所需的数据版本、schema、tombstone/索引/文件及 W 后日志，并让 snapshot/pin 获取与 GC 无竞态。

持久 manifest 记录 source epoch、W、可读版本、schema、扫描/重放进度和 generation；重启后仍恢复同一 W。日志按消费者/build pin 保留，数据版本按可达性独立 GC。容量不足时限制新构建/慢任务或背压，绝不删除仍需版本；旧版本保留成本计入预算。

### 3.3 Generation 与输出隔离

generation 更替须证明旧的在途请求不能覆盖新结果。停止本地线程不是远端 fence；可选择隔离的物理目标代际，或经实测成立的远端 fencing/drain 协议。发布前必须达到相应完整性和 VISIBLE 条件。目录/路由切换与 StarRocks 换表能力分别验收，不预设可原子交换表名。

输出主键必须稳定、确定；UPDATE 按旧值撤回再加入新值处理，过滤条件改变和 DELETE 必须撤回旧结果。失败目标保留 outbox 和必要状态、有限重试后进入 degraded；其他独立目标继续。未知请求的自动对账、解除隔离和代际修复仍需实现，不能为了进度直接确认成功或重放。

## 4. 状态放置与安全复用

### 4.1 P6A：先证明可行，再比较成本

不把 local、StarRocks、hybrid 当作已可互换的三个 backend，也不同时建设三个完整运行时。先实现一个满足上述合同的最小方案，再用相同语义/耐久性比较候选。

| 候选 | 进入性能比较前的能力门禁 |
|---|---|
| 本地 versioned state | 原子事务、可恢复 fixed-W、在线 pin/GC；比较 KV 与压缩列式 base + delta/index |
| StarRocks 基础镜像 | 实测证明 source seq → 可读版本、旧值/删除保留、分页一致性和跨重启续建；“查询当前主键表”不算证明 |
| Hybrid | 明确每层权威性与 crash 责任；证明 base 与本地 delta/index 的一致 cut 和可恢复更新 |

统一比较**唯一物理字节**、CPU/I/O、构建/恢复和 compaction，不重复计算共享 segment/SST，也不隐藏 StarRocks 成本。

### 4.2 Identity 相同只是复用的必要条件

现有 `state_compatible(..., minimum_watermark)` 只证明当前最小的 hash/语义与进度条件，**不是 fixed-W 可读性证明**。最终复用必须拆成三个独立判断：`semantic_compatible(state, query)`、`version_readable(state, W)`、`physically_reusable(state, consumer)`；任一失败都不能直接共享。

语义 identity 已开始覆盖 source epoch、稳定 relation identity、schema/type hash、NULL/bag 语义与当前受限算子的 collation/IR identity；watermark、backend/format、generation、health、refs 属于实例状态。current-only SQLite backing state 明确拒绝把逻辑 catalog pin 当作历史版本保留，fixed-W 复用使用同一 BEGIN IMMEDIATE 内校验并原子 clone/copy backing bytes。时区、更广 SQL 类型/collation、宏/UDF 跨算子依赖及未来 backend ABI 仍需继续扩展。

最小共享接口只需先支持：获取/构建状态、fixed-version read、change subscription、retain/release、持久进度与安全 GC。当前 `source_state.py` 已有 fixed-W pin/read、版本/changelog GC 和 durable consumer watermark；consumer 可跨“零输出事务”单调推进，GC 自动受最慢 consumer/pin 约束。新增 `physical_state_catalog.py` 把 semantic identity、backend/format、generation、可读版本区间、health、refs、pins 与 GC eligibility 持久化，并将 `semantic_compatible` / `version_readable(W)` / `physically_reusable` 三个判断拆开。第一个正确 JOIN 不等待通用优化器。

shared 模式下内部 `source_commits.seq` 现在继续写入 durable `jobs.source_seq`，目标确认 VISIBLE 后推进 `applied.source_seq`（per lane ordered domain）。lane FIFO 仍是第一道顺序保证，ack 额外拒绝 source_seq 回退，避免损坏状态或未来并发改造让旧写覆盖新写。

## 5. SQL、规划与性能路线

编译目标是 **SQL → 规范关系 IR → 带撤回语义的增量 IR → 物理计划**。初期采用受限算子集合，允许少量手写物理算子；不为每条 SQL 永久添加独立 handler。先做 COUNT/SUM/AVG 和 INNER JOIN 的完整撤回/恢复，再考虑 LEFT JOIN、MIN/MAX、DISTINCT、窗口。类型、溢出、NULL、重复值、复合键及确定性需统一语义，不能只比较打印出来的值。

DuckDB、DBSP/OpenIVM、DataFusion、现有 C 内核和自研算子均只是候选；选择依据是语义覆盖、可恢复状态接口和实测总成本。语言或 FFI 本身不是性能结论，只有 profile 与 A/B 收益支持时才下沉热点。

规划策略包括复用、增量构建、局部重算和全量重算。**P6/P7 首版只使用成本向量、资源硬约束和可解释规则；P9 再优化整个共享 DAG 的长期成本。** 时间、字节、放大率等量纲不同，下面只是成本项，不能直接相加成一个无单位分数。

```text
cost_vector = {
  bootstrap, catchup, shared_state_creation,
  future_maintenance, storage_retention, recovery,
  downstream_delta_amplification, GC/compaction_debt
}
```

约束至少包括已有任务 SLO、新任务 `time_to_ready`、CPU/内存/磁盘/网络预算和恢复正确性。成本模型记录峰值 RSS、唯一状态字节、source/base read、CPU、网络/磁盘写入、spill/compaction、恢复时间、输出放大和目标未可见队列；加入运行反馈与切换滞后，避免策略振荡。高 fan-out JOIN 不能只按“源 50 行/秒”估算能力，预计无法追上的任务必须拒绝或等待。

调度先保障已就绪任务，再给新构建配额；版本压力升高时限速/降并发并保留可恢复进度，不能通过关闭 fsync、修改 StarRocks 默认安全参数或无界重试通过验收。

## 6. 实施顺序与退出门禁

编号沿用现有追踪，按可运行闭环推进；不把所有研究候选变成第一版的强制依赖。

| 阶段 | 下一步与通过标准 |
|---|---|
| P0–P3 / P10 | 保持现有差分/故障测试；定义上述事务、水位、完整性、未知请求和代际协议；跨目标原子性未证明则明确不承诺 |
| **P6A/P6B** | **SQLite correctness-first 路径已接 daemon 并通过真实 E2E**：authoritative log durable 与 base applied 分离、fixed-W pin/read、版本 GC、crash replay 已工作；本次继续加入 durable consumer frontier。尚未完成 50M 规模存储选型、schema epoch 在线迁移、空间耗尽/compaction 长跑 |
| **P6C 最小接口 + P7** | **shared 模式的单源投影与受限 stateful task 已支持无需重启的 hot-add/drop；单源投影通过 GTID ON/OFF × transaction/merge_async 真实 E2E、构建中强退和重启续建。** generation 的 fixed-W/pin、drop/drain/retire、同名 re-add fail-closed，以及 retained semantic change 的在线 new-generation/shadow rebuild、远端 marker/fence/swap、强退续建均已进入代码与合同测试。仍需更多并发 replacement/cancel 压力、source scope/DDL 场景和正式长跑证明 |
| **P8A/P8B** | **P8A stateless IR 已闭环；P8B 已推进到 COUNT/SUM/AVG + 受限双源 INNER equi-join 的完整 durable runtime 并暴露给 catalog/daemon。** 两者均有 canonical IR、撤回语义、fixed-W bootstrap、atomic state/consumer/outbox、generation、writer bridge 与 restart-safe descriptor/runner；聚合与 JOIN 的 StarRocks 4.1.1 transaction/merge_async 合同均已通过；JOIN 另有固定 seed 的 2000 事务 randomized/full-state oracle。在线新增/删除和 retained semantic change 的 new-generation rebuild 已进入 hot cutover；**尚未完成的是更多 SQL 语义以及跨算子通用增量编译。** |
| **P9A/P9B/P9C** | **correctness-first P9 基线已进入代码**：aggregate/JOIN exact sharing、aggregate 输出子集、JOIN projection subview、durable follower binding、owner promotion、dependency ref/retention GC；100-task scale contracts 验证一个 compute state 服务 100 个 exact/subview follower。stateful_share_policy.py 提供 compatible/off/adaptive、lag/fanout/surplus/observed-visible-lag fence、durable decision/telemetry 与确定性的 whole-graph leader preference。stateful_admission.py 已在 hot-add / semantic rebuild 建表和注册前按 task/building 数、当前 state payload bytes、pending bytes、source lag 与可配置 per-task state reserve 做 fail-closed 资源准入，并加入 durable wait/backoff、异常退避、plan supersede 清理，以及“admission 成功→descriptor/rebuild intent durable”崩溃边界与无 owner reservation 回收。默认预算为 0 时保持现有行为；它仍只约束当前可观测压力/显式 reserve，不宣称已经预测未来基数或 lifetime cost。仍需真实大负载下校准成本向量/滞后、跨 operator 的共享 arrangement/factorized state、50M/72h 与故障下策略切换验证 |
| P4–P5 | 按 profile 插入 event/transaction batching、布局融合、native socket/snapshot；同 raw binlog 差分先通过，再重复 Python/native A/B，报告两进程总 CPU/RSS 和 IPC 成本 |
| **P11** | **可执行 long-haul driver/gate 已进入代码，但尚未完成正式 50M/72h 认证运行。** 固定机器目标仍是 50M 初始行 + 50 rows/s × 72h，并动态新增任务/注入故障。canonical `v4` profile 固定保留 aggregate 与 INNER JOIN 两类初始 stateful task，10 个动态任务按 5 aggregate + 5 JOIN 交替 hot-add；每次强退窗口还会在 daemon 停止期间提交一笔 JOIN 右侧 `dimensions` 更新，迫使恢复跨过双边变化。两类 follower sharing、time-to-ready、强退追赶与最终 exactness 都必须单独通过；短版真实 mixed smoke 已在 [E2E run 37013697722](https://github.com/justgo4/m2s/actions/runs/37013697722) 通过，但这不是 50M/72h 认证证据。daemon 精确累计全程 `>5s`/`>10s` CDC 样本；workload 在故障期间继续向 MySQL 产数并要求恢复追到持续推进的 live frontier。报告同时保存机器/cgroup 指纹、代码 revision、MySQL/StarRocks/关键 Python 依赖版本，以及 m2s daemon、MySQL、StarRocks 服务 scope 的 RSS/CPU/实际读写字节；相同 StarRocks 容器 scope 会去重，source/sink scope 混叠则 fail-closed。正式长跑还支持显式保留工作目录与原子 progress checkpoint，避免中途失败只剩控制台日志。gate 仍单独报告 recovery latency、time-to-ready、空间与版本债务；下一步是在固定资源上完成正式 50M/72h 证据 |
| P12 / P13 | 公平对标后再给优势结论；补可解释 deploy/explain/status/cancel、预算准入、权限、版本化升级/回滚，MCP 接同一控制面；catalog 已保存不等于任务已激活 |

最短主线现在是：**让真实 stateful catalog/P11 smoke 持续通过 → 用固定资源完成 50M/72h gate → 只把实测有收益的共享 arrangement/状态布局下沉到更通用物理计划。** 1/10/100 task sharing A/B、在线 rebuild/new-generation、远端 swap/fence 与 durable admission wait/backoff 已进入代码和合同测试；当前 whole-graph preference、adaptive sharing 与 stateful resource admission 仍是可解释规则，不把它们宣传成完整成本优化器。

测量分 decoder、local durable pipeline、snapshot、端到端四层；最终 gate 固定机器/资源并计入 Python/C、source/sink、compaction、存储和网络。与 Flink、RisingWave、Materialize、Bytewax、Pathway、Proton、Arroyo 对比时保持 SQL/结果语义、源/目标、耐久性和恢复要求一致，分别报告吞吐、延迟、构建、空间、恢复与功能缺失；不宣称任意 SQL 下全面领先。

## 7. 研究输入及适用边界

以下作为设计候选，不表示已经实现；新论文也不自动优于经过验证的工程方案。

| 一手资料 | 可借鉴内容与边界 |
|---|---|
| [DBSP](https://arxiv.org/abs/2203.16684) | 增量化/撤回代数；不含本项目跨系统事务协议 |
| [Shared Arrangements](https://arxiv.org/abs/1812.02639) | 跨查询共享索引；仍需版本、pin、恢复和资源归属 |
| [OpenIVM](https://arxiv.org/abs/2404.16486) | SQL-to-SQL incremental compilation 候选 |
| [Enzyme（2026）](https://arxiv.org/abs/2603.27775) | 增量/局部/全量与整图成本；不能替代 fixed-W 合同 |
| [Streaming View（2025）](https://www.vldb.org/pvldb/vol18/p5153-zhou.pdf) | 仓内增量维护；m2s 额外跨三个一致性域 |
| [RisingWave backfill（2026）](https://www.risingwave.com/blog/backfilling-in-risingwave-from-historical-initialization-to-continuous-streaming/) / [Noria](https://www.usenix.org/conference/osdi18/presentation/gjengset) | fixed snapshot/catch-up、共享/部分物化 |
| [Heavy-Light IVM（2026）](https://arxiv.org/abs/2605.08397) | skew 算法候选；放在正确 JOIN 基线之后 |
| [StarRocks SQL transaction](https://docs.starrocks.io/docs/loading/SQL_transaction/) | 事务/隔离边界；不等于可恢复 time travel |

## 8. 给外部评审者的问题

请基于当前实现和本文待实现合同，先给反例和优先级，再推荐组件。尤其检查：

1. W=100、共享状态已到 200：哪些持久版本和 pin 才足以安全复用？构建强退后源继续更新、GC/compaction，恢复协议是否仍成立？
2. 跨表初始镜像、源事务、task state/outbox 和 **per-target visible frontier** 之间是否有遗漏、重复、越洞或错误确认窗口？请给最小事件序列。
3. local 或跨引擎 base/log/cursor 的 durable commit boundary 在 crash 的每个切点是否可重放？hybrid 是否存在无法收敛的 split-brain？
4. 旧 generation 的 HTTP 请求晚到、部分目标已可见、替换发布中强退：如何避免污染新结果？哪些目标能力尚未实测？
5. `semantic_compatible` / `version_readable(W)` / `physically_reusable` 三层检查是否足够？最小共享接口能否先做正确 JOIN？
6. local 列式 base + delta、KV、StarRocks 镜像或 hybrid，哪个首先满足版本/恢复合同？哪些存储与 compaction 成本容易漏算？
7. 单任务最优为何可能损害整个 DAG？100 个任务、热点 key、高 fan-out 与未来复用不确定性下，lifetime cost、GC、SLO 准入及策略切换如何失效？
8. 联合设计相对已有工作有何可验证贡献？应删去哪些路线、补哪些反例和实验，才能对优势给出可信结论？

建议评审输出：**必须修正 / 可后置 / 无证据的主张**，每项附失败机制、最小修正和验收测试。不要把路线图当成已实现能力，也不要仅凭组件名称评价性能。

## 9. 运行现有基线

Linux x86_64、Python 3.12/3.14、C11、CMake ≥ 3.20。Python 保持函数式/下划线命名，不引入 typing/logging，运行日志使用 `print(..., flush=True)`。

```bash
python -m pip install -r requirements.txt
cmake -S native -B build/native -DCMAKE_BUILD_TYPE=Release && cmake --build build/native --parallel 2
python native/native_abi_selftest.py
python j4.py selftest
cp setup.sql.example setup.sql
# 修改 setup.sql 中的本地连接配置
python j4.py                  # daemon
python j4.py sql setup.sql    # 另一终端部署
python j4.py cli              # 或交互部署
```

正式 P11 认证使用代码内唯一 profile，避免把自定义 smoke 当成 50M/72h 结果。认证前工作树必须 clean；MySQL/StarRocks 必须是一次性测试实例；必须显式提供持久 `--work-directory`，且该目录必须为空或不存在，避免 72h 中途失败后证据随临时目录清理。以下命令会固定为 **50,000,000 初始行、50 rows/s、72h、10 个动态任务（5 aggregate + 5 INNER JOIN）、每 6h 强退一次且在每个故障窗口提交 JOIN 右侧维表更新、8 GiB m2s 内存预算、adaptive sharing**，并保留中途 checkpoint：

```bash
git status --porcelain
python tools/longhaul_workload.py --isolated --certification-profile \
  --work-directory /data/m2s-p11-run/work \
  --output /data/m2s-p11-run/longhaul-workload.json

python tools/longhaul_gate.py /data/m2s-p11-run/longhaul-workload.json \
  --require-profile \
  --output /data/m2s-p11-run/longhaul-gate.json
```

`--certification-profile` 对任一 workload 参数偏离 canonical profile 都直接拒绝；正式 gate 还要求相同 profile 身份、clean worktree 证据、稳定的服务资源 scope/配额、完整 source/sink 资源计量、故障恢复、最终 drain、空间/rowset/version debt 与结果精确性。自定义短测不使用该开关。

公开仓库只提交通用代码、合成配置/数据和公开测量；真实凭据、地址、业务数据、生产日志、SQLite/WAL 和 metrics 不得提交。

## 10. 2026-10-02 当前主线同步

此前独立核查写入 README 时固定在较早 revision（`54a5925`、`fdc3089`、`65b1b188`），用于记录当时真实存在的风险；**这些文字是历史快照，不应继续被解释为当前 HEAD 仍未修复。** 截至本次同步，主线已继续收口多个当时的 P0/P1 问题，但正式 50M/72h 认证仍未完成。

### 10.1 后续主线已实现修复并有局部回归的旧审计项

- **shared follower promotion / physical-state 竞态**：`bbf173bd` 在 promotion 后重新解析 follower 当前物理状态，`f40caa0b` / `f3b79ed3` 增加 aggregate/JOIN stale-result 回归；`0c47b39d` 保留 stateful E2E 完整失败现场。旧 README 中“physical state does not exist 尚未闭环”的表述不再代表当前实现。
- **P11 exactness oracle**：`9a54a88f` / `e40cf5fb` 把 raw 与 JOIN 校验改为 full-row partitioned exactness；`84e5d3a5`、`71a44413`、`79ed720d` 加入 summary-collision 负例与 gate 合同。旧的 COUNT/SUM/MIN/MAX 摘要漏检问题已不再作为当前门禁。
- **大源事务内存上限**：`15b0ca11` 实现 shared-source transaction disk spool，`2a5e7b89` 与 `420698fe` 补合同与 baseline CI。旧文档里的 “disk-spooled source parts are not implemented yet” 已过时。
- **JOIN 热键全量重算**：`fbf4961b` 将增量路径从整组 before/after pair 重算收缩到 changed-row delta，`3e93ad33` 锁定 hot-key changed-row 工作量，`308db848` 将合同接入 baseline CI。仍需真实大负载 profile，但旧的“单行更新固定触发完整 L×R 重算”已不再描述 HEAD。
- **多 sink retained semantic rebuild**：`9a0391fb` 在全部 rebuild cutover 前 fence 全局 plan activation，`01fb9db7` 支持多 stateful rebuild generation 安全激活，`dae5c588` 校验 multi-sink semantic rebuild plan，`2086a537` 补跨重启恢复。README 前文已同步为 online new-generation/shadow rebuild，而不是 retained semantic change 一律 fail-closed。

### 10.2 仍然成立的边界

1. **正式 P11 认证尚未完成。** 仍缺固定资源上的 50,000,000 初始行 + 50 rows/s × 72h 结果；短时 hosted smoke 只能证明功能/恢复边界，不能替代 P95≤5s、P99≤10s 的长期证据。
2. **Merge Commit async 的“已接收但客户端未拿到事务身份”仍按未知结果处理。** 当前正确行为仍是持久化 marker、隔离该目标、不盲目重放；自动远端对账/安全解隔离尚未形成可证明协议。
3. **source ingress 与任务消费仍未完全解耦，Python/SQLite 仍是 correctness-first 状态基线。** 是否下沉 C/Rust、替换状态布局或引入更通用 shared arrangement，必须由固定资源下的 CPU、锁等待、RSS、I/O、唯一物理字节和恢复时间 profile 决定。
4. **SQL 覆盖仍然受限。** COUNT/SUM/AVG 与受限 INNER equi-join 已闭环，但 LEFT JOIN、MIN/MAX、DISTINCT、窗口、JOIN+聚合与跨算子通用增量编译尚未完成；不支持的语义继续 fail-closed。
5. **不能把不同 revision 的绿色 Actions 拼成一个“当前版本已认证”的结论。** baseline/native/E2E、stateful lifecycle、quarantine 与 P11 结果必须在明确固定的 milestone SHA 上分别记录。

### 10.3 当前最短主线

先固定一个 milestone SHA，完整跑 baseline + native + 八格真实 CDC E2E + stateful lifecycle/quarantine 重点合同；随后在同一资源拓扑做 1/10/100 task、JOIN skew/fan-out、动态 add/drop/rebuild 与长事务 profile；最后再启动正式 `p11-50m-50rps-72h-v4`。只有在这些证据完成后，才决定下一轮状态布局、shared arrangement 或 native 热路径下沉。

因此当前结论保持克制：**架构主线可继续，多个旧审计 P0 已有针对性修复与回归；但没有正式 50M/72h 报告之前，不宣称生产认证、全面超过其他引擎或达到物理极限。**

### 10.4 第二轮独立复核：已修项、剩余缺口与最新 CI（2026-10-02）

本轮先固定 [`48b8fba1`](https://github.com/justgo4/m2s/tree/48b8fba1fb0af7ea1564c182ddbe0f0853137e6e) 复核，随后跟进到 [`f0f58c67`](https://github.com/justgo4/m2s/tree/f0f58c67c8dc81e320435deeaf1010ba9653fd06) 和 `61dbe802`。审查期间实现者仍持续提交，因此下文按具体版本描述；“已有修复/局部回归通过”与“固定版本完整真实 E2E 已通过”必须分开。**主线方向仍合理，上轮修正有实质进展；目前最该收口的是 oracle 覆盖和同一 SHA 的完整验收，不宜继续以新功能数量代替证据。**

#### 已确认的修复进展

| 上轮问题 | 本轮复核 | 仍需的证据 |
|---|---|---|
| shared step 返回旧 leader 后，promotion/GC 使 registry 查询过期状态 | `stateful_physical_registry.sync_runtime_result()` 的 shared 分支已在 `BEGIN IMMEDIATE` 事务内重读 durable task、binding、stream 和 dependency refs；合法 promotion 解析到私有 backing，非法依赖仍拒绝。aggregate/JOIN 已增加 stale-result 回归 | 当前最终 SHA 的真实 owner drop/promotion/GC/restart 联合 E2E；本地没有运行含 Arrow 的完整合同，不能提前写成全部闭环 |
| JOIN 分组摘要漏检字段交换 | 新 oracle 读取 `event_id,bucket,label,v` 完整投影行；本轮执行原始函数及 SQL，交换 `id=1/2` 的 v 后，`all_match=False`，原负例已正确失败 | 下述“分区之外多余行”负例仍需修复 |
| 单行 JOIN 更新完整重算 L×R | 原始 `tools/join_incremental_delta_test.py` 本地通过：100×100 热键，一行更新，100 条 delta、**200 次 projection**，此前为 20,000。另用保留的完整配对路径对照，5 个种子、共 **1,000 个有效随机事务**的 delta 全部一致，覆盖 NULL join key、双边变化、同键多次更新/删除与 key move | 仍读取 affected key 的 before/after 两侧行集，不等于仅按 delta 读取，也未证明大 fan-out 的 RSS/锁时长；下一步测真实规模，而非再次重写已改善的路径 |
| 失败时丢失完整 daemon 日志 | `evidence_directory()` 在 TemporaryDirectory 清理前调用 `preserve_failure_evidence()`，将完整 daemon log、metrics/summary 和合成 durable-state 诊断复制到输出目录；workflow 的 failure artifact 可以覆盖这些文件 | 在一次人为确定性失败中检查 artifact 的完整 traceback 和状态内容；不要只检查上传 step 成功 |
| 大源事务 source parts 尚未落盘 | native reader 现用 `SpooledTemporaryFile`、顺序 source-part record 与大小/磁盘门禁；`commit_spool()` 在同一 SQLite 事务中迭代 parts、记录 source log/jobs、推进 cursor，保持截断失败回滚边界 | 下述 base apply 的整事务内存放大仍存在；本轮没有运行需 Arrow 的 spool 合同 |
| 连续 push 取消昂贵 E2E | 新增 `milestone.yml`，复用 baseline/native/E2E，milestone 子 workflow concurrency 按 SHA 分组，产出同版本 manifest | workflow 已存在不等于这份 manifest 已全绿；其 `certified` 只表示声明 scope 的 CI 合同通过，不是正式 P11 50M/72h 认证 |

#### 已收口：full-row oracle 覆盖与正式 gate 证据合同

此前按 `bucket` 分 1024 次查询的 oracle 已不再是当前实现。自 `cbeff49b` 起，raw/JOIN source 与每个 target 都改为 unbuffered 全表流式扫描；`streamed_full_rows_v3` 对完整投影计算 `sha256_multiset_v1`，同时记录 source/target 全表行数、scan passes、uncovered rows、digest 与 mismatch。这样 `bucket=NULL`、负数、`>=1024` 或其他任何未落入旧分区域的额外 target 行都会进入全表计数/摘要并导致不匹配，而不再依赖 bucket 域假设。相关合同由 `dc65582c` / `da3929f7` 覆盖。

正式 P11 gate 也已在 `395c9cd1` 后 fail-closed 校验 `comparison=streamed_full_rows_v3`、`scan_mode=full_table_unbuffered`、digest algorithm、scan-pass 覆盖、source/target 总行数、required target 集合、digest、mismatch 和 uncovered rows；旧 summary-only 或只保留 top-level `all_match=True` 的报告不能再作为正式 profile 通过。gate 测试已有旧 comparison、scan coverage、source count、target rows/digest、缺 target 等负例。

这两项因此从“当前 P0”降为已修历史问题。剩余风险转为**正式规模下全表 oracle 本身的耗时/I/O，以及固定 SHA 的完整认证证据**，不能把合同正确性与 50M 运行成本混为一谈。

#### Actions：当前主线回归处理

持续 push 会取消旧 workflow，因此只把明确结束的运行当证据。最近一次已完成 baseline 在 source apply worker 新合同里暴露了测试代码缺少 `import pickle`；运行时 source snapshot/set-wise apply 合同在到达该点前均已通过。该测试导入回归已由 `c9eab593` 修复，后续 baseline 以更新 SHA 的最终结果为准。此前审计记录的 `stateful_rebuild_multi_test.py` 缺 `cfg["state"]` 已不是当前 HEAD 的阻断点，不再作为现存 P0 重复列出。

#### 多 sink rebuild：审查期间的新修复须保留，不再重复归为当前错误

- 在 `48b8fba1`，本轮对原始 `stateful_rebuild_try_cutover()` / `stateful_rebuild_switch_runtime()`，使用真实 SQLite phase 和双线程屏障构造时序：两个 sink 都在 sibling 完成前判断 `plan_complete=False`，随后两者 phase 均 complete，但 durable/runtime active plan 都仍为 3 而非 9，rebuild_plans 已空。远端 SWAP/lifecycle 边界被 stub；这是本地并发协议反例，非真实 StarRocks E2E。
- `f0f58c67` 新增/接入 durable cohort、共同 frontier、cohort readiness gate 和共享 cohort lock，针对了上述旧并发窗口，应保留该方向。不能绕过 guarded step 的锁直接调用 cutover，然后把旧反例当成新版本的实际失败。
- 在 `f0f58c67`，另一个真实持久状态负例是“所有 individual rebuild 已 complete，最后 cohort mark_complete 尚未 durable 就强退”：旧 startup 只看 individual active rows，忽略仍 cleanup 的 cohort，导致新 cohort begin 报已有 active owner。
- 本轮继续跟进到 `61dbe802`：startup 已增加 active cohort reconciliation 与恢复 cohort locks。对**同一份** orphaned-cohort SQLite 状态执行该版本的原始 startup 函数，旧 cohort 正确变 complete，下一 plan 的 cohort 可创建。这个具体恢复缺口已有本地正向证据，**不再列为当前待修问题**；仍应把共同 F、两个成员先后/并发 swap、每个 durable 边界强退、partial swap restart 纳入固定 SHA 的真实合同。跨多个 StarRocks 表的 SWAP 仍按表依次发生，共同 frontier 与全局版本标志不能额外承诺多表对外原子切换。

#### P1：为正式规模保留的性能与内存边界

1. **完整行 oracle 已消除 1024 次 bucket 扫描，但仍需测 50M 全表扫描成本。** 当前 raw/JOIN source 与每个 required target 各做一次 unbuffered full-table pass；访问形状从可能的 1024 次全表扫描降到每表一次，但 50M × 多 target 的网络、hash、CPU 与 wall time 仍必须在固定资源下记录。oracle 正确性已收口，不等于正式规模成本已证明。
2. **source capture/base apply 已从旧的整事务 Python dict 路径推进到 set-wise SQLite DML，并进一步将 durable capture 与 base apply 异步解耦。** durable source log/版本/cursor 仍在主 SQLite；`source_apply_actions` 与 `source_snapshot_rows` 只承担单事务内 scratch，当前连接会用同名 TEMP shadow table 承载它们；staging 入口会在创建任何 TEMP 对象前强制并校验 `temp_store=FILE`，若 SQLite 编译配置强制 MEMORY 则 fail-closed。安装/升级同时删除旧版遗留的主库 scratch 表，避免把可重建中间态留在主 WAL/schema，也不把大事务 scratch 的内存上界交给 SQLite 环境默认值。pending-byte 背压、独立 apply worker、snapshot set-wise staging 和 crash/race 合同均已进入主线。durable source log 对重复 source epoch + binlog file/pos 只接受 GTID 与全部 parts 完全一致的幂等重放，位置复用/内容不一致直接 fail-closed。当前继续测大事务 rollover、并发 capture/apply、TEMP staging 的 RSS/临时文件成本、WAL/锁时长与 drain tail。
3. **source cost counters 仍只是阶段工作计量。** 不能用内部 work timer 代替最终 COMMIT/fsync、锁等待、decode/transform/fanout、WAL 与 backlog 的端到端测量。已新增独立 apply、snapshot、overlapped capture/apply，以及 bounded history GC 与 capture/apply 同机竞争的 benchmark；后者直接对照 GC 开/关时 durable commit P50/P95/P99/max、吞吐、GC 写锁耗时与 RSS，不设置共享 CI 上的武断性能阈值。GC worker 对预期 SQLite/I/O 问题仍 advisory 重试，但未预期的 invariant 异常现在进入统一 guarded-worker fail-stop，避免 daemon 在 GC 线程已死时继续长期运行。`.github/workflows/state.yml` 现在对 `source_state.py` 及这些 benchmark 触发，并保留 JSON artifact。下一步用这些固定证据决定 staging/GC batch 是否需要继续调整、TEMP/新布局或更低层实现。
4. **50M/72h 仍是唯一正式认证缺口。** 短测、100k benchmark、baseline/native/E2E 只能证明局部合同或趋势；在同一最终 SHA、固定服务资源和持久工作目录下完成 `p11-50m-50rps-72h-v4` 之前，不升级生产认证结论。

截至当前主线，旧 oracle 域外漏检与 gate 证据 schema 两个 P0 已有代码和负例合同；source base apply 的旧整事务 dict 内存问题也已被 disk staging/set-wise apply 取代。最新阶段仍必须区分“合同测试通过”“benchmark 有数字”和“固定 SHA 正式认证”三个层级；其中正式 50M/72h 尚未完成。

**当前实施顺序：保持 baseline/native/真实 E2E 在最终 SHA 全绿 → 收集 source apply/snapshot/overlap/GC-contention 与 full-table oracle 的固定资源 artifact → 根据 RSS/WAL/锁等待/写放大决定状态布局下一刀 → 冻结 milestone SHA 跑 lifecycle/quarantine 重点合同 → 启动正式 P11 50M/72h。** 历史反例保留其版本边界，但已修问题不再重复标成当前 HEAD 缺口。
