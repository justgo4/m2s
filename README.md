# m2s

单机 MySQL → StarRocks 实时同步与动态 SQL 计算项目。

开发续做入口：[PROGRESS.md](PROGRESS.md) 保存当前任务、分支、固定版本、CI 与阻断项；[AGENTS.md](AGENTS.md) 保存授权和执行约定。额度/会话中断后先读这两份记录。

目标：一次捕获源数据，已有任务持续更新；运行期间新增 SQL 和下游表，完成历史构建后持续增量维护。共享源镜像完整后，新任务正常情况下不再扫描 MySQL，也不默认复制整份基础数据。未来 MCP 自然语言入口复用同一套 SQL 校验与部署协议。

**当前已是可运行的 CDC + shared fixed-W 动态 SQL 基线；COUNT/SUM/AVG 与受限双源 INNER equi-join 已接入用户 catalog/daemon，支持无需重启的 stateful hot-add/drop，并持续用真实 MySQL→StarRocks 合同验证。** shared 模式已经接入 authoritative source log/base、generation 生命周期、drop/drain，以及 correctness-first 的跨任务状态复用：相同 aggregate/JOIN 可只维护一个 compute state；aggregate 的聚合输出子集与 JOIN 的投影子集可复用 superset state，保留独立 target/outbox/frontier；owner 退役时 follower 可在固定 frontier 提升为私有投影状态。当前还加入 durable sharing decision/telemetry、compatible/off/adaptive 准入和确定性的整图 leader preference。legacy 模式仍保留 MySQL snapshot 路径。本文区分已有证据、待实现协议和研究候选；不宣称已达到物理极限、生产就绪或全面超过其他引擎。

## 当前进度评审（2026-10-03，Asia/Taipei）

本次核对主线 `612253709ff0e1119e24b431ed7eea10c970e11b`、[PROGRESS.md](PROGRESS.md)、开放 PR 与最新主线验证。**判断：受限 v1 的功能与恢复基线已经形成，当前主线任务应收敛到有界构建、写锁成本和端到端验收；最终规模与延迟目标尚未达到。** 下文历史记录保留其当时语境，最新状态以本节及 PROGRESS 为准。

| 工作 | 核对结果 | 我的评价 |
|---|---|---|
| 共享源状态、fixed-W、动态增删/重建、聚合与受限 JOIN、exact/subview sharing | 已接入 daemon，已有真实下游合同与故障恢复证据 | 功能闭环有实质进展；短测与 100-task 合同不能推导 50M 生产容量 |
| JOIN 流式出口与测量 | [PR #16](https://github.com/justgo4/m2s/pull/16) 已合并；[PR #18](https://github.com/justgo4/m2s/pull/18) 已合并生产拓扑参数化测量 | 内存改善已被合成全行摘要验证，但初始化及 job 登记的总写锁仍随输出规模增长 |
| 本地 delivery preparation 写锁恢复 | [PR #20](https://github.com/justgo4/m2s/pull/20) 已合并为 `146499d2`；测试头 `7819653e` 的 baseline/native/state、八格 daemon E2E、supervised smoke 均通过 | 只重试尚未进入事务的 BEGIN BUSY，属于恢复修复；不能据此宣布吞吐或 SLO 达标 |
| 批次候选 | [PR #12](https://github.com/justgo4/m2s/pull/12)、[PR #19](https://github.com/justgo4/m2s/pull/19) 仍开放；#19 的严格 small P95/P99 为 28.651/36.650 秒，完整结果与恢复正确 | 正确性绿色不足以合并性能候选；保留 P95≤5 秒、P99≤10 秒门禁 |
| 最新主线综合验证 | [run 37103497183](https://github.com/justgo4/m2s/actions/runs/37103497183)，实际运行 SHA `146499d2`，已结束；small 与 million 均在 “Supervised workload and full evidence gate” 步骤失败 | 之前“正在运行”的交接状态已过时。本次快审未重新解析失败 artifacts，不能把旧版锁错误或候选延迟直接当作本次根因 |
| 正式 P11 | `p11-50m-50rps-72h-v4` 尚无完成认证；交接记录中持久隔离测试机仍未配置 | 最终验收仍开放，不给出无依据的完成百分比或日期 |

测量口径必须保持清楚：[生产拓扑合成报告](reports/join-production-topology-local-20261003.json) 的 **100 万输出行**中，当前流式方案为 3920 jobs、峰值 RSS 252,301,312 bytes；配置批次候选为 320 jobs、368,746,496 bytes，完整行袋摘要一致。这说明批次大小存在内存与 job 数量的取舍，不能等同于百万源行 daemon 测试通过，也不能把不同 hosted runner 的结果当作受控 A/B。

**建议按以下顺序继续：**

1. **先补齐最新失败证据。** 下载并核对上述主线 small/million 的报告 SHA、首个失败边界、oracle、延迟与锁等待，把结论记入 PROGRESS；不要继续沿用前一轮错误推断当前根因。
2. **优先完成可恢复的分块构建与有界发布。** JOIN bootstrap 与 enqueue 需要 durable `building/sealed` 状态；每块限制扫描工作量、输出行数和字节，游标同时记录左右主键以恢复高 fan-out。固定 W 的 pin 保留到最终激活；jobs、links、游标和计费原子提交。未完成结果不得进入 claim、ready 或 visible，最终发布也不能重新复制全量数据形成另一个大事务。
3. **用竞争与崩溃证据证明边界。** 覆盖每块提交/封口后的强退续建、旧游标重试、drop/GC、owner promotion、最后 ack、旧状态升级，并让真实并发 writer 能在块间前进。同步测量写锁等待与持有时间、TEMP/WAL/GC、capture/apply/writer 各阶段成本。逐处 BUSY 重试可以防止退出，但不能替代这项结构改造。
4. **保持受限 v1 范围，分层验收。** 先在固定资源下通过严格 small，再推进 million、资源/低磁盘/升级恢复演练，最后运行原规格 50M/72h。主机准备可并行推进，代码改造无需因此停摆；在 profile 证明收益前，不优先更换 SQLite/Python 或扩大 SQL、通用优化器与 MCP 范围。未知 Merge Commit 请求继续隔离，禁止为通过测试盲目重放。

本次仅更新进度判断与建议，未修改运行时代码、验收阈值或候选 PR 状态。后续每个里程碑继续把精确 SHA、证据和下一步写入仓库，避免会话中断后重复试验或把未完成工作标为完成。

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

最短主线现在是：**保持真实 stateful catalog/P11 smoke → 按第 11 节逐级测规模、时间与资源 → 收口受限 SQL v1 的运行交付 → 对冻结版本完成声明范围所需的正式 50M/72h gate。** 更广 SQL、通用共享 arrangement、成本优化器与 MCP 分阶段后置；状态布局/native 替换只在 profile 证明必要后实施。1/10/100 task sharing A/B、在线 rebuild/new-generation、远端 swap/fence 与 durable admission wait/backoff 已进入代码和合同测试；当前 whole-graph preference、adaptive sharing 与 stateful resource admission 仍是可解释规则，不把它们宣传成完整成本优化器。

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
4. **正式 P11 规模长跑证据尚缺；这不代表通用 SQL、优化器及运行交付已经全部完成。** 短测、100k benchmark、baseline/native/E2E 只能证明局部合同或趋势；在同一最终 SHA、固定服务资源和持久工作目录下完成 `p11-50m-50rps-72h-v4` 之前，不声称取得该 profile 的正式认证。完整交付的其他缺口仍按第 6、11 节分别收口。

截至当前主线，旧 oracle 域外漏检与 gate 证据 schema 两个 P0 已有代码和负例合同；source base apply 的旧整事务 dict 内存问题也已被 disk staging/set-wise apply 取代。最新阶段仍必须区分“合同测试通过”“benchmark 有数字”和“固定 SHA 正式认证”三个层级；其中正式 50M/72h 尚未完成。

**当前实施顺序：保持 baseline/native/真实 E2E 在最终 SHA 全绿 → 收集 source apply/snapshot/overlap/GC-contention 与 full-table oracle 的固定资源 artifact → 根据 RSS/WAL/锁等待/写放大决定状态布局下一刀 → 冻结 milestone SHA 跑 lifecycle/quarantine 重点合同 → 启动正式 P11 50M/72h。** 历史反例保留其版本边界，但已修问题不再重复标成当前 HEAD 缺口。

### 10.5 第三轮独立复核与接手修复（2026-10-02）

本轮固定审查 [`01417f7e`](https://github.com/justgo4/m2s/tree/01417f7e65e5c3eea04f1239dd1cc65583972b4e)。**主线没有走偏**：full-table 流式 oracle、set-wise source apply/snapshot、FILE TEMP staging、独立 base apply worker、durable pending-byte 背压与有界 history GC，都在实质解决前两轮问题。source cursor 重放身份校验、merge-uncertain immutable request identity 也应保留。本次接手优先修复下面的历史保留并发缺口。

#### P0：GC 水位读取与 reader 登记必须共享写事务

在被审查版本中，`gc()` 先在事务外调用 `retention_floor()`，随后才 `BEGIN IMMEDIATE` 并删除历史。确定性双连接 SQLite WAL 时序如下：

1. 已应用至 W=10，尚无旧 consumer，GC 读得 floor=10。
2. 删除开始前，另一个连接成功登记 W=5 的 consumer。
3. GC 仍按过期 floor=10 删除版本/commit，并将 `min_readable_seq` 推至 10；已经登记的 W=5 reader 因而失去需要的历史。

同一个事务外多次 SELECT 还可能把较早的 applied 水位与较新的 consumer 水位拼在一起，触发本不应出现的 retention invariant 异常。独立 GC worker 已采用 guarded fail-stop，因此仅修异常处理不能消除根因。

另一个 admission 缺口是：`register_consumer()` 只检查 0≤W≤applied，没有检查物理可读下界，也在取得写事务前验证 applied。即使修好 GC 的读取窗口，新 consumer 仍可能在 GC 完成后成功登记到已经回收的 W。**有界 GC 尚有残留旧行也不能证明该 W 可读**，必须以 durable `min_readable_seq` 为准。

本次实现将 GC 的全部 retention 输入读取移入执行删除的同一 `BEGIN IMMEDIATE` 事务；consumer 的 applied/readability 检查和 INSERT 也在同一写事务内完成，并拒绝 W<`min_readable_seq`。这样只有两个合法次序：reader 先登记，GC 保留其所需历史；或 GC 先完成，过期 reader 的登记明确失败。既有 reader 的单调推进和正常 W=frontier 登记仍保留。

新增 `tools/source_gc_concurrency_test.py` 使用真实 SQLite WAL 双连接和受控插入点，覆盖 GC 计算后尝试登记、登记检查期间尝试推进回收下界、既有 consumer 的版本/后续 commit 保留、推进后回收，以及 bounded delete 留有旧行时仍拒绝过期登记。三个用例在原版原始 SQLite 函数上均失败，在修复后均通过；并已接入 baseline 的 Python 3.12/3.14 矩阵。本地环境缺少 Arrow，复核通过 AST 装载原始 SQLite 函数执行这些用例；完整模块导入和完整 source protocol 合同以 GitHub CI 为准。本地语法检查、longhaul gate 与 JOIN 热键 changed-row delta 合同也通过。

#### Actions 与下一步的证据边界

- 被审查 SHA `01417f7e` 的 [baseline](https://github.com/justgo4/m2s/actions/runs/37076884585) 与 [native](https://github.com/justgo4/m2s/actions/runs/37076884654) 已成功。
- [八格真实 CDC E2E](https://github.com/justgo4/m2s/actions/runs/37076871794) 已全部成功，但对应 `92337a9d`，不能把它直接记为 `01417f7e` 或本次修复的同 SHA 验收。
- 当前并不存在“Actions 永远无法通过”的证据；曾有测试缺导入、并发协议缺口和连续 push 取消运行，应按具体失败日志修复。历史绿色记录不能替代本次修复自身的验收。
- 优先保留现有架构，先收集固定资源下 capture/apply/GC contention、大事务 TEMP/WAL/RSS 和 JOIN fan-out 的结果，再决定状态引擎/native 下沉。短时 CI 仍不能代替正式 50M/72h；Merge Commit 未知结果的自动对账、受限 SQL 覆盖与多表对外原子切换边界也未因本次修复改变。

#### 本次修复的固定提交验收与合并记录

修复提交 [`b30fd98d`](https://github.com/justgo4/m2s/commit/b30fd98dbf878873247457ac9d9bc2a9ddb09bdd) 的下列四类 GitHub CI **全部成功**；[PR #4](https://github.com/justgo4/m2s/pull/4) 已合并，merge commit 为 [`55fa2a94`](https://github.com/justgo4/m2s/commit/55fa2a940a3174e0916b2b64b1d6180d8f1527df)，其 Git tree 与已验收的修复提交一致。

| 本次 CI | 结果与范围 |
|---|---|
| [Public baseline CI](https://github.com/justgo4/m2s/actions/runs/37078480776) | Python 3.12/3.14 全部通过，包括新增三个 SQLite 并发回归、原有 source protocol、set-wise apply/snapshot、异步 apply worker 与其余 baseline 合同；此处完整模块使用真实依赖导入 |
| [Native contracts and isolated MySQL](https://github.com/justgo4/m2s/actions/runs/37078480762) | sanitizer、真实 MySQL differential/snapshot 和 StarRocks 合同全部通过 |
| [Candidate source-state durability and layouts](https://github.com/justgo4/m2s/actions/runs/37078480742) | Python 3.12/3.14 的 crash/fixed-W/replay、100k source apply/snapshot、capture/apply overlap 与 bounded GC contention 全部通过，并保留 benchmark JSON artifact |
| [Actual daemon MySQL to StarRocks contract](https://github.com/justgo4/m2s/actions/runs/37078480757) | merge_async/transaction × 两个开关的八格矩阵全部通过，包括并发 backfill/change、强退恢复、共享状态 aggregate/JOIN lifecycle 与 P11 mixed fault smoke |

以上是本次修复的 PR CI 记录；合并后主线自动复跑应按自己的 SHA/运行分别查询，后续 README 证据补记也不改变已验收代码的版本。这里仍未宣称正式 50M/72h 认证。

## 11. 验收成本、受限 v1 出口与剩余工作

**不要求每次改代码都跑 50M/72h。** 50M 检验大基数下的回填、状态/索引/输出空间和查询校验成本；72h 检验泄漏、版本/compaction 债务、持续 SLO 与多次故障后的稳态。二者不能互相替代。若继续声明 `p11-50m-50rps-72h-v4` 通过，冻结版本仍须取得同等规模、时长与资源的真实证据；短测必须保留自己的负载身份，不能降低正式 gate 的阈值后沿用认证名称。

### 11.1 先用分层测试减少失败重跑成本

下表是建议的后续计划，不是已完成的测量。时长指 daemon 状态初始化后持续施加 CDC 的窗口；前置 seed、启动及最终 drain/全行校验另计，初始 snapshot/bootstrap 与 CDC 并行、计入该窗口。前一层不通过，先修复而不直接进入昂贵长跑。

| 层级 | 计划负载与时长 | 解决的问题 |
|---|---|---|
| 日常回归 | 现有 baseline/native/真实 E2E；小规模 smoke | 事务、水位、增量语义、hot-add/rebuild、强退恢复；文档或一般小改不重跑 72h |
| 性能摸底 | 100k–1M 行，30–60 分钟 | CPU、RSS、SQLite/WAL、写锁等待、drain、full-row oracle 成本；先识别瓶颈 |
| 中型综合 | 5M–10M 行，2–4 小时 | mixed aggregate/JOIN、动态任务、skew/fan-out、sharing A/B 与多次故障 |
| 分开筛查规模与时间 | 50M 行跑 4–8 小时；另以 1M–10M 行跑 12–24 小时 | 先低成本排查大基数问题和长期累积问题；此组合仍不等于 50M/72h 联合验收 |
| 首次正式 P11 验收 | 前述层级通过后，固定 SHA/资源跑一次 50M/72h | 形成原声明范围的正式证据；后续状态布局、正确性或性能关键变更再安排相应长跑 |

现有 workload 已支持自定义 `--rows`、`--duration-seconds`、`--rows-per-second`、动态任务和故障间隔。短测不使用 `--certification-profile`，对应 gate 的自定义阈值/报告范围也须一致；正式 profile 与 `--require-profile` 保持原标准。加快 rows/s 或提前触发故障能加速暴露累计量/协议问题，不能换算为已经真实稳定运行 72h。

### 11.2 72h 不需要 AI 连续在线，机器成本才是主要支出

当前 workload、gate 与 daemon 不调用 LLM/OpenAI API。测试机运行 Python、MySQL、StarRocks 72h 本身不产生模型 token；额外 AI 用量来自读日志、分析故障和修改代码。采用独立进程监督、周期性 JSON 指标/异常摘要，阶段结束或异常时再审查即可，无须让聊天会话反复读取完整日志。

正式运行需要固定资源、持久磁盘和独立测试服务，使用测试机上的 systemd/容器或合适的 self-hosted runner。现有 E2E workflow 是 45 分钟限时的小规模 smoke，不能直接承担 72h。当前交互工作区的生命周期也不是 72h 机器存活保证。profile 的 8 GiB 是 m2s 配置预算，不是 MySQL、StarRocks、runner 合计的整机预算。

成本须按真实 fan-out 估算：50 rows/s × 72h 新增 **12,960,000 行**，初始 50M 加上后源表约 **62.96M 行**。canonical mixed 一对一 JOIN 负载具有 1 个初始 JOIN 和 5 个动态 JOIN，另有 raw target；共享 compute 不会消除七个大型目标各自的存储。最终 raw source/target、JOIN source/六个 target 的 full-row oracle 约需九个大表 pass，扫描行量可达 **566.64M**，另有 aggregate 校验。因此先用短测估算 SSD 容量、服务资源与 oracle wall time，不能从小规模 rows/s 直接线性宣称整体达标。总墙钟还包含 50M seed、启动与最终排空/校验，超过 72h。

当前原子 checkpoint 仅保存进度证据，**尚不支持整场 workload runner 中断后的续跑**；daemon 强退恢复是另一项已有能力。runner 没有 resume 参数，重新启动会初始化测试数据库，工作目录要求空目录。昂贵正式运行前应安排稳定的独立监督；如需 runner 续跑，另行实现数据集身份、时间窗口、故障/任务序列与分段证据协议，不能把现有 checkpoint 直接当作该能力，也不能直接对旧目录执行新的初始化命令。

新增的独立监督入口为 `tools/validation_run.py`，它直接复用原 workload/gate，不调用模型、不降低正式门禁。`validation_profiles.py` 提供 smoke/small/million/medium/scale-short/soak/p11 固定参数；p11 精确复用 canonical profile，其余报告均为 development evidence。运行目录必须是新路径，持久保存 plan/status、阶段日志、workload checkpoint、最终报告和 gate。进程身份以 Linux boot ID/start ticks 校验，取消使用 PIDFD，避免误杀复用 PID；workload 收到 SIGTERM 会清理独立 daemon 进程组。

```bash
# 先检查计划，不初始化数据库
python tools/validation_run.py plan --profile million --run-directory /data/m2s/run-001
# 已配置一次性 MySQL/StarRocks 和持久测试机后运行；detach 无需聊天保持在线
python tools/validation_run.py run --isolated --detach \
  --profile million --run-directory /data/m2s/run-001
python tools/validation_run.py status --run-directory /data/m2s/run-001
python tools/validation_run.py cancel --run-directory /data/m2s/run-001
# 仅当 status.can_resume_gate=true：重评已完成且摘要/版本一致的报告，不重跑造数
python tools/validation_run.py resume-gate --run-directory /data/m2s/run-001
```

**监督程序不伪造完整 workload 续跑。** workload 中断时保留原目录并明确标记；恢复 gate 仅对已成功完成、SHA-256 校验一致且代码/参数身份未变的 workload 开放。host 本身停机、资源失效或 SIGKILL 不能靠 detach 保证存活，正式长跑仍需独立持久监督环境。

`.github/workflows/validation.yml` 在 PR 跑真实 smoke，合并相关代码后跑 small/million，并支持手动 medium；它保留失败状态/日志/报告 artifact。公开仓库标准 hosted runner 的免费分钟数不消除单 job 时长、磁盘/内存和临时生命周期限制。soak/p11/scale-short 不映射到该 hosted workflow，不能通过分段换机冒充连续 72h；self-hosted 的注册/云测试机创建还依赖对应管理权限和资源，仓库连接器并不自动提供它们。

smoke 精确沿用已有 E2E 的 60 秒 cold-start/mixed-fault workload 与开发门槛（P95≤30s、P99≤60s、sample density≥0.2），只验收协议/恢复/精确性，**不宣称达到正式 SLO**。small/million/medium/scale-short/soak 保留 P95≤5s、P99≤10s 的性能门槛；p11 使用原正式 gate 默认值和 require-profile。所有 plan.json 显式列出 gate_thresholds，不能把不同层级的绿色结果混为一谈。首次新增 smoke 用正式延迟门槛运行时，[run 37083541672](https://github.com/justgo4/m2s/actions/runs/37083541672) 精确性全过，但 P95/P99≈15.09s、density≈0.664 未达 5/10s 与 0.75；该失败 artifact 保留，性能瓶颈仍应由后续分层测量判断，而非通过修改正式 gate 消除。

分级 hosted 测试固定 m2s CPU cap=2；small/million 均用 4 GiB、4 个动态任务，比较 100k/1M 基数下的相同拓扑，服务仍独立计量。首个 million 计划的 6 个动态任务使最终 9 个 sink 超出 4 GiB 的引擎预算，被 [run 37084867442](https://github.com/justgo4/m2s/actions/runs/37084867442) 在 seed 前拒绝；artifact `11259906985` 保留。修正测试计划后须重新取证，不能把预检失败称为性能通过。其他机器/CPU cap 仍须接受实际资源预检。

监督执行器 [PR #5](https://github.com/justgo4/m2s/pull/5) 的最终提交 `2837773a` 已通过 baseline/native、八格真实 E2E 与新增真实 supervised smoke。runner 检查 [PR #6](https://github.com/justgo4/m2s/pull/6) 的 [实际管理接口查询](https://github.com/justgo4/m2s/actions/runs/37084999017) 返回 HTTP 403，明确为“注册情况未知”；当前 Actions token 没有 runner 管理读取权限。工具 `tools/runner_inventory.py` 只公开计数，不公开主机名/标签。现有配置未提供云身份或持久主机入口，因此尚未创建或确认 self-hosted runner；公开 Actions 免费额度可用于当前短测，无法提供单台 72h 持久机器。

测试计划修正后的 [million run 37085851322](https://github.com/justgo4/m2s/actions/runs/37085851322) 在约 180s 时失败：source apply 在 `sync_source_base_catalog` 的 BEGIN IMMEDIATE 等写锁超过 30s，artifact `11261215092` 保留。修复仅在无打开事务、前次事务已回滚时重试 SQLITE_BUSY，等待可停止、告警限频，并在已提交 apply prefix 后持续重试物理 catalog 同步；即使无新输入也不会漏同步。FULL/损坏/其他 SQLite 错误及仍打开的事务继续 fail-closed。竞争锁持有者和长事务成本尚未实证，不把该修复称为吞吐/1M/正式长跑通过；JOIN 初始化/共享出口一次缓存全部 pair 的规模风险仍须解决。

### 11.3 先完成有边界的 v1，再推进完整愿景

**受限 v1** 的范围为单 MySQL→StarRocks、已登记的源表/列和稳定主键、共享源状态、运行时新增/删除/重建任务、投影/过滤、COUNT/SUM/AVG 与受限双源 INNER equi-join，以及明确的故障恢复/隔离边界。上述功能闭环已有，剩余重点是固定资源下的规模与压力证据、实测预算/准入、积压与低磁盘处置、告警/操作手册和版本化升级/回滚验证。破坏性 DDL/未支持 SQL 仍明确拒绝或重建；Merge Commit 未知结果仍隔离目标，在自动对账完成前不承诺自动解隔离。

运行与恢复步骤见 [OPERATIONS.md](OPERATIONS.md)。新增 `tools/operational_check.py` 以新鲜 daemon 摘要、durable status 与磁盘余量评估未知输出/停止/积压/低空间，退出码 0/1/2 分别为本次未触发阈值/告警/证据未知；它不自动解隔离或发送消息。`tools/state_backup.py` 用 SQLite online backup 生成私有单文件快照，校验完整性/字节摘要并保留 incomplete 证据；不覆盖旧目录、不提供盲目 restore。真实 WAL 并发、锁超时、损坏/篡改和中断检查已加入测试。catalog/远端目标与 state 的联合恢复、真实跨版本升级/回滚仍需隔离演练。

首个 100k/5 分钟性能测试 [run 37084867442](https://github.com/justgo4/m2s/actions/runs/37084867442) 的 raw、aggregate、三个 JOIN 的全行 oracle 和动态任务/强退恢复/drain 均正确，sample density=0.899 达标；仅延迟门禁失败，P95≈21.25s、P99≈41.99s。恢复追赶约 190s、m2s 峰值 RSS≈605MiB、累计写入约 2.79GB；日志中有 CPU 压力触发的 snapshot pause 和任务就绪尾延迟。artifact `11260627960` 保留，性能根因仍需分析；这些数据不支持生产 SLO 或 50M/72h 声明，也不直接证明必须更换 SQLite/native。

近期继续改造：[PR #10](https://github.com/justgo4/m2s/pull/10) 仅对 `source_state_apply_worker` 的明确 `SQLITE_BUSY` 且事务已退出情况做停止可打断的重试；已提交的 source apply 前缀即使没有新输入，也必须补做 physical catalog 同步。FULL/损坏/仍有活动事务等错误保持 fail-stop，busy timeout 不提高。四个真实 SQLite 连接用例覆盖提交后登记争用、释放锁后同步、争用中取消与非 BUSY/活动事务错误。PR #10 已合并并通过完整 CI；后续 [PR #11](https://github.com/justgo4/m2s/pull/11) 对 stateful worker 的明确 BUSY 做同样的 durable-prefix 恢复，并保留已提交结果补做 registry 同步。两者均未宣称 million 或长期锁争用问题已经解决。

[PR #9](https://github.com/justgo4/m2s/pull/9) 的批次候选 `5b382ef4` 和 PR #10 初版 `ea913d53` 已完成 workload，但监督器因报告版本不一致拒绝证据（runs [37087609367](https://github.com/justgo4/m2s/actions/runs/37087609367)、[37087862226](https://github.com/justgo4/m2s/actions/runs/37087862226)；artifacts `11261655549`/`11261028417`/`11261248141`）。原因是报告函数优先取 `GITHUB_SHA`，而 workflow 已检出 PR head；两者在 PR 事件中不同。修复为真实 Git HEAD 优先、无 Git 时仅允许显式 source archive 身份，附真实临时 Git 回归；旧报告不改写 SHA，新 checks 分别对应 `bdff6c88`/`d988e1e5`。被拒绝的小型报告观察到 P95≈11.17/P99≈13.19s，不能据此宣布新批次改善或门禁通过。

恢复报告的 `catchup_seconds` 也有测量边界：`recover_after_fault()` 持续造数并把新 marker 加入待校验集合，等待所有 marker 可见且 source log/apply 水位相等后才返回。约 180s 的数字可能包含持续追逐移动尾部直至负载结束，不能直接等同于进程停机时间或证明某个锁持有 180s。修改这一测量协议需要独立定义/回归，当前正式 P11 和恢复判据保持原样。

**完整愿景** 还包括更多 SQL/跨算子通用增量编译、通用 arrangement/factorized state、整图长期成本优化、公平对标以及 P12/P13 的运维控制面/权限/MCP。它们不是受限 v1 的强制前置依赖；“任意 SQL”“达到物理极限”“全面领先”不能成为没有可验收边界的完成定义。Python/SQLite 或 native 的替换也不先验必做。

| 出口 | 条件化规划预算 | 完成的含义 |
|---|---|---|
| 受限 v1 可监督试点 | 5–10 个工程工作日 | 机器/范围就绪、分层测试和运行手册收口；如尚无正式长跑则明确声明试点证据范围 |
| 受限 v1 稳定交付 | 2–4 周 | 完成必要的规模/故障/资源与升级恢复验收；原 50M/72h 声明需包括对应真实运行 |
| 结构性瓶颈触发状态布局/引擎改造 | 在 v1 预算外增加 2–4 周或更多 | 先有 profile 证据，再实现、迁移与重新验收，不能仅靠 AI 写代码速度压缩 |
| README 的更广愿景 | 3–6 个月以上，逐项单独验收 | 覆盖扩展 SQL、共享/优化研究、对标和产品控制面；没有测量/排期之前不承诺确定日期 |

这些是按持续工程投入、测试机就绪、v1 范围不扩张且未遇结构性瓶颈给出的预算，**不是已测得的完成日期**。当前最大的未知数仍是目标机器上的 50M bootstrap、状态/目标真实物理字节、WAL/GC 锁等待、JOIN 输出放大、恢复追赶及最终 oracle 耗时。下一步优先取性能摸底/中型综合证据，依据数字更新排期，再决定首次昂贵认证的开始时间。


### 2026-10-03 规模改造续接

[PR #13](https://github.com/justgo4/m2s/pull/13) 的 JOIN bootstrap 改为索引枚举和流式 durable outbox 写入，保持原子初始化与逐行精确重试；[PR #14](https://github.com/justgo4/m2s/pull/14) 对等待下游可见性或 leader 的空闲 worker 做 50ms 等待，并仅在共享绑定改变时打印复用信息。两项均在各自最终 SHA 通过 baseline/native/state、八格真实 daemon E2E 与 supervised smoke 后合并，具体 SHA/run 见 [PROGRESS.md](PROGRESS.md)。

JOIN 出口的下一候选改为有界 mutation/Arrow 批次与磁盘 spool，原子登记 jobs/links，并仅在整批全部 ack 后推进可见水位。独立进程百万输出 A/B 的完整行袋摘要一致：峰值 RSS 1,821,097,984→241,070,080 bytes，桥接耗时 18.008→14.775s；job 数量256→980，仍须验证真实下游成本。结果见 [合成数字摘要](reports/join-bridge-stream-local-20261003.json)，不是 daemon/SLO/50M 认证。现有初始化与 job 登记的**总写锁时长仍随输出规模增长**，后续须引入可恢复的分块构建/发布协议，不能靠流式内存或扩大超时宣称已解决。

主线914c163a的小型综合仍是完整精确性/故障恢复/排空通过、严格延迟失败；百万行在 native capture 的本地 SQLite 写锁超时停止。PR #12 的跨页回填批次候选也未因正确性绿色而提前合并。正式 P95≤5s/P99≤10s 与固定50M/72h目标保持不变；持久固定资源主机尚未配置。

[PR #15](https://github.com/justgo4/m2s/pull/15) 已在最终组合 SHA 通过全部合同并合并：远端事务明确 VISIBLE 后，本地登记遇 SQLite BUSY 时仅重试该本地事务；取消保留 journal，重启沿用 durable TxnId，未知远端请求仍隔离且禁止重发。主线7fe6小型综合的 P95/P99=25.345/33.752s 仍未通过正式延迟阈值；空转日志降至四条和合成内存改善不能代替端到端验收。
