# m2s

单机 MySQL → StarRocks 实时同步与动态 SQL 数据加工项目。

> **项目定位：把多个当前最先进的研究方向收敛到一个非常具体、苛刻的 MySQL → StarRocks 动态实时计算场景，并尝试解决它们交界处尚未被很好解决的问题。**

目标不是“再做一个 CDC 工具”，而是让一个长期运行中的系统支持：

```text
MySQL
  │
  │ 一次捕获
  ▼
共享、可恢复的源关系状态 + 有界 changelog
  │
  ├─ 已有 SQL 任务持续实时更新
  └─ 任意后来新增的 SQL
          │
          ├─ 不重新扫描 MySQL
          ├─ 不复制整份 base
          ├─ 从 fixed-W 一致状态构建
          ├─ 追赶 W 之后的变化
          └─ 完成后持续增量维护
                          │
                          ▼
                      StarRocks
```

后续可通过 MCP 将自然语言转换成可检查、可解释、可部署的 SQL 任务。

**当前仓库仍是演进中的基线，不是已经完成的新引擎。** 任何“生产候选”“物理极限”“全面超过其他系统”的结论，都必须由对应的正确性、故障恢复、长跑和公平对标数据支持。

---

## 1. 当前已经有什么

- `j4.py`：当前 daemon；MySQL snapshot 与 binlog CDC、StarRocks 输出、动态部署基线。
- `cdc_catalog.py`：SQL catalog、部署状态和运行时计划控制。
- `incremental_contract.py`：共享物理状态 identity、fixed-W retention、候选计划和 Pareto frontier 的 backend-neutral 合同。
- `cdc_selftest.py`：离线恢复和正确性回归。
- `native/`：C 行事件解码、Arrow IPC、分区/JSON 内核和 ABI 自测。
- `tools/`：真实 MySQL/StarRocks contract、故障注入、snapshot/state benchmark、端到端测试。

当前 SQL 仅支持确定性的单源投影、过滤、宏和模型视图链。**有状态 JOIN、聚合、窗口及跨源查询尚未开放，当前 catalog 会拒绝这些计划。**

当前在线新增下游已经可工作，但历史初始化仍会启动新的 MySQL `snapshot_worker`。这正是 P6/P7 要替换掉的路径。

---

## 2. 问题定义

第一版边界：

- 一个 MySQL 实例、一个可确认事务流。
- 显式登记需要镜像的库/表及全部允许列。
- 先要求稳定主键；不支持项、破坏性 DDL 和日志不足必须明确拒绝或受控重建。
- MySQL 使用 ROW binlog + FULL row image；支持 GTID ON/OFF 恢复。
- StarRocks 目标版本固定为 **4.1.1**，使用主键表。
- 基础镜像和已就绪任务的新数据可见目标：**5–10 秒**。
- 新增有状态任务另报告 `time-to-ready`，不能拿初始化时间掩盖实时链路延迟。
- 不要求永久保存全部历史版本；默认语义是“当前关系 + 有限变化历史”，不是数据库出生以来的事件仓库。

最重要的产品要求：

> 系统运行几个月后，今天新增一条此前从未存在的 SQL，也能基于完整当前关系得到正确结果并继续实时更新，同时正常情况下不再回源扫描 MySQL。

这意味着系统某处必须保存足以重建当前关系的信息；“什么都不保存 + 不回源 + 支持未来任意 SQL”在信息上不可实现。

---

## 3. 目标架构

```text
                         MySQL
                           │
                    single CDC reader
                           │
                 source transaction Δ
                           │
            ┌──────────────┴──────────────┐
            ▼                             ▼
   authoritative base state        bounded changelog
   current relation @ versions     commit_seq / schema epoch
            │                             │
            └──────────────┬──────────────┘
                           │
                shared physical-state layer
          arrangements / materialized subviews
                           │
                           ▼
             SQL → relational IR → incremental IR
                           │
                  physical candidates
          ┌────────────────┼────────────────┐
          │                │                │
       reuse           incremental      partial/full
       state              build           recompute
          └────────────────┼────────────────┘
                           │
                  cost/SLO planner
                           │
                           ▼
                       StarRocks
```

这个设计刻意把 **source capture、共享状态、任务 generation、SQL 计算、目标可见性** 分开。

### 三个独立水位

1. **source durable watermark**：源事务已原子持久化到共享 base/changelog 的位置。
2. **task compute watermark**：某个 SQL generation 已计算到的位置。
3. **target visible watermark**：结果已经在 StarRocks 可查询的位置。

不能把“HTTP 接受”“StarRocks 事务完成”“用户已可见”混成一个状态。

---

## 4. 核心设计合同

### 4.1 Source-centric capture

最终 source capture 不应知道当前有多少下游 SQL：

```text
错误方向：
source event → Q1 / Q2 / Q3 各自持久化

目标方向：
source event → shared base + changelog（一次）
                         ↓
                    Q1 / Q2 / Q3
```

新增 Q1000 不改变 MySQL replication 拓扑。

### 4.2 Fixed-W 动态初始化

新任务部署时：

```text
选择一致 W
→ pin changelog retention
→ 从 S(W) 构建 generation
→ CDC 继续推进
→ replay Δ(W, now]
→ catch up
→ fence 旧 generation
→ publish
```

不能扫描一个持续变化的“latest state”后再从旧 W 全量重放，否则会产生重复或缺失。

### 4.3 Base / shared state / task state 分层

- **authoritative base**：足以重建当前源关系。
- **arrangement / materialized subview**：多个 SQL 可复用的派生物理状态。
- **task generation state**：某个任务私有的 build progress、operator delta、outbox 等。

新任务**不得默认复制整份 base**。

### 4.4 Bounded changelog

日志只需覆盖最慢有效消费者和所有 fixed-W build pin：

```text
retention_floor =
    min(active consumer watermarks,
        fixed-W pins)
```

没有消费者和 pin 时才允许回收到 source durable watermark。容量不足时暂停/拒绝新 build，而不是偷偷删除仍需重放的历史。

### 4.5 Physical state identity

共享状态必须有稳定语义 identity，至少覆盖：

- state kind
- normalized relations
- key/value expressions
- predicate
- schema epoch
- collation
- semantics version

watermark、存储路径、refcount 描述的是**实例进度**，不属于语义 identity。

`incremental_contract.py` 已实现这一层最小合同并校验 handle 自身 spec，避免损坏 metadata 导致误共享。

### 4.6 SQL 不直接绑定手写 handler

目标编译链：

```text
SQL
 ↓
normalized relational IR
 ↓
incremental IR
 ↓
physical candidates
```

DBSP / differential semantics、OpenIVM SQL-to-SQL、自研算子、DataFusion 等都是候选，不预先锁死。

P8 不采用“每多一种 SQL 就写一个新的永久 handler”作为长期架构。

### 4.7 多策略，而不是“所有查询都强制 IVM”

一个任务可以有多个候选：

- reuse existing state
- incremental build
- partial recompute
- full recompute

先做 Pareto 淘汰，再由显式 SLO / 资源策略选择。核心成本至少记录：

- time-to-ready
- source/base read bytes
- state bytes
- steady-state CPU
- write amplification
- catch-up lag

`incremental_contract.py` 已提供 backend-neutral 的 candidate / Pareto 合同；真实统计和执行器尚未接入。

---

## 5. P6 最重要的未决问题：authoritative base 放哪里

不预设“RocksDB 再完整复制一份 MySQL”就是答案。必须用同一语义/耐久性合同比较三类方案。

### A. Local versioned state

```text
MySQL → m2s local state → StarRocks
```

优点是 fixed-W 和恢复最可控；缺点是可能与 StarRocks 重复保存大体量数据。

### B. StarRocks base mirror

```text
MySQL → StarRocks base mirror
          ↑
      m2s metadata/delta
```

优点是避免第三份完整 base；难点是必须证明 StarRocks 能提供与 W 绑定、跨重启可恢复的一致读语义。**查询“当前值”不能冒充 fixed-W snapshot。**

### C. Hybrid

```text
StarRocks columnar base
+
m2s local key/index/delta
```

这可能同时降低大规模 base 的重复存储，又保留增量计算需要的低延迟索引与版本信息。

P6A 的任务不是“挑一个 KV 引擎”，而是确定**谁是 authoritative source relation，哪些状态放在哪一层**。

---

## 6. Shared arrangements 必须早于通用 JOIN

如果先实现 JOIN/aggregation，再考虑共享，很容易演化成：

```text
Q1 → 自己一份 hash/index
Q2 → 再一份
Q3 → 再一份
```

因此在开放通用 JOIN 前先稳定 P6C：

```text
arrangement identity
+ watermark
+ schema epoch
+ refcount
+ health
+ retention / GC
```

相同 identity 的 1/10/100 个任务应尽量只维护一份共享物理状态。

---

## 7. 路线图

| 阶段 | 目标 | 当前状态 |
|---|---|---|
| P0–P3 | 基线、语义/故障合同、真实测量 | 部分完成；已有差分、恢复、真实 MySQL/StarRocks 短测 |
| P4–P5 | 仅按 profile 下沉 socket、批处理、布局/融合 | 候选优化，不是 P6/P7 前置 |
| **P6A** | local / StarRocks / hybrid authoritative base 对比 | state 候选 benchmark 已有；尚未选型 |
| **P6B** | commit sequence、schema epoch、fixed-W pin、bounded changelog | fixed-W 原型已有；在线 retention/GC 未接 daemon |
| **P6C** | shared arrangement / materialized-state catalog | identity/retention/Pareto 合同已有；runtime 未实现 |
| **P7** | 新任务从 shared state 做 W→build→catch-up→publish，不重扫 MySQL | 当前 hot-add 功能存在，但仍重扫 MySQL |
| **P8A** | SQL → normalized/incremental IR | 计划 |
| **P8B** | COUNT/SUM/AVG、INNER JOIN，再扩展 LEFT JOIN/MIN/MAX/DISTINCT | 计划 |
| **P9A** | shared arrangements / subviews / common subgraphs | 计划 |
| **P9B** | cost-based reuse / incremental / partial/full recompute | Pareto 合同已有；真实 planner 未实现 |
| **P9C** | workload-driven materialization / GC | 计划 |
| P10 | StarRocks 输出顺序、未知状态恢复、回填调度 | 部分完成 |
| P11 | 50M 初始行 + 50 rows/s + 动态任务，72h | 未完成 |
| P12 | 与 Flink/RisingWave/Materialize/Bytewax/Pathway/Proton/Arroyo 公平对标 | 未完成 |
| P13 | deploy/explain/status/cancel、MCP、升级/回滚 | 计划 |

当前主线：

```text
P1/P2/P3 + P10 最小闭环
        ↓
P6A → P6B → P6C
        ↓
P7
        ↓
P8A → P8B
        ↓
P9A → P9B → P9C
```

P4/P5 只有真实 profile 指向瓶颈时才插入。

---

## 8. 当前已经验证的东西

### 真实 daemon / 动态下游

[CI 36787425832](https://github.com/justgo4/m2s/actions/runs/36787425832) 验证：

- MySQL 8.4.6 → 实际 `j4.py` → StarRocks 4.1.1
- transaction / merge_async
- GTID ON / OFF
- 动态创建第二个不同投影/过滤的下游
- 源断线恢复
- 强制退出后重启
- 最终结果逐键逐字段等于独立 MySQL SELECT

共享 runner 上的小规模观察值：

| 协议 | GTID | 初始阶段 P99 | 新增任务 P99 |
|---|---|---:|---:|
| transaction | ON | 2.197 s | 1.657 s |
| transaction | OFF | 2.249 s | 1.677 s |
| merge_async | ON | 3.781 s | 3.478 s |
| merge_async | OFF | 3.729 s | 3.892 s |

这些是功能短测，不是 50M / 72h SLO 认证。

新增任务回填中强退/续建也通过 [CI 36787945824](https://github.com/justgo4/m2s/actions/runs/36787945824)。

### MySQL/native 正确性和恢复

- Python/C row decoder 差分、真实 MySQL 最终状态对照。
- GTID / 文件位置 durable cursor 恢复。
- 进程 kill / decoder restart / source reconnect 覆盖。
- sanitizer、非法 metadata、NULL/DECIMAL/UTF-8/BINARY/复合键等测试。

相关 CI：
- [36738492453](https://github.com/justgo4/m2s/actions/runs/36738492453)
- [36738492665](https://github.com/justgo4/m2s/actions/runs/36738492665)

### StarRocks 输出协议

真实 4.1.1 测试确认：

- transaction 2PC 与 merge_async 是两条独立协议路径。
- 不能因为请求同时带某些 header 就宣称二者已组合。
- merge_async 未知响应可隔离受影响目标，其他独立目标继续运行。

真实网络丢响应测试：
- [CI 36795348744](https://github.com/justgo4/m2s/actions/runs/36795348744)

### P6 状态候选

`tools/state_layout_benchmark.py` 当前比较：

- SQLite Arrow + key index
- DuckDB typed state
- RocksDB Arrow + key index

均已覆盖：

- state + changelog + watermark 原子边界
- fixed-W checkpoint/replay
- commit 前/后 crash
- 重启恢复
- 冲突/非法输入

公开样本：
- [state-layout-20261001.json](reports/state-layout-20261001.json)
- [state-rocks-20261001.json](reports/state-rocks-20261001.json)

这些只是候选评测，**尚未选定 P6 authoritative backend，也尚未接入 daemon。**

### 最新架构合同

提交到当前主线的 `incremental_contract.py` 已测试：

- deterministic state identity
- schema epoch / predicate / collation 不兼容隔离
- state handle 自身 spec/hash 一致性
- fixed-W pin 对 changelog GC 的约束
- plan candidate validation
- 多维 Pareto frontier

最终相关 Public baseline CI 与 integration CI 均通过。

---

## 9. 明确尚未完成

当前不能宣称完成的核心事项：

- P6 shared authoritative source state 接入 daemon
- 在线 bounded changelog pin / GC
- dynamic task 不再重扫 MySQL
- physical-state catalog / shared arrangements
- SQL incremental IR
- JOIN / aggregation retract semantics
- partial/full recompute planner
- workload-driven materialization / GC
- 50M + 50 rows/s 的 72 小时长跑
- 七个系统同机同语义公平对标
- 生产级升级/回滚和 MCP 控制面

所以目前最准确的定位是：

> **研究级增量数据库系统设计正在形成；组成原理大多有世界一流先例，但跨 MySQL / m2s / StarRocks 的 shared state、fixed-W bootstrap、state placement 和 cost-based IVM 联合设计仍有明显原创实现空间。**

---

## 10. 性能与正确性原则

- 正确性和恢复语义优先于微基准。
- decoder、local pipeline、snapshot、end-to-end 四层分开测。
- 不能隐藏 C 子进程、compaction、source/sink CPU 或额外 state storage。
- 云 runner 只用于回归和发现明显退化；最终性能 gate 在固定机器执行。
- 不用关闭 fsync、恢复能力或 StarRocks 默认安全参数换 benchmark。
- 每个性能结论必须给 workload、资源、重复次数、区间和原始样本。
- “物理极限”只表示持续消除已测瓶颈，不是对任意 SQL 的数学极限。

---

## 11. 运行基线

要求 Linux x86_64、Python 3.12/3.14、C11、CMake >= 3.20、Git。

```bash
python -m venv .venv
. .venv/bin/activate
python -m pip install -r requirements.txt

cmake -S native -B build/native -DCMAKE_BUILD_TYPE=Release
cmake --build build/native --parallel 2

python native/native_abi_selftest.py
python j4.py selftest
```

运行：

```bash
cp setup.sql.example setup.sql
chmod 600 setup.sql
# 编辑本地连接配置，不要提交真实凭据

python j4.py
python j4.py sql setup.sql
# 或
python j4.py cli
```

默认 `CDC_LOAD_MODE='merge_async'`，也支持 `'transaction'`。两者独立验收。

公开仓库只允许代码、合成配置、合成 benchmark 结果和通用文档；真实日志、生产配置、SQLite/WAL、metrics、业务数据和凭据不得提交。

---

## 12. 理论与系统参考

这些工作是设计输入，不代表 m2s 已实现对应能力：

- [DBSP: Automatic Incremental View Maintenance for Rich Query Languages](https://arxiv.org/abs/2203.16684) — 自动 incrementalization / retract semantics。
- [OpenIVM](https://arxiv.org/abs/2404.16486) — SQL-to-SQL incremental computation。
- [Enzyme: Incremental View Maintenance for Data Engineering](https://arxiv.org/abs/2603.27775) — cost-based incremental/full refresh。
- [Noria](https://www.usenix.org/conference/osdi18/presentation/gjengset) — dynamic partially materialized dataflow。
- [Differential Dataflow](https://arxiv.org/abs/1812.02639) / Materialize — shared arrangements。
- RisingWave backfill / snapshot epoch + log-store — fixed snapshot + catch-up。
- Alibaba Streaming View — shared delta / adaptive indexing / warehouse-native incremental maintenance。
- RocksDB snapshot/checkpoint — durable local state candidate。
- StarRocks Stream Load / transaction interface / async MV — sink 和可复用 warehouse state 的现实边界。

m2s 不宣称发明这些已有思想；真正需要证明的是它们在本项目场景中的**联合协议、状态放置、恢复正确性和实际成本**。

---

## 13. 希望外部评审重点挑刺的问题

如果由另一个 AI / 工程师评审，优先检查这些问题：

1. **P6A 是否真的需要 local / StarRocks / hybrid 三选一？** 是否存在更好的 authoritative base 设计？
2. **若 StarRocks 没有足够的 time-travel/MVCC 合同，如何构造可恢复 fixed-W？** 是否必须引入显式 version/tombstone/staging generation？
3. **`state identity` 当前字段是否足以安全共享 arrangement？** 还缺哪些 type/collation/null/order/optimizer semantics？
4. **shared arrangement 应该如何跨 watermark 复用？** 是等待推进、增量补齐还是 fork generation？
5. **SQL → incremental IR** 应优先采用 DBSP、OpenIVM 还是自研受限 IR？怎样避免把 SQL 方言/StarRocks 语义做错？
6. **cost planner 的候选和指标是否足够？** 是否应加入 memory peak、recovery cost、compaction debt、network bytes、future maintenance cost？
7. **跨 MySQL / m2s state / StarRocks 三个一致性域的原子边界是否设计合理？** 哪些地方存在 silent split-brain / stale generation 风险？
8. **在 1/10/100 动态任务下，shared state 的 GC、热点和 refcount/watermark 设计是否会形成新的全局瓶颈？**

如果这些问题的答案导致路线变化，应先更新架构合同，再继续大规模实现。
