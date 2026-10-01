# m2s

单机 MySQL → StarRocks 实时同步与动态 SQL 计算项目。

目标：一次捕获源数据，已有任务持续更新；运行期间新增 SQL 和下游表，完成历史构建后持续增量维护。共享源镜像完整后，新任务正常情况下不再扫描 MySQL，也不默认复制整份基础数据。未来 MCP 自然语言入口复用同一套 SQL 校验与部署协议。

**当前是可运行的 CDC/动态投影基线，目标中的共享源状态与有状态 SQL 引擎尚未完成。** 本文区分已有证据、待实现协议和研究候选；不宣称已达到物理极限、生产就绪或全面超过其他引擎。

## 1. 场景与待验证假说

第一期限定一个 MySQL 实例、单机运行、稳定主键、ROW binlog + FULL row image，支持 GTID 和文件位置恢复。显式登记源库表及全部允许列；后续 SQL 只能使用已捕获的信息。StarRocks 固定 **4.1.1 主键表、默认服务端参数**。不支持的 SQL、破坏性 DDL、日志缺口必须拒绝或进入显式重建流程。

核心负载为 **5000 万历史行 + 每秒 50 行增量**。基础同步的初始回填与 CDC 并行，旧回填不得覆盖新版本或复活已删除记录。目标是已有就绪任务在健康运行期间，从 MySQL 事务提交到 StarRocks 可查询 **P95 ≤ 5 秒、P99 ≤ 10 秒**；另报最大延迟、超标次数和故障期间表现。新增任务另报 `time_to_ready` 和是否完整，不能用初始化期混淆实时 SLO。

默认保存当前关系和有界变化历史，不永久保存数据库全部历史。未来任意新增查询仅指届时支持的、确定性的 SQL 范围；未捕获的列和已丢弃的历史无法凭空重建。完整当前关系必须在某处持久保存。

待验证的价值是：**在既有任务 SLO 和资源预算内，结合可恢复的在线构建、共享状态与状态放置，降低新增任务的构建时间、重复存储和持续维护成本。** 增量计算、共享索引和代价规划均有先例；这里的贡献需要联合协议、实验和反例检验来证明。

## 2. 当前代码与证据

- [j4.py](j4.py)、[cdc_catalog.py](cdc_catalog.py)：daemon、SQL catalog、动态部署。当前支持确定性的单源投影、过滤、宏和模型视图链；拒绝有状态 JOIN、聚合、窗口及跨源计划。**当前 hot-add 历史初始化仍启动 MySQL `snapshot_worker`。**
- 当前组件包括 Python 控制/恢复、C 行事件解码与批处理内核、Arrow 批表示、DuckDB SQL/列计算、SQLite 持久队列。native 路径仍有 Python 网络/协议与 IPC 成本，不等于完整 C replication client，也不等于零拷贝。不预先决定移除 DuckDB 或用某种语言重写所有组件。
- [incremental_contract.py](incremental_contract.py)：独立于 daemon 的纯合同模块，已有 state spec/hash、handle 校验、日志保留水位计算和六维候选 Pareto 筛选。**尚无在线共享状态、实际统计、全局优化器或执行器接入。** 表达式规范化仍由调用方负责。
- [native/](native/)、[tools/](tools/)、[cdc_selftest.py](cdc_selftest.py)：解码差分、恢复、协议、故障注入和候选性能测量。

| 已验证范围 | 可核查证据 | 证据边界 |
|---|---|---|
| 实际 daemon；MySQL 8.4.6 → StarRocks 4.1.1；GTID ON/OFF × transaction/merge_async；动态第二下游、断线、强退重启；逐键逐字段 oracle | [端到端 CI](https://github.com/justgo4/m2s/actions/runs/36787425832)、[公开样本](reports/e2e-20261001.json)；[回填中强退 CI](https://github.com/justgo4/m2s/actions/runs/36787945824) | 合成短测、共享 runner，不能证明 50M/72h SLO |
| Python/C 解码差分、真实 MySQL、多类型、durable cursor、decoder 重启、sanitizer | [native/integration CI](https://github.com/justgo4/m2s/actions/runs/36795348569)、[baseline CI](https://github.com/justgo4/m2s/actions/runs/36795348655) | 正确性/恢复证据，不是性能极限证明 |
| merge 请求已接受但响应丢失；保留未知请求、隔离目标；重启后其他目标仍正确；未知 payload 未重复发送 | [真实网络故障 CI](https://github.com/justgo4/m2s/actions/runs/36795348744)、[报告](reports/merge-quarantine-20261001.json) | 不代表受影响目标已自动完成远端对账和恢复 |
| SQLite Arrow/key index、DuckDB typed state、RocksDB Arrow/key index；原子提交、fixed-W 原型、崩溃恢复 | [layout 报告](reports/state-layout-20261001.json)、[RocksDB 报告](reports/state-rocks-20261001.json) | 小规模候选；部分构建暂停写入并复制 checkpoint，未证明在线版本 pin/GC，未接 daemon |

2PC 与 merge_commit async 是已独立验收的两条输出协议。默认 `merge_async`，可选 `transaction`；不能因同时设置 header 就声称双机制已叠加。已知事务 ID 的恢复与“是否被接收也未知”的请求必须分开处理，后者不得盲目重放。

## 3. 必须先成立的运行协议

### 3.1 事务、水位与完整性

目标以源为中心：单一事务流只向共享基础状态和 changelog 持久化一次，再由任务消费；新增任务不新增 replication reader。基础状态保存可重建当前关系的信息，共享 arrangement/subview 保存可复用索引或派生结果，task generation 保存私有算子状态、构建进度和 outbox。

必须分别持久记录：

| 水位 | 含义与原子边界 |
|---|---|
| `source_durable` | 完整源事务的 base 变更、changelog、源 cursor 原子提交；多张源表的同一事务不能被拆成不一致切面 |
| `task_compute` | 某个 generation 的算子状态、输出 delta/outbox、计算进度原子提交 |
| `target_visible` | 所需输出已在 StarRocks 可查询的**连续完成前缀**；并发完成的最大序号不能越过未完成的空洞 |

提交序号须与 source UUID/epoch、GTID 或文件位置持久关联。初始跨表镜像也须证明共同切面；每张表分别扫描完成不足以证明全局一致。跨表 JOIN 在同一个源事务切面计算。HTTP 接收不等于可见；本地原子性也不等于多个 StarRocks 目标的跨表原子可见性，后者只有独立协议证明后才能承诺。

`complete(W)` 与 watermark 分开记录：完整扫描前，进度领先不代表关系完整。单源同步可明确提供未完整的实时预览；JOIN/聚合未完成初始化时不能发布为完整结果。

### 3.2 Fixed-W 构建与回收

新任务流程为：**原子取得 W 与保留句柄 → 读取一致 S(W) → 构建 generation → 重放 Δ(W, now] → 追赶 → 输出可见 → 发布**。源 CDC 在构建期间继续推进。

不能扫描不断变化的 latest 后重放旧 W；也不能只 pin changelog。必须共同保留 S(W) 所需的数据版本、schema、删除标记、索引/文件及 W 之后日志。获取快照和登记 pin 与 GC 之间不能留竞态窗口。

持久 manifest 至少记录 source epoch、W、可读版本/文件、schema、扫描进度、重放进度和 generation。重启后仍能恢复同一 W；不能仅依赖进程内 snapshot。日志保留下界取有效消费者和 build pin 的最小水位，数据版本按独立的可达性规则回收。没有消费者/pin 才可推进到源持久水位。

容量不足时优先限制新构建和慢任务，必要时对源施加背压；不得删除仍需读取的版本。释放 pin 必须与完成或已隔离的取消绑定。长期保留旧版本的空间成本必须计入预算；“不复制整份 base”不意味着零额外空间。

### 3.3 Generation 与输出隔离

generation 更替须证明旧的在途请求不能覆盖新结果。停止本地线程不是远端 fence；可选择隔离的物理目标代际，或经实测成立的远端 fencing/drain 协议。发布前必须达到相应完整性和 VISIBLE 条件。目录/路由切换与 StarRocks 换表能力分别验收，不预设可原子交换表名。

输出主键必须稳定、确定；UPDATE 按旧值撤回再加入新值处理，过滤条件改变和 DELETE 必须撤回旧结果。失败目标保留 outbox 和必要状态、有限重试后进入 degraded；其他独立目标继续。未知请求的自动对账、解除隔离和代际修复仍需实现，不能为了进度直接确认成功或重放。

## 4. 状态放置与安全复用

### 4.1 P6A：先证明可行，再比较成本

不把 local、StarRocks、hybrid 当作已可互换的三个 backend，也不同时建设三个完整运行时。先实现一个满足上述合同的最小方案，再用相同语义/耐久性比较候选。

| 候选 | 进入性能比较前的能力门禁 |
|---|---|
| 本地 versioned state | 原子事务、可恢复 fixed-W、在线版本 pin/GC；比较 KV 与**不可变压缩列式 base + 版本 delta/键索引**，不默认每行全进 KV |
| StarRocks 基础镜像 | 在实际 4.1.1 证明源序号 → 可读版本、旧值/删除保留、分页一致性、跨重启续建；READ COMMITTED 或查询当前主键表本身不构成该证明 |
| Hybrid | 明确每层的权威数据和崩溃恢复责任；证明 base 与本地索引/delta 的一致 cut 和可恢复更新协议，不能仅凭架构图判定可行 |

比较重复数据、索引、存活版本、日志、checkpoint 的**唯一物理字节**以及 CPU、I/O、构建/恢复时间；共享 segment/SST 不重复计算，也不能隐藏 StarRocks 的存储与 compaction 成本。

### 4.2 Identity 相同只是复用的必要条件

现有 `state_compatible(..., minimum_watermark)` 表示 hash/语义匹配且进度不低于下限，**不是 fixed-W 可读性证明**。例如任务需要 W=100，状态已到 200，却不保留 100 的版本，就不能直接拿它初始化。

目标复用检查须分开验证：语义兼容、所需版本可读、可 pin/追赶、健康与可恢复性、backend 格式兼容。语义 identity 还需源实例/epoch 和关系身份、列类型/精度、时区、NULL/bag 语义、表达式 collation、宏/UDF 定义版本。watermark、路径、refcount 属于实例状态，不能混进语义身份。以上扩展**尚未全部在当前合同模块实现**。

先稳定最小接口：获取/构建状态、读取固定版本、订阅变化、retain/release、持久进度与安全 GC；不要求完成通用优化器后才能做第一个 JOIN。相同 identity、相容版本与消费进度的任务才可共享，慢消费者的保留成本须显式归属，不能只凭 refcount 回收。

## 5. SQL、规划与性能路线

编译目标是 **SQL → 规范关系 IR → 带撤回语义的增量 IR → 物理计划**。初期采用受限算子集合，允许少量手写物理算子；不为每条 SQL 永久添加独立 handler。先做 COUNT/SUM/AVG 和 INNER JOIN 的完整撤回/恢复，再考虑 LEFT JOIN、MIN/MAX、DISTINCT、窗口。类型、溢出、NULL、重复值、复合键及确定性需统一语义，不能只比较打印出来的值。

DuckDB、DBSP/OpenIVM 路线、DataFusion、现有 C 内核和自研算子均是可复用候选。选择看覆盖语义、可恢复状态接口和实测总成本；C/C++/Rust/Zig、PyO3 不是性能结论。Python 保持函数式、下划线命名，不引入 OOP、typing、logging，日志使用 `print(..., flush=True)`；只有 profile 与 A/B 收益支持时才下沉热点。

规划策略包括复用、增量构建、局部重算和全量重算。**先按语义、恢复能力、资源/SLO 过滤，再在相同输入切面和完整性条件下比较成本。** 单任务 Pareto 不等于整图最优：上游较便宜的重算可能放大下游 delta；共享状态的新建成本、复用成本及持续维护成本也不同。

目标成本覆盖整个 DAG：构建/追赶时间、峰值 RSS、唯一状态字节、source/base 读取、CPU、磁盘/网络写入、spill/compaction 债务、恢复时间、输出 delta 大小、StarRocks 未可见队列和发布耗时。加入估计误差、运行反馈和切换滞后，避免策略振荡。高 fan-out JOIN 的输出可能远大于输入，不能只按“源 50 行/秒”估算能力。准入需验证构建期间有足够追赶余量；预计无法追上的任务不能无条件接受。

资源调度先保障已就绪任务，再分配新构建的 CPU、I/O、内存和输出配额。版本压力升高时合批、降低回填速率/并发，并保留可恢复进度；不能依靠修改 StarRocks 默认参数、关闭 fsync 或无界重试通过验收。

## 6. 实施顺序与退出门禁

编号沿用现有追踪，按可运行闭环推进；不把所有研究候选变成第一版的强制依赖。

| 阶段 | 下一步与通过标准 |
|---|---|
| P0–P3 / P10 | 保持现有差分/故障测试；定义上述事务、水位、完整性、未知请求和代际协议；跨目标原子性未证明则明确不承诺 |
| **P6A/P6B** | 选择一个可行放置方案接入源镜像；原子 base/log/cursor；版本、schema、tombstone 与日志共同保留。构建过半强退，继续源 UPDATE/DELETE 并执行 compaction/GC 后重启，仍从同一 W 正确续建；测试 W 获取/pin 与 GC 竞态、空间耗尽和 schema epoch 变化 |
| **P6C 最小接口 + P7** | daemon 接入共享状态，先让单源投影 hot-add 不回源；镜像完整后 MySQL 历史 SELECT 次数为 0，不默认复制 base。并发新增/取消/替换至少 10 个 generation，故意延迟旧请求、乱序完成输出并在发布前后强退；逐字段 oracle 正确且 visible 水位不越洞 |
| **P8A/P8B** | 受限增量 IR、COUNT/SUM/AVG、INNER JOIN；至少 10,000 次带多表同事务、UPDATE/DELETE、重复值、NULL、复合键和 skew 的随机操作，对照独立完整查询；跨构建/计算/outbox 崩溃点恢复后保持一致，再扩展其他算子 |
| **P9A/P9B/P9C** | 共享 arrangement/subview、整图策略、统计反馈与 GC；1/10/100 个语义相同/部分共享任务，验证只维护所需共享状态、慢任务取消后安全回收；比较共享开/关、固定 IVM/自适应、不同放置，其他语义与耐久性保持一致 |
| P4–P5 | 按 profile 插入 event/transaction batching、布局融合、native socket/snapshot；同 raw binlog 差分先通过，再重复 Python/native A/B，报告两进程总 CPU/RSS 和 IPC 成本 |
| **P11** | 固定机器 50M 初始行 + 50 rows/s，72h 并动态新增任务/注入故障；报告健康区间 P95/P99、违规数、恢复区间、time-to-ready、空间与版本债务；每个部署给可完成的预算，超预算明确拒绝/等待 |
| P12 / P13 | 公平对标后再给优势结论；补可解释 deploy/explain/status/cancel、预算准入、权限、版本化升级/回滚，MCP 接同一控制面；catalog 已保存不等于任务已激活 |

最短主线是：**可恢复源镜像与 fixed-W → 不回源的单源动态构建 → 最小有状态 SQL 闭环 → 共享/代价策略**。完整优化器和更多语言重写不应阻塞前两步。

测量分 decoder、local durable pipeline、snapshot、端到端四层。最终 gate 用固定机器、固定资源，记录数据宽度、事务/event 大小、重复次数和原始样本；提交/可见时间的观测方法与时钟误差也要说明。计算 Python/C 子进程、source/sink CPU、compaction、存储及网络的全部成本。

与 Flink、RisingWave、Materialize、Bytewax、Pathway、Proton、Arroyo 对比时，采用相同 SQL/结果语义、源与目标、耐久性和恢复要求；分别报告共同支持的负载与功能缺失。吞吐、延迟、构建时间、空间与恢复成本分开列出。单机源/网卡/目标写入能力是需实测的上界；任意 SQL 下全面超越所有系统没有普遍保证。

## 7. 研究输入及适用边界

以下作为设计候选，不表示已经实现；新论文也不自动优于经过验证的工程方案。

| 一手资料 | 可借鉴内容与边界 |
|---|---|
| [DBSP](https://arxiv.org/abs/2203.16684) | 增量化与撤回代数；不自动提供本项目的 source/state/sink 事务协议 |
| [Shared Arrangements](https://arxiv.org/abs/1812.02639) | 跨查询共享维护索引；需补版本读取、持久 pin、恢复与资源归属 |
| [OpenIVM](https://arxiv.org/abs/2404.16486) | SQL-to-SQL 编译路线；论文原型覆盖不能当成当前通用 SQL/CDC 能力证明 |
| [Enzyme（2026）](https://arxiv.org/abs/2603.27775) | 增量/局部/全量策略及整图成本；依赖源版本与变化跟踪，不能省略本项目的 fixed-W 合同 |
| [Streaming View（2025）](https://www.vldb.org/pvldb/vol18/p5153-zhou.pdf) | 仓内增量维护与运行策略；本项目额外面对 MySQL/m2s/StarRocks 三个一致性域 |
| [RisingWave backfill（2026）](https://www.risingwave.com/blog/backfilling-in-risingwave-from-historical-initialization-to-continuous-streaming/) / [Noria](https://www.usenix.org/conference/osdi18/presentation/gjengset) | 固定快照追赶、共享状态构建及部分物化；部分结果不能冒充完整下游关系 |
| [Heavy-Light IVM（2026）](https://arxiv.org/abs/2605.08397) | skew 下的分区/增量维护算法候选；常数延迟枚举不是总输出成本常数，SQL bag/NULL、持久化和恢复仍需验证；放在正确 JOIN 基线之后评测 |
| [StarRocks SQL transaction 官方文档](https://docs.starrocks.io/docs/loading/SQL_transaction/) | 事务/隔离能力边界；文档的事务承诺不等于可恢复 time travel，实际 4.1.1 门禁由集成测试确认 |

## 8. 给外部评审者的问题

请基于当前实现和本文待实现合同，先给反例和优先级，再推荐组件。尤其检查：

1. W=100、共享状态已到 200：哪些持久版本和 pin 才足以安全复用？构建强退后源继续更新、GC/compaction，恢复协议是否仍成立？
2. 跨表初始镜像、源事务、task state/outbox 和可见前缀之间是否有遗漏、重复或错误确认窗口？请给最小事件序列。
3. 旧 generation 的 HTTP 请求晚到、部分目标已可见、替换发布中强退：如何避免污染新结果？哪些目标能力尚未实测？
4. local 列式 base + delta、KV、StarRocks 镜像或 hybrid，哪个首先满足版本/恢复合同？是否有更简单可行的首版，哪些存储成本容易漏算？
5. 最小共享接口是否足以先做一个正确 JOIN？IR 的类型/NULL/bag/撤回语义和 identity 是否缺字段？哪些共享必须拒绝？
6. 单任务最优为何可能损害整个 DAG？100 个任务、热点 key 或高 fan-out 下，预算、GC、SLO 准入及策略切换如何失效？
7. 联合设计相对已有工作有何可验证贡献？应删去哪些研究路线，补哪些实验，才能对性能优势给出可信结论？

建议评审输出：**必须修正 / 可后置 / 无证据的主张**，每项附失败机制、最小修正和验收测试。不要把路线图当成已实现能力，也不要仅凭组件名称评价性能。

## 9. 运行现有基线

Linux x86_64、Python 3.12/3.14、C11、CMake ≥ 3.20。

```bash
python -m venv .venv
. .venv/bin/activate
python -m pip install -r requirements.txt
cmake -S native -B build/native -DCMAKE_BUILD_TYPE=Release
cmake --build build/native --parallel 2
python native/native_abi_selftest.py
python j4.py selftest

cp setup.sql.example setup.sql
chmod 600 setup.sql
# 编辑本地连接配置
python j4.py
```

保持 daemon 运行，在另一终端使用 SQL 文件部署或交互 CLI：

```bash
python j4.py sql setup.sql
# 或
python j4.py cli
```

公开仓库只提交通用代码、合成配置/数据和可公开的测量。真实凭据、连接地址、业务字段/数据、生产日志、SQLite/WAL 和运行 metrics 不得提交；公开报告也需检查隐私。
