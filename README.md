# m2s

单机 MySQL → StarRocks 实时同步与动态 SQL 数据加工项目。目标是让用户随时部署一个 SQL 任务，自动创建下游主键表，同时完成历史初始化与持续增量更新；后续通过 MCP 将自然语言转换成可检查、可部署的 SQL。

本仓库从可执行的 CDC 基线开始，逐阶段向原生增量计算引擎演进。**当前代码是基线，不是已经完成的新引擎；尚无“达到物理极限”或“全面超过其他系统”的测量结论。**下面的验收条件是后续工作的合同，只有提供对应测试和原始结果才能标记完成。

## 当前交付

- `j4.py`：基线主程序，默认启动 daemon；全量回填与增量捕获并发运行。
- `cdc_catalog.py`：持久化 SQL catalog、部署文件、REPL、运行中任务发布。
- `cdc_selftest.py`：离线故障、状态恢复和数据正确性回归。
- `native/`：原生行事件解码器、Arrow IPC、稳定计数分区、JSON 编码、ABI 测试和可复现构建源码。
- `setup.sql.example`：使用保留的 `.invalid` 域名、占位密码和合成表结构的配置示例。
- `tools/privacy_check.py`：发布文件、硬编码凭据、地址与原生 bundle 扫描。
- `tools/partition_benchmark.py`：排序分区与计数分区的重复 A/B 微基准，输出 JSON。
- `.github/workflows/ci.yml`：公有 runner 上的隐私检查、源码构建、原生自测与 Python 回归矩阵。
- `.github/workflows/benchmark.yml`：手动运行合成微基准，上传允许的 JSON 结果。

公开版本仅包含代码、合成测试和通用文档。业务日志、生产数据、连接地址、密码、运行状态和原有提交历史均不进入本仓库。不会从另一个仓库拉取源代码；构建依赖只有明确声明的公开第三方组件。

## 基线运行

当前基线保留 DuckDB 和 Python 调度，避免在建立测量基准之前改变数据语义。SQL 支持单源确定性投影、过滤、宏及模型视图链；**有状态 JOIN、聚合、窗口和跨源查询尚未实现，当前发布会拒绝这些计划。**当前按已部署任务捕获数据，不等于已具备可供任意未来 SQL 使用的全库源状态。

要求 Linux x86_64、Python 3.12 或 3.14、C11 编译器、CMake >= 3.20、Git。使用 MySQL ROW binlog 和 FULL row image；GTID 开启时使用 GTID 恢复，关闭时使用已持久化的文件/位置，源日志过期需要明确报错或受控重建，不能静默跳过。StarRocks 目标版本为 4.1.1，目标表为主键表。安装依赖固定版本，升级也须通过完整回归。

```bash
python -m venv .venv
. .venv/bin/activate
python -m pip install -r requirements.txt
cmake -S native -B build/native -DCMAKE_BUILD_TYPE=Release
cmake --build build/native --parallel 2
cp build/native/mysql_arrow_reader mysql_arrow_reader-linux-x86_64
python native/native_abi_selftest.py
python j4.py selftest
```

上述测试使用合成数据及回环服务，不需要生产数据库凭据。`native/libj4_native.so.gz.b64` 是经过校验的 libc-free x86_64 ABI bundle；构建也会生成本地 `.so`，两种加载路径都纳入 CI。编译产物不提交到 Git。

部署前先在自有 MySQL 中创建 `demo_source.orders`，至少包含主键 `id BIGINT`、`display_name VARCHAR(255)`、`amount DECIMAL(19,4)`，并创建目标数据库。为源端用户授予复制和读取所需权限，为目标用户授予读取 schema、建表和导入所需权限。示例域名不能连接真实服务，需要替换成本地配置。

```bash
mkdir -p runtime
cp setup.sql.example setup.sql
chmod 600 setup.sql
# 在本地编辑 setup.sql，填写自己的地址、账号和密码。
python j4.py
# 另一个终端部署 SQL：
python j4.py sql setup.sql
# 或进入交互终端：
python j4.py cli
```

不需要额外的 `cdc` 参数。daemon、部署和自测均可使用此入口。当前 catalog 保存连接配置，包含本地密码，因此 catalog、状态及其备份也需要访问控制，不得上传为公开 artifact。长时间运行应由 systemd 等进程管理器托管，终端后台任务不作为生产运行方式。

基线默认 `CDC_LOAD_MODE='merge_async'`，另有 `'transaction'` 模式。前者使用 Merge Commit async 并确认事务最终 VISIBLE；后者使用事务接口 begin/load/prepare/commit。**这是两种路径，不是已经验证在同一导入事务中叠加两阶段提交与 Merge Commit。**双机制的兼容性必须以固定版本真实服务的行为验证，不能仅凭请求包含两个 header 判定成功。所有优化维持 StarRocks 默认服务参数；异步请求返回不等于可见，也不能据此删除本地 outbox。对于历史结果查询不到的未知事务，保持可诊断的受控状态，不能擅自当作未执行而覆盖新数据。

## 目标架构与组件决策

数据运行时独立持有数据：MySQL 原生接入 → 事务变化批 → 共享源状态/索引 → SQL 增量执行图 → 持久化 outbox → StarRocks。Python/REPL/MCP 发送低频管理命令，不接收和转发每行业务数据。先采用可恢复、可验证的实现，再用实测替换瓶颈。

| 组件/技术 | 使用范围 | 保留或替换的依据 |
| --- | --- | --- |
| Rust + 现有 C 内核 | 原生运行时、网络接入、调度、增量算子、编码 | 不构造 Python 行对象；C 内核通过 FFI 直接复用。语言不决定性能排名 |
| PyO3 | Python 管理接口、启动、部署、状态、预览 | 长任务脱离 Python 运行时；业务数据不逐行跨边界。独立 daemon 也是隔离候选 |
| DataFusion 模块 | SQL 解析、类型绑定、优化及可复用表达式 | 模块化和扩展能力是选择理由；必须自建增量规划/执行，不把批量引擎直接当作 IVM |
| DuckDB / OpenIVM | 基线、结果 oracle、全量初始化与增量 SQL 候选 | 与 DataFusion/专用算子做相同语义 A/B；不因语言名称排除它，也不让多个引擎重复扫描 |
| Arrow C Data/Stream Interface | 同进程批量交换和接口边界 | 共享兼容缓冲区；不是所有操作零复制，稀疏更新和索引允许专用布局 |
| RocksDB | 持久化共享源状态、事务批、索引/outbox 候选 | 原子批、WAL、恢复；测量读写放大、compaction、长快照和全量扫描后决定是否默认 |
| SQLite | 任务目录与 SQL 定义 | 不让 catalog 承担每行业务数据；数据检查点与状态/outbox 必须有明确原子边界 |
| libcurl / 原生 HTTP 客户端 | StarRocks 连接复用、流式发送、重定向 | 连接、编码和恢复分开测；不手写 TLS；禁止把压缩省流量等同于一定更快 |
| SIMD、融合内核、SIMD JSON、LZ4/Zstd、PGO/LTO、NUMA | 被 profile 证明的热点 | 逐项基准；通用二进制有 ISA fallback，不能靠不兼容指令获得虚假优势 |
| 成本驱动增量/局部重算 | 更新密度和共享计划优化 | 利用 DBSP 的差分语义与 Enzyme 的策略选择思路；首先证明结果等价，再比较成本 |

不立即重写通用 SQL 语义、TLS、持久化存储引擎。自研重点是共享状态、增量算法、数据布局、算子融合和调度。任何新增组件必须解释省掉了什么工作、增加了什么成本，不能因“先进”而堆叠到热路径。

## 分阶段任务与验收标准

按依赖顺序完成。每阶段交付实现、测试、可复现命令、公开合成结果及相对基线的报告。标为“计划”的条目并未实现；只有 CI 通过仍不代表生产认证完成。

| 阶段 | 接下来做什么 | 完成必须达到的标准 | 当前状态 |
| --- | --- | --- | --- |
| P0 公开基线 | 导入可执行主程序、必要模块和 C 源码；重建示例/文档；独立 CI | 无业务配置、数据、源仓库地址或源提交历史；源码构建、ABI bundle/本地库和完整离线回归通过 | 本次建立，结果见 Actions |
| P1 正确性合同 | 明确源事务、主键、NULL、时区、撤回、过滤变化、schema 变化和状态版本 | 同一 raw event 由参考解码器与原生解码器双跑，比较 Arrow schema/null/value/op/order；覆盖整数边界、DECIMAL、UTF-8/emoji、二进制、日期时间、复合键、更新主键、多行/多事件事务；不支持类型显式拒绝 | 进行中：已加同字节差分与真实 MySQL CI，完整语义 gate 未完成 |
| P2 故障与输出协议 | 注入建连失败、断网、decoder kill、短写、缺失/忽略 TABLE_MAP、帧损坏、进程强杀、磁盘满、输出超时及历史清理 | 至少 1,000 个可重放故障案例，恢复后逐键结果等于 oracle；checkpoint 不越过持久化状态；GTID/文件位置双模式；未知输出事务不被静默丢弃或无保护重放；4.1.1 真实验证两种协议和双机制兼容性 | 进行中：已加 1,000 个原生协议故障用例，完整恢复和真实 SR gate 未完成 |
| P3 测量平台 | 建 decoder、local pipeline、snapshot、end-to-end 四层基准和热点 profile | 每层记录 wall/CPU、RSS、rows/s、MB/s、IPC、分配、磁盘写入和输出放大；端到端计时从 MySQL COMMIT 到查询可见；公开环境、种子、版本和原始 JSON；保持相同耐久性 | 计划 |
| P4 原生接入与批处理 | 将复制协议和必要 snapshot 读取下沉，复用 C 解码；事件/事务组批，移除业务数据经 Python 与子进程一发一等 | Python 不再创建源业务行；事务/巨型事务/重连语义通过 P1/P2；相比基线在 source-bound 负载吞吐不下降且总 CPU/row 下降至少 20%，否则不设为默认 | 计划 |
| P5 原生布局和融合 | 零额外格式交换、选择向量、稳定分区、表达式/编码融合、自适应批大小 | 避免不必要 combine/take/IPC 往返；公布实际复制和分配字节；窄/宽/稀疏批均测；错误、NULL、溢出语义不变；至少一个已识别热点 CPU/row 下降 20% | 计划 |
| P6 共享源状态 | 全库/订阅源的持久化最新状态、共享主键索引、事务 changelog、保留策略；基准 RocksDB 与适合扫描的布局 | 同一源只捕获一次；源状态与位置原子提交；任意未来任务可用列不能因当前投影而被丢弃；内存预算下溢写、恢复、schema 演进通过；明确并发跨表 snapshot 的一致性边界 | 计划 |
| P7 动态新增下游 | 固定水位 W、历史构建、保留 W 后变化、追赶、切换；取消、失败和重启可恢复 | 回填期间既有任务持续运行；新增任务结果等于水位一致的 oracle；交错更新/删除/主键改变不丢不倒序；10 次部署和故障混合测试；恢复不依赖内存中的 RocksDB snapshot 句柄 | 计划 |
| P8 原生增量 SQL | 先过滤/投影，再索引 INNER/LEFT JOIN、COUNT/SUM/AVG/MIN/MAX、DISTINCT；支持撤回 | 至少 10,000 组随机有重复/NULL/删除的事务与全量 SQL 结果等价；LEFT JOIN 和 MIN/MAX 删除测试；输出主键和跨表事务语义明确；缺状态时不声称完整结果 | 计划 |
| P9 共享执行与自适应策略 | 多任务公共子图、共享 arrangements；稀疏增量/密集分区重算；数据密度驱动调度 | 1/10/100 个任务的共享与非共享 A/B；任务新增不复制整份源状态；策略切换结果等价；输出 fanout 和 hot key 成本明确；性能收益覆盖策略维护开销 | 计划 |
| P10 StarRocks 与回填调度 | 原生编码/合批、可见性确认、按 tablet 压力和可见延迟限制回填；恢复后渐进提速 | 默认服务参数下版本压力可自动限速/恢复；不把 Merge Commit 当作免 compaction；JSON/CSV/压缩分别验证适用性；背压不能无上限堆内存；取消和未知事务不污染新结果 | 计划 |
| P11 目标规模与长跑 | 5,000 万历史行 + 持续 50 行/s，期间动态新增 SQL 任务；小事务、高吞吐、宽行、倾斜、JOIN/聚合分别测 | 固定规格机器上运行至少 72 小时；正常时段 commit→VISIBLE P95 <= 5 秒、P99 <= 10 秒；排空后逐键/逐字段正确；故障时段单独报告且恢复可界定；不靠修改 StarRocks 默认参数过关 | 计划 |
| P12 逼近硬件上限与对标 | 测源读取、内存带宽、耐久写入和 sink 单独上限；比较 Flink、RisingWave、Materialize、Pathway、Bytewax、Arroyo、Proton | 同机/同资源/同语义/同耐久性，固定版本；至少 5 次重复，报告中位数和区间、失败及不支持项；解释剩余 CPU/复制/I/O/输出成本；只对已测负载声明领先 | 计划 |
| P13 MCP 与发布 | 自然语言→SQL→EXPLAIN/资源估算→部署→状态/取消；版本化 API、二进制发布、运维流程 | MCP 不执行任意 shell；权限、SQL 成本和 DDL 边界明确；无凭据回显；可回滚；72 小时与恢复 gate 通过后才建立生产候选版本 | 计划 |

P1/P2 与 P3 可以交替推进；任何性能实现必须通过已有正确性 gate。P4 对 snapshot 原生化先验证瓶颈：如果 sink 已饱和，优先减少回填和输出成本，不为了下沉而下沉。分布式集群化在单机合同、状态格式和恢复机制稳定以后另行规划。

## 性能合同

“物理极限”是固定硬件、数据语义、持久性和目标可见性下，持续识别并减少瓶颈的工程目标。不会用某个内核的几十倍加速，推导整套系统几十倍加速。

四层测试必须分开：解码器只测 raw event→变化批；local pipeline 包含路由、状态和 durable journal；snapshot 包含实际 MySQL 读取和状态准入；end-to-end 包含真实 COMMIT、StarRocks 查询确认和数据正确性。最后一层才证明 5–10 秒新数据可见。每层性能必须同时记录两进程/多线程总成本，不能把 C 子进程、compaction 或源/目标 CPU 隐藏掉。

对低频增量使用时间上限触发的小批，对高吞吐使用字节/行数阈值；不能靠扩大 batch 牺牲延迟。回填调度给 CDC 留出预算，并通过队列年龄、VISIBLE 延迟、目标版本压力和本地磁盘余量调整。源停机、sink 不可用时不可能继续满足可见性 SLO，需要报告不可用时长和恢复追赶成本。

资源参考上限分别测量：MySQL 拉取能力、内存读写带宽、带 fsync 的本地状态写入、网络以及 StarRocks 持续导入/compaction 能力。它们只是经验参考上界，不是对任意 SQL 的数学极限证明。关联一对多、多个下游 fanout 和聚合热点必须计入输出/状态放大。

现有 `tools/partition_benchmark.py` 比较的是两种**分区算法**，不是完整 reader 的 A/B，更不是各实时计算系统的对标。数据构造和加载准备不计入内核计时；Arrow `take` 计入完整分区耗时；每次比较检查结果相等。云 runner 的微基准用于发现明显回退，生产性能 gate 必须在固定机器执行。

```bash
python tools/privacy_check.py
python tools/partition_benchmark.py --rows 1000000 --repeats 7 \
  --output benchmark-results/partition.json
```

后续性能默认 gate：正确性零差异、固定机器吞吐不回退超过 5%、P99 延迟不回退超过 5%；宣称优化时需要重测排除噪声，并满足对应阶段的热点成本改善。存储 fsync、检查点频率、编译 ISA、压缩、源目标资源都必须列出，不允许关闭恢复能力换成绩。

## 公开 CI 与隐私边界

CI 使用公有仓库 runner，不限制后续需要的运行次数；每个任务有 timeout 和并发取消，避免无意义重复消耗。当前 CI 不连接任何生产服务。后续 MySQL/StarRocks 集成环境只使用隔离容器和合成数据，50M/72h 测试使用专用隔离 benchmark 机器。

Actions 固定到已核实的公开发布 commit，权限仅 `contents: read`，checkout 不保留凭据。隐私 gate 只检查版本控制的文件；发布前还需检查新 commit 和 artifact 白名单。运行 catalog、日志、metrics、SQLite/WAL、真实 SQL 部署文件和 core dump 不上传。公开 artifact 仅限原生构建产物与合成 benchmark JSON。

示例中的占位密码和离线测试中的短假密码仅用于合成测试，不能替换成真实配置后提交。未来引入 secret provider、依赖 SBOM、构建签名、sanitizer/fuzz 和 release 审查；公有库本身不保存业务凭据。隐私扫描只是自动 gate，不能替代发布前人工审查。

## 理论与官方资料

- [DBSP: Automatic Incremental View Maintenance for Rich Query Languages](https://arxiv.org/abs/2203.16684)：差分、积分和有撤回的增量语义。
- [Enzyme: Incremental View Maintenance for Data Engineering（2026）](https://arxiv.org/abs/2603.27775)：成本驱动刷新策略和管线优化，作为策略候选而非速度保证。
- [OpenIVM: a SQL-to-SQL Compiler for Incremental Computations](https://arxiv.org/abs/2404.16486)：评估复用 DuckDB 的增量 SQL 路线。
- [DataFusion optimizer](https://datafusion.apache.org/library-user-guide/query-optimizer.html) 与 [SQL 扩展](https://datafusion.apache.org/library-user-guide/extending-sql.html)：复用模块与自定义计划。
- [Arrow C Data Interface](https://arrow.apache.org/docs/format/CDataInterface.html)：同进程数据交换和资源生命周期。
- [PyO3 performance](https://pyo3.rs/main/performance.html)：控制跨语言调用与运行时约束。
- [RocksDB Overview](https://github.com/facebook/rocksdb/wiki/RocksDB-Overview)：原子批、WAL、快照及 compaction 成本；快照句柄不跨重启持久化。
- [StarRocks Stream Load](https://docs.starrocks.io/docs/loading/StreamLoad/) 与 [事务接口](https://docs.starrocks.io/docs/loading/Stream_Load_transaction_interface/)：当前官方资料，固定 4.1.1 实测优先于滚动更新的 Latest 文档。

每个后续里程碑都更新这里的状态和公开结果链接。提交了一份计划不等于完成计划；代码通过离线测试不等于生产认证。

## 实施记录

2026-09-30：新增独立 Python ROW/FULL 解码 oracle；同一 TABLE_MAP/row event 与 C decoder 对比 Arrow schema、值、NULL、操作和顺序，并交叉检查 native snapshot。合成用例覆盖 21 列、整数 signed/unsigned 边界、DECIMAL(p,p)、UTF-8/emoji、二进制、DATE/DATETIME(6)、复合键和更新主键。新增隔离 MySQL 8.4.6 的 1,000 事务差分 workflow，并将重放后的结果与真实 MySQL 最终 SELECT 比较。

本地 1,000 个事件（3,332 个行镜像）和 267 个 snapshot 行差分通过；另有 1,000 个协议故障用例通过。修复 DECIMAL(p,p) snapshot 前导零误计 precision，以及截断 CONFIG 清理路径可能解引用未分配列的问题；拒绝非法类型、decimal/fsp 元数据和缺失 nullable bitmap。ASan/UBSan 与真实 MySQL 验证由 Actions 执行。此结果不是全套 P1/P2 完成，也不是生产性能证明。

```bash
python tools/binlog_parity.py --cases 1000 --faults
# 仅对可删除 synthetic 数据库的隔离 MySQL：
python tools/binlog_parity.py --live --cases 1000
cmake -S native -B build/sanitized -DM2S_SANITIZERS=ON
cmake --build build/sanitized --parallel 2
python tools/binlog_parity.py --binary build/sanitized/mysql_arrow_reader --cases 1000 --faults
```

组批候选：`CDC_NATIVE_EVENT_GROUP_EVENTS=128` 将同一源事务内的 TABLE_MAP/row events 合并发送，在 C 内按源表共享 Arrow builder；单批达到 8,192 行或累计 16 MiB Arrow 容量后输出，源事务的 SQLite commit 仍在最终 ACK 与 COMMIT/XID 后发生。Python 输入缓存最多 4 MiB（单个更大合法事件沿用原限制）；单个源事件的峰值仍需测量。默认为 `1`，保留逐事件 A/B 和旧 decoder 兼容性，不在完整性能 gate 前默认为新路径。

真实 MySQL 最终 SELECT 对照发现并修复固定长度 BINARY 的 binlog 尾部零填充与 snapshot 不一致。新增两表交错组批差分与损坏 group 故障；新增实际 `capture_binlog_native` 恢复循环测试，注入首次建连失败、事务中断和子进程 kill，确认 GTID/文件位置都从 durable cursor 重放、CONFIG 重建、SQLite 仅提交一次。

```bash
python tools/native_recovery_test.py --cases 1000
python tools/decoder_benchmark.py --layer decoder --events 1000 --rows-per-event 1 --repeats 5
python tools/decoder_benchmark.py --layer local --events 1000 --rows-per-event 8 --repeats 5
```

A/B 每个样本独立 Python 进程，输出 Python/C/总 CPU、wall、RSS、rows/s、IPC 字节、SQLite 文件总字节、事务批 P50/P95/P99。fixtures 构造、初始化不计 wall；C CPU 计入子进程完整生命周期。此测试不含 MySQL socket/StarRocks，不把批延迟解释为端到端延迟；RSS 无读取权限时为 unknown。local 模式保持 SQLite FULL/WAL 与相同源事务边界。

`tools/starrocks_contract.py` 与 Actions 的隔离 StarRocks 4.1.1 probe 验证：2PC 在 commit 前不可见、merge async 实际共享 TxnId 并最终 VISIBLE；组合 header 需同时通过功能与无效参数负对照，不能把 HTTP success 当作双机制叠加成功。只上传合成计数和结论，不连接生产服务。
