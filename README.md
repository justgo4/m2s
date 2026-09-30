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

## 全局路线修订（2026-10-01）

保留正确性、故障恢复、差分测试和实测驱动优化。修正此前将 C socket、运行时重写和底层布局排在共享源状态、动态部署之前的顺序：用户最重要的能力是“运行中新增 SQL 下游，自动历史初始化并持续更新”，必须先形成可恢复的最小端到端闭环。性能工作贯穿闭环建设，但某个解码热点的改善不是产品能力的前置条件。

当前 native reader 仍由 Python 读取复制包，再交给 C 解码；已有组批是候选优化。当前 SQLite 保存 CDC 作业和位置，snapshot 使用分块扫描、水位屏障及 touched 键；这些机制不能直接当作任意多表 SQL 在同一时点的完整源状态。当前 SQL 为 DuckDB 方言的受限单源子集。以下目标合同均是待实现的设计，不表示基线已经满足。

### 先明确数据与可见性合同

- **源范围**：第一版以一个 MySQL 实例、一个可确认的事务流为边界，显式登记需要镜像的库/表及其全部允许列；不因当前 SQL 投影而丢弃未来加工需要的列。全库模式也要处理新表发现、权限、容量和 DDL，不能直接宣称支持所有 MySQL 类型、无主键表或任意新增表。先要求稳定源主键；无主键、不可支持类型及破坏性 DDL 有明确拒绝/隔离策略。源镜像是最新状态加有限 changelog，不默认永久保存全部历史版本。
- **语义**：将确定性的 SQL 子集、NULL、DECIMAL 溢出、时区、字符排序/主键比较、JSON、更新前后镜像、删除及 schema 版本形成合同。明确源键等价规则与输出键编码；不能用字符串哈希碰撞概率代替键正确性。不支持的查询部署前拒绝。DDL 与数据事件的先后顺序也要持久化。
- **新鲜度与完整度分别报告**：单源复制/投影允许历史不完整时持续输出新记录，但旧回填不得覆盖新版本、删除不得被复活。新建 JOIN/聚合在依赖历史尚不完整时，不能承诺其结果已等于完整 SQL：默认先构建隔离 generation，完成初始化、追赶和校验后发布；如提供进度预览，必须标为不完整。已有已就绪任务持续运行。5–10 秒目标用于基础镜像和已就绪任务；新增有状态任务另外报告 time-to-ready，不能借此推迟基础镜像的新数据可见。
- **一致性边界**：单个源事务在本地以完整提交为单位处理，巨型事务允许分段落盘但不能提前推进提交水位。本地源事务完整性、每键输出顺序和 StarRocks 跨表查询原子性是不同保证；默认不承诺多个下游表同时可见。先验证单 MySQL 实例的多表一致初始化，再开放跨表 JOIN；跨实例一致性留待后续。

### 目标架构与组件决策

目标数据流：一次源捕获 → 带事务边界的变化日志与共享源状态 → 有各自水位的 SQL 任务 → 持久化 outbox → StarRocks 可见性确认。源状态支持按键更新和批量扫描；CDC 不应为每个下游再次解析或捕获同一份源数据。Arrow 用于批量计算/交换，索引和稀疏更新可采用专用行布局，不要求所有状态都转成 Arrow。

先用当前 Python/C/DuckDB 基线贯通功能，定义稳定的事务批、状态、算子与输出接口；一次只替换一个被测量证明的瓶颈。原生运行时可以逐步接管数据路径，最终 Python/REPL/MCP 以控制为主。不会同时维护两个功能完整的生产运行时，也不为了消除所有 Python 调用而重复实现成熟协议。

| 组件/技术 | 当前决策 | 引入或替换条件 |
| --- | --- | --- |
| 现有 Python + C | 继续作为可运行基线；复用已验证的 decoder、分区和编码内核 | 保持函数式、下划线命名，Python 不用 typing/logging，日志用 print(..., flush=True)；语义稳定后逐段替换 |
| DuckDB + 当前 SQL 解析层 | 保留单源表达式、全量构建及参考结果能力 | 先固定方言和语义；参考 oracle 必须对齐类型/排序规则，不能只比较打印字符串 |
| Rust / C / C++ 运行时 | 候选，尚未锁定整套重写 | 用同一完整工作负载比较 CPU、内存、恢复和维护成本；选择一种主运行时，复用现有 C，避免多个语言各自建一套 scheduler |
| DataFusion / 自研 IVM / OpenIVM 路线 | 比较候选，而非全部引入 | 先做受限增量算子和全量 SQL oracle 对照；是否替换 planner 取决于语义兼容、增量计划和实测，不直接将 batch executor 当作 IVM |
| RocksDB 或其他持久化状态布局 | 共享源状态的候选，尚非默认 | 比较按键更新、50M 扫描、持久化水位、重启恢复和 compaction；先一个权威数据状态域，再决定是否需要列式基底+增量层 |
| SQLite | 保留当前运行状态，未来可承担控制目录 | 迁移前仍是现有数据状态权威；禁止先删现有 journal。跨 SQLite/catalog 与新数据引擎不能靠两个独立 commit 假装原子 |
| Arrow C Data/Stream / IPC | 同进程批接口或进程隔离接口 | 按实际部署选择；只有同进程兼容缓冲区可直接共享，IPC、重排和变长字段分配成本都计入 |
| PyO3 / 独立 daemon RPC | 二选一的管理边界候选 | 嵌入时才需要 PyO3；独立 daemon 用版本化控制协议。低频管理调用不作为优先优化点 |
| 成熟 MySQL 客户端、libcurl/HTTP 客户端 | 复用认证、TLS、网络协议 | 原生接入须保留 MySQL GTID/文件位置、校验和、超时和取消语义；不为“自研”手写 TLS |
| SIMD、压缩、PGO/LTO、NUMA、算子融合 | 按 profile 逐项评估 | 全路径收益、ISA fallback、内存和延迟一起验证；不强制每个负载使用同一批大小或编码 |

不立即自研通用数据库、SQL 优化器和存储引擎。共享状态、可恢复动态初始化、有撤回的增量算法、输出顺序与调度才是核心工程投资；前沿论文提供算法候选，不能替代本项目的语义证明和测量。

### 持久化、回填与恢复设计约束

1. **区分三个进度**：源已持久化水位、各任务已计算水位、各目标已可见水位。源事务赋予本地递增 commit sequence，保留源 UUID/epoch、GTID 或文件位置及 schema version；GTID 用于身份/恢复，不直接当全局可比较序号。源状态、事务 changelog 和源水位必须原子持久化；每个任务的算子状态、消费水位和 outbox 必须原子持久化。可采用同一引擎原子批，或显式可重放提交协议；目录的部署意图通过幂等协调与 runtime 对账，不引入未经设计的跨库双写。
2. **动态任务固定 W**：只从依赖源表已完成一致初始化的状态选择 W，登记任务 generation 和日志保留 pin，再从可恢复的 W 状态构建。持久化扫描范围、构建进度及重放位置；不能一边扫描不断变化的最新值，一边又从 W 全量重放导致双算。方案在持久 checkpoint、版本化状态或可重建物化基底中选择并实测。RocksDB 普通 snapshot 不跨重启持久化，checkpoint 可形成持久基底，但仍需应用层把它与 W/manifest 绑定，并计算存储保留成本。
3. **初次镜像与新增任务分开**：初次 50M 初始化先建立可恢复的分块 low/high 水位合并和 touched/tombstone 规则，持续接收 CDC。跨表一致状态必须有经过验证的共同完成边界，不能假设各表独立完成 snapshot 就是同一个时点。源状态尚不完整时，新有状态任务等待依赖就绪；基础复制仍可提供带初始化进度的新数据。已有镜像上的新增 SQL 优先复用本地状态，不为每个下游重扫 MySQL。
4. **追赶与发布**：初始化后按事务顺序消费 W 后变化，再以明确 cutover 水位发布 generation；旧 generation 的 writer 必须被 fencing，取消/重启不能使旧请求写入新目标；fencing 必须覆盖已发出但未确认的请求，不能只更换本地 generation 数字。独立 staging 表或版本化目标的切换机制需要在 4.1.1 实测，未验证前不承诺原子替表或跨表原子发布。主键改变视为旧键撤回、新键插入；JOIN 同一事务同时改两侧时须覆盖交叉增量项，过滤翻转及聚合撤回均纳入校验。
5. **输出协议**：Merge Commit async 和 transaction 保留为独立路径。已测 4.1.1 的组合 header 不能证明双机制叠加，不再将叠加作为交付目标。提交接受、事务完成、查询可见三者分别记录；未知事务先查可用历史证据，再进入有界对账/修复。没有证据时不得静默标成功，也不得无限重试阻塞全部任务；将受影响有序分区隔离，其他独立任务在保留预算内继续。每次请求还须核对接受/过滤/错误行，不能只用共享 TxnId 的完成状态掩盖该请求的数据错误。每目标键或其有序分区禁止旧请求晚于新值覆盖；不确定请求未收敛前不能仅靠主键 upsert 就宣称幂等。删除修复须有 tombstone 或可验证差集，不能只重发当前存在的行。
6. **资源和保留**：统一预算源状态、索引、changelog、任务构建基底、outbox、事务 spill、compaction 及临时文件。由最慢有效消费者/初始化 pin 决定回收边界；设定最大落后量、磁盘低水位、任务暂停/取消/重建政策，不能静默删仍需重放的数据。满盘时停止推进相应水位并诊断；源 binlog 保留不足时受控重建。调度保证 CDC 优先，同时给回填可测的非零剩余预算，避免永久饥饿；无剩余容量时报告容量不足而非承诺同时满足所有目标。

## 分阶段任务与验收标准

保留 P0–P13 编号供实施记录引用，编号不再代表严格串行顺序。下一条交付主线是 **P1/P2/P3 + P10 最小闭环 → P6 → P7 → P8**；P4/P5 只在测出瓶颈后插入，P9 在已有多任务数据后推进。P13 的控制协议尽早定义、MCP 在动态任务稳定后接入，P11 长跑在可用闭环上逐步扩容，P12 公平对标最后执行。每阶段都交付实现、测试、命令和公开合成结果，不用测试用例数量代替故障边界覆盖。

| 阶段 | 接下来做什么 | 完成必须达到的标准 | 当前状态 |
| --- | --- | --- | --- |
| P0 公开基线 | 保留独立可运行仓库及隐私边界 | 公共依赖、合成配置、源码/ABI 和离线回归通过；没有业务数据、配置和原有历史 | 已建立，见实施记录 |
| P1 正确性合同 | 固定 SQL/类型/键语义、事务边界、初始化完整度与 schema 行为 | 同字节 Python/C 差分；再以真实数据库语义和事务级最终状态独立校验；DDL、键改变、过滤翻转、删除重插及多表事务覆盖；不支持项部署前拒绝 | 部分完成：原生差分与真实 MySQL 通过，完整 SQL/初始化合同待实现 |
| P2 故障与输出协议 | 源/计算/输出各提交边界的恢复；进程 kill、断网、磁盘满、未知事务、日志过期、升级 | 至少 1,000 可重放故障案例，并逐个覆盖提交前后边界；GTID/文件模式恢复无漏无旧值覆盖；真实管线端到端校验；非法数据与暂时故障分流，不无限重启同一错误 | 部分完成：协议故障、capture 恢复和 SR 协议测试通过，完整故障矩阵未完成 |
| P3 测量平台 | 补实际 snapshot 和 MySQL COMMIT→目标查询四层测量 | 保留 decoder/local；增加生产 reader 对照，独立 Python oracle 不能代表生产 Python 性能；同时记录 source/sink、本地 CPU、RSS、spill、磁盘实际写入与空间、积压斜率及延迟 | 部分完成：decoder/local A/B、真实 COMMIT→查询短测已有；完整性能 profile 未完成 |
| P4 原生接入与批处理 | 按 profile 决定 socket、snapshot、组批是否下沉 | P1/P2 无回退；认证/TLS/GTID/取消/巨型事务支持明确；相同耐久性下 source-bound 与端到端验证收益，小事务不能等待不确定时长才发批；没有收益则不替换默认 | 部分完成：有界组批候选已实现；原生 socket 未实现，非 P6/P7 前置 |
| P5 布局和融合 | 去掉有证据的重复解码、复制、分区和编码 | 窄/宽行、稀疏/密集变化均测；公布复制分配、内存和总 CPU；单核收益不得掩盖全路径回退，按性能合同晋升默认 | 计划，按瓶颈插入 |
| P6 共享源状态 | 源捕获与任务解耦；全允许列镜像、changelog、schema、可扫描持久状态 | 源状态+源水位原子提交；任务状态+任务水位+outbox 原子提交；内存预算内恢复；源表增加/移除、DDL、日志保留和一致初始化可诊断；状态引擎经更新/扫描/恢复比较后选择 | 候选布局、原子恢复及固定 W 原型已有；尚未接入 daemon |
| P7 动态新增下游 | 先完成单源投影/过滤的 W→构建→追赶→发布→取消闭环 | 10 次交错部署/更新/删除/重启场景；旧任务继续运行；新目标最终逐键逐字段等于 oracle；构建进度可恢复、旧 generation 被隔离，最新行不被历史覆盖；登记 completeness 和 time-to-ready | 基线在线新增下游通过四组真实数据库测试；本地状态 W/generation 闭环仍依赖 P6 |
| P8 增量 SQL | 先 COUNT/SUM/AVG 及索引 INNER JOIN，再 LEFT JOIN、MIN/MAX、DISTINCT | 至少 10,000 组有重复/NULL/撤回/跨表同事务的随机用例；SQL 三值逻辑、空分组、匹配数归零及输出主键正确；状态/输出放大可界定；每类算子单独验收，窗口不隐含支持 | 计划，逐算子开放 |
| P9 共享执行与策略 | 多任务共享扫描/索引/子图，必要时增量与重算切换 | 1/10/100 任务真实成本对照；持久基底/索引可共享但不强求零构建成本；热点/fanout/策略维护计入；切换可恢复且结果等价 | 计划，等待多任务 profile |
| P10 输出与回填调度 | 前置最小可用输出：顺序、可见性、未知状态恢复、预算；后续再编码优化 | 默认 SR 服务参数下回填可限速也可恢复；同键旧请求不覆盖新数据；不丢 delete；积压有界且回填不永久饥饿；merge/2PC 分开验收，不承诺消除 compaction | 四组短时端到端正确性通过；未知请求/完整压力故障矩阵仍待验收 |
| P11 目标规模与长跑 | 从小规模持续测试扩到 50M + 50 行/s，并动态建任务 | 固定资源连续至少 72 小时；基础镜像及已就绪任务正常时段 P95 <= 5 秒、P99 <= 10 秒；记录最大延迟和违约率，故障期单报；排空后逐键字段正确；测 time-to-ready、回填总时长和容量余量；默认 SR 参数 | 计划；机器/行宽/任务数与资源预算须固定 |
| P12 硬件参考与对标 | 分层瓶颈上界与七系统相同语义比较 | 同资源/版本/耐久性/正确性、至少 5 次重复及区间；不支持项单列；复制、状态计算、目标导入分开比较，禁止用微基准宣称全面领先；低负载测延迟，高负载扫描饱和点及持续积压，50 行/s 本身不能证明吞吐极限 | 计划，不作为首个可用版本的阻塞条件 |
| P13 控制接口、MCP 与发布 | 先定版本化 deploy/explain/status/cancel，再自然语言入口和可回滚发布 | MCP 复用相同校验与部署事务，不绕过权限/成本检查；状态格式迁移、升级失败/回滚及备份恢复实测；72h、恢复和目标能力 gate 通过才标对应范围生产候选 | 计划；接口先行，MCP 不要求先完成七系统对标 |

### 下一轮具体交付顺序

1. 补真实端到端 harness：基础表 snapshot 与 CDC 同时运行，验证更新、删除、回填覆盖、输出未知事务和压力恢复；记录可见延迟及最终状态。用现有管线建立可用基准，先小规模后放大。
2. 写清并验证 P6 的事务批/水位/存储合同，做有限的状态布局比较，选定一个权威数据状态引擎；复用已有解码器，避免同时更换网络、存储和 SQL 三层。初次源镜像必须有完整度和一致性标记。
3. 实现 P7 的单源动态任务闭环及 generation 恢复；随后逐类开放 P8 的聚合和 JOIN。控制 API 与部署状态机同时形成，MCP 复用它。
4. 在上述真实负载 profile 指向瓶颈时推进 P4/P5/P9；固定生产候选后完成目标规模长跑、升级回滚和公平对标。保留基线到状态迁移验收通过，不能把旧状态目录直接交给不兼容的新运行时。

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

性能候选晋升默认的 gate：正确性零差异；预先固定负载、资源、稳定测量区间和重复次数，报告中位数、区间及原始样本。吞吐或 P99 恶化超过 5% 触发调查，只有排除噪声后的变化才作结论；不能用一个普遍的“热点必须降低 20%”门槛替代全路径收益。新产品能力与等功能性能优化分开验收，不拿新增持久状态的成本与无此能力的解码器直接排名。存储 fsync、检查点频率、编译 ISA、压缩、源目标资源都必须列出，不允许关闭恢复能力换成绩。

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
- [RocksDB Overview](https://github.com/facebook/rocksdb/wiki/RocksDB-Overview)、[Snapshot](https://github.com/facebook/rocksdb/wiki/Snapshot) 与 [Checkpoints](https://github.com/facebook/rocksdb/wiki/Checkpoints)：原子批、WAL、快照及 compaction 成本；普通 snapshot 不跨重启，持久 checkpoint 与应用水位的绑定仍需设计。
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

A/B 每个样本独立 Python 进程，输出 Python/C/总 CPU、wall、RSS、rows/s、IPC 字节、SQLite 文件总字节（空间占用，不等于实际写入字节或写放大）、事务批 P50/P95/P99。fixtures 构造、初始化不计 wall；C CPU 计入子进程完整生命周期。此测试不含 MySQL socket/StarRocks，不把批延迟解释为端到端延迟；RSS 无读取权限时为 unknown。local 模式保持 SQLite FULL/WAL 与相同源事务边界。

`tools/starrocks_contract.py` 与 Actions 的隔离 StarRocks 4.1.1 probe 验证：2PC 在 commit 前不可见、merge async 实际共享 TxnId 并最终 VISIBLE；组合 header 需同时通过功能与无效参数负对照，不能把 HTTP success 当作双机制叠加成功。只上传合成计数和结论，不连接生产服务。

### 已验证结果（2026-09-30）

提交 `e7d69685a3acc76e357c414d32a06c014d8302b0` 的 [集成与 sanitizer CI](https://github.com/justgo4/m2s/actions/runs/36738492453) 和 [Python 3.12/3.14 基线 CI](https://github.com/justgo4/m2s/actions/runs/36738492665) 全部通过。真实 MySQL 测试覆盖 1,000 个事务、3,834 个行镜像，逐事件及组批解码均与同一原始事件的 Python oracle 比较，最终状态也与 MySQL SELECT 比较。每个基线版本的 1,000 次 capture 恢复测试均没有重复提交。

隔离 StarRocks `4.1.1-14b7e3f` 使用镜像默认服务配置：8 个 async Merge Commit 请求实际共享 1 个事务并全部可见；2PC 只有显式 commit 后可见。事务接口接受无效 Merge Commit 参数，而普通 Stream Load 拒绝相同无效参数，说明事务接口忽略这些参数。结论是本次测试没有证明双机制叠加，运行时继续保留两条独立协议路径。

新增 `Native decoder and durable pipeline A/B` workflow：decoder/local 两层 × tiny/dense/wide 三种合成负载，每种方法独立进程、随机执行顺序、5 次重复，先运行差分和故障 gate，再发布 JSON 样本。GitHub runner 性能波动较大，只用于发现回退，不作为固定机器性能验收。当前 local 包含 DuckDB 路由和 SQLite FULL/WAL，但尚未包含未来共享状态引擎。

本地 tiny 解码器样本中组批减少 IPC 输出约 88%；本地 4,096 行、8 个 durable 源事务样本中，逐事件/组批 wall 中位数为 0.264/0.151 秒，总 CPU 为 0.324/0.162 秒（各 5 次）。这些仅是小规模合成测量，不能推导真实 MySQL→StarRocks 加速比，也不能证明超过其他系统。

仍未通过的验收：C 直接管理 MySQL socket 的 P4 路径、P6 共享源状态、P7 动态部署水位恢复、P8 JOIN/聚合撤回、完整 snapshot/end-to-end A/B、默认参数下 50M+50 rows/s 的 72 小时长跑，以及七个系统的同机对标。当前版本不能据此称为“物理极限”或生产候选；阶段状态保留为部分完成或计划。

宽行反例：同机 2,048 行、每行 4 KiB BLOB、4 个 SQLite FULL/WAL 源事务（各 5 次）中，逐事件/组批 wall 中位数 0.191/0.190 秒，总 CPU 0.224/0.211 秒，区间重叠且 Python 参考路径 wall 0.177 秒。此样本没有显著组批 wall 收益，也不能证明 native 总是更快。默认保持逐事件模式；优化选择需要负载和固定机器实测。

本地完整样本：[tiny decoder](reports/local-decoder-tiny.json)、[narrow durable local](reports/local-durable-narrow.json)、[wide durable local](reports/local-durable-wide.json)。六个 CI A/B job 已全部通过：[run 36743206446](https://github.com/justgo4/m2s/actions/runs/36743206446)，Actions artifact 提供 runner 样本；每个 workload 只在自身同一 runner 内比较方法，不跨 runner 排名。


### 2026-10-01：真实 daemon 与在线新增下游

[四组端到端 CI 36787425832](https://github.com/justgo4/m2s/actions/runs/36787425832) 全部通过：MySQL 8.4.6 → 实际 `j4.py` → StarRocks 4.1.1；两种协议各测 GTID ON/OFF。合成初始 2,048 行、80 个源事务，覆盖过滤翻转、NULL、DECIMAL、主键修改、删除重插；实际断开复制连接一次；运行中用 `j4.py sql` 新建第二个不同投影/过滤的下游，两任务均持续处理增量；排空后强制退出，再产生数据并重启，两个目标的最终逐键逐字段结果均等于独立 MySQL SELECT。

| 协议 | GTID | 初始阶段 P95 / P99 / 最大观察延迟（秒） | 新增任务 P99（秒） |
|---|---|---|---|
| transaction | ON | 1.948 / 2.197 / 2.319 | 1.657 |
| transaction | OFF | 1.999 / 2.249 / 2.385 | 1.677 |
| merge_async | ON | 3.614 / 3.781 / 3.865 | 3.478 |
| merge_async | OFF | 3.473 / 3.729 / 3.816 | 3.892 |

这是从客户端发起 COMMIT 到第一次成功目标查询的观察上界，包含提交往返和轮询等待；不是服务器精确提交时刻。仅为共享 runner 上的小规模功能短测，不是 50M、50 行/s 或 72 小时 SLO 认证。聚合记录见 [e2e aggregate](reports/e2e-20261001.json)，每条 marker 的完整样本见该 workflow artifacts。StarRocks 使用镜像默认服务参数；隔离测试表副本数为 1。固定测试程序逻辑内存预算 2,048 MB、每个 DuckDB 引擎 64 MB，为第二任务留出预算，不调整 StarRocks 配置。

修复与诊断：虚拟地址空间限制为线程栈/Arrow 映射留出空间，RSS 仍由原有资源预算监测，不能把 RLIMIT_AS 当作即时物理内存硬限。暂时源连接故障超过重试窗口后进入可取消的低频等待，仍从 durable cursor 重建；decoder 故障计数独立，持续同一非法输入或权限/日志缺失仍会明确停止。SQL 文件部署若已经提交 catalog、但安装要求重启，CLI 返回非零并显示原因；**这不是 catalog 回滚，不能把失败码理解为未保存任务**。

新增任务回填中强制退出及续建的更强测试已加入 `tools/e2e_contract.py`，与排空后的重启分开记录；其验收结果以对应 commit 的 CI 为准。即使这两种退出测试通过，也不能声称精确覆盖 HTTP 提交前后所有边界。当前基线新增下游仍会重新扫描 MySQL，不等于已完成共享本地源状态的 P6/P7。

```bash
# 仅对可删除 m2s_e2e_contract 的隔离服务执行：
python tools/e2e_contract.py --isolated --load-mode merge_async --group-size 128
python tools/e2e_contract.py --isolated --load-mode transaction --group-size 128
```

### P6 状态布局与固定水位原型

`tools/state_layout_benchmark.py` 比较 SQLite FULL/WAL 的不可变 Arrow 批次+键引用，与 DuckDB 默认事务 WAL 的类型化最新状态。每种布局将状态、changelog 和源水位一起提交；检查同键多个镜像（包含打乱物理行顺序）、删除、NULL、Decimal、精确重放、冲突身份和非法源输入拒绝。两个布局均测试 commit 前/后 `os._exit`，恢复后没有重复提交且完整结果一致。固定 W 原型将 W 保存在持久数据库自身，从 W 副本重建并重放后缀，源继续推进也不会改变原 W；强制退出后同样验证。

这仍是独立候选评测，未接入 daemon，不能标 P6 完成。固定 W 当前采用暂停写入的完整副本，不是在线 MVCC；没有实现日志 pin/GC、有限保留、多表一致初始化、任务 outbox、50M 有界扫描或字符串主键排序规则。候选 schema 为整数复合键的合成数据。禁止把这个原型当作生产状态迁移工具。

[高熵宽行原始结果](reports/state-layout-20261001.json)：10,000 初始行、100 个事务、每事务 50 个变化及一个重复键最终镜像、2 KiB 合成高熵字符串、各 3 个独立进程样本。报告包含更新/扫描 CPU 与 wall、提交延迟、固定 W 副本耗时/空间、完整进程 RSS 和耐久文件空间；文件空间不等于实际 I/O 写放大，RSS 包含 fixtures 和 oracle。更新快与扫描快的布局不同，目前不据此选定生产引擎。需要补真实 churn/多任务扫描、保留和 compaction 成本后决策。

```bash
python tools/state_layout_benchmark.py --rows 1000 --transactions 10 --changes 20 --repeats 2 --faults
python tools/state_layout_benchmark.py --rows 10000 --transactions 100 --changes 50 --width 2048 --entropy high --repeats 3 --faults
```


补充 P3 实际源读取测量：`tools/snapshot_benchmark.py` 在隔离 MySQL 的现有 21 列合成 fixture 上，分别运行生产 `fetch_snapshot` 的 Python tuple→Arrow 与 native packet→Arrow 路径，各 5 个新进程样本、随机先后顺序；每个样本检查 Arrow 类型/NULL/全部值和分块 op/order。记录 Python/C CPU、scan wall、读取行数、RSS 与 Python 进程 I/O 计数。source/server CPU、网络及 IPC 字节尚未记录，报告明确列为未测；不把 warm-cache 小 fixture 结果外推到 50M 或下游吞吐。该项已接入隔离 MySQL CI，验收以最新 workflow 结果为准。

```bash
# 先准备同一个隔离合成 fixture，再执行只读 benchmark：
python tools/binlog_parity.py --live --cases 1000
python tools/snapshot_benchmark.py --isolated --repeats 5 --batch-rows 512
```
