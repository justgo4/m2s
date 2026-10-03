# m2s 受限 v1 运行与恢复

适用范围是 README 所列 SQL 子集和单 MySQL→StarRocks 拓扑。进度与真实
验收见 [PROGRESS.md](PROGRESS.md)。本手册给出可操作的边界；正式 50M/72h、
跨版本迁移/回滚、破坏性 DDL 和自动远端解隔离尚未全部验收。

## 配置与持久运行

- 固定 Git SHA、依赖和 native ABI；保留该版本的代码/构建产物。
- catalog、state、WAL、临时文件、日志、指标与备份均放持久私有磁盘；所有
  数据库账号、地址和运行证据保存在主机上，不提交到公开仓库。
- 启动前用 `python j4.py check` 检查服务/schema，记录可用 binlog/GTID
  保留窗口、源主键/列、目标代际及资源指纹。先在隔离服务运行该 SHA 的合同。
- 配置整机预算时分别计算 m2s、MySQL、StarRocks、SQLite/TEMP 与备份。
  m2s 的 `CDC_RESOURCE_MEMORY_MB` 不是整机限制。保留 `CDC_MIN_FREE_BYTES`
  和日志/指标轮转；不要通过清空 WAL、删 pin 或提高 GC 水位释放空间。
- 用持久主机的进程监督运行 `python j4.py run`；用户退出聊天不影响已启动
  的进程。SIGINT/SIGTERM 请求正常停止、等待线程、保留 journal；停止不保证
  所有 source 数据已变成目标 VISIBLE。操作系统强退后使用原状态恢复。

`tools/runner_inventory.py` 从配置好的 `GITHUB_TOKEN` 读取注册计数，只输出
汇总。403/网络失败表示未知。当前真实查询返回 403；没有可用的云身份或持久
主机入口，尚未确认/创建 self-hosted runner。公开 hosted Actions 的免费分钟
不能提供连续 72h 主机。已有分级 workflow 可执行短测；不要把其测试数据库
初始化命令指向生产服务或旧 run 目录。

## 检查和告警

`j4.py status` 读 durable 状态，`j4.py explain` 读执行计划/共享关系；前者
不证明 daemon 存活。daemon 心跳摘要为 `<state>.summary.json`，指标为
`<state>.metrics.jsonl`。终止摘要的 `event=run_summary` 即使很新也表示已停止。

下面示例要求这些路径属于同一个实例。用私有目录和原子替换保存 status，
避免监控读取半个 JSON。时间/阈值须匹配实际部署，尤其 idle 心跳间隔：

```bash
umask 077
CDC_STATE_FILE=/data/m2s/state.sqlite3 python j4.py status > /data/m2s/status.json.tmp
mv /data/m2s/status.json.tmp /data/m2s/status.json
python tools/operational_check.py \
  --summary /data/m2s/state.sqlite3.summary.json \
  --status /data/m2s/status.json --state-directory /data/m2s \
  --max-age-seconds 660 --min-free-bytes 2147483648 \
  --max-pending-bytes 268435456 --max-queue-seconds 120
```

退出码：0 表示本次新鲜证据未触发所配阈值，1 表示告警，2 表示证据缺失/
陈旧/不可读，不能当健康。脚本不调用模型、不发送外部消息、不自动改预算或
解除隔离。定时器/监控系统应处理 1 和 2；所列数值是示例，不是正式 SLO。
服务远端资源、真实源→目标延迟、全行正确性与进程监督仍须分别检查。

| 告警/观察 | 处理 |
|---|---|
| `unknown_output_isolated` | 保留 `merge_uncertain`、label、payload/generation 与目标信息；隔离目标继续隔离。不得手动删除记录或直接重发含 delete 的未知请求。独立目标可继续。当前无自动对账/解隔离；确认远端结果及旧请求 fence 后，按经验证的人工流程或隔离新目标重建 |
| `low_disk` | 停止新增任务/重建，检查 state/WAL/TEMP/目标/日志容量；扩容或限流。删除独立过期日志/可验证的旧备份前确认保留策略；严禁删活跃 SQLite/WAL、consumer/pin。正常停止保留证据，恢复足够余量后原状态重启 |
| `backlog_bytes` / `backlog_age` | 检查 source capture→base apply、各 sink VISIBLE、锁等待/GC、CPU/RSS、StarRocks rowset/version debt；保留消费水位。按瓶颈调整输入/预算，先在隔离负载实测，不跳过积压 |
| `daemon_stopped` / 退出/陈旧心跳 | 检查最终 reason/worker errors、空间与服务连接；监督系统用同一配置/代码/状态重启。若发生未知远端结果，保持隔离而不是循环重发 |
| building/rebuild/draining/admission waiting 长时间不动 | 检查固定 W/source pin、consumer frontier、容量准入与远端 fence；不手改 lifecycle/GC。SQL 未支持或源 scope/DDL 变化按拒绝/受控重建处理 |

## 一致备份及验证

直接复制活跃 `.sqlite3` 而漏 WAL 不是一致备份。`tools/state_backup.py` 使用
SQLite online backup：源连接只读，完成后转为 DELETE journal 的单文件快照，
通过 quick_check/SHA-256/长度检查，fsync 后发布 manifest/status。超时/中断
保留 incomplete 证据；已有备份目录拒绝覆盖；不含 restore 命令。

```bash
python tools/state_backup.py create --state /data/m2s/state.sqlite3 \
  --backup-directory /data/m2s-backups/run-001-state --timeout-seconds 60
python tools/state_backup.py verify --backup-directory /data/m2s-backups/run-001-state
```

这是**单 SQLite 文件**的本地一致性快照。catalog、其他状态库、代码、native
ABI、私有配置与远端 StarRocks/source 身份不在该快照里。升级检查点应暂停
catalog 改动，正常停止 daemon，分别备份 catalog 和 state，再保留版本/配置/
远端代际及 binlog 保留证据；全部验证成功前不能称为一套可恢复检查点。
空间不足应先扩容，备份也需要额外磁盘；持续写入下备份可能超时。

## 重启、升级与回滚边界

1. **同版本重启**：保留 state/catalog 和配置，确认源历史仍覆盖保存的
   durable cursor，原状态重启。既有真实 E2E 验证过同版本强退/replay、
   fixed-W 与未知输出隔离；监控 backfill/drain/frontier 后做对应全行校验。
2. **升级前**：先在隔离环境针对待升级 SHA 验证 source/state format、
   fingerprint、task generation、native ABI 和远端 load 协议。检查升级
   支持的旧格式及实际迁移代码，不能只比较 `STATE_FORMAT` 数字。
   停止所有旧进程/旧请求，取得完整私有检查点，再用保留的原状态启动。
3. **升级后**：检查 active plan、所有 generation/rebuild/retirement、
   `merge_uncertain`、源 cursor 与目标 VISIBLE；比较全行结果，记录新 SHA。
   迁移后即使升级程序失败，也可能已经改变 durable schema，不盲目启动旧版。
4. **回滚**：只有经该版本对验证的本地格式/配置兼容且远端请求/代际安全时，
   才能在当前完整状态上回到旧代码。**不能将旧本地备份覆盖已经继续写入的
   目标对应状态**：远端 frontier 和在途请求可能领先，会造成重放/覆盖。
   需要隔离并 fence 旧写入者、核对 source history、在新物理目标代际做受控
   重建/对账；旧状态和目标证据保留。当前没有通用一键 rollback/restore。

跨版本、长事务低空间、并发 replacement/cancel 组合与真实升级/回滚演练
仍在 PROGRESS.md 的验收队列。这里的步骤不把未测过的迁移宣称成已通过。

## 已测试的工具边界

`tools/state_backup_test.py` 使用真实 SQLite：备份分批期间另一连接提交 WAL
事务，快照一致；已有目录/缺失源不重置；锁定源有期限且保留 incomplete；
中断/损坏源不发布 ready；篡改字节/遗留 WAL/记录不一致拒绝验证。
`tools/operational_check_test.py` 验证阈值、durable 未知输出不会被健康 runtime
掩盖、终止摘要/陈旧/非法值和 CLI 的 0/1/2 边界。这些不是生产故障演练或
正式规模证明，工具结果需按自己的代码 SHA 与 CI 记录评估。


### Optional SQLite writer timing

Set `CDC_SQLITE_WRITE_TIMING=1` before launching the daemon to add
`sqlite_write_timing` to periodic metrics and the final run summary. Default
is disabled and uses the original SQLite connection. Development staged Actions
enable this diagnostic; formal P11 thresholds and profile remain unchanged.

`acquire` measures explicit BEGIN IMMEDIATE/EXCLUSIVE call time, including
waiting, scheduling and SQLite overhead. `hold` measures from successful BEGIN
return through COMMIT/rollback return, including durable fsync and scheduling.
Each operation records count, total and maximum; these are process-lifetime
cumulative counters, not interval values or percentiles. Operation labels contain
module/function names only. SQL, parameters, row values and database paths are
not retained. At most128 metric buckets and128 open-transaction descriptions
are retained; overflow totals and active_unlisted report truncated detail.

Coverage is limited to `Connection.execute` writer BEGIN boundaries. Deferred
transactions, implicit writes, cursor-based controls and scripts are excluded.
A script crossing an observed transaction invalidates its hold sample and
increments excluded_holds. Active entries report unfinished observations; they
are not proof of a currently held OS lock if excluded APIs are used. Instrumented
calls add overhead, so compare performance with that setting recorded. Do not
interpret one operation's waiting time as identifying another process's lock
owner. Preserve per-process summaries across daemon restarts when comparing
phases, because counters reset with each process.
