# 斷點

更新：2026-11-12。新會話先讀本檔，再讀 [ROADMAP.md](ROADMAP.md)。不要重做已合併的里程碑，也不要重跑已通過的 TPC-H 差分，除非編譯器又改了。

## 當前里程碑

第二輪第 9 項：TPC-H Q1–Q22 對 DuckDB 的增量差分。
分支 `cursor/tpch-diff-23fa`，基線 `aaac2b681d738380ee0ca6b4c633dfed36f72f87`。
不修改 workflow。門檻不改。查詢文字由 DuckDB `tpch_queries()` 在執行時取得，不進 Git。
22 條都是增量計畫（沒有 `recompute` 或 `theta_join`），scale 0.01 與獨立 DuckDB 逐列一致。
實測程式 SHA：`7ba9cf9398aefa1e7287feb6606ee91858dc8218`。

## 倉庫位置

- 分支：`cursor/stream-sql-m2-23fa`
- 基線：`adba11f117915818ad5d299651a00783062d43e3`（開始時 `origin/main` 同一 SHA）
- 方案提交：`5f3cabef404d8b536dfa112aefc68224fffcaf13`
- 實作提交：`bef1ed2c397144821a9bb2b29b587d93f6d05989`
- 合併到 `main`：`7c6662709e9a5cf2243ca22dad6052b8e62c3f66`（PR #65，已合併）
- 遠端：`justgo4/m2s`
- 開放中的舊 PR 不要合併。

## 已完成

- Z-set（`stream_zset.py`）：整數權重、零權重刪除、主鍵鏡像。delete 是 `-1`，upsert 既有鍵先撤回舊列。錯誤的 before image、缺鍵刪除、非整數權重都 fail-closed。
- DAG（`stream_ir.py`）：`input`／`filter`／`project`／`aggregate`／`equi_join`。`lower_stateless`、`lower_aggregate`、`lower_join`。既有 IR 的 canonical bytes 沒改。
- 參考引擎（`stream_eval.py`）：filter／project 只吃 delta；COUNT／SUM／AVG 用群組累加器發 `+1`／`-1`；INNER equi-join 權重相乘，NULL 鍵不匹配。同一批先套用全部左側再套用右側。失敗的一批會回到該批之前的狀態。
- 接線：`j4.runtime_plan_entry` 寫入 `_stream_plan_id`，熱更新不一致就拒絕。`stateful_task_plan.compile_ir`／`compile_task` 帶 stream plan id，不放進既有 descriptor hash。
- 熱路徑仍是原來的 DuckDB SQL 與 SQLite 聚合／JOIN 引擎。參考引擎沒有放上每筆 CDC。

測試在建立 `bef1ed2` 之前、於同一棵原始碼上跑過。Python 3.12.3，依賴為 `requirements.txt`（DuckDB 1.5.5、PyArrow 25.0.1、sqlglot 30.18.0）。

| 命令 | 結果 |
|---|---|
| `python3 tools/stream_zset_test.py` | `stream_zset_test ok` |
| `python3 tools/incremental_ir_test.py` | ok |
| `python3 tools/relational_ir_test.py` | ok |
| `python3 tools/aggregate_ir_test.py` | ok |
| `python3 tools/join_ir_test.py` | ok |
| `python3 tools/incremental_ir_state_oracle.py` | ok，seed=20261001，operations=5000，final_rows=35 |
| `python3 tools/aggregate_state_oracle.py` | ok，seed=20261001，transactions=1500，rows=104 |
| `python3 tools/join_state_oracle.py` | ok，seed=20261001，transactions=2000，left=78，right=34，pairs=17 |
| `python3 tools/aggregate_state_test.py` | ok |
| `python3 tools/join_state_test.py` | ok |
| `python3 tools/join_incremental_delta_test.py` | ok |
| `python3 tools/source_log_consumer_test.py` | ok |
| `python3 tools/stateful_task_plan_test.py` | ok |
| `python3 tools/incremental_contract_test.py` | ok |
| `python3 tools/p11_profile_test.py` | ok，`p11-50m-50rps-72h-v4` 身份未改 |
| `python3 tools/privacy_check.py` | `PRIVACY PASS files=236 findings=0` |
| `python3 j4.py selftest` | `SELFTEST OK: 37 offline regression groups`。線上檢查跳過：沒有資料庫設定 |

`stream_zset_test.py` 的差分是：無狀態 projection／filter 對 DuckDB；COUNT／SUM／AVG 對 DuckDB 與 `aggregate_state`；受限 INNER JOIN 對 DuckDB 與 `join_state`；另有 filter→project→aggregate 與 join→aggregate 兩條組合 DAG。隨機種子 `20261111`。

## 正在做

TPC-H Q1–Q22 已在本機差分通過。下一步是開 PR，然後做 Nexmark。
離線包仍只有 1126 位元組的目錄打包，不能算完成；要在 Nexmark 之後做成含全部 wheels 的安裝包，並在斷網環境安裝、跑 `j4.py selftest`。

## 里程碑 3 程式

- `m2s_tumble`／`m2s_hop`／`m2s_session`／`m2s_cumulate` 視窗聚合。
- `m2s_watermark(time, lateness, policy)`：`drop` 丟掉遲到列。`side` 加 `_late`。`recompute` 仍收進狀態。
- `OVER` 只重算受影響分區。`ORDER BY ... LIMIT n` 是 Top-N。
- 非遞迴 CTE，以及 `WITH RECURSIVE` 的半樸素固定點，上限 32 次，超過就失敗。
- 區間包含（`contains_point`）按 equi-key 重算。
- `random`／`now`／`uuid` 預設拒絕。`nondeterministic="timed_recompute"` 才把計畫標成 `timed_recompute`。
- v1 stateless plan id 仍是 `1283fb55eb3b4b4408aa8095e8e9c66255383bb60fa630d38d6f401b109fc8ef`。

`python3 tools/stream_time_test.py`、`python3 tools/stream_sql_test.py`、`python3 tools/privacy_check.py`（files=243）已通過。

## 里程碑 2 已完成的程式

分支 `cursor/stream-sql-m2-23fa`，從 `main` `44cd2b8ea7d20ba826933eb1ede5e1f5df75ed72` 切開。
起點提交 `46db647229eba47d8597bcfdca8f26c1c7cdb9ba`。

- `stream_sql.compile_sql(sql, sources, read="duckdb")`。預設 duckdb，與現有 mapping SQL 相同。`read="mysql"` 才接受反引號與 `GROUP_CONCAT`。`j4` 熱路徑解析沒有改。
- 參考引擎新增：MIN／MAX、COUNT DISTINCT、有 `ORDER BY` 的 STRING_AGG、DISTINCT、UNION／UNION ALL／INTERSECT／EXCEPT（含 ALL）、INNER 以外的 equi JOIN（LEFT／RIGHT／FULL／SEMI／ANTI）、多表 JOIN。
- 非等值 JOIN 與去不掉的 `NOT IN` 標成 `full_input_recompute`，用 DuckDB 對該運算子的輸入重算後做權重差。無 `ORDER BY` 的 STRING_AGG 與 LIMIT／OFFSET 直接拒絕。
- GROUP BY ROLLUP／CUBE／GROUPING SETS 展開成多個聚合再 `union_all`。
- `stream_mv.MaterializedView` 把上游 delta 送進下游。watermark 必須每次加一，下游不能超前或倒退。
- 既有三個 v1 plan semantic id 仍是  
  stateless `1283fb55eb3b4b4408aa8095e8e9c66255383bb60fa630d38d6f401b109fc8ef`、  
  aggregate `8ff37078393ac336c586fa6ef3138bcedfb99c1add01c914c298b9f93fd2b9ef`、  
  join `573cac45023a5f0717b085c4b5f0027196774508357723da29b7e103d0ea4990`。
- `aggregate_state` 仍只接受 COUNT／SUM／AVG。

測試（Python 3.12.3，DuckDB 1.5.5）：

| 命令 | 結果 |
|---|---|
| `python3 tools/stream_sql_test.py` | `stream_sql_test ok` |
| `python3 tools/stream_zset_test.py` | `stream_zset_test ok` |
| `python3 tools/privacy_check.py` | `PRIVACY PASS files=240 findings=0` |

`stream_sql_test.py` 含腳本化與種子 `20261111` 的隨機增刪改，每批結束與 DuckDB 全量重算逐列一致。也含方言、MV watermark 與 v1 semantic id。

## TPC-H 差分（scale 0.01）

命令：`python3 tools/tpch_diff_test.py`。Python 3.12.3，DuckDB 1.5.5。
`python3 tools/stream_sql_test.py` 為 `stream_sql_test ok`。
`python3 tools/stream_zset_test.py` 為 `stream_zset_test ok`。
`python3 tools/privacy_check.py` 為 `PRIVACY PASS files=282 findings=0`。
每一題都印 `incremental`。列數是輸出權重和：

| 題 | 模式 | 列數 |
|---|---|---|
| Q1 | incremental | 4 |
| Q2 | incremental | 4 |
| Q3 | incremental | 10 |
| Q4 | incremental | 5 |
| Q5 | incremental | 5 |
| Q6 | incremental | 1 |
| Q7 | incremental | 4 |
| Q8 | incremental | 2 |
| Q9 | incremental | 173 |
| Q10 | incremental | 20 |
| Q11 | incremental | 359 |
| Q12 | incremental | 2 |
| Q13 | incremental | 32 |
| Q14 | incremental | 1 |
| Q15 | incremental | 1 |
| Q16 | incremental | 296 |
| Q17 | incremental | 1 |
| Q18 | incremental | 2 |
| Q19 | incremental | 1 |
| Q20 | incremental | 1 |
| Q21 | incremental | 1 |
| Q22 | incremental | 7 |

最後一行：`tpch_diff_test ok queries=22 scale=0.01`。沒有用 `force_recompute`。

## 下一步

開 TPC-H 的 PR 並合併後做 Nexmark，再做真正的離線包，最後在當時的 `main` 重跑 strict-small。
舊 strict-small SHA `96501dcd0d7bf3c7b84b7d2ac17aaed7d759a9d0` 不能算到新 SHA。
正式 72h 仍無數據，阻塞原因是沒有可連續 72 小時的隔離主機。

## 未解決

- strict-small 在對帳之前的 SHA `490c74dae268e3d9f5f076ad5d03851266507c88` 沒有通過：`agg_000` 被隔離，pending 停在 887。對帳之後的 SHA `96501dcd0d7bf3c7b84b7d2ac17aaed7d759a9d0` 通過。目錄 `/tmp/m2s-validation-small-m5`。健康樣本 n=293，P95/P99=2.028/2.038s，超過 5s 與 10s 的都是 0。恢復樣本 n=7，P95/P99/max=1.620/1.620/2.264s。事件、三個聚合、三個 JOIN 的全列摘要一致。pending=0。門檻仍是 5s／10s。
- E2E 通過。同一 SHA，MySQL 8.0.46、StarRocks `4.1.1-14b7e3f`，`--rows 1024 --transactions 40`。`exact_final_result=true`。延遲 P50/P95/P99/max=1.068/1.692/1.798/1.807s。這是短功能探針，不是 5s/10s 認證。
- 正式 `p11-50m-50rps-72h-v4`：無數據。沒有持久隔離主機。
- 參考引擎是語意對照，不是熱路徑。里程碑 4 之前不要用它替換 SQLite／原生熱點。
- GitHub Actions 不跑。提交訊息帶 `[skip ci]`。

## 本地環境如何重建

工作目錄 `/workspace`。系統 Python 3.12.3。沒有虛擬環境。

```bash
git fetch origin main
git checkout main
python3 -m pip install -r requirements.txt

python3 tools/stream_sql_test.py
python3 tools/stream_zset_test.py
python3 tools/incremental_ir_test.py
python3 tools/aggregate_ir_test.py
python3 tools/join_ir_test.py
python3 tools/aggregate_state_oracle.py
python3 tools/join_state_oracle.py
python3 tools/incremental_ir_state_oracle.py
python3 tools/stateful_task_plan_test.py
python3 tools/privacy_check.py
python3 j4.py selftest
```

本機沒有 Docker。MySQL 與 StarRocks 是直接裝的，帳號留在機器上，不要寫進 Git。

- MySQL：`sudo DEBIAN_FRONTEND=noninteractive apt-get install -y mysql-server` 得到 8.0.46。`policy-rc.d` 會拒絕 service start，所以用 `sudo mysqld --user=mysql --daemonize`。`/etc/mysql/mysql.conf.d/m2s-local.cnf` 打開 GTID、`server-id=1`、binlog。實測 `binlog_format=ROW`、`binlog_row_image=FULL`。root 只能走 socket；TCP 要用另外的帳號。
- StarRocks：`https://releases.starrocks.io/starrocks/StarRocks-4.1.1-ubuntu-amd64.tar.gz` 解到 `/tmp/sr`。FE `JAVA_OPTS` 從 `-Xmx8192m` 改成 `-Xmx1536m`，BE `mem_limit=2147483648`，`priority_networks=127.0.0.1/32`。`sudo sysctl -w vm.max_map_count=2000000` 成功。`bin/start_fe.sh --daemon` 與 `bin/start_be.sh --daemon` 之後，`ALTER SYSTEM ADD BACKEND "127.0.0.1:9050"`。`current_version()` 是 `4.1.1-14b7e3f`。預設 `replication_num=3`，單節點要設成 1。煙霧查詢 `INSERT` 1、2 後 `SELECT` 回了這兩列。
- `python3 tools/e2e_contract.py --isolated --load-mode merge_async --rows 1024 --transactions 40` 跑了約 246 秒後失敗：reader 已連上，但 `snapshot_pause=1`、`memory_pressure=1`（當時 `memory_available_mb=3628`，FE+BE 已佔住記憶體），`source_base` 停在 0。這不是新 SQL 編譯器的路徑。strict-small 因此沒有跑。無數據。

```bash
sudo DEBIAN_FRONTEND=noninteractive apt-get install -y mysql-server
sudo sysctl -w vm.max_map_count=2000000
# 下載並啟動 StarRocks 4.1.1 ubuntu amd64 官方包，FE heap 與 BE mem_limit 要按本機可用記憶體下調。
```

`CDC_EVENT_TRACE` 保持關閉。不要把 workload 初始化到舊 run 目錄或生產庫。

## 提交

- `5f3cabef404d8b536dfa112aefc68224fffcaf13` 方案與第一個斷點
- `bef1ed2c397144821a9bb2b29b587d93f6d05989` 里程碑 1 實作，測試見上表
- `7c6662709e9a5cf2243ca22dad6052b8e62c3f66` 合併 PR #65 到 `main`
- `82a931f560880228c216c79ef4a3b205a87e2996` 里程碑 2 實作
- `7805cbcaa0dbad9965dbe05468764aaee73028b7` 合併 PR #66 到 `main`
