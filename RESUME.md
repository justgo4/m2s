# 斷點

更新：2026-11-11。新會話先讀本檔，再讀 [ROADMAP.md](ROADMAP.md)。不要重做里程碑 1 的降下與差分測試。

## 當前里程碑

里程碑 1 已合併。PR [#65](https://github.com/justgo4/m2s/pull/65)，合併提交
`7c6662709e9a5cf2243ca22dad6052b8e62c3f66`。下一步是里程碑 2，不要重做 Z-set DAG。

## 倉庫位置

- 分支：`cursor/stream-zset-ir-23fa`
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

沒有進行中的編輯。里程碑 1 已在 `main`。

## 下一步

從 `main` 的 `7c6662709e9a5cf2243ca22dad6052b8e62c3f66` 開里程碑 2 分支。先做這幾項，每項都要能對 DuckDB 全量重算，並且既有 stream plan semantic id 保持不變：

1. 新的 SQL 入口使用 sqlglot，`read` 可設定，預設 `duckdb`，與現有 `j4` 解析相容。`read=mysql` 只在明確設定時啟用。
2. 在參考引擎加上 MIN／MAX、COUNT DISTINCT。不要把這些函式送進現有 `aggregate_state`，那個引擎仍只接受 COUNT／SUM／AVG。
3. 接著才是 LEFT／RIGHT／FULL／SEMI／ANTI、多表與非等值 JOIN、DISTINCT、UNION／INTERSECT／EXCEPT、子查詢去關聯化、GROUPING SETS／ROLLUP／CUBE、MV on MV。
4. 沒有增量索引的運算子標成受影響鍵的 DuckDB 局部重算，不要假裝是增量。
5. 舊的 projection／filter、COUNT／SUM／AVG、INNER JOIN 差分要再跑一次。strict-small 仍然沒有 Docker，沒有新數據就寫無數據。

## 未解決

- strict-small、真實 daemon E2E、故障注入：無數據。這台 VM 沒有 Docker（`docker version` 失敗，沒有 `/var/run/docker.sock`），也沒有 MySQL／StarRocks 服務。`j4.py selftest` 的線上檢查因此跳過。本里程碑沒有改每筆 CDC 的 SQL 或 SQLite 狀態機，延遲數字不能從這次測試外推。
- 正式 `p11-50m-50rps-72h-v4`：無數據。沒有持久隔離主機。
- 參考引擎是語意對照，不是熱路徑。里程碑 4 之前不要用它替換 SQLite／原生熱點。
- GitHub Actions 不跑。提交訊息帶 `[skip ci]`。

## 本地環境如何重建

工作目錄 `/workspace`。系統 Python 3.12.3。沒有虛擬環境。

```bash
git fetch origin cursor/stream-zset-ir-23fa
git checkout cursor/stream-zset-ir-23fa
python3 -m pip install -r requirements.txt

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

strict-small 需要新的空資料目錄，以及 MySQL 8.4（`--cpus=0.5 --memory=1g`，GTID）和 StarRocks 4.1.1（`--cpus=2 --memory=8g`）。本機目前沒有 Docker，所以上面的服務沒有啟動。映像名稱尚未在本輪核對，不要把未跑的命令寫成已通過。

```bash
docker run -d --name m2s-mysql --cpus=0.5 --memory=1g \
  -e MYSQL_ROOT_PASSWORD=CHANGE_ME_LOCAL -e MYSQL_DATABASE=m2s \
  -p 3306:3306 mysql:8.4 \
  --gtid-mode=ON --enforce-gtid-consistency=ON \
  --server-id=1 --log-bin=mysql-bin
# StarRocks 使用官方 4.1.1 allin1。本輪沒有拉過映像。
docker run -d --name m2s-starrocks --cpus=2 --memory=8g \
  -p 9030:9030 -p 8030:8030 -p 8040:8040 \
  starrocks/allin1-ubuntu:4.1.1
```

`CDC_EVENT_TRACE` 保持關閉。不要把 workload 初始化到舊 run 目錄或生產庫。

## 提交

- `5f3cabef404d8b536dfa112aefc68224fffcaf13` 方案與第一個斷點
- `bef1ed2c397144821a9bb2b29b587d93f6d05989` 里程碑 1 實作，測試見上表
- `7c6662709e9a5cf2243ca22dad6052b8e62c3f66` 合併 PR #65 到 `main`
