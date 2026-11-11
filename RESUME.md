# 斷點

更新：2026-11-11。新會話先讀本檔，再讀 [ROADMAP.md](ROADMAP.md)、[AGENTS.md](AGENTS.md)、[README.md](README.md)、[PROGRESS.md](PROGRESS.md)。不要重做已完成步驟。

## 當前里程碑

里程碑 1：統一 IR + Z-set（權重 +1／-1）+ 可組合運算子 DAG。把現有 projection／filter、COUNT／SUM／AVG、受限 INNER JOIN 降下到這張 DAG，並證明整合結果不偏離 DuckDB 與既有引擎。

尚未開始寫程式。本檔與 ROADMAP.md、PROGRESS.md、README.md 的指標是第一個斷點。

## 倉庫位置

- 分支：`cursor/stream-zset-ir-23fa`
- 基線：`adba11f117915818ad5d299651a00783062d43e3`（`origin/main` 同一 SHA）
- 遠端：`justgo4/m2s`
- 開放 PR 是舊實驗，不要合併，也不要重寫 main。

## 已完成

- 對過 `origin/main` 與開放 PR。main 就是 `adba11f117915818ad5d299651a00783062d43e3`。
- 寫下 ROADMAP.md 與本檔。PROGRESS.md 已記下本輪起點。README.md 加了兩份文件的入口。

尚無實作 commit。無測試數據。

## 正在做

把方案與斷點推上 `cursor/stream-zset-ir-23fa`。

## 下一步

1. 新增 `stream_zset.py`：Z-set、權重合併、列身份、主鍵 changelog 鏡像（delete=`-1`，upsert 先撤回舊列）。權重只接受整數；負權重或撤回不存在的鍵要 fail-closed。
2. 新增 `stream_ir.py`：DAG 驗證與 semantic id。`lower_stateless`、`lower_aggregate`、`lower_join`。既有 IR 的 canonical bytes 不要改。
3. 新增 `stream_eval.py`：線性 filter／project（DuckDB 算表達式）、聚合累加器、雙線性 equi-join。`push` 失敗要還原本批之前的狀態。NULL join 鍵不匹配。
4. 接線：`j4.runtime_plan_entry` 寫入 `_stream_plan_id`，熱更新比對時納入；`stateful_task_plan.compile_ir`／`compile_task` 帶上 stream plan，但不要放進既有 descriptor hash。
5. `tools/stream_zset_test.py`：腳本案例 + 隨機增刪改。對 DuckDB 全量；聚合再對 `aggregate_state`；JOIN 再對 `join_state`；另測 filter→project→aggregate 與 join→aggregate 兩條組合 DAG。
6. 把新測試掛進 `.github/workflows/ci.yml` 的本地對應清單（本程式不跑 Actions）。
7. 本地跑：`python3 tools/stream_zset_test.py`、既有 IR／state oracle、`python3 tools/privacy_check.py`、`python3 j4.py selftest`。熱路徑 SQL 若未改，strict-small 能跑就跑一輪；跑不了就在本檔寫原因。
8. 更新本檔與 PROGRESS.md 的精確 SHA 與命令結果，推分支，開里程碑 1 的 PR。驗證通過再合併 main。

## 未解決

- 正式 `p11-50m-50rps-72h-v4` 沒有持久隔離主機。無數據。不要用 strict-small 代替。
- 里程碑 1 不替換 SQLite 熱狀態，也不把參考引擎放上每筆 CDC。原因寫在 ROADMAP：還沒有同機延遲證據之前不能換熱路徑。
- GitHub Actions 有儲存額度限制，本程式不跑、不依賴它。commit 訊息加 `[skip ci]`，避免 push／PR 觸發 workflow。

## 本地環境如何重建

工作目錄：`/workspace`。目前檢查到的解釋器是系統 Python。下列服務指令尚未在本輪執行，機器上若還沒有容器就先安裝 Docker 再跑。

```bash
git fetch origin main
git checkout cursor/stream-zset-ir-23fa
python3 -m pip install -r requirements.txt

# 對照用的服務上限與 2026-11-04 記錄相同。資料目錄用新的空目錄。
docker run -d --name m2s-mysql --cpus=0.5 --memory=1g \
  -e MYSQL_ROOT_PASSWORD=CHANGE_ME_LOCAL -e MYSQL_DATABASE=m2s \
  -p 3306:3306 mysql:8.4 \
  --gtid-mode=ON --enforce-gtid-consistency=ON \
  --server-id=1 --log-bin=mysql-bin
# StarRocks 映像名稱尚未在本輪核對。用官方 4.1.1 allin1，資源上限保持：
docker run -d --name m2s-starrocks --cpus=2 --memory=8g \
  -p 9030:9030 -p 8030:8030 -p 8040:8040 \
  starrocks/allin1-ubuntu:4.1.1

python3 tools/stream_zset_test.py
python3 tools/incremental_ir_test.py
python3 tools/aggregate_ir_test.py
python3 tools/join_ir_test.py
python3 tools/aggregate_state_oracle.py
python3 tools/join_state_oracle.py
python3 tools/incremental_ir_state_oracle.py
python3 tools/privacy_check.py
python3 j4.py selftest
```

strict-small 沿用 `python3 tools/validation_run.py run --isolated --profile small --run-directory <新目錄>`。不要指向舊 run 目錄或生產庫。`CDC_EVENT_TRACE` 保持關閉，除非要測的就是 trace。

## 提交

尚無。推上去之後把 SHA 補在這裡。
