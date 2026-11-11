# 第一輪驗證

更新：2026-11-11。這份文件收 PR #75。它只記錄第一輪真正跑過的命令。
把某一項寫成「未納入」不是完成。那些項目在 [ROADMAP.md](ROADMAP.md) 的第二輪裡，必須逐項實作並在本機 VM 量測。

不使用 GitHub Actions。門檻仍是 P95≤5s、P99≤10s。正式 `p11-50m-50rps-72h-v4` 沒有跑，原因在文末。

## 第一輪已經合併、而且量過的

| 項目 | PR | 合併 SHA | 本機命令與結果 |
|---|---|---|---|
| Z-set DAG | [#65](https://github.com/justgo4/m2s/pull/65) | `7c6662709e9a5cf2243ca22dad6052b8e62c3f66` | `python3 tools/stream_zset_test.py` 通過 |
| SQL 子集 | [#66](https://github.com/justgo4/m2s/pull/66) | `7805cbcaa0dbad9965dbe05468764aaee73028b7` | `python3 tools/stream_sql_test.py` 通過 |
| 事件時間與視窗 | [#67](https://github.com/justgo4/m2s/pull/67) | `d051855e3f80212b64314e63485e384729ced327` | `python3 tools/stream_time_test.py` 通過 |
| Merge Commit 對帳 | [#68](https://github.com/justgo4/m2s/pull/68) | `ce94b7aac303cb83422f5cedb72b109edb28572f` | strict-small 通過。SHA `96501dcd0d7bf3c7b84b7d2ac17aaed7d759a9d0`。健康樣本 n=293，P95=2.028193623s，P99=2.038108948s，超過 5s 與 10s 的都是 0。恢復樣本 n=7，P95=1.620141182s，P99=1.620141182s，max=2.264278048s |
| SQLite / RocksDB 後端 | [#69](https://github.com/justgo4/m2s/pull/69) | `8c3ccdb3434c563b4677dcc275be9037d4072cea` | `python3 tools/state_backend_test.py` 通過。CDC 熱路徑沒有換成這兩個後端 |
| checkpoint / savepoint | [#70](https://github.com/justgo4/m2s/pull/70) | `299fee56ed86c476224d72ce0d0a5c91391fab6b` | `python3 tools/stream_checkpoint_test.py` 通過。這是行程內呼叫，不是殺掉行程 |
| 單機多進程骨架 | [#71](https://github.com/justgo4/m2s/pull/71) | `e6b217968382dbe2cee69a3cbce835f5cabc5f9e` | `python3 tools/stream_cluster_test.py` 通過。沒有量擴縮與接管秒數 |
| MinIO 增量 checkpoint | [#72](https://github.com/justgo4/m2s/pull/72) | `7890bbb1f5e2bd5ec61e328dac2141d6fe4531c2` | `python3 tools/stream_remote_test.py` 印出 `minio_ok`。官方 `dl.min.io` 回 410，二進位是本機 Go 建的 |
| 連接器 SDK | [#73](https://github.com/justgo4/m2s/pull/73) | `e91fb8cb1cf0d754ccd0125909546242f9f6a0fd` | 合成事件。沒有連上 PostgreSQL 或 Kafka |
| UDF / 說明 / 包裝檔 | [#74](https://github.com/justgo4/m2s/pull/74) | `bf75573c0795b3d13bec0d9438d15fe07bae701f` | `python3 tools/stream_ops_test.py` 通過。沒有啟動的 Web UI，也沒有建出映像 |
| 三條自寫查詢與缺口清單 | [#75](https://github.com/justgo4/m2s/pull/75) | `fbf951272a4170514edef84feb22d2d4472f42e8` | `python3 tools/stream_gap_test.py` 與 `python3 tools/stream_sql_test.py` 通過 |

目前 `main` 文件提交是 `79279976e0f7ed112c3c0d618079b9b6a5b1bbc1`。

## 第一輪不夠，第二輪必須做完

下面每一項都還沒有達標。不得再用「未納入」把它們劃掉。

1. 多進程分散執行：Arrow Flight 在 worker 之間搬批次，動態擴縮後總和不變，並量測 worker 被殺掉之後的接管時間。
2. exactly-once：行程在 checkpoint 提交前被強退，重啟後不重複套用輸出。
3. MinIO 存算分離：新行程只從 MinIO 恢復，並記錄位元組數與耗時。
4. PostgreSQL CDC：真實 WAL，插入／更新／刪除與來源表一致。
5. Kafka：真實 broker，生產與消費後與送出的記錄一致，並標明量到的語意等級。
6. 結構演進：可加的空值欄位會傳播；不能安全刪除的變更進入重建，不靜默丟列。要在真實資料庫上做。
7. Prometheus 與 Web UI：本機 HTTP，`/metrics` 與頁面都用 HTTP 客戶端打過。
8. Docker 映像、Helm 範本、離線包：在 VM 上建或打包，並檢查裡面沒有憑證。做不到的只限工具本身無法安裝，要寫明命令與錯誤。
9. TPC-H Q1–Q22：用 DuckDB 的 TPC-H 產生器取數與查詢文字（不把官方原文另存進 Git），每一題的結果與獨立 DuckDB 執行逐列一致。做不到的題目要留下錯誤，不能從清單刪掉。
10. Nexmark：合成事件，查詢結果與 DuckDB 全量重算逐列一致。
11. 重跑 strict-small：新的 SHA，門檻仍是 P95≤5s、P99≤10s。不把 `96501dcd` 的數字算到新 SHA 上。

## 目前唯一可以維持阻塞的項目

正式 `p11-50m-50rps-72h-v4`：這台 VM 沒有可連續 72 小時、與生產隔離的主機，也沒有 5,000 萬列的持久 run 目錄。在第二輪其他項目做完之前不啟動這份剖面。無數據。
