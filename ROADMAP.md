# m2s 通用增量 SQL 路線

更新：2026-11-11。基線 `main`：`adba11f117915818ad5d299651a00783062d43e3`
（PR 64 合併 `728fcb74ab25cf0280dae30f53805ce2b86f1e36` 之後的文件提交）。
中斷後先讀 [RESUME.md](RESUME.md)，再讀本文件，從記錄的下一步繼續。

本路線把現有 MySQL→StarRocks 4.1.1 受限增量計算，補成可與 Flink、RisingWave、
Materialize、Bytewax、Proton、Pathway、Arroyo 對照的通用增量／串流 SQL。
比較對象的能力只作為驗收範圍，不複製它們的程式。

## 第二輪

第一輪 PR #69–#75 有多項只是單機或最小切片。`VERIFICATION.md` 列出它們沒有達標的地方。
第二輪要把那些項目做出可量測的行為。每一項獨立 PR，附命令、SHA 與數字。
不能把「未納入」寫成完成。只有正式 50M／72h 可以維持阻塞，原因是這台 VM
沒有可連續 72 小時的隔離主機。

1. 分散執行：多個 worker 行程用 Arrow Flight 交換批次。擴縮之後總和不變。
   殺掉一個 worker，量接管時間，總和仍與擴縮前一致。
2. exactly-once：行程在 checkpoint 提交前被 `SIGKILL`。重啟載入上一份已提交
   的切面，同一輸出 epoch 不套用第二次。
3. MinIO：新行程只從物件儲存恢復狀態，記錄物件數、位元組與耗時。
4. PostgreSQL CDC：邏輯複寫的插入、更新、刪除與來源表逐列一致。
5. Kafka：broker 上的生產與消費與送出的記錄一致，並寫明量到的語意等級。
6. 結構演進：在真實 PostgreSQL 上加可空欄位會出現在後續變更裡；
   刪除欄位不會靜默丟列，而是標成需要重建。
7. Prometheus 與 Web UI：本機埠的 `/metrics` 與 HTML 頁面由 HTTP 客戶端讀到。
8. Docker／Helm／離線包：映像與 Helm 已在 PR #84 量過。
   離線包已完成。SHA `ba76eac23ca0b4d4bfb9641e874216ef760f0adb`。
   `python3 tools/offline_install_test.py` 產生 106913552 位元組、18 個 wheels、
   `mysql_arrow_reader` 306272 位元組與 `stream_hot_rs.so` 529632 位元組。
   `unshare -n` 只開 loopback，探針 `network-blocked URLError`。
   `SELFTEST OK: 37 offline regression groups`。包不進 Git。
9. TPC-H Q1–Q22：已完成。DuckDB 產生器提供資料與查詢文字，原文不進 Git。
   SHA `7ba9cf9398aefa1e7287feb6606ee91858dc8218`，`python3 tools/tpch_diff_test.py`，
   scale 0.01。Q1–Q22 都是 incremental，列數 4,4,10,5,5,1,4,2,173,20,359,2,32,1,1,296,1,2,1,1,1,7。
   結尾 `tpch_diff_test ok queries=22 scale=0.01`。沒有用 `force_recompute`。
10. Nexmark：已完成。合成 person 50、auction 40、bid 400，種子 `20261112`。
    `python3 tools/nexmark_diff_test.py`。q0–q9 都是 incremental。
    插入兩批再撤回一筆，三階段都與 DuckDB 逐列一致。
    撤回後列數 399,399,120,10,5,35,35,135,50,5。
    結尾 `nexmark_diff_test ok queries=10 persons=50 auctions=40 bids=400`。
    SHA `13f52326b54e06ecd74fba49648d4c0968855ef4`。
11. 重跑 strict-small：已通過。SHA `adbbfb8dc317eb46fa149b6e43050701df999c3f`。
    目錄 `/tmp/m2s-validation-small-r2`。`gate.json` ok=true。
    健康樣本 n=291，P95=2.0328105999997206 秒，P99=3.0322560660006275 秒，
    超過 5 秒與 10 秒都是 0。恢復樣本 n=9，P95=P99=1.624497284001336 秒，
    max=2.031456826998692 秒。兩次 fault。全列摘要一致。pending=0。
    elapsed 304.28334542000084 秒。門檻仍是 5 秒／10 秒。

## 第四輪

1. TPC-DS：進行中。sf=0.01，查詢文字不進 Git。已與 DuckDB 逐列一致的是
   1,3,4,5,6,7,11,15,16,17,18,19,21,24,25,26,27,29,30,31,32,33,34,37,39,40,42,43,46,48,50,52,55,56,59,60,62,64,65,66,68,69,71,73,74,75,76,78,80,81,82,84,87,91,92,94,95,96,97。
   共 59 條，都是 incremental。SHA `f8086a70e3b4d2430a5cec26e96e52b23825f83d`。
   其餘還沒通過：有的退回全量，有的計畫或列值不一致，Q72 在這台 VM 上被 OOM 殺掉（exit 137）。
2. ClickHouse、Doris、Iceberg、Paimon 下游寫入。
3. CDC 熱路徑對照。新的沒有比較快就不切換。

## 第三輪

對照原定 10 個里程碑，第二輪沒有真正執行的能力在這裡逐項實作。
每項獨立 PR。正式 72h 仍阻塞：這台 VM 沒有可連續 72 小時的隔離主機。

1. UDF：Python 聚合與表函數在 `python -I` 沙盒執行。WASM 與 Rust
   都進 wasmtime，Rust 先編成 wasm32。拒絕 import，fuel 用盡就失敗。
   同一輸入跑兩次結果相同。SHA `2c3779a331ef9929ff458e9913b144f9c6c175e0`。
   `udf_exec python_udaf=2 python_udtf=[3, 4] wasm=5 wasm_fuel=4 rust=42 rust_bytes=361 rust_fuel=4`。
2. Web UI：作業 DAG、運算子吞吐與延遲、checkpoint，以及暫停／恢復。
   `python3 tools/ops_ui_test.py`：operators=4，output_rows=101，
   paused_rejects=1，checkpoint_epoch=1，max_operator_latency_seconds=0.038459。
   Chrome `--headless=new --dump-dom` 讀到 job、checkpoint_epoch=1、svg 與表格。
   SHA `d42c18dada0aea9cb37736bd45a0efbfe5f89fe0`。
3. 跨節點：已完成。3 個 network namespace，各自的 veth，再擴到 4。
   `python3 tools/flight_netns_test.py`：sum=600，bytes=9996，ping_ms=0.139，
   exchange_seconds=0.014985，rtt_seconds=0.001443，
   failover_seconds=0.119745，scale_seconds=0.181041。
   這不是分散式 SLO。SHA `ebc780671b9a16409c1e5670af4a91a6d77d0906`。
4. DDL：已完成。MySQL 與 PostgreSQL 都做了加可空欄、int 放寬成 bigint、
   varchar(32) 縮成 varchar(8)、刪欄。來源列數維持 1。
   下游 MV：加欄與放寬是 propagate，縮窄與刪欄是 rebuild，drop_rows=0，retained_rows=1。
   `python3 tools/ddl_mv_test.py`。SHA `a3074c85f20e129f02688b692143cfeed2bf225f`。

## 實作順序

里程碑 1、2、3 已完成。strict-small 卡在未知 Merge Commit 的
`merge_uncertain` 隔離，所以先做里程碑 5 的自動對帳與安全解隔離，
獨立 PR。里程碑 4 接在這次對帳之後。里程碑 5 其餘項目
（barrier、checkpoint、savepoint）仍在里程碑 4 之後，按本文件往下做。

## 不可放寬的契約

- 正式 profile `p11-50m-50rps-72h-v4` 的定義、門檻與計時起點保持不變。
  健康路徑仍是 MySQL commit 之後到 StarRocks 可查詢：P95≤5s、P99≤10s。
  不刪慢樣本，不把 HTTP accepted 當成 VISIBLE。
- 交易邊界、fixed-W pin／consumer／GC、generation、未知輸出 fail-closed
  保持有效。既有全列 oracle 與失敗產物保留。
- 每個里程碑先證明既有 contracts、E2E、strict-small、oracle、故障注入不退化，
  再合併。數據只寫該 SHA 的實測；沒有就寫「無數據」。不同 SHA 不併成一次認證。
- 不用 GitHub Actions。開發、建置、測試都在本機 VM。
  可安裝 MySQL、StarRocks 4.1.1（或官方容器）、Rust、RocksDB、MinIO。
- Python 沿用現有函式與底線風格，不加 typing、不加 logging。
  執行期輸出用 `print(..., flush=True)`。
- GitHub 只放合成程式、設定與資料。憑證、生產位址、真實日誌、
  SQLite／WAL、run 目錄與 metrics 不進版本庫。
- 無高效增量形式的運算子，退回受影響 key／分區的 DuckDB 局部重算，
  並在計畫裡標成 `partial_recompute`。非確定性函式拒絕，或明示定時重算模式。
- 開放中的舊 PR（1–3、12、19、29、31、33、34、36–40、42–54、56）維持未合併。
  不要把那些實驗整進本路線，也不要重寫 `main` 歷史。
- 長時間負載使用新的 run 目錄與隔離服務，不覆蓋舊 run 或生產庫。
  無法跑 50M／72h 時記下阻塞原因後繼續下一項。

## 現況（里程碑開始前）

已有、且本路線必須保住的能力：

- 單 MySQL 實例到 StarRocks 4.1.1；全量與 CDC 並行；GTID／binlog 恢復。
- projection／filter 的無狀態增量 IR（`relational_ir.py`、`incremental_ir.py`）。
- GROUP BY 的 COUNT／SUM／AVG（`aggregate_ir.py` 與 SQLite 累加器）。
- 雙表 INNER equi-join（`join_ir.py` 與 pair identity 狀態）。
- fixed-W、版本化 source、pin／consumer、共享狀態、generation、outbox。
- 動態任務 hot-add／drop／rebuild、未知 Merge Commit 隔離。
- strict-small 最近實測在 PR 64：250ms merge interval，門檻內。
  正式 50M／72h 仍無持久隔離主機，無數據。

還沒有、且按下面順序補的能力：通用運算子 DAG、完整 JOIN／集合／子查詢、
視窗與事件時間、檢查點與 exactly-once、分散式、物件儲存、多來源多目標、
UDF 與運維面。

## 目標架構

一條 SQL 編譯成有向無環的運算子圖。邊上流的是 Z-set：有限列到整數權重，
`+1` 插入、`-1` 撤回、權重和為 0 的列刪除。既有 delete／upsert
（`_sync_op` 1／0）只在來源邊界轉成權重，並以主鍵鏡像保證 upsert 是
「先撤回舊列再插入新列」。

運算子分成線性（filter、project：只作用在 delta 上）與需要狀態的
（aggregate、equi-join，以及後續視窗、distinct、集合運算）。
JOIN 使用雙線性規則：輸出權重是左右權重的乘積；NULL 鍵不匹配。
聚合維護群組累加器，群組從空到非空發出 `+1`，值變化發出 `-1` 舊結果與
`+1` 新結果，回到空群組發出 `-1`。

耐久狀態仍先用現有 SQLite 引擎。新的記憶體內引擎是語意參考實作，
用來證明與 DuckDB 全量重算、以及與現有 COUNT／SUM／AVG、INNER JOIN
引擎的整合結果一致。在有同機延遲證據之前，不把參考引擎換上 CDC 熱路徑。

計畫身份：既有 IR 的 canonical bytes 與 semantic id 不變，避免已耐久的
任務身份被換掉。stream plan 是由既有 IR 導出的第二身份，計畫建立時一定
要能降下來，熱更新時身份不同就拒絕就地切換。

## 里程碑

每一里程碑單獨分支、單獨 PR、本地驗證後再合併。分支名前綴 `cursor/`，
後綴保持任務要求的固定尾碼。合併前記下精確 SHA、命令與結果。

### 1. 統一 IR 與 Z-set DAG

範圍：

- Z-set、權重合併、主鍵 changelog 邊界。
- 可組合運算子：`input`、`filter`、`project`、`aggregate`（COUNT／SUM／AVG）、
  `equi_join`（現有受限 INNER）。
- 既有 projection／filter、聚合 IR、JOIN IR 降下到這張 DAG。
- 無狀態計畫建立與 stateful catalog 編譯都產出 stream plan id。
- 參考引擎按批吃 delta；多批整合結果等於一次全量重算。
- 差分：隨機增刪改對 DuckDB；聚合再對 `aggregate_state`；
  JOIN 再對 `join_state`。另有一條手寫的 filter／project／aggregate
  與 join／aggregate 組合，證明 DAG 不是三條不相連的計畫。

不做：改熱路徑 SQL 執行、改 SQLite 狀態機、改 merge interval、
改 `p11-50m-50rps-72h-v4`、LEFT／OUTER、MIN／MAX、DISTINCT。

驗收：新差分測試通過；既有 incremental／aggregate／join IR 與 state oracle、
離線 `j4.py selftest`、privacy 通過。熱路徑程式未改時，strict-small
若環境可跑就跑一輪對照；不可跑就記錄原因。無數據不得寫成加速。

### 2. 完整 SQL 之一

範圍：

- SQL 前端：sqlglot `read=mysql` 再產生 DuckDB，方言可設定，預設保持現有相容。
- JOIN：INNER／LEFT／RIGHT／FULL／SEMI／ANTI、多表、非等值。
  非等值沒有增量索引時走受影響鍵的局部重算。
- 聚合補齊 MIN／MAX、COUNT DISTINCT、STRING_AGG 等；DISTINCT。
- UNION／INTERSECT／EXCEPT（bag 與 set 兩種，權重規則寫進計畫）。
- 子查詢去關聯化。去不掉的子查詢標成受影響分區重算。
- GROUPING SETS／ROLLUP／CUBE。
- MV on MV：下游任務消費上游已整合的 Z-set，沿用 fixed-W 與 generation。

驗收：每種新運算子有對 DuckDB 的隨機差分；舊子集的降下結果與里程碑 1
的 semantic id 相容或明示需要新 generation。strict-small 不退化。

### 3. 完整 SQL 之二：時間與視窗

範圍：

- 事件時間與 watermark、遲到策略（丟棄、進側輸出、或受控重算）。
- 滾動、滑動、會話、累積視窗。
- 視窗函式 `OVER`：只重算受影響分區。
- Top-N。
- CTE，含遞迴的半樸素固定點，並有迭代上限。
- 時態 JOIN、區間 JOIN。
- 狀態 TTL。
- 無增量形式的運算子退回受影響 key／分區的 DuckDB 局部重算。
- `random`／`now`／`uuid` 一類非確定性函式預設拒絕；定時重算模式必須在計畫裡寫明。

驗收：腳本化遲到、亂序與固定點案例對 DuckDB 或對獨立的全量重算；
不把非確定性函式悄悄算成常數。

### 4. 狀態後端與熱點

範圍：

- 狀態後端介面，SQLite 為預設實作，RocksDB 為第二實作。
- Rust（PyO3）只做已用小剖面量過的熱點：JOIN 索引、有序狀態、權重合併、序列化。
- 自適應批次：低流量用小批次保延遲，高流量合批。預設行為在小剖面上
  先對照現有門檻，再考慮改預設值。

驗收：同一差分在兩個後端結果一致。Rust 熱點有 Python fallback。
沒有小剖面對照數據之前不替換現有原生熱路徑。

### 5. 檢查點與 exactly-once

範圍：

- 全域 barrier 與非同步 checkpoint。
- 端到端 exactly-once：來源位置、狀態、輸出一起切。
- savepoint。
- `merge_uncertain` 自動對帳與安全解隔離。這項提前到里程碑 4 之前。
  用 StarRocks label／txn 狀態確認遠端結果。舊 txn 要先 fence。
  已確認 VISIBLE 才前進；已確認中止才做 delete-safe 重放。
  無法確認就維持隔離，不重送未知 delete。

驗收：進程強退、重放、重複 checkpoint、未知 HTTP 的故障注入。
既有 fail-closed 測試仍通過。不能對帳的案例必須仍隔離。

### 6. 分散式

範圍：

- 協調、計算、來源三種節點。
- hash 分片。
- Arrow Flight 交換與背壓。
- 擴縮容時狀態重分片。
- 故障轉移。
- 熱點 key 預聚合。

驗收：單機多進程即可跑通的擴容與轉移場景，加上里程碑 1–3 的差分。
不把單機 strict-small 數字說成分散式 SLO。

### 7. 存算分離

範圍：

- S3 相容物件儲存（內網用 MinIO）放狀態與 checkpoint。
- 增量 checkpoint。
- 分層快取：本地熱狀態，遠端冷狀態。

驗收：MinIO 上的 checkpoint 能恢復到與本地後端相同的查詢結果。
物件裡不進 GitHub。

### 8. 連接器與 schema 演進

範圍：

- 來源：多 MySQL、PostgreSQL、Kafka。
- 目標：StarRocks（現有）、Doris、ClickHouse、Kafka、Iceberg、Paimon、MySQL／PostgreSQL。
- 統一連接器 SDK，並標語意等級（至少一次、效果上恰好一次、或需下游去重）。
- DDL：能傳播的變更傳播；不能安全傳播的進入受控重建，不靜默丟列。

驗收：每種已實作連接器用合成資料跑通一條增量路徑。未實作的連接器拒絕，
不假裝支援。

### 9. 擴充與運維

範圍：

- Python／Rust／WASM 的 UDF、UDAF、UDTF。沙盒執行。呼叫方宣告確定性；
  未宣告確定性的函式走里程碑 3 的拒絕或定時重算。
- Prometheus 指標、Web UI、`EXPLAIN ANALYZE`。
- Docker 映像、Helm chart、離線內網安裝包。
- 多租戶配額與權限稽核。

驗收：UDF 確定性宣告會進入計畫身份。EXPLAIN 能印出 DAG 與選中的
增量或局部重算策略。映像只含合成設定。

### 10. 驗證與對標

範圍：

- 隨機 SQL 加隨機增刪改，每批與 DuckDB 全量重算逐列一致。
- TPC-H 22 條、TPC-DS 子集、Nexmark。只提交合成資料產生器與查詢，不提交受版權限制的原文資料檔。
- 故障注入：節點宕機、網路分區、磁碟滿。
- 原 5s／10s 門檻的 strict-small，以及環境允許時的正式 72h。
- 分散式版本完成後補擴容與故障轉移場景。

驗收：報告寫明查詢子集、SHA、命令與未跑項目。通不過的查詢列為缺口，
不從門檻裡拿掉。

## 建議的本地重建

具體指令以 RESUME.md 當前環境為準。原則：

- MySQL 8.4 開 GTID；StarRocks 固定 4.1.1。
- 對照 strict-small 時沿用已記錄的容器上限：MySQL `--cpus=0.5 --memory=1g`，
  StarRocks `--cpus=2 --memory=8g`，空資料目錄，90 秒 settle，每輪新 run 目錄。
- `CDC_EVENT_TRACE` 預設關閉。`p11` 正式剖面不拿來做開發迭代。
