# m2s 通用增量 SQL 路線

更新：2026-11-11。基線 `main`：`adba11f117915818ad5d299651a00783062d43e3`
（PR 64 合併 `728fcb74ab25cf0280dae30f53805ce2b86f1e36` 之後的文件提交）。
中斷後先讀 [RESUME.md](RESUME.md)，再讀本文件，從記錄的下一步繼續。

本路線把現有 MySQL→StarRocks 4.1.1 受限增量計算，補成可與 Flink、RisingWave、
Materialize、Bytewax、Proton、Pathway、Arroyo 對照的通用增量／串流 SQL。
比較對象的能力只作為驗收範圍，不複製它們的程式。

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
- `merge_uncertain` 自動對帳與安全解隔離。解不了就維持現有隔離，不重送未知 delete。

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
