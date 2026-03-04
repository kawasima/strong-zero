# Strong-Zero 改善事項

## A. 設計上の指摘（中〜高）

### ~~A-1. Consumer の `consumed()` 呼び出しがライブラリ利用者任せ~~ → 対応済み

auto-acknowledge モード追加済み（デフォルト有効）。手動モードは `setAutoAcknowledge(false)` で切替可能。

### ~~A-2. Pump の `waitForNotification` が二重構造~~ → 対応済み

`fetchMessages` 内の `waitForNotification` を削除し、通知待ちは `run()` 側のみに集約。`retryPolicy` の `handleResultIf(List::isEmpty)` も削除済み。

### ~~A-4. `produced_zero` テーブルに PRIMARY KEY / INDEX がない~~ → 対応済み

`id VARCHAR(32) NOT NULL PRIMARY KEY` に変更済み。

### ~~A-5. `consumed_zero` テーブルに PRIMARY KEY がない~~ → 対応済み

`producer_id VARCHAR(16) NOT NULL PRIMARY KEY` に変更済み。

---

## B. コード品質の指摘（低〜中）

### ~~B-1. StrongZeroSender がリソースリーク可能~~ → 対応済み

`AutoCloseable` を実装し `close()` で `ZContext` を閉じるようにした。

### ~~B-2. ExampleProducer で二重通知~~ → 対応済み

`sender.updated()` の手動呼び出しを削除。通知は `send()` 内の `thresholdUntilNotify` ベースの自動通知に統一。

### ~~B-3. autocommit と commit/rollback の不整合~~ → 対応済み

`getLastId()` と `consumed()` で `connection.setAutoCommit(false)` を明示。`consumed()` にも `rollback()` を追加し、commit/rollback が正しく機能するようにした。

### ~~B-4. Pump の SQL 文字列結合~~ → 対応済み

`LIMIT ?` をプレースホルダーにし `stmt.setInt(2, batchSize)` で設定するよう変更。

---

## C. テストの指摘（中）

### ~~C-1. テストカバレッジが極めて低い~~ → 対応済み

7つのテストクラス（37テストケース）を追加。pom.xml に mockito-core、mockito-junit-jupiter、h2 を test スコープで追加。

- `FlakeTest` (6件) — ID形式、ユニーク性、時間順序、シーケンスインクリメント、シーケンスリセット、マルチスレッド安全性
- `StrongZeroSenderTest` (5件) — DB挿入、ユニークID、通知しきい値、シリアライズエラー、close
- `StrongZeroConsumerTest` (9件) — consumed DB更新、lastId更新、DBエラー、getLastId既存/初回、handler登録、autoAcknowledge、autoCommit/commit検証、rollback検証
- `StrongZeroPumpTest` (6件) — メッセージ取得、lastIdフィルタ、batchSize制限、空結果、SQLリトライ、ペイロード検証
- `ZeroMessageTest` (6件) — NoArgs/AllArgsコンストラクタ、MessagePack往復、equals/hashCode、toString、null処理
- `ConsumerMetricsTest` (3件) — カウンタ、タイマー、メトリクス名
- `ProducerMetricsTest` (1件) — Gauge登録と値変化

### ~~C-2. テストが `ipc://` に依存~~ → 対応済み

`ipc://` を `inproc://` に変更し、Windows でも動作するようにした。

---

## D. 運用面の指摘（低）

### D-1. produced_zero のガベージコレクションが未考慮

Consumer が consume 済みのメッセージは `produced_zero` に残り続ける。長期運用するとテーブルが肥大化する。全 Consumer の最小 `last_id` 以下のレコードを定期削除する仕組みが必要。

### D-2. メトリクス名が汎用的すぎる

`consume.messages`、`consume.time`、`pump.count` はアプリケーション内で名前衝突する可能性がある。`strong_zero.consumer.messages` のようにプレフィックスを付けるとベター。

---