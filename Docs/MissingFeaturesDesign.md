# Missing Features Design Document

FDB Record Layerとの比較に基づく不足機能の設計書

## 概要

本ドキュメントでは、FDB Record Layerに存在し、database-frameworkに不足している機能の設計を記述する。
各機能は汎用的な命名を採用し、"Record"に特化した名前は使用しない。

---

## Phase 1: データ管理基盤

### 1.1 LargeValueSplitter (大きな値の分割)

#### 目的
FoundationDBの100KB値サイズ制限を透過的に処理し、大きなデータを複数のキー・バリューペアに分割・再結合する。

#### 参照
- FDB Record Layer: `SplitHelper.java`

#### API設計

```swift
/// 分割設定
public struct SplitConfiguration: Sendable {
    /// 単一値の最大サイズ（バイト）デフォルト: 90KB
    public let maxValueSize: Int
    /// 分割を有効化
    public let enabled: Bool
}

/// 大きな値の分割・再結合を処理
public struct LargeValueSplitter: Sendable {
    /// 値を保存（必要に応じて分割）
    public func save(_ data: FDB.Bytes, for key: FDB.Bytes, transaction: TransactionProtocol) throws

    /// 値を読み込み（分割されていれば再結合）
    public func load(for key: FDB.Bytes, transaction: TransactionProtocol) async throws -> FDB.Bytes?

    /// 値を削除（分割された全パーツを削除）
    public func delete(for key: FDB.Bytes, transaction: TransactionProtocol) async throws

    /// 値が分割されているか確認
    public func isSplit(for key: FDB.Bytes, transaction: TransactionProtocol) async throws -> Bool
}
```

#### キー構造

```
非分割値:
  Key: [baseKey]
  Value: [data]

分割値:
  Key: [baseKey][0x00]     → Header: [totalSize:Int64][partCount:Int32]
  Key: [baseKey][0x01]     → Part 1 data
  Key: [baseKey][0x02]     → Part 2 data
  ...
```

#### 統合ポイント
- `FDBDataStore`: 保存・読み込み時に`LargeValueSplitter`を使用
- `DataAccess`: シリアライズ後のデータに適用

---

### 1.2 TransformingSerializer (圧縮・暗号化対応シリアライザ)

#### 目的
シリアライズされたデータに圧縮・暗号化などの変換を適用する。

#### 参照
- FDB Record Layer: `TransformedRecordSerializer.java`, `TransformedRecordSerializerJCE.java`

#### API設計

```swift
/// 変換タイプ
public enum TransformationType: UInt8, Sendable {
    case none = 0x00
    case compressed = 0x01
    case encrypted = 0x02
    case compressedAndEncrypted = 0x03
}

/// 変換設定
public struct TransformConfiguration: Sendable {
    /// 圧縮を有効化
    public let compressionEnabled: Bool
    /// 圧縮レベル (0-9)
    public let compressionLevel: Int
    /// 圧縮の最小サイズ（これより小さいデータは圧縮しない）
    public let compressionMinSize: Int
    /// 暗号化を有効化
    public let encryptionEnabled: Bool
    /// 暗号化キープロバイダ
    public let keyProvider: EncryptionKeyProvider?
}

/// 暗号化キープロバイダプロトコル
public protocol EncryptionKeyProvider: Sendable {
    /// 暗号化キーを取得
    func getKey(for keyId: String) async throws -> Data
    /// 現在のキーIDを取得
    func currentKeyId() -> String
}

/// 変換シリアライザ
public struct TransformingSerializer: Sendable {
    /// 変換を適用してシリアライズ
    public func serialize(_ data: Data) throws -> Data

    /// 変換を解除してデシリアライズ
    public func deserialize(_ data: Data) throws -> Data
}
```

#### データ形式

```
[1 byte: TransformationType][payload]

圧縮時:
[0x01][compressed data]

暗号化時:
[0x02][keyId length:UInt8][keyId][IV:16 bytes][encrypted data][auth tag:16 bytes]

圧縮+暗号化時:
[0x03][keyId length:UInt8][keyId][IV:16 bytes][encrypted compressed data][auth tag:16 bytes]
```

#### 統合ポイント
- `DataAccess.serialize()` / `DataAccess.deserialize()`
- `FDBContainer`の設定オプション

---

### 1.3 ReadVersionCache (弱い読み取りセマンティクス)

#### 目的
キャッシュされた読み取りバージョンを再利用し、厳密な一貫性が不要な場合のパフォーマンスを向上させる。

#### 参照
- FDB Record Layer: `WeakReadSemantics.java`, `FDBDatabase.java`

#### API設計

```swift
/// 弱い読み取りの設定
public struct WeakReadSemantics: Sendable {
    /// キャッシュの最大鮮度（秒）
    public let maxStalenessSeconds: Double
    /// 最小読み取りバージョン（これより古いバージョンは使用しない）
    public let minReadVersion: Int64?
    /// キャッシュを使用するか
    public let useCachedReadVersion: Bool

    public static let none = WeakReadSemantics(maxStalenessSeconds: 0, useCachedReadVersion: false)
    public static let bounded(seconds: Double) = WeakReadSemantics(maxStalenessSeconds: seconds, useCachedReadVersion: true)
}

/// 読み取りバージョンキャッシュ
public final class ReadVersionCache: Sendable {
    /// キャッシュされた読み取りバージョンを取得
    public func getCachedVersion(semantics: WeakReadSemantics) -> Int64?

    /// 読み取りバージョンを更新
    public func updateVersion(_ version: Int64, timestamp: Date)

    /// コミットバージョンを記録（読み取りバージョンより新しい場合は更新）
    public func recordCommitVersion(_ version: Int64)
}
```

#### 統合ポイント
- `FDBContext`: トランザクション開始時に`ReadVersionCache`を参照
- `FDBContainer`: `ReadVersionCache`インスタンスを保持

---

## Phase 2: オンラインインデクサー機能強化

### 2.1 IndexFromIndexBuilder (既存インデックスからのインデックス構築)

#### 目的
既存のインデックスをスキャンして新しいインデックスを構築する。元データを読まずに構築可能な場合、I/Oを大幅に削減。

#### 参照
- FDB Record Layer: `IndexingByIndex.java`

#### API設計

```swift
/// インデックスからインデックスを構築するビルダー
public final class IndexFromIndexBuilder<Item: Persistable>: Sendable {
    /// ソースインデックス
    public let sourceIndex: Index

    /// ターゲットインデックス
    public let targetIndex: Index

    /// ソースインデックスのメンテナー
    public let sourceMaintainer: any IndexMaintainer<Item>

    /// ターゲットインデックスのメンテナー
    public let targetMaintainer: any IndexMaintainer<Item>

    /// インデックスを構築
    public func build(clearFirst: Bool = false) async throws

    /// 進捗を取得
    public func getProgress() async throws -> IndexBuildProgress
}

/// ソースインデックスの適合性チェック
public enum IndexSourceCompatibility {
    case compatible           // ソースから直接構築可能
    case requiresDataFetch    // ソースからキーは取得可能だが、データ読み込みが必要
    case incompatible         // ソースからの構築不可
}
```

#### 利用条件
- ソースインデックスがターゲットインデックスに必要なフィールドをすべて含む
- ソースインデックスが`readable`状態

#### 統合ポイント
- `OnlineIndexer`: 構築ストラテジーの選択肢として追加
- `IndexBuildStrategy`: 新しいストラテジータイプを追加

---

### 2.2 AdaptiveThrottler (適応型スロットリング)

#### 目的
インデックス構築のスループットを動的に調整し、本番負荷への影響を最小化しながら効率的に構築する。

#### 参照
- FDB Record Layer: `IndexingThrottle.java`

#### API設計

```swift
/// スロットリング設定
public struct ThrottleConfiguration: Sendable {
    /// 初期バッチサイズ
    public let initialBatchSize: Int
    /// 最小バッチサイズ
    public let minBatchSize: Int
    /// 最大バッチサイズ
    public let maxBatchSize: Int
    /// 成功時のバッチサイズ増加率
    public let increaseRatio: Double
    /// 失敗時のバッチサイズ減少率
    public let decreaseRatio: Double
    /// 最小遅延（ミリ秒）
    public let minDelayMs: Int
    /// 最大遅延（ミリ秒）
    public let maxDelayMs: Int
}

/// 適応型スロットラー
public final class AdaptiveThrottler: Sendable {
    /// 現在のバッチサイズを取得
    public var currentBatchSize: Int { get }

    /// 現在の遅延を取得
    public var currentDelayMs: Int { get }

    /// 成功を記録（バッチサイズ増加の可能性）
    public func recordSuccess(itemCount: Int, durationNs: UInt64)

    /// 失敗を記録（バッチサイズ減少）
    public func recordFailure(error: Error)

    /// リトライ可能なエラーかどうか
    public func isRetryable(_ error: Error) -> Bool

    /// 次のバッチの前に待機
    public func waitBeforeNextBatch() async throws
}
```

#### 統合ポイント
- `OnlineIndexer`: 固定バッチサイズの代わりに`AdaptiveThrottler`を使用
- `MultiTargetOnlineIndexer`: 同様に統合
- `MutualOnlineIndexer`: 同様に統合

---

## Phase 3: クエリ最適化

### 3.1 InPredicateOptimizer (IN述語最適化)

#### 目的
IN述語を効率的なJOINまたはUNION操作に変換し、クエリパフォーマンスを向上させる。

#### 参照
- FDB Record Layer: `InExtractor.java`

#### API設計

```swift
/// IN述語の最適化結果
public enum InOptimizationStrategy {
    /// インデックスユニオン（各値に対してインデックススキャン）
    case indexUnion(values: [any TupleElement])
    /// インジョイン（値リストをジョインソースとして使用）
    case inJoin(values: [any TupleElement])
    /// 展開なし（値が少ない場合やインデックスがない場合）
    case noExpansion
}

/// IN述語オプティマイザ
public struct InPredicateOptimizer {
    /// IN述語を抽出して最適化
    public func optimize(
        condition: QueryCondition,
        availableIndexes: [Index],
        statistics: StatisticsProvider?
    ) -> (optimizedCondition: QueryCondition, strategy: InOptimizationStrategy)

    /// IN展開の閾値（これより多い値はUNIONではなくJOINを使用）
    public var unionThreshold: Int
}
```

#### 最適化ルール
1. IN値が少ない（< unionThreshold）: インデックスユニオンに展開
2. IN値が多い: インジョインを使用
3. インデックスがない: 展開しない

#### 統合ポイント
- `QueryPlanner`: プラン生成時にIN述語を最適化
- `PlanEnumerator`: IN展開されたプランを列挙

---

### 3.2 PlanComplexityLimit (プラン複雑度制限)

#### 目的
クエリプランの複雑度を制限し、過度に複雑なプランによるリソース消費を防ぐ。

#### 参照
- FDB Record Layer: `RecordQueryPlannerConfiguration.java`

#### API設計

```swift
/// プランナー設定
public struct QueryPlannerConfiguration: Sendable {
    /// 複雑度の閾値
    public let complexityThreshold: Int
    /// 最大プラン列挙数
    public let maxPlanEnumerations: Int
    /// 最大ルール適用回数
    public let maxRuleApplications: Int
    /// タイムアウト（秒）
    public let timeoutSeconds: Double

    public static let `default` = QueryPlannerConfiguration(
        complexityThreshold: 1000,
        maxPlanEnumerations: 100,
        maxRuleApplications: 10000,
        timeoutSeconds: 30.0
    )
}

/// プラン複雑度エラー
public struct PlanComplexityExceededError: Error {
    public let complexity: Int
    public let threshold: Int
    public let suggestion: String
}
```

#### 複雑度計算
```
complexity = Σ(operator_cost)
where:
  - SeqScan: 1
  - IndexScan: 1
  - Filter: children_complexity + 1
  - Join: left_complexity * right_complexity
  - Union: Σ(child_complexity)
```

#### 統合ポイント
- `QueryPlanner`: プラン生成時に複雑度をチェック
- `CascadesOptimizer`: 探索中に複雑度を監視

---

## Phase 4: トランザクション管理

### 4.1 TransactionPriority (トランザクション優先度)

#### 目的
トランザクションに優先度を設定し、重要な操作を優先的に処理する。

#### 参照
- FDB Record Layer: `FDBTransactionPriority.java`

#### API設計

```swift
/// トランザクション優先度
public enum TransactionPriority: Int, Sendable {
    /// バッチ処理用（低優先度、高レイテンシ許容）
    case batch = 0
    /// デフォルト
    case `default` = 1
    /// システム即時（高優先度）
    case systemImmediate = 2
}

/// トランザクション設定
public struct TransactionConfiguration: Sendable {
    /// 優先度
    public let priority: TransactionPriority
    /// タイムアウト（ミリ秒）
    public let timeoutMs: Int?
    /// リトライ制限
    public let retryLimit: Int?
    /// トランザクションID（ログ用）
    public let transactionId: String?
}
```

#### 統合ポイント
- `FDBContext`: トランザクション作成時に優先度を設定
- `DatabaseProtocol.withTransaction()`: 設定オプションを追加

---

### 4.2 AsyncCommitHook (非同期コミットフック)

#### 目的
コミット前の検証とコミット後の処理を非同期で実行する。

#### 参照
- FDB Record Layer: `CommitCheckAsync`, `PostCommit`

#### API設計

```swift
/// コミット前チェック
public protocol PreCommitCheck: Sendable {
    /// コミット前に実行される検証
    func validate(transaction: TransactionProtocol) async throws
}

/// コミット後アクション
public protocol PostCommitAction: Sendable {
    /// コミット成功後に実行されるアクション
    func execute(commitVersion: Int64) async throws
}

/// 拡張されたコミットフック
public struct CommitHooks: Sendable {
    /// コミット前チェック
    public var preCommitChecks: [PreCommitCheck]
    /// コミット後アクション
    public var postCommitActions: [PostCommitAction]

    /// コミット前チェックを追加
    public mutating func addPreCommitCheck(_ check: PreCommitCheck)
    /// コミット後アクションを追加
    public mutating func addPostCommitAction(_ action: PostCommitAction)
}
```

#### 統合ポイント
- `FDBContext`: コミット時にフックを実行
- 既存の`CommitHook`を拡張

---

## Phase 5: その他

### 5.1 FormatVersion (ストレージフォーマットバージョン)

#### 目的
ストレージフォーマットのバージョンを管理し、将来の変更に対する後方互換性を確保する。

#### 参照
- FDB Record Layer: `FDBRecordStore.FormatVersion`

#### API設計

```swift
/// フォーマットバージョン
public struct FormatVersion: Comparable, Sendable {
    public let major: Int
    public let minor: Int
    public let patch: Int

    /// 現在のフォーマットバージョン
    public static let current = FormatVersion(major: 1, minor: 0, patch: 0)

    /// 最小サポートバージョン
    public static let minimumSupported = FormatVersion(major: 1, minor: 0, patch: 0)
}

/// フォーマットバージョンマネージャー
public struct FormatVersionManager {
    /// 保存されているバージョンを読み込み
    public func loadVersion(transaction: TransactionProtocol) async throws -> FormatVersion?

    /// バージョンを保存
    public func saveVersion(_ version: FormatVersion, transaction: TransactionProtocol)

    /// バージョン互換性をチェック
    public func checkCompatibility(_ stored: FormatVersion) throws

    /// アップグレードが必要か
    public func needsUpgrade(_ stored: FormatVersion) -> Bool
}
```

#### 統合ポイント
- `FDBContainer`: オープン時にバージョンチェック
- マイグレーションシステムと連携

---

### 5.2 BatchFetcher (バッチ読み込み最適化)

#### 目的
インデックススキャン結果から複数レコードを効率的にバッチ読み込みする。

#### 参照
- FDB Record Layer: Remote Fetch optimization

#### API設計

```swift
/// バッチフェッチ設定
public struct BatchFetchConfiguration: Sendable {
    /// バッチサイズ
    public let batchSize: Int
    /// 並列読み込み数
    public let parallelism: Int
    /// プリフェッチを有効化
    public let prefetchEnabled: Bool
}

/// バッチフェッチャー
public struct BatchFetcher<Item: Persistable> {
    /// 主キーのリストからアイテムをバッチ読み込み
    public func fetch(
        primaryKeys: [Tuple],
        transaction: TransactionProtocol
    ) async throws -> [Item]

    /// インデックススキャン結果からアイテムをバッチ読み込み
    public func fetchFromIndex(
        indexEntries: AsyncStream<(key: FDB.Bytes, value: FDB.Bytes)>,
        transaction: TransactionProtocol
    ) -> AsyncStream<Item>
}
```

#### 統合ポイント
- `FDBContext.fetch()`: インデックス使用時にバッチフェッチを適用
- `QueryPlanner`: フェッチ戦略の選択

---

## 実装順序

### Phase 1 (データ管理基盤) - 最優先
1. `LargeValueSplitter`
2. `TransformingSerializer` (圧縮のみ)
3. `ReadVersionCache`

### Phase 2 (オンラインインデクサー強化)
4. `AdaptiveThrottler`
5. `IndexFromIndexBuilder`

### Phase 3 (クエリ最適化)
6. `InPredicateOptimizer`
7. `PlanComplexityLimit`

### Phase 4 (トランザクション管理)
8. `TransactionPriority`
9. `AsyncCommitHook`

### Phase 5 (その他)
10. `FormatVersion`
11. `BatchFetcher`
12. `TransformingSerializer` (暗号化)

---

## ファイル構成

```
Sources/DatabaseEngine/
├── Serialization/
│   ├── LargeValueSplitter.swift
│   ├── TransformingSerializer.swift
│   └── CompressionProvider.swift
├── Transaction/
│   ├── ReadVersionCache.swift
│   ├── TransactionPriority.swift
│   ├── TransactionConfiguration.swift
│   └── CommitHook.swift (既存を拡張)
├── QueryPlanner/
│   ├── InPredicateOptimizer.swift
│   └── PlanComplexityLimit.swift (QueryPlannerに統合)
├── IndexFromIndexBuilder.swift
├── AdaptiveThrottler.swift
├── FormatVersion.swift
└── BatchFetcher.swift
```

---

## 依存関係

```
LargeValueSplitter
    └── ByteConversion

TransformingSerializer
    ├── Compression (Foundation)
    └── CryptoKit (optional)

ReadVersionCache
    └── Synchronization.Mutex

AdaptiveThrottler
    └── Synchronization.Mutex

IndexFromIndexBuilder
    ├── OnlineIndexer
    └── IndexMaintainer

InPredicateOptimizer
    └── QueryCondition

BatchFetcher
    └── DataAccess
```

---

## Phase 6: 追加機能 (FDB Record Layer 詳細比較による)

### 6.1 InstrumentedTransaction (計装トランザクション)

#### 目的

FDB Record Layer の `InstrumentedTransaction` / `InstrumentedReadTransaction` に相当する機能。
トランザクションレベルで詳細なメトリクスを収集する。

#### 参照
- FDB Record Layer: `InstrumentedTransaction.java`, `InstrumentedReadTransaction.java`

#### API設計

```swift
// Sources/DatabaseEngine/Instrumentation/InstrumentedTransaction.swift

/// Transaction wrapper that collects detailed metrics
///
/// Reference: FDB Record Layer InstrumentedTransaction.java
///
/// **Metrics collected**:
/// - Read/write operation counts
/// - Bytes read/written
/// - Transaction timing
/// - Empty scan detection
///
/// **Delayed events**:
/// Some metrics (writes, deletes) are only recorded on successful commit.
/// This prevents counting operations that were rolled back.
public final class InstrumentedTransaction: Sendable {

    /// The underlying FDB transaction
    nonisolated(unsafe) private let underlying: any TransactionProtocol

    /// Timer for immediate metrics
    private let timer: StoreTimer

    /// Timer for delayed metrics (recorded on commit only)
    private let delayedTimer: StoreTimer

    /// Enable assertion checks for key/value sizes
    private let enableAssertions: Bool

    /// Maximum key length (bytes) before warning
    private static let maxKeyLength: Int = 10_000

    /// Maximum value length (bytes) before warning
    private static let maxValueLength: Int = 100_000

    public init(
        transaction: any TransactionProtocol,
        timer: StoreTimer,
        enableAssertions: Bool = false
    )

    // MARK: - Read Operations

    /// Get a value with instrumentation
    public func getValue(for key: FDB.Bytes) async throws -> FDB.Bytes?

    /// Get a range with instrumentation
    public func getRange(
        begin: FDB.Bytes,
        end: FDB.Bytes,
        limit: Int32 = 0,
        mode: FDB.StreamingMode = .wantAll,
        reverse: Bool = false
    ) async throws -> FDB.KeyValuesResult

    // MARK: - Write Operations (delayed metrics)

    /// Set a value with instrumentation (delayed until commit)
    public func setValue(_ value: FDB.Bytes, for key: FDB.Bytes)

    /// Clear a key with instrumentation (delayed until commit)
    public func clear(key: FDB.Bytes)

    /// Clear a range with instrumentation (delayed until commit)
    public func clearRange(begin: FDB.Bytes, end: FDB.Bytes)

    // MARK: - Commit

    /// Commit with instrumentation
    /// On success: merge delayed metrics into main timer
    /// On failure: discard delayed metrics
    public func commit() async throws -> Bool
}
```

#### 追加の StoreTimerEvent

```swift
// StoreTimer.swift に追加

extension StoreTimerEvent {
    // Read operations
    public static let reads = StoreTimerEvent(name: "reads", isCount: true)
    public static let rangeReads = StoreTimerEvent(name: "range_reads", isCount: true)
    public static let bytesRead = StoreTimerEvent(name: "bytes_read", isSize: true)
    public static let emptyScans = StoreTimerEvent(name: "empty_scans", isCount: true)

    // Write operations (delayed until commit)
    public static let writes = StoreTimerEvent(name: "writes", isCount: true)
    public static let deletes = StoreTimerEvent(name: "deletes", isCount: true)
    public static let rangeDeletes = StoreTimerEvent(name: "range_deletes", isCount: true)
    public static let bytesWritten = StoreTimerEvent(name: "bytes_written", isSize: true)

    // Transaction lifecycle
    public static let commits = StoreTimerEvent(name: "commits", isCount: true)
    public static let rollbacks = StoreTimerEvent(name: "rollbacks", isCount: true)
    public static let cancellations = StoreTimerEvent(name: "cancellations", isCount: true)
}
```

#### 統合ポイント
- `FDBContext`: トランザクション作成時に`InstrumentedTransaction`でラップ
- `StoreTimer`: 新しいイベントタイプを追加

---

### 6.2 IN-Join / IN-Union プランオペレーター

#### 目的

FDB Record Layer の `RecordQueryInJoinPlan` / `RecordQueryInUnionPlan` に相当する機能。
IN 述語を効率的に実行するための特殊なプラン。

#### 参照
- FDB Record Layer: `RecordQueryInJoinPlan.java`, `RecordQueryInUnionPlan.java`, `InExtractor.java`

#### 戦略選択

| 条件 | 戦略 | 理由 |
|------|------|------|
| IN値 < 20 かつ ソート不要 | IN-Join | 低オーバーヘッド |
| IN値 >= 20 または ソート必要 | IN-Union | マージソートで効率的 |

#### API設計

```swift
// Sources/DatabaseEngine/QueryPlanner/PlanOperator.swift に追加

/// Operators that make up a query plan
public indirect enum PlanOperator<T: Persistable>: @unchecked Sendable {
    // ... existing cases ...

    // === IN Predicate Operators ===

    /// IN-Join: Nested loop join for small IN lists
    /// Best when: Small parameter list, no sorting required
    case inJoin(InJoinOperator<T>)

    /// IN-Union: Union of index scans for large IN lists with ordering
    /// Best when: Large parameter list OR sorting required
    case inUnion(InUnionOperator<T>)
}

/// IN-Join operator for small IN predicate lists
///
/// Executes as nested loop: for each value in the IN list,
/// perform an index seek and collect results.
///
/// **When to use**:
/// - Small IN list (< 20 values typically)
/// - No ordering requirement OR ordering matches index
/// - High selectivity (few matches per value)
///
/// **Complexity**: O(|IN list| * seek cost + |results| * fetch cost)
public struct InJoinOperator<T: Persistable>: @unchecked Sendable {
    /// The index to seek in
    public let index: IndexDescriptor

    /// Position of the IN parameter in the index key
    public let parameterPosition: Int

    /// The IN values (parameter binding)
    public let inValues: [AnySendable]

    /// Fixed prefix values (before the IN parameter)
    public let prefixValues: [AnySendable]

    /// Additional filter after index seek
    public let postFilter: Predicate<T>?

    /// Estimated total results
    public let estimatedResults: Int
}

/// IN-Union operator for large IN predicate lists with ordering
///
/// Creates a union of index scans, one per IN value, then merges
/// results maintaining sort order using a priority queue.
///
/// **When to use**:
/// - Large IN list (20+ values)
/// - Ordering requirement that differs from index order
/// - Need to limit results before fetching all
///
/// **Complexity**: O(|results| * log(|IN list|)) for merge
/// **Memory**: O(|IN list|) for priority queue
public struct InUnionOperator<T: Persistable>: @unchecked Sendable {
    /// The index to use for each scan
    public let index: IndexDescriptor

    /// Position of the IN parameter in the index key
    public let parameterPosition: Int

    /// The IN values (parameter binding)
    public let inValues: [AnySendable]

    /// Fixed prefix values (before the IN parameter)
    public let prefixValues: [AnySendable]

    /// Comparison key for merge ordering
    public let comparisonKey: [SortDescriptor<T>]

    /// Whether to scan each branch in reverse
    public let reverse: Bool

    /// Maximum results to return (enables early termination)
    public let limit: Int?

    /// Estimated total results
    public let estimatedResults: Int
}
```

#### PlanExecutor 実装

```swift
// Sources/DatabaseEngine/QueryPlanner/PlanExecutor.swift に追加

extension PlanExecutor {

    /// Execute IN-Join using nested loop strategy
    ///
    /// **Algorithm**:
    /// 1. For each value in the IN list
    /// 2. Build seek key with prefix + IN value
    /// 3. Execute index seek
    /// 4. Collect IDs (deduplicated)
    /// 5. Batch fetch all records
    private func executeInJoin(_ op: InJoinOperator<T>) async throws -> [T] {
        var allIds: Set<Tuple> = []

        // Nested loop: seek for each IN value
        for inValue in op.inValues {
            // Build seek key: prefix + inValue
            var seekElements: [any TupleElement] = []
            for prefixValue in op.prefixValues {
                if let element = anyToTupleElement(prefixValue.value) {
                    seekElements.append(element)
                }
            }
            if let element = anyToTupleElement(inValue.value) {
                seekElements.append(element)
            }

            // Execute seek
            let query = ScalarIndexQuery.equals(seekElements)
            let entries = try await searcher.search(query: query, ...)

            // Collect IDs
            for entry in entries {
                allIds.insert(entry.itemID)
            }
        }

        // Batch fetch all unique records
        var results = try await batchFetchItems(ids: Array(allIds), type: T.self)

        // Apply post-filter if present
        if let postFilter = op.postFilter {
            results = results.filter { evaluatePredicate(postFilter, on: $0) }
        }

        return results
    }

    /// Execute IN-Union using merge-sort strategy
    ///
    /// **Algorithm**:
    /// 1. For each IN value, scan index and collect entries with sort keys
    /// 2. Sort all entries by comparison key
    /// 3. Deduplicate by ID while maintaining order
    /// 4. Apply limit if present
    /// 5. Batch fetch records
    private func executeInUnion(_ op: InUnionOperator<T>) async throws -> [T] {
        var allEntries: [(id: Tuple, sortKey: [Any])] = []

        for inValue in op.inValues {
            // Build seek key and scan
            let entries = try await scanForInValue(inValue, op: op)
            allEntries.append(contentsOf: entries)
        }

        // Sort by comparison key
        let sortedEntries = sortEntriesByComparisonKey(allEntries, comparisonKey: op.comparisonKey)

        // Deduplicate while maintaining order
        var seenIds: Set<Tuple> = []
        var uniqueIds: [Tuple] = []
        for entry in sortedEntries {
            if !seenIds.contains(entry.id) {
                seenIds.insert(entry.id)
                uniqueIds.append(entry.id)
                if let limit = op.limit, uniqueIds.count >= limit { break }
            }
        }

        // Batch fetch records in order
        return try await batchFetchItems(ids: uniqueIds, type: T.self)
    }
}
```

#### InPredicateOptimizer 拡張

```swift
// Sources/DatabaseEngine/QueryPlanner/InPredicateOptimizer.swift

extension InPredicateOptimizer {

    /// Threshold for choosing IN-Join vs IN-Union
    private static let inJoinThreshold = 20

    /// Create optimal plan for IN predicate
    public func createInPredicatePlan<T: Persistable>(
        index: IndexDescriptor,
        parameterPosition: Int,
        inValues: [AnySendable],
        prefixValues: [AnySendable],
        orderBy: [SortDescriptor<T>]?,
        limit: Int?,
        estimatedSelectivity: Double,
        totalRecords: Int
    ) -> PlanOperator<T> {
        let estimatedResults = Int(Double(totalRecords) * estimatedSelectivity * Double(inValues.count))

        // Choose strategy
        let useUnion = inValues.count > Self.inJoinThreshold || (orderBy != nil && !orderBy!.isEmpty)

        if useUnion {
            return .inUnion(InUnionOperator(
                index: index,
                parameterPosition: parameterPosition,
                inValues: inValues,
                prefixValues: prefixValues,
                comparisonKey: orderBy ?? [],
                reverse: false,
                limit: limit,
                estimatedResults: estimatedResults
            ))
        } else {
            return .inJoin(InJoinOperator(
                index: index,
                parameterPosition: parameterPosition,
                inValues: inValues,
                prefixValues: prefixValues,
                postFilter: nil,
                estimatedResults: estimatedResults
            ))
        }
    }
}
```

#### 統合ポイント
- `PlanOperator`: 新しい case を追加
- `PlanExecutor`: `executeInJoin`, `executeInUnion` を実装
- `InPredicateOptimizer`: 戦略選択ロジックを拡張
- `PlanEnumerator`: IN プランの列挙を追加

---

### 6.3 Aggregation 実行の完成

#### 目的

現在の `PlanExecutor.executeAggregation` は未実装（エラーをスロー）。
既存の Aggregation Index (Count, Sum, MinMax, Average) を活用した実行を実装。

#### API設計

```swift
// Sources/DatabaseEngine/QueryPlanner/AggregationResult.swift

/// Aggregation result container
public struct AggregationResult: Sendable {
    public let aggregationType: AggregationType
    public let value: Double
    public let count: Int
    public let groupKey: [String: AnySendable]?
}

// PlanExecutor.swift の executeAggregation を実装

extension PlanExecutor {

    /// Execute aggregation using pre-computed aggregation indexes
    ///
    /// **Implementation Strategy**:
    /// - COUNT: Read from CountIndex
    /// - SUM: Read from SumIndex
    /// - MIN/MAX: Read from MinMaxIndex
    /// - AVG: Compute from SumIndex / CountIndex
    ///
    /// If no aggregation index exists, falls back to full scan.
    public func executeAggregationQuery(_ op: AggregationOperator<T>) async throws -> AggregationResult {
        switch op.aggregationType {
        case .count:
            // Read from count index or fallback
            ...
        case .sum(let field):
            // Read from sum index or fallback
            ...
        case .min(let field):
            // Read from min/max index or fallback
            ...
        case .max(let field):
            // Read from min/max index or fallback
            ...
        case .avg(let field):
            // AVG = SUM / COUNT
            ...
        }
    }
}
```

#### 統合ポイント
- `PlanExecutor`: 新しい `executeAggregationQuery` メソッド
- 既存の `CountIndexMaintainer`, `SumIndexMaintainer`, `MinMaxIndexMaintainer` と連携

---

## 更新された実装順序

### Phase 1 (データ管理基盤) - 完了済み ✅
1. ✅ `LargeValueSplitter`
2. ✅ `TransformingSerializer`
3. ✅ `ReadVersionCache`

### Phase 2 (オンラインインデクサー強化) - 完了済み ✅
4. ✅ `AdaptiveThrottler`
5. ✅ `IndexFromIndexBuilder`
6. ✅ `MultiTargetOnlineIndexer`
7. ✅ `MutualOnlineIndexer`

### Phase 3 (クエリ最適化) - 部分完了
8. ✅ `InPredicateOptimizer` (基本)
9. ✅ `PlanComplexityLimit`
10. 🔄 `IN-Join/IN-Union Plans` ← **新規追加**

### Phase 4 (トランザクション管理) - 完了済み ✅
11. ✅ `TransactionPriority`
12. ✅ `AsyncCommitHook`

### Phase 5 (その他) - 完了済み ✅
13. ✅ `FormatVersion`
14. ✅ `BatchFetcher`

### Phase 6 (追加機能) - **新規**
15. 🆕 `InstrumentedTransaction` - **優先度: 高**
16. 🆕 `IN-Join/IN-Union PlanExecutor実装` - **優先度: 高**
17. 🆕 `AggregationResult + executeAggregationQuery` - **優先度: 中**

---

## ファイル構成 (更新)

```
Sources/DatabaseEngine/
├── Instrumentation/
│   ├── StoreTimer.swift              # ✅ 既存
│   ├── TransactionListener.swift     # ✅ 既存
│   └── InstrumentedTransaction.swift # 🆕 新規
├── QueryPlanner/
│   ├── PlanOperator.swift            # 🔄 IN-Join/IN-Union 追加
│   ├── PlanExecutor.swift            # 🔄 executeInJoin/executeInUnion 追加
│   ├── InPredicateOptimizer.swift    # 🔄 戦略選択拡張
│   └── AggregationResult.swift       # 🆕 新規
└── ...
```
