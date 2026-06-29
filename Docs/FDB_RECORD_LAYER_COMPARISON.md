# FDB Record Layer vs database-framework 詳細比較レポート

このドキュメントは FDB Record Layer と database-framework の機能を詳細に比較し、不足している機能を特定します。

## サマリー

| カテゴリ | FDB Record Layer | database-framework | 状態 |
|---------|------------------|-------------------|------|
| トランザクション管理 | ✅ 完全 | ✅ 完全 | **同等** |
| エラーハンドリング | ✅ 完全 | ⚠️ 部分的 | **要実装** |
| スキーマ進化 | ✅ 完全 | ⚠️ 部分的 | **要実装** |
| JOIN/Relationship | ✅ SyntheticRecordType | ✅ RelationshipQueryExecutor | **同等** (アプローチ異なる) |
| カーソルシステム | ✅ 高度 | ⚠️ 基本的 | **要強化** |
| クエリ最適化 | ✅ Cascades | ✅ Cascades | **同等** |
| インデックス種類 | 6種類 | 13種類 | **優位** |
| オンラインインデクシング | ✅ 完全 | ✅ 完全 | **同等** |
| シリアライゼーション | ✅ 完全 | ✅ 完全 | **同等** |

---

## 1. トランザクション管理 ✅ 完全実装済み

### FDB Record Layer の機能

| 機能 | 説明 |
|------|------|
| FDBRecordContext | トランザクションラッパー |
| CommitCheckAsync | コミット前非同期バリデーション |
| PostCommit | コミット後フック |
| TransactionListener | ライフサイクルイベント |
| FDBStoreTimer | 計装・メトリクス |
| WeakReadSemantics | 弱い読み取り一貫性 |
| Transaction ID | ログ相関ID |

### database-framework の実装状況

| 機能 | ファイル | 状態 |
|------|---------|------|
| TransactionContext | `Transaction/TransactionContext.swift` | ✅ |
| CommitCheck | `Transaction/CommitCheck.swift` | ✅ |
| PostCommit | `Transaction/PostCommit.swift` | ✅ |
| TransactionListener | `Transaction/TransactionListener.swift` | ✅ |
| StoreTimer | `Instrumentation/StoreTimer.swift` | ✅ |
| CachePolicy | `Transaction/CachePolicy.swift` | ✅ |
| TransactionConfiguration | `Transaction/TransactionConfiguration.swift` | ✅ |

**結論**: トランザクション管理は同等レベルで実装済み。CachePolicyはWeakReadSemanticsの簡素化された代替実装。

---

## 2. エラーハンドリング ⚠️ 要実装

### FDB Record Layer の機能

```java
// FDBError - 包括的エラーコード列挙
public enum FDBError {
    TIMED_OUT(1004),
    TRANSACTION_TOO_OLD(1007),
    NOT_COMMITTED(1020),
    TRANSACTION_TOO_LARGE(2101),
    // ... 多数のエラーコード
}

// リトライ分類
public class RecordCoreRetriableTransactionException extends RecordCoreException {
    // リトライ可能なエラーを示す
}

// コンフリクトキーのレポート
context.setReportConflictingKeys(true);
// トランザクションコンフリクト時に競合キーを取得可能
```

### database-framework の現状

現在、エラーは複数のモジュールに分散:
- `FDBContextError`
- `FDBLimitError`
- `ItemEnvelopeError`
- `TransformError`
- `FormatVersionError`
- 各インデックス固有のエラー

### 不足機能

#### 2.1 統一エラーコードシステム 🔴 高優先度

```swift
// 提案: DatabaseErrorCode.swift
public enum DatabaseErrorCode: Int, Sendable {
    // FoundationDB エラー
    case timedOut = 1004
    case transactionTooOld = 1007
    case notCommitted = 1020
    case transactionTooLarge = 2101
    case futureVersion = 1009

    // Record Layer エラー
    case uniquenessViolation = 10001
    case indexStateError = 10002
    case serializationError = 10003
    case schemaValidationError = 10004

    var isRetriable: Bool {
        switch self {
        case .timedOut, .transactionTooOld, .notCommitted:
            return true
        default:
            return false
        }
    }
}
```

#### 2.2 RetryableError プロトコル 🟡 中優先度

```swift
// 提案
public protocol RetryableError: Error {
    var isRetriable: Bool { get }
    var errorCode: DatabaseErrorCode { get }
    var shouldLessenWork: Bool { get }  // バッチサイズを減らすべきか
}
```

#### 2.3 コンフリクトキーレポート 🟡 中優先度

```swift
// 提案
public struct ConflictInfo: Sendable {
    public let conflictingKeys: [FDB.Bytes]
    public let readConflictRanges: [(begin: FDB.Bytes, end: FDB.Bytes)]
    public let writeConflictRanges: [(begin: FDB.Bytes, end: FDB.Bytes)]
}

extension TransactionConfiguration {
    public var reportConflictingKeys: Bool
}
```

---

## 3. スキーマ進化 ⚠️ 要実装

### FDB Record Layer の機能

```java
// MetaDataEvolutionValidator - スキーマ変更の検証
public class MetaDataEvolutionValidator {
    // 後方互換性のルールを強制
    // - レコードタイプやフィールドの削除禁止
    // - フィールド型変更の制限
    // - インデックス変更の検証
}

// FormatVersion - ストレージフォーマットバージョン
public enum FormatVersion {
    SAVE_UNSPLIT_WITH_SUFFIX_FORMAT_VERSION,
    SAVE_VERSION_WITH_RECORD_FORMAT_VERSION,
    HEADER_USER_FIELDS_FORMAT_VERSION,
    // ...
}

// RecordMetaData - スキーマメタデータ管理
public class RecordMetaData {
    private int version;
    private Map<String, RecordType> recordTypes;
    private Map<String, Index> indexes;
}
```

### database-framework の現状

| コンポーネント | 状態 |
|--------------|------|
| FormatVersion | ✅ 実装済み |
| FormatVersionManager | ✅ 実装済み |
| DBContainer統合 | ❌ 未実装 |
| MetaDataEvolutionValidator | ❌ 未実装 |

### 不足機能

#### 3.1 DBContainer フォーマットバージョン統合 🔴 高優先度

```swift
// 提案: ストアオープン時のバージョンチェック
extension DBContainer {
    public func open<T: Persistable>(
        for type: T.Type,
        allowUpgrade: Bool = true
    ) async throws -> FDBDataStore {
        // 1. 現在のフォーマットバージョンを読み取り
        let storedVersion = try await readFormatVersion(for: type)

        // 2. 互換性チェック
        guard storedVersion.isCompatible(with: FormatVersion.current) else {
            throw FormatVersionError.incompatible(
                stored: storedVersion,
                required: FormatVersion.current
            )
        }

        // 3. 必要に応じてアップグレード
        if allowUpgrade && storedVersion < FormatVersion.current {
            try await upgradeFormatVersion(for: type, from: storedVersion)
        }

        return store
    }
}
```

#### 3.2 MetaDataEvolutionValidator 🟡 中優先度

```swift
// 提案: スキーマ変更の検証
public struct MetaDataEvolutionValidator {
    public struct ValidationResult {
        public let isValid: Bool
        public let violations: [Violation]
        public let warnings: [Warning]
    }

    public enum Violation {
        case recordTypeRemoved(name: String)
        case fieldRemoved(recordType: String, field: String)
        case fieldTypeChanged(recordType: String, field: String, from: String, to: String)
        case indexIncompatibleChange(index: String, reason: String)
    }

    public func validate(
        oldMetadata: SchemaMetadata,
        newMetadata: SchemaMetadata
    ) -> ValidationResult
}
```

---

## 4. JOIN / Relationship 機能 ✅ 実装済み（アプローチが異なる）

### FDB Record Layer の機能

```java
// SyntheticRecordType - 合成レコードの基底クラス
// データベースに直接保存されず、他のレコードから構成される

// JoinedRecordType - 複数レコードのJOIN
JoinedRecordTypeBuilder joined = rmd.addJoinedRecordType("CustomerOrder");
joined.addConstituent("Customer", customerType);
joined.addConstituent("Order", orderType);
joined.addJoin("Customer", "id", "Order", "customerId");

// JOINしたレコードにインデックスを作成可能
Index joinIndex = new Index("customer_order_by_date",
    concat(field("Customer").nest("name"), field("Order").nest("orderDate")));
```

### database-framework の実装

database-framework はクエリ時JOIN方式を採用:

#### 4.1 RelationshipQueryExecutor - クエリ時JOIN

```swift
// To-one JOIN (Order -> Customer)
let orders = try await context.fetch(Order.self)
    .joining(\.customerID, as: Customer.self)
    .execute()

for order in orders {
    let customer = order.ref(Customer.self, \.customerID)
    print(customer?.name)
}

// To-many JOIN (Customer -> Orders)
let customers = try await context.fetch(Customer.self)
    .joining(\.orderIDs, as: Order.self)
    .execute()

for customer in customers {
    let orders = customer.refs(Order.self, \.orderIDs)
}
```

**ファイル**: `Sources/RelationshipIndex/QueryExecutor+Relationship.swift`

#### 4.2 FDBContext.get() with JOIN

```swift
// Single item with to-one relationship
let snapshot = try await context.get(
    Order.self, id: "O001",
    joining: \.customerID, as: Customer.self
)
let customer = snapshot?.ref(Customer.self, \.customerID)

// Single item with to-many relationship
let snapshot = try await context.get(
    Customer.self, id: "C001",
    joining: \.orderIDs, as: Order.self
)
let orders = snapshot?.refs(Order.self, \.orderIDs)
```

**ファイル**: `Sources/RelationshipIndex/RelationshipQuery.swift`

#### 4.3 FDBContext.related() - 遅延ロード

```swift
let order = try await context.model(for: "O001", as: Order.self)!
let customer = try await context.related(order, \.customerID, as: Customer.self)
```

#### 4.4 IN-Join / IN-Union - クエリプランレベル

```swift
// IN predicate は自動的に最適な戦略を選択:
// - IN-Union: 小さいリスト (< 15値) → 並列インデックスシーク
// - IN-Join: 大きいリスト (15-1000値) → ハッシュセット付きスキャン
// - Bounded Range Scan: 値が集中している場合

// Query planner が自動選択
let users = try await context.fetch(User.self)
    .where(\.status, in: ["active", "pending", "verified"])
    .execute()
```

**ファイル**: `Sources/DatabaseEngine/QueryPlanner/InJoinExecutor.swift`, `PlanOperator.swift`

#### 4.5 Delete with Relationship Rules

```swift
// Delete rules: cascade, deny, nullify, noAction
try await context.deleteEnforcingRelationshipRules(customer)
```

### アプローチの違い

| 観点 | FDB Record Layer | database-framework |
|------|------------------|-------------------|
| JOIN方式 | 事前定義 (SyntheticRecordType) | クエリ時 (RelationshipQueryExecutor) |
| JOINインデックス | ✅ 可能 | ❌ 不可 |
| 柔軟性 | 事前定義が必要 | 任意のFKフィールドでJOIN可能 |
| バッチ最適化 | ✅ | ✅ (FK値を収集してバッチロード) |

### 未実装機能

| 機能 | 状態 | 優先度 |
|------|------|--------|
| SyntheticRecordType (事前定義JOIN) | ❌ | 🟢 低 |
| JOINフィールドへのインデックス | ❌ | 🟡 中 |
| UnnestedRecordType (配列フラット化) | ❌ | 🟢 低 |

---

## 5. カーソルシステム ⚠️ 要強化

### FDB Record Layer の機能

```java
// RecordCursor - 非同期イテレーター
public interface RecordCursor<T> {
    CompletableFuture<RecordCursorResult<T>> onNext();

    // パイプライン化された操作
    <V> RecordCursor<V> mapPipelined(
        Function<T, CompletableFuture<V>> func,
        int pipelineSize
    );

    <V> RecordCursor<V> flatMapPipelined(
        Function<T, RecordCursor<V>> func,
        int pipelineSize
    );
}

// RecordCursorResult - 結果とメタデータ
public class RecordCursorResult<T> {
    T value;
    RecordCursorContinuation continuation;
    NoNextReason noNextReason;
}

// NoNextReason - 終了理由
public enum NoNextReason {
    SOURCE_EXHAUSTED,      // データ終了
    RETURN_LIMIT_REACHED,  // 件数制限
    TIME_LIMIT_REACHED,    // 時間制限
    SCAN_LIMIT_REACHED,    // スキャン制限
    BYTE_LIMIT_REACHED     // バイト制限
}

// Pipeline Sizing - 並列処理数の制御
interface PipelineSizer {
    int getPipelineSize(PipelineOperation operation);
}
```

### database-framework の現状

| 機能 | 状態 |
|------|------|
| QueryCursor | ✅ 基本実装 |
| ContinuationToken | ✅ 基本実装 |
| Async iteration | ✅ AsyncSequence |
| Pipeline sizing | ❌ 未実装 |
| NoNextReason | ❌ 未実装 |
| Time/Scan/Byte limits | ❌ 未実装 |

### 不足機能

#### 5.1 リッチなカーソル結果 🟡 中優先度

```swift
// 提案: CursorStopReason
public enum CursorStopReason: Sendable {
    case sourceExhausted
    case returnLimitReached(count: Int)
    case timeLimitReached(elapsed: TimeInterval)
    case scanLimitReached(scanned: Int)
    case byteLimitReached(bytes: Int)

    var isInBand: Bool {
        switch self {
        case .sourceExhausted, .returnLimitReached:
            return true
        default:
            return false
        }
    }
}

// 強化されたカーソル結果
public struct EnhancedCursorResult<T: Sendable>: Sendable {
    public let value: T?
    public let continuation: ContinuationToken?
    public let stopReason: CursorStopReason?
    public let scannedCount: Int
    public let bytesRead: Int
}
```

#### 5.2 パイプライン処理 🟡 中優先度

```swift
// 提案: パイプライン化されたカーソル操作
extension QueryCursor {
    /// パイプライン化されたmap（先行してpipelineSize個のfutureを開始）
    public func mapPipelined<V: Sendable>(
        pipelineSize: Int = 10,
        transform: @escaping @Sendable (T) async throws -> V
    ) -> PipelinedCursor<T, V>

    /// パイプライン化されたflatMap
    public func flatMapPipelined<V: Sendable>(
        pipelineSize: Int = 10,
        transform: @escaping @Sendable (T) async throws -> QueryCursor<V>
    ) -> FlatMapPipelinedCursor<T, V>
}

// PipelineSizer プロトコル
public protocol PipelineSizer: Sendable {
    func pipelineSize(for operation: PipelineOperation) -> Int
}

public enum PipelineOperation {
    case indexScan
    case recordFetch
    case indexMaintenance
    case onlineIndexBuild
}
```

#### 5.3 スキャン/時間/バイト制限 🟡 中優先度

```swift
// 提案: クエリ制限の設定
public struct QueryLimits: Sendable {
    /// 返却するレコード数の上限
    public var returnLimit: Int?

    /// スキャンするレコード数の上限
    public var scanLimit: Int?

    /// 読み取りバイト数の上限
    public var byteLimit: Int?

    /// 実行時間の上限
    public var timeLimit: TimeInterval?

    public static let `default` = QueryLimits()

    public static let batch = QueryLimits(
        scanLimit: 10000,
        byteLimit: 1_000_000,
        timeLimit: 5.0
    )
}

// 使用例
let results = try await context.fetch(User.self)
    .where(\.age > 18)
    .limits(QueryLimits(returnLimit: 100, scanLimit: 1000))
    .execute()
```

---

## 6. クエリシステム ✅ 同等

### 比較

| 機能 | FDB Record Layer | database-framework |
|------|------------------|-------------------|
| Cascades Optimizer | ✅ | ✅ |
| Cost-based planning | ✅ | ✅ |
| Statistics/Histograms | ✅ | ✅ (HyperLogLog含む) |
| Plan caching | ✅ | ✅ (PreparedPlan) |
| IN-JOIN execution | ✅ | ✅ |

**結論**: クエリ最適化は同等レベル。

---

## 7. インデックス種類 ✅ 優位

### 比較

| インデックスタイプ | FDB Record Layer | database-framework |
|------------------|------------------|-------------------|
| Value (Scalar) | ✅ | ✅ ScalarIndex |
| Rank | ✅ | ✅ RankIndex |
| Count | ✅ | ✅ AggregationIndex |
| Sum | ✅ | ✅ AggregationIndex |
| Spatial | ✅ | ✅ SpatialIndex |
| Lucene (Full-text) | ✅ | ✅ FullTextIndex |
| **Vector** | ❌ | ✅ VectorIndex (HNSW, Flat) |
| **Graph** | ❌ | ✅ GraphIndex (adjacency, tripleStore, hexastore) |
| **Permuted** | ❌ | ✅ PermutedIndex |
| **Bitmap** | ❌ | ✅ BitmapIndex |
| **Leaderboard** | ❌ | ✅ LeaderboardIndex |
| **Version** | ⚠️ 基本 | ✅ VersionIndex |

**結論**: database-framework は 7種類の追加インデックスを持ち、優位。

---

## 8. オンラインインデクシング ✅ 同等

### 比較

| 機能 | FDB Record Layer | database-framework |
|------|------------------|-------------------|
| OnlineIndexer | ✅ | ✅ |
| Multi-target | ✅ IndexingMultiTargetByRecords | ✅ MultiTargetOnlineIndexer |
| Index-from-Index | ✅ IndexingByIndex | ✅ IndexFromIndexBuilder |
| Mutual indexing | ✅ IndexingMutuallyByRecords | ✅ MutualOnlineIndexer |
| Scrubber | ✅ | ✅ OnlineIndexScrubber |
| Throttling | ✅ IndexingThrottle | ✅ AdaptiveThrottler |
| RangeSet progress | ✅ | ✅ |

**結論**: オンラインインデクシングは同等レベル。

---

## 9. シリアライゼーション ✅ 同等

### 比較

| 機能 | FDB Record Layer | database-framework |
|------|------------------|-------------------|
| 基本シリアライゼーション | Protobuf | JSON (Codable) |
| 圧縮 | Deflate | LZ4, zlib, LZMA, LZFSE |
| 暗号化 | AES-CBC | AES-256-GCM |
| 大規模レコード分割 | ✅ SplitHelper | ✅ LargeValueSplitter |
| データ検証 | ✅ | ✅ ItemEnvelope (magic number) |
| 変換パイプライン | TransformedRecordSerializer | TransformingSerializer |

**結論**: シリアライゼーションは同等レベル。圧縮オプションは database-framework が豊富。

---

## 10. その他の機能

### database-framework のみの機能

| 機能 | 説明 |
|------|------|
| **Polymorphable** | Union Record Type のSwift実装 |
| **Dynamic Directories** | フィールド値に基づくパーティショニング |
| **Fusion Query** | 複数インデックスの結果統合 |
| **@Persistable マクロ** | 宣言的モデル定義 |
| **Fluent API** | `context.fetch(User.self).where(...)` |

### FDB Record Layer のみの機能

| 機能 | 説明 | 実装優先度 |
|------|------|----------|
| **JOINフィールドへのインデックス** | JOINしたフィールド値にインデックス作成 | 🟡 中 |
| **SyntheticRecordType** | 事前定義の合成レコードタイプ | 🟢 低 |
| **UnnestedRecordType** | 配列フラット化 | 🟢 低 |
| **SQL Interface** | ANTLR パーサーによるSQL | 🟢 低 |

**注**: JOINクエリ自体は `RelationshipQueryExecutor.joining()` で実装済み。FDB Record Layerとの違いは、JOINフィールドにインデックスを作成できるかどうか。

---

## 実装優先度サマリー

### 🔴 高優先度（プロダクション必須）

| 機能 | 工数見積 |
|------|---------|
| 統一エラーコードシステム | 3-4日 |
| DBContainer フォーマットバージョン統合 | 2日 |
| RetryableError プロトコル | 1日 |

### 🟡 中優先度（運用品質向上）

| 機能 | 工数見積 |
|------|---------|
| コンフリクトキーレポート | 1日 |
| カーソルStopReason | 1日 |
| パイプライン処理 | 2-3日 |
| クエリ制限 (scan/time/byte limits) | 2日 |
| MetaDataEvolutionValidator | 3日 |
| Delayed Events | 1日 |

### 🟢 低優先度（機能拡張）

| 機能 | 工数見積 |
|------|---------|
| JOINフィールドへのインデックス | 5-7日 |
| SyntheticRecordType (事前定義JOIN) | 5-7日 |
| UnnestedRecordType | 3日 |
| Database-level metrics aggregation | 1日 |
| Serialization round-trip validation | 1日 |

---

## 結論

**database-framework は FDB Record Layer と比較して、トランザクション管理、クエリ最適化、オンラインインデクシング、シリアライゼーションにおいて同等以上の機能を持つ。**

**主な優位点**:
1. より多様なインデックスタイプ（特にVector, Graph）
2. Swift ネイティブの宣言的API
3. より多くの圧縮オプション
4. Dynamic Directories によるパーティショニング

**主な不足点**:
1. エラーハンドリングの統一性
2. スキーマ進化の検証
3. Synthetic Records (JOIN インデックス)
4. カーソルのパイプライン処理と制限機能

**推奨アクション**:
高優先度の機能（エラーコード統一、フォーマットバージョン統合）を先に実装し、プロダクション品質を確保する。
