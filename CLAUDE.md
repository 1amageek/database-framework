# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build and Test Commands

```bash
# Build the project
swift build

# Run all tests (requires local FoundationDB running)
swift test

# Run a specific test file
swift test --filter DatabaseEngineTests.ScalarIndexKindTests

# Run a specific test function
swift test --filter "DatabaseEngineTests.ScalarIndexKindTests/testScalarIndexKindIdentifier"

# Build with release optimization
swift build -c release
```

**Prerequisites**: FoundationDB must be installed and running locally for tests. The linker expects `libfdb_c` at `/usr/local/lib`.

## Architecture Overview

This is the **server-side execution layer** for FoundationDB persistence. It implements index maintenance logic for index types defined in the sibling `database-kit` package.

### Two-Package Design

```
database-kit (client-safe)          database-framework (server-only)
├── Core/                           ├── DatabaseEngine/
│   ├── Persistable (protocol)      │   ├── FDBContainer
│   ├── IndexKind (protocol)        │   ├── FDBContext
│   └── IndexDescriptor             │   ├── IndexMaintainer (protocol)
│                                   │   └── IndexKindMaintainable (protocol)
├── Vector/, FullText/, etc.        ├── VectorIndex/, FullTextIndex/, etc.
│   └── VectorIndexKind             │   └── VectorIndexKind+Maintainable
```

- **database-kit**: Platform-independent model definitions and index type specifications (works on iOS clients)
- **database-framework**: FoundationDB-dependent index execution (server-only, requires libfdb_c)

### Core Protocol Bridge

`IndexKindMaintainable` bridges metadata (IndexKind) with runtime (IndexMaintainer):

```swift
// In database-kit: Defines WHAT to index
public struct ScalarIndexKind: IndexKind { ... }

// In database-framework: Defines HOW to maintain index
extension ScalarIndexKind: IndexKindMaintainable {
    func makeIndexMaintainer<Item>(...) -> any IndexMaintainer<Item>
}
```

### Module Dependency Graph

```
Database (re-export all)
    ↓
┌───┴───┬───────┬───────┬───────┬───────┬───────┬───────┬───────┐
Scalar  Vector  FullText Spatial Rank   Permuted Graph  Aggregation Version
    ↓      ↓       ↓        ↓      ↓       ↓       ↓        ↓        ↓
    └──────┴───────┴────────┴──────┴───────┴───────┴────────┴────────┘
                                   ↓
                            DatabaseEngine
                                   ↓
                    ┌──────────────┼──────────────┐
                Core (database-kit)          FoundationDB (fdb-swift-bindings)
```

### Key Types

| Type | Role |
|------|------|
| `FDBContainer` | SwiftData-like container managing schema, migrations, and contexts |
| `FDBContext` | Change tracking and batch operations (insert/delete/fetch/save) |
| `DataStore` | Storage backend protocol (default: `FDBDataStore`) |
| `IndexMaintainer<Item>` | Protocol for index update logic (`updateIndex`, `scanItem`) |
| `IndexKindMaintainable` | Bridge protocol connecting IndexKind to IndexMaintainer |
| `OnlineIndexer` | Background index building for schema migrations |
| `MultiTargetOnlineIndexer` | Build multiple indexes in single data scan |
| `MutualOnlineIndexer` | Build bidirectional indexes (e.g., followers/following) |
| `OnlineIndexScrubber` | Detect and repair index inconsistencies |
| `IndexFromIndexBuilder` | Build new index from existing index |
| `CascadesOptimizer` | Cascades framework query optimizer |
| `TransactionConfiguration` | Transaction priority, timeout, retry settings |
| `LargeValueSplitter` | Handle values exceeding FDB's 100KB limit |
| `TransformingSerializer` | Compression (LZ4/zlib/LZMA/LZFSE) and encryption (AES-256-GCM) |
| `Polymorphable` | Protocol enabling union types with shared directory and polymorphic queries |
| `IndexStateManager` | Manages index lifecycle states (disabled/writeOnly/readable) |

### Data Layout in FoundationDB

```
[fdb]/R/[PersistableType]/[id]           → Protobuf-encoded item
[fdb]/I/[indexName]/[values...]/[id]     → Index entry (empty value for scalar)
[fdb]/_metadata/schema/version           → Tuple(major, minor, patch)
[fdb]/_metadata/index/[indexName]/state  → IndexState (readable/write_only/disabled)
```

Subspace keys are single characters for storage efficiency. Use `SubspaceKey.items`, `SubspaceKey.indexes`, etc. for semantic clarity in code.

## Implemented Features

### Index Types (13 types)

| Index | Module | Description |
|-------|--------|-------------|
| Scalar | `ScalarIndex` | Equality/range queries on single/compound fields |
| Vector | `VectorIndex` | Semantic search (Flat brute-force, HNSW approximate) |
| FullText | `FullTextIndex` | Text search with stemming, fuzzy matching, highlighting |
| Spatial | `SpatialIndex` | Geographic queries (Geohash, Morton Code, S2 cells) |
| Rank | `RankIndex` | Leaderboard-style ranking with position queries |
| Permuted | `PermutedIndex` | Permutation-based multi-field queries |
| Graph | `GraphIndex` | Graph traversal with adjacency index |
| Triple | `TripleIndex` | RDF/semantic triple storage (subject-predicate-object) |
| Aggregation | `AggregationIndex` | Materialized aggregations (Count, Sum, Min/Max, Average) |
| Version | `VersionIndex` | Temporal versioning with FDB versionstamps |
| Bitmap | `BitmapIndex` | Set membership queries using Roaring Bitmaps |
| Leaderboard | `LeaderboardIndex` | Time-windowed leaderboards |

### Online Indexing

| Component | File | Description |
|-----------|------|-------------|
| `OnlineIndexer` | `OnlineIndexer.swift` | Batch processing with resumable progress via RangeSet |
| `MultiTargetOnlineIndexer` | `MultiTargetOnlineIndexer.swift` | Build multiple indexes in single scan |
| `MutualOnlineIndexer` | `MutualOnlineIndexer.swift` | Build bidirectional indexes simultaneously |
| `OnlineIndexScrubber` | `OnlineIndexScrubber.swift` | Two-phase consistency verification and repair |
| `IndexFromIndexBuilder` | `IndexFromIndexBuilder.swift` | Build index from existing index (reduces I/O) |
| `AdaptiveThrottler` | `AdaptiveThrottler.swift` | Backpressure-aware rate limiting |

### Query Planning (Cascades Framework)

| Component | File | Description |
|-----------|------|-------------|
| `CascadesOptimizer` | `Cascades/CascadesOptimizer.swift` | Top-down rule-based optimizer (Graefe 1995) |
| `Memo` | `Cascades/Memo.swift` | Memoization structure for equivalence classes |
| `Rule` | `Cascades/Rule.swift` | Transformation and implementation rules |
| `Expression` | `Cascades/Expression.swift` | Logical and physical operators |
| `CostEstimator` | `CostEstimator.swift` | Statistics-driven cost model |
| `StatisticsProvider` | `StatisticsProvider.swift` | Column cardinality, histograms, HyperLogLog |

### Serialization & Storage

| Component | File | Description |
|-----------|------|-------------|
| `LargeValueSplitter` | `Serialization/LargeValueSplitter.swift` | Handle values > 100KB (FDB limit) |
| `TransformingSerializer` | `Serialization/TransformingSerializer.swift` | Compression (LZ4/zlib/LZMA/LZFSE) + encryption (AES-256-GCM) |
| `RecordEncryption` | `Serialization/RecordEncryption.swift` | Key providers (Static, Rotating, Derived, Environment) |
| `StoragePipeline` | `Serialization/StoragePipeline.swift` | Composable transformation chain |

### Transaction Management

| Component | File | Description |
|-----------|------|-------------|
| `TransactionConfiguration` | `Transaction/TransactionConfiguration.swift` | Priority (default/batch/system), timeout, retry |
| `TransactionRunner` | `Transaction/TransactionRunner.swift` | Retry logic with exponential backoff |
| `CommitHook` | `Transaction/CommitHook.swift` | Synchronous callbacks before commit |
| `AsyncCommitHook` | `Transaction/AsyncCommitHook.swift` | Asynchronous callbacks before commit |

### Polymorphable (Union Record Type)

Polymorphable enables multiple `Persistable` types to share a directory and indexes, allowing polymorphic queries across different concrete types (similar to FDB Record Layer's Union Record Type).

**Core Concept**:
```swift
// In database-kit: Define a polymorphic protocol
@Polymorphable
protocol Document: Polymorphable {
    var id: String { get }
    var title: String { get }

    #Directory<Document>("app", "documents")
}

// Conforming types share the protocol's directory
@Persistable
struct Article: Document {
    var id: String = ULID().ulidString
    var title: String
    var content: String

    #Directory<Article>("app", "articles")  // Optional: own directory for dual-write
}

@Persistable
struct Report: Document {
    var id: String = ULID().ulidString
    var title: String
    var data: Data
    // No #Directory: uses default (persistableType)
}
```

**Property Separation** (due to Swift type system limitations):

| Property | Protocol | Description |
|----------|----------|-------------|
| `directoryPathComponents` | `Persistable` | Type-specific directory |
| `polymorphicDirectoryPathComponents` | `Polymorphable` | Shared protocol directory |
| `directoryLayer` | `Persistable` | Type-specific layer |
| `polymorphicDirectoryLayer` | `Polymorphable` | Shared protocol layer |
| `indexDescriptors` | `Persistable` | Type-specific indexes |
| `polymorphicIndexDescriptors` | `Polymorphable` | Shared protocol indexes |

**Dual-Write Behavior**:
When a type conforms to `Polymorphable` AND has its own `#Directory`:
- Save: Data written to both type-specific AND polymorphic directories
- Delete: Data removed from both directories

**Storage Layout**:
```
[polymorphic-directory]/R/[typeCode]/[id] → Protobuf-encoded item
[type-directory]/R/[PersistableType]/[id] → Protobuf-encoded item (if dual-write)
```

`typeCode` is a deterministic Int64 hash (DJB2 algorithm) of the type name.

**Usage**:
```swift
// Save (automatic dual-write if applicable)
context.insert(article)
try await context.save()

// Polymorphic query - returns all conforming types
let allDocuments = try await context.fetchPolymorphic(Article.self)
// Returns [Article, Report, ...] as [any Persistable]

// Fetch by ID from polymorphic directory
let doc = try await context.fetchPolymorphic(Article.self, id: someId)
```

**Swift Type System Limitation**:
Protocol types cannot be passed to generic functions requiring `Polymorphable` conformance:
```swift
// ❌ Compile error: 'any Document' cannot conform to 'Polymorphable'
try await context.fetchPolymorphic(Document.self)

// ✅ Use concrete type (all conforming types share the same polymorphic directory)
try await context.fetchPolymorphic(Article.self)
```

**Schema.Entity Design Principle**:
`Schema.Entity` is pure data type metadata. Storage information (directories, polymorphic settings) is accessed from the type at runtime via `entity.persistableType`:
```swift
// Schema.Entity only contains:
// - name: String
// - allFields: [String]
// - indexDescriptors: [IndexDescriptor]
// - enumMetadata: [String: EnumMetadata]
// - persistableType: any Persistable.Type  (for runtime type access)

// To check if a type is polymorphic:
if let polyType = entity.persistableType as? any Polymorphable.Type {
    let typeCode = polyType.typeCode(for: entity.name)
    let polyDir = polyType.polymorphicDirectoryPathComponents
}
```

### Comparison with FDB Record Layer

| Feature | Record Layer | database-framework | Status |
|---------|--------------|-------------------|--------|
| Index Types | Value, Rank, Lucene, Count, Sum, Spatial | 13 types including Vector, Graph, Triple | ✅ More |
| Online Indexer | ✅ | ✅ Multi-target, Mutual, Index-from-Index | ✅ |
| Index Scrubbing | ✅ | ✅ Two-phase verification | ✅ |
| Query Planner | Cascades | ✅ Cascades framework | ✅ |
| Large Record Splitting | ✅ SplitHelper | ✅ LargeValueSplitter | ✅ |
| Compression | ✅ Deflate | ✅ LZ4/zlib/LZMA/LZFSE | ✅ |
| Encryption | ✅ AES-CBC | ✅ AES-256-GCM | ✅ |
| Transaction Priority | ✅ | ✅ default/batch/system | ✅ |
| Version History | ✅ FDBRecordVersion | ✅ VersionIndex with versionstamps | ✅ |
| Synthetic Records | ✅ JoinedRecordType | ❌ | Not implemented |
| Union Record Type | ✅ | ✅ Polymorphable (dual-write) | ✅ |
| Uniqueness Enforcement | ✅ checkUniqueness | ✅ Index state-based | ✅ |
| SQL Interface | ✅ ANTLR parser | ❌ | Not implemented (SwiftData-like API) |
| Weak Read Semantics | ✅ Cached read version | ❌ | Not implemented |

### Extension Pattern for Optional Features

**設計原則**: このプロジェクトは SPM dependencies でカスタマイズ可能なデータベースを目指している。オプション機能は extension で提供し、コアを変更しない。

**パターン**:
```swift
// 各機能モジュールが FDBContext に extension で API を追加
// ユーザーは必要なモジュールを import して使う

// VectorIndex モジュール
extension FDBContext {
    public func findSimilar<T: Persistable>(_ type: T.Type) -> VectorQueryBuilder<T>
}

// FullTextIndex モジュール
extension FDBContext {
    public func search<T: Persistable>(_ type: T.Type) -> FullTextQueryBuilder<T>
}

// Relationship (DatabaseEngine 内)
extension FDBContext {
    public func related<T, R>(_ item: T, _ relationship: KeyPath<T, R?>) async throws -> R?
    public func related<T, R>(_ item: T, _ relationship: KeyPath<T, [R]>) async throws -> [R]
}
```

**重要な禁止事項**:
- ❌ コアの `FDBContext.save()` や `delete()` を直接変更してオプション機能を埋め込まない
- ❌ 使わないユーザーにオーバーヘッドを強制しない
- ✅ 新しいメソッドを extension で追加
- ✅ ユーザーが明示的に呼び出す

**Relationship の Delete Rule Enforcement**:
```swift
// 基本の delete (relationship rules なし)
context.delete(customer)
try await context.save()

// Delete rule enforcement が必要な場合は extension メソッドを使用
try await context.deleteEnforcingRelationshipRules(customer)
```

### Index Maintainer Pattern

Each index module follows this pattern:

1. `*IndexKind.swift` - Extends IndexKind with `IndexKindMaintainable` conformance
2. `*IndexMaintainer.swift` - Implements `IndexMaintainer` protocol

```swift
// Example: ScalarIndex/
├── ScalarIndexKind.swift           // extension ScalarIndexKind: IndexKindMaintainable
└── ScalarIndexMaintainer.swift     // struct ScalarIndexMaintainer<Item>: IndexMaintainer
```

### Testing Pattern

Tests use a shared singleton for FDB initialization to avoid "API version may be set only once" errors:

```swift
import Testing
@testable import DatabaseEngine

@Suite struct MyTests {
    init() async throws {
        try await FDBTestSetup.shared.initialize()
    }

    @Test func myTest() async throws { ... }
}
```

Test models are defined in `Tests/Shared/TestModels.swift`.

### テスト分離パターン

**重要**: テストは並列実行されるため、ハードコードされた ID を使用すると他のテストと干渉する。

**問題**:
- `cleanup()` で全データを削除すると、並列実行中の他のテストのデータも削除される
- 固定 ID（例: `"C001"`, `"O-test-001"`）を使用すると、複数のテストが同じデータを操作してしまう

**解決策**: 各テストで UUID ベースのユニーク ID を生成する

```swift
@Suite("My Tests", .serialized)
struct MyTests {

    /// Generate unique test ID to avoid conflicts with parallel tests
    private func uniqueID(_ prefix: String) -> String {
        "\(prefix)-\(UUID().uuidString.prefix(8))"
    }

    @Test func testSomething() async throws {
        let container = try await setupContainer()
        let context = container.newContext()

        // ✅ 良い例: ユニーク ID を使用
        let customerId = uniqueID("C-test")
        let orderId = uniqueID("O-test")

        var customer = Customer(name: "Alice")
        customer.id = customerId
        context.insert(customer)
        try await context.save()

        // テストロジック...
    }
}
```

**ガイドライン**:
1. ✅ `uniqueID()` ヘルパー関数を各テストスイートに追加
2. ✅ 全ての ID を `uniqueID("prefix")` で生成
3. ✅ 検証対象のデータのみを検証（他のテストのデータを想定しない）
4. ❌ `cleanup()` で `clearAll()` を呼ばない（他のテストに影響）
5. ❌ ハードコードされた ID（`"C001"`, `"test-id"` など）を使用しない

**例**: `IndexStateBehaviorTests`, `RelationshipIndexTests` を参照

## Adding a New Index Type

1. Define `IndexKind` in database-kit (e.g., `TimeSeriesIndexKind`)
2. Create new module in `Sources/` (e.g., `TimeSeriesIndex/`)
3. Add `IndexKindMaintainable` extension
4. Implement `IndexMaintainer` struct
5. Add module to `Package.swift` and `Database` target dependencies

## Swift Concurrency Pattern

### final class + Mutex パターン

**重要**: このプロジェクトは `actor` を使用せず、`final class: Sendable` + `Mutex` パターンを採用。

**理由**: スループット最適化
- actor はシリアライズされた実行 → 低スループット
- Mutex は細粒度ロック → 高い並行性
- データベース I/O 中も他のタスクを実行可能

**実装パターン**:
```swift
import Synchronization

public final class ClassName: Sendable {
    // 1. DatabaseProtocol は内部的にスレッドセーフ
    nonisolated(unsafe) private let database: any DatabaseProtocol

    // 2. 可変状態は Mutex で保護（struct にまとめる）
    private struct State: Sendable {
        var counter: Int = 0
        var isRunning: Bool = false
    }
    private let state: Mutex<State>

    public init(database: any DatabaseProtocol) {
        self.database = database
        self.state = Mutex(State())
    }

    // 3. withLock で状態アクセス（ロックスコープは最小限）
    public func operation() async throws {
        let count = state.withLock { state in
            state.counter += 1
            return state.counter
        }

        // I/O 中はロックを保持しない
        try await database.withTransaction { transaction in
            // 他のタスクは getProgress() などを呼べる
        }
    }
}
```

**ガイドライン**:
1. ✅ `final class: Sendable` を使用（actor は使用しない）
2. ✅ `DatabaseProtocol` には `nonisolated(unsafe)` を使用
3. ✅ 可変状態は `Mutex<State>` で保護（State は `Sendable` な struct）
4. ✅ ロックスコープは最小限（I/O を含めない）
5. ❌ `NSLock` は使用しない（async context で問題が発生する）
6. ❌ `@unchecked Sendable` は避ける（Mutex で適切に保護）

## 実装品質ガイドライン

### 基本方針

**実装コストは考慮しない。最適化と完成度が最優先。**

### 必須要件

1. **学術的根拠**: アルゴリズムは論文・教科書に基づく。独自手法は禁止
2. **参照実装の調査**: PostgreSQL, FoundationDB Record Layer, CockroachDB 等を必ず参照
3. **TODO/FIXME の扱い**: 作業継続のマーカーとして使用可。ただし放置せず完了まで実装を続ける
4. **包括的テスト**: 新機能には必ずユニットテストを追加
5. **エッジケース網羅**: 境界値、空入力、大規模データを全てカバー

### 実装プロセス

```
1. 調査フェーズ（省略禁止）
   ├── 学術論文を検索（Google Scholar, ACM DL）
   ├── 成熟したOSS実装を分析
   ├── 計算量・空間量を確認
   └── 既知の限界を把握

2. 設計フェーズ
   ├── 複数アプローチの比較表を作成
   ├── 最適なアルゴリズムを選定
   ├── データ構造を決定
   └── API設計

3. 実装フェーズ
   ├── テストを先に書く（TDD）
   ├── 参照論文・実装をコメントで明記
   ├── 定数には根拠をコメント
   └── 性能特性をドキュメント化

4. 検証フェーズ
   ├── 全テスト通過
   ├── ベンチマーク測定（必要に応じて）
   └── コードレビュー
```

### コーディング規約

```swift
// ✅ 良い例: 根拠を明記
/// Reservoir Sampling (Algorithm R)
/// Reference: Vitter, J.S. "Random Sampling with a Reservoir", ACM TOMS 1985
/// Time: O(n), Space: O(k) where k = reservoir size
public struct ReservoirSampling<T> { ... }

// ✅ 良い例: 定数に根拠
/// HyperLogLog precision parameter
/// - p=14 gives 16384 registers, ~0.8% standard error
/// - Memory: 2^p bytes = 16KB
/// Reference: Flajolet et al., "HyperLogLog: the analysis of a near-optimal cardinality estimation algorithm"
private let precision: Int = 14

// ❌ 悪い例: 根拠不明のマジックナンバー
let base = 256.0  // なぜ256？
let threshold = 0.5  // なぜ0.5？
```

### デフォルト値の扱い

**デフォルト値がある場合、Optional + nil合体演算子ではなく、直接デフォルト値を設定する。**

```swift
// ❌ 悪い例: Optional + nil合体演算子
public let retryLimit: Int?

public init(retryLimit: Int? = nil) {
    self.retryLimit = retryLimit
}

func run() {
    let maxRetries = configuration.retryLimit ?? 10  // 使用側で毎回デフォルト値を指定
}

// ✅ 良い例: 直接デフォルト値を設定
public let retryLimit: Int

public init(retryLimit: Int = 5) {
    self.retryLimit = retryLimit
}

func run() {
    let maxRetries = configuration.retryLimit  // そのまま使用可能
}
```

**理由**:
- デフォルト値が一箇所で定義され、一貫性が保たれる
- 使用側でのnil チェックや `??` が不要になり、コードがシンプルになる
- 型が非Optionalになり、意図が明確になる

### 参照すべきリソース

| 分野 | 参照先 |
|------|--------|
| クエリ最適化 | PostgreSQL src/backend/optimizer/ |
| 統計・ヒストグラム | PostgreSQL src/backend/utils/adt/selfuncs.c |
| インデックス設計 | FoundationDB Record Layer |
| 分散システム | CockroachDB, TiDB |
| アルゴリズム全般 | CLRS "Introduction to Algorithms" |
| データベース理論 | "Database System Concepts" (Silberschatz) |

## 開発方針

### プロジェクト状態

**このプロジェクトは開発中であり、後方互換性を考慮する必要はない。**

### コード管理原則

1. **不要なコードは削除**: 混乱を避けるため、使用されていないコード・互換性レイヤー・デッドコードは即座に削除
2. **破壊的変更を恐れない**: 本番データがないため、API・データフォーマットの変更は自由に行う
3. **最適な設計を優先**: 後方互換性のためのワークアラウンドよりも、正しい設計を選択
4. **マイグレーションコード不要**: 旧フォーマットから新フォーマットへの変換コードは書かない

### 具体例

```swift
// ❌ 不要: 互換性のための両方サポート
func readKey() -> Value {
    if let newFormat = tryNewFormat() { return newFormat }
    return tryOldFormat()  // ← 削除すべき
}

// ✅ 正しい: 新しい設計のみ
func readKey() -> Value {
    return readNewFormat()
}
```

### Subspace キー設計

文字列キーよりも整数キーを使用し、効率性を優先：

```swift
// ❌ 避ける: 文字列キー（デバッグ可読性のため等）
subspace.subspace("nodes")

// ✅ 推奨: 整数キー（効率的）
subspace.subspace(SubspaceKey.nodes.rawValue)  // enum SubspaceKey: Int64
```
