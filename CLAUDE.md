# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Vision

**database-framework** is a **protocol-extensible, customizable index database** designed for the AI era, built on FoundationDB's transactional guarantees.

### Design Philosophy

```
┌─────────────────────────────────────────────────────────────────────┐
│                     Protocol-Based Extensibility                     │
│                                                                      │
│   IndexKind (protocol)  ───▶  IndexMaintainer (protocol)            │
│        ↓                            ↓                                │
│   User defines           Framework executes                          │
│   WHAT to index          HOW to maintain                            │
└─────────────────────────────────────────────────────────────────────┘
```

1. **Protocol-Driven**: New index types can be added without modifying core code
2. **Composable**: Combine Vector + Graph + FullText in unified queries (Fusion API)
3. **Transactional**: All operations backed by FoundationDB's ACID guarantees
4. **AI-Native**: First-class support for embeddings, knowledge graphs, and RAG patterns

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

## Dependencies

### database-kit

**database-kit は安定版のため、GitHub URL を使用すること。**

```swift
// ✅ 正しい: GitHub URL を使用
.package(url: "https://github.com/1amageek/database-kit.git", branch: "main"),

// ❌ 間違い: ローカルパスは使用しない（修正が必要な場合のみ）
.package(path: "../database-kit"),
```

ローカルパスを使用するのは、database-kit 自体に変更・修正が必要な場合のみ。

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
| `FDBContainer` | Application resource manager: database connection, schema, securityDelegate, directory resolution. Does NOT create transactions |
| `FDBContext` | Transaction manager + User-facing API: owns ReadVersionCache, creates transactions via TransactionRunner, change tracking |
| `FDBDataStore` | Data operations within transactions: receives transaction as parameter, does NOT create transactions |
| `IndexMaintainer<Item>` | Protocol for index update logic (`updateIndex`, `scanItem`) |
| `IndexMaintenanceService` | Centralized index maintenance: uniqueness checking, index updates, violation tracking |
| `IndexKindMaintainable` | Bridge protocol connecting IndexKind to IndexMaintainer |

### Core Architecture Design Principles (Context-Centric)

**設計哲学**: Container/Context パターンに準拠

| コンポーネント | 責務 | トランザクション |
|--------------|------|-----------------|
| `FDBContainer` | リソース管理（DB接続、Schema、Directory） | **作成しない** |
| `FDBContext` | データ操作、トランザクション管理、キャッシュ | **作成する** |
| `FDBDataStore` | 低レベル操作（トランザクション内） | **受け取る** |

**禁止事項**:
- ❌ `FDBContainer` にトランザクション作成メソッドを追加しない
- ❌ `FDBDataStore` が独自にトランザクションを作成しない
- ✅ トランザクション作成は `FDBContext` に集約

### Transaction API の使い分け（設計原則）

本フレームワークには**3つのトランザクション API** があり、それぞれ明確な用途がある：

| API | 戻り値 | ReadVersionCache | アクセスレベル | 用途 |
|-----|--------|------------------|---------------|------|
| `context.withTransaction()` | `TransactionContext` | ✅ 使用 | `public` | ユーザー向け高レベルAPI |
| `context.withRawTransaction()` | `TransactionProtocol` | ✅ 使用 | `internal` | 内部インフラ（キャッシュ必要） |
| `database.withTransaction()` | `TransactionProtocol` | ❌ 不使用 | `public` | システム操作 |

```
ユーザーデータの読み書き？
    │
    ├─ YES → context.withTransaction()
    │
    └─ NO → ReadVersionCache が必要？
              │
              ├─ YES → context.withRawTransaction()  [internal]
              │
              └─ NO → database.withTransaction()
                        (DirectoryLayer, Migration, OnlineIndexer, Graph Algorithms)
```

**⚠️ 重要: withTransaction 内で commit() を呼ばないこと**

`withTransaction` はクロージャが正常終了した後、自動的にコミットします。

### Data Layout in FoundationDB

```
[fdb]/R/[PersistableType]/[id]           → ItemEnvelope(JSON-encoded item)
[fdb]/B/[blob-key]                       → Large value blob chunks
[fdb]/I/[indexName]/[values...]/[id]     → Index entry (empty value for scalar)
[fdb]/_metadata/schema/version           → Tuple(major, minor, patch)
[fdb]/_metadata/index/[indexName]/state  → IndexState (readable/write_only/disabled)
```

**ItemEnvelope Format**: All items are wrapped in `ItemEnvelope` with magic number `ITEM` (0x49 0x54 0x45 0x4D). Reading raw data without `ItemStorage.read()` will fail.

## Implemented Features

### Index Types

各インデックスの詳細は各モジュールの README.md を参照してください。

| Index | Module | README |
|-------|--------|--------|
| Scalar | `ScalarIndex` | [README](Sources/ScalarIndex/README.md) |
| Vector | `VectorIndex` | [README](Sources/VectorIndex/README.md) |
| FullText | `FullTextIndex` | [README](Sources/FullTextIndex/README.md) |
| Spatial | `SpatialIndex` | [README](Sources/SpatialIndex/README.md) |
| Rank | `RankIndex` | [README](Sources/RankIndex/README.md) |
| Permuted | `PermutedIndex` | [README](Sources/PermutedIndex/README.md) |
| Graph | `GraphIndex` | [README](Sources/GraphIndex/README.md) |
| Aggregation | `AggregationIndex` | [README](Sources/AggregationIndex/README.md) |
| Version | `VersionIndex` | [README](Sources/VersionIndex/README.md) |
| Bitmap | `BitmapIndex` | [README](Sources/BitmapIndex/README.md) |
| Leaderboard | `LeaderboardIndex` | [README](Sources/LeaderboardIndex/README.md) |
| Relationship | `RelationshipIndex` | [README](Sources/RelationshipIndex/README.md) |

### Extension Pattern for Optional Features

**設計原則**: このプロジェクトは SPM dependencies でカスタマイズ可能なデータベースを目指している。オプション機能は extension で提供し、コアを変更しない。

```swift
// 各機能モジュールが FDBContext に extension で API を追加
extension FDBContext {
    public func findSimilar<T: Persistable>(_ type: T.Type) -> VectorQueryBuilder<T>
    public func search<T: Persistable>(_ type: T.Type) -> FullTextQueryBuilder<T>
}
```

**重要な禁止事項**:
- ❌ コアの `FDBContext.save()` や `delete()` を直接変更してオプション機能を埋め込まない
- ✅ 新しいメソッドを extension で追加

## Index Implementation Pattern

インデックスモジュールは**書き込み（維持）**と**読み取り（クエリ）**の2つの責務を持つ。

### モジュール構成

```
Sources/{IndexName}Index/
├── {IndexName}IndexKind+Maintainable.swift  # IndexKindMaintainable conformance
├── {IndexName}IndexMaintainer.swift         # IndexMaintainer 実装 + search()
├── {IndexName}Query.swift                   # FDBContext extension + QueryBuilder
└── README.md                                # 使用方法とユースケース
```

### IndexMaintainer プロトコル

```swift
public protocol IndexMaintainer<Item>: Sendable {
    associatedtype Item: Persistable

    func updateIndex(oldItem: Item?, newItem: Item?, transaction: any TransactionProtocol) async throws
    func scanItem(_ item: Item, id: Tuple, transaction: any TransactionProtocol) async throws
    func computeIndexKeys(for item: Item, id: Tuple) async throws -> [FDB.Bytes]
    var customBuildStrategy: (any IndexBuildStrategy<Item>)? { get }
}
```

### Query Builder パターン

```
FDBContext.{queryMethod}()     ← Entry Point (extension で追加)
    ↓
{Index}EntryPoint<T>           ← フィールド/オプション選択
    ↓
{Index}QueryBuilder<T>         ← パラメータ設定 (Fluent API)
    ↓
execute()                      ← IndexQueryContext 経由で実行
```

### 命名規約

| コンポーネント | 命名パターン | 例 |
|--------------|-------------|-----|
| Entry Point メソッド | `context.{動詞}(Type.self)` | `findSimilar`, `search`, `nearby`, `rank` |
| Entry Point 型 | `{Index}EntryPoint<T>` | `VectorEntryPoint`, `FullTextEntryPoint` |
| Query Builder 型 | `{Index}QueryBuilder<T>` | `VectorQueryBuilder`, `SpatialQueryBuilder` |
| Maintainer 型 | `{Algorithm}IndexMaintainer<T>` | `HNSWIndexMaintainer`, `FlatVectorIndexMaintainer` |

## Testing Pattern

### FDB 初期化

Tests use a shared singleton for FDB initialization:

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

### @Persistable マクロ必須

**重要**: テストモデルを含む全ての `Persistable` 型は `@Persistable` マクロを使用する必要がある。手動実装は `ItemEnvelope` 形式との互換性問題が発生する。

```swift
// ✅ 良い例
@Persistable
struct MyModel {
    #Directory<MyModel>("test", "models")
    var id: String = UUID().uuidString
    var name: String = ""
}
```

### テスト分離パターン

テストは並列実行されるため、UUID ベースのユニーク ID を使用する：

```swift
@Suite("My Tests", .serialized)
struct MyTests {
    private func uniqueID(_ prefix: String) -> String {
        "\(prefix)-\(UUID().uuidString.prefix(8))"
    }

    @Test func testSomething() async throws {
        let customerId = uniqueID("C-test")
        // ...
    }
}
```

**ガイドライン**:
- ✅ 全ての ID を `uniqueID("prefix")` で生成
- ❌ `cleanup()` で `clearAll()` を呼ばない
- ❌ ハードコードされた ID を使用しない

## Adding a New Index Type

1. Define `IndexKind` in database-kit (e.g., `TimeSeriesIndexKind`)
2. Create new module in `Sources/` (e.g., `TimeSeriesIndex/`)
3. Add `IndexKindMaintainable` extension
4. Implement `IndexMaintainer` struct
5. Add module to `Package.swift` and `Database` target dependencies
6. Create `README.md` with use cases and examples

## Swift Concurrency Pattern

### final class + Mutex パターン

**重要**: このプロジェクトは `actor` を使用せず、`final class: Sendable` + `Mutex` パターンを採用。

**理由**: スループット最適化
- actor はシリアライズされた実行 → 低スループット
- Mutex は細粒度ロック → 高い並行性
- データベース I/O 中も他のタスクを実行可能

```swift
import Synchronization

public final class ClassName: Sendable {
    nonisolated(unsafe) private let database: any DatabaseProtocol

    private struct State: Sendable {
        var counter: Int = 0
    }
    private let state: Mutex<State>

    public func operation() async throws {
        let count = state.withLock { state in
            state.counter += 1
            return state.counter
        }
        // I/O 中はロックを保持しない
        try await database.withTransaction { transaction in ... }
    }
}
```

**ガイドライン**:
- ✅ `final class: Sendable` を使用（actor は使用しない）
- ✅ `DatabaseProtocol` には `nonisolated(unsafe)` を使用
- ✅ 可変状態は `Mutex<State>` で保護
- ✅ ロックスコープは最小限（I/O を含めない）
- ❌ `@unchecked Sendable` は避ける

## 実装品質ガイドライン

### 基本方針

**実装コストは考慮しない。最適化と完成度が最優先。**

### 必須要件

1. **学術的根拠**: アルゴリズムは論文・教科書に基づく。独自手法は禁止
2. **参照実装の調査**: PostgreSQL, FoundationDB Record Layer, CockroachDB 等を必ず参照
3. **包括的テスト**: 新機能には必ずユニットテストを追加
4. **エッジケース網羅**: 境界値、空入力、大規模データを全てカバー

### コーディング規約

```swift
// ✅ 良い例: 根拠を明記
/// Reservoir Sampling (Algorithm R)
/// Reference: Vitter, J.S. "Random Sampling with a Reservoir", ACM TOMS 1985
/// Time: O(n), Space: O(k) where k = reservoir size
public struct ReservoirSampling<T> { ... }

// ❌ 悪い例: 根拠不明のマジックナンバー
let base = 256.0  // なぜ256？
```

### デフォルト値の扱い

**デフォルト値がある場合、Optional + nil合体演算子ではなく、直接デフォルト値を設定する。**

```swift
// ✅ 良い例
public let retryLimit: Int
public init(retryLimit: Int = 5) {
    self.retryLimit = retryLimit
}

// ❌ 悪い例
public let retryLimit: Int?
public init(retryLimit: Int? = nil) { ... }
```

### 参照すべきリソース

| 分野 | 参照先 |
|------|--------|
| クエリ最適化 | PostgreSQL src/backend/optimizer/ |
| インデックス設計 | FoundationDB Record Layer |
| 分散システム | CockroachDB, TiDB |
| アルゴリズム全般 | CLRS "Introduction to Algorithms" |

## 開発方針

### プロジェクト状態

**このプロジェクトは開発中であり、後方互換性を考慮する必要はない。**

### コード管理原則

1. **不要なコードは削除**: 使用されていないコード・互換性レイヤー・デッドコードは即座に削除
2. **破壊的変更を恐れない**: API・データフォーマットの変更は自由に行う
3. **最適な設計を優先**: 後方互換性のためのワークアラウンドよりも、正しい設計を選択

### Subspace キー設計

整数キーを使用し、効率性を優先：

```swift
// ✅ 推奨: 整数キー
subspace.subspace(SubspaceKey.nodes.rawValue)

// ❌ 避ける: 文字列キー
subspace.subspace("nodes")
```

### Persistable フィールドアクセス（Mirror禁止）

`Persistable`型のフィールドアクセスには`Mirror`を使用しない。

```swift
// ✅ 良い例: dynamicMember subscript を使用
func extractField<T: Persistable>(from item: T, fieldName: String) -> Any? {
    return item[dynamicMember: fieldName]
}

// ❌ 悪い例: Mirror を使用
let mirror = Mirror(reflecting: item)
```

## Known Limitations

### Watch機能 (fdb-swift-bindings拡張が必要)

`WatchManager`はスタブ実装。fdb-swift-bindings に `watch(key:)` メソッドの追加が必要。

### インデックス再構築 (簡易実装)

`AdminContext.rebuildIndex()`は簡易実装。本番環境では`OnlineIndexer`を使用すべき。
