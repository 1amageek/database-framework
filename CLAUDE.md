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
┌───┴───┬───────┬───────┬───────┬───────┬───────┬───────┬───────┬─────────┐
Scalar  Vector  FullText Spatial Rank   Permuted Graph  Aggregation Version QueryAST
    ↓      ↓       ↓        ↓      ↓       ↓       ↓        ↓        ↓        ↓
    └──────┴───────┴────────┴──────┴───────┴───────┴────────┴────────┴────────┘
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

### QueryAST (クエリ抽象構文木)

SQL/SPARQL クエリの解析・変換・シリアライズを提供するモジュール。詳細は [README](Sources/QueryAST/README.md) を参照。

| コンポーネント | 説明 |
|--------------|------|
| `SQLParser` | SQL クエリのパーサー |
| `SPARQLParser` | SPARQL クエリのパーサー |
| `SelectQuery` | SELECT クエリの AST 表現 |
| `Expression` | 式（比較、算術、論理、集約） |
| `GraphTableSource` | SQL/PGQ GRAPH_TABLE 句 |
| `GraphPattern` | SPARQL グラフパターン |
| `SQLEscape` | SQL/SPARQL インジェクション対策ユーティリティ |

**主な機能**:
- SQL/SPARQL クエリの解析と AST 生成
- プログラマティックなクエリ構築（ビルダーパターン）
- AST から SQL/SPARQL への安全なシリアライズ（識別子エスケープ）
- SQL/PGQ グラフパターンマッチング対応 (ISO/IEC 9075-16:2023)
- SPARQL 1.1/1.2 プロパティパス対応
- クエリ分析（変数参照、集約関数検出）

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

## Value Access Architecture（値アクセス3層アーキテクチャ）

フィルタリング・ソートはすべてのインデックスモジュールから統一的に利用される共通基盤。各インデックスが独自に評価ロジックを実装してはならない。

### 3層構造

```
Layer 1: ゼロコピーパス（構築時クロージャ）
    FieldComparison._evaluate / SortDescriptor._compare
    → 型付き KeyPath<T, V> でネイティブ比較、存在型を経由しない

Layer 2: FieldReader パス（型消去後のフォールバック）
    FieldReader.read() → FieldValue 変換 → FieldValue 比較
    → QueryRewriter/DNFConverter で型情報が失われた場合のみ使用

Layer 3: DataAccess（ストレージ層）
    DataAccess.deserialize() → throws（エラーを伝播、握りつぶさない）
```

### ゼロコピー評価の仕組み

演算子（`==`, `<` 等）で `Predicate` を構築すると、型付きクロージャが `FieldComparison._evaluate` にキャプチャされる。

```swift
// 構築時: 型情報がクロージャに閉じ込められる
let predicate: Predicate<User> = \.age == 30
// 内部:
//   nonisolated(unsafe) let kp = keyPath  // KeyPath<User, Int>
//   _evaluate = { model in model[keyPath: kp] == v }

// 評価時: Any / FieldValue を経由しない
comparison.evaluate(on: user)  // → _evaluate?(user) → user[keyPath: kp] == 30
```

`SortDescriptor` も同様に `_compare` クロージャでゼロコピー比較を行う。

### 評価フロー

```
evaluate(on: model)
  ├─ _evaluate != nil → ゼロコピーパス（通常のユーザークエリ）
  └─ _evaluate == nil → evaluateViaFieldReader()（型消去後のフォールバック）

orderedComparison(lhs, rhs)
  ├─ _compare != nil → ゼロコピーパス
  └─ _compare == nil → compareViaFieldReader()
```

### 禁止事項

- ❌ `FDBDataStore`、`PlanExecutor`、各インデックスモジュールが独自の `evaluateComparison` を実装しない
- ❌ `try?` でエラーを握りつぶさない（`extractIndexValues`、`DataAccess.deserialize`、`valueToTuple` は全て `throws`）
- ❌ `String(describing:)` で型変換のフォールバックをしない（インデックスキーの順序が壊れる）
- ❌ `PartialKeyPath` の戻り値に対して `raw == nil` で null 判定しない（Optional-in-Any ボクシング問題）
- ✅ null 判定には `FieldValue` 変換後の `.isNull` を使用
- ✅ 全ての呼び出し元は `comparison.evaluate(on:)` と `descriptor.orderedComparison()` を使用

### KeyPath と Sendable

`KeyPath` は `Sendable` に準拠していないため、`@Sendable` クロージャでキャプチャする際は `nonisolated(unsafe) let` を使用する。

```swift
// ✅ 正しい
nonisolated(unsafe) let kp = keyPath
let closure: @Sendable (T) -> Bool = { model in model[keyPath: kp] == value }

// ❌ コンパイルエラー
let closure: @Sendable (T) -> Bool = { model in model[keyPath: keyPath] == value }
```

### 関連ファイル

| ファイル | 責務 |
|---------|------|
| `Sources/DatabaseEngine/Fetch/FDBFetchDescriptor.swift` | `FieldComparison.evaluate(on:)`, `SortDescriptor.orderedComparison()`, 演算子オーバーロード |
| `Sources/DatabaseEngine/Core/FieldReader.swift` | Layer 2: 非 throwing フィールド読み取り（dynamicMember / ネストフィールド） |
| `Sources/DatabaseEngine/Internal/FDBDataStore.swift` | `evaluatePredicate()` が `evaluate(on:)` を呼ぶ |
| `Sources/DatabaseEngine/QueryPlanner/PlanExecutor.swift` | フィルタ・ソートが `evaluate(on:)` / `orderedComparison()` を呼ぶ |
| `Sources/DatabaseEngine/QueryPlanner/AggregationPlanExecutor.swift` | 集約クエリのフィルタが `evaluate(on:)` を呼ぶ |

## Tuple Encoding Convention

### TupleEncoder / TupleDecoder

**必須**: 全ての Index モジュールは `TupleEncoder` / `TupleDecoder` を使用すること。

```swift
// ✅ 正しい: TupleEncoder / TupleDecoder を使用
import DatabaseEngine

let element = try TupleEncoder.encode(value)
let score = try TupleDecoder.decode(element, as: Int64.self)

// ❌ 禁止: 独自のエンコーディング実装
switch value {
case let d as Double:
    return String(format: "%020.6f", d)  // 禁止！順序が壊れる
case let i as Int:
    return String(format: "%020d", i)    // 禁止！
}
```

### 型マッピング

| Swift Type | TupleElement | Notes |
|------------|--------------|-------|
| String | String | そのまま |
| Int, Int8-64 | Int64 | 拡張 |
| UInt, UInt8-64 | Int64 | オーバーフローチェック付き |
| Double | Double | IEEE 754 (FDBが順序保証) |
| Float | Double | 拡張 |
| Bool | Bool | そのまま |
| Date | Date | fdb-swift-bindings が処理 |
| UUID | UUID | そのまま |
| Data | [UInt8] | バイト配列に変換 |

### 理由

- FDB Tuple Layer は Double の辞書順を保証する（IEEE 754 準拠）
- String フォーマットへの変換は順序を破壊する
- 型の一貫性がないとインデックスの整合性が失われる

### 参照

- `Sources/DatabaseEngine/Core/TupleEncoder.swift`
- `Sources/DatabaseEngine/Core/TupleDecoder.swift`

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

## 統一型変換規約

**必須**: 全モジュールは `TypeConversion` を使用すること。独自の型変換実装は禁止。

```swift
import DatabaseEngine

// ✅ 正しい: TypeConversion を使用
let int64 = TypeConversion.asInt64(value)           // 値抽出 (比較用)
let double = TypeConversion.asDouble(value)         // 値抽出 (集計用)
let string = TypeConversion.asString(value)         // 値抽出 (文字列比較用)
let fieldValue = TypeConversion.toFieldValue(value) // FieldValue 変換
let element = try TypeConversion.toTupleElement(value)  // TupleElement 変換

// TupleElement からの抽出
let score = try TypeConversion.int64(from: element)
let price = try TypeConversion.double(from: element)
let name = try TypeConversion.string(from: element)

// ❌ 禁止: 独自の型変換実装
switch value {
case let v as Int64: return v
case let v as Int: return Int64(v)  // 禁止！
...
}
```

### 型マッピング仕様

| Swift Type       | Int64 | Double | String | FieldValue      |
|------------------|-------|--------|--------|-----------------|
| Int, Int8-64     | ✓     | ✓      | -      | .int64          |
| UInt, UInt8-64   | ✓*    | ✓      | -      | .int64          |
| Double, Float    | -     | ✓      | -      | .double         |
| String           | -     | -      | ✓      | .string         |
| Bool             | ✓**   | -      | -      | .bool           |
| Date             | -     | ✓***   | -      | .double         |
| UUID             | -     | -      | ✓      | .string         |

\* UInt64 > Int64.max はオーバーフロー（nil）
\** Bool: true=1, false=0
\*** Date: timeIntervalSince1970

### ラッパーメソッド禁止（直接呼出の原則）

`TypeConversion` / `TupleEncoder` / `TupleDecoder` を呼ぶだけの private メソッドを作成しない。直接呼び出すこと。

```swift
// ❌ 禁止: パススルーラッパー
private func convertToTupleElement(_ value: any Sendable) throws -> any TupleElement {
    try TupleEncoder.encode(value)
}

// ❌ 禁止: エラー再ラップだけのラッパー
private func extractScore(from element: any TupleElement) throws -> Score {
    do {
        return try TupleDecoder.decode(element, as: Score.self)
    } catch {
        throw MyError.invalidScore("...")
    }
}

// ❌ 禁止: 手動型スイッチ（TypeConversion/TupleDecoder が内部で処理済み）
private func extractNumericValue(from element: any TupleElement) -> Double? {
    if let d = element as? Double { return d }
    if let i = element as? Int64 { return Double(i) }
    return nil
}

// ✅ 正しい: 呼出元で直接使用
let element = try TupleEncoder.encode(value)
let score = try TupleDecoder.decode(element, as: Score.self)
let double = try TypeConversion.double(from: element)
let value = TypeConversion.asDouble(rawValue)
```

**許容されるケース**: 複数の API を組み合わせて構造化された結果を返す場合のみ、ユーティリティ関数を作成してよい。

```swift
// ✅ 許容: 複合的なロジックを持つユーティリティ
public static func extractNumeric<Value>(
    from element: any TupleElement, as valueType: Value.Type
) throws -> (int64: Int64?, double: Double?, isFloatingPoint: Bool) {
    switch valueType {
    case is Int64.Type, is Int.Type, is Int32.Type:
        return (int64: try TypeConversion.int64(from: element), double: nil, isFloatingPoint: false)
    case is Double.Type, is Float.Type:
        return (int64: nil, double: try TypeConversion.double(from: element), isFloatingPoint: true)
    default:
        throw IndexError.invalidConfiguration("Unsupported: \(valueType)")
    }
}
```

### 関連ファイル

- `Sources/DatabaseEngine/Core/TypeConversion.swift` - 統一型変換ユーティリティ
- `Sources/DatabaseEngine/Core/TupleEncoder.swift` - Any → TupleElement 変換
- `Sources/DatabaseEngine/Core/TupleDecoder.swift` - TupleElement → T 変換

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
