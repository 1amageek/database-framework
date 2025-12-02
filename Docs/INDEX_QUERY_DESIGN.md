# Index Query Design

このドキュメントは、各インデックスタイプ固有のクエリインターフェース設計を定義します。

## 設計原則

### 1. モジュール単位のExtension提供

各インデックスモジュールは、`FDBContext`へのExtensionを通じて専用のクエリAPIを提供します。
ユーザーは必要なモジュールをSPM依存関係に追加するだけで、対応するAPIが自動的に利用可能になります。

```swift
import DatabaseEngine      // 基本機能 (Scalar Query)
import FullTextIndex       // context.search() が利用可能に
import VectorIndex         // context.findSimilar() が利用可能に
import SpatialIndex        // context.findNearby() が利用可能に
import AggregationIndex    // context.aggregate() が利用可能に
```

### 2. 型安全なQuery Builder

各インデックスタイプは、そのセマンティクスに適した専用のQuery Builderを提供します。
これにより、コンパイル時に不正なクエリを検出できます。

### 3. Index固有の戻り値型

各クエリは、インデックスタイプに応じた適切な情報を含む戻り値を返します：

| Index Type | Return Type |
|------------|-------------|
| Scalar | `[T]` |
| FullText | `[T]` または `[(item: T, relevance: Double)]` |
| Vector | `[(item: T, distance: Double)]` |
| Spatial | `[(item: T, distance: Double?)]` |
| Aggregation | `[AggregateResult<T>]` |
| Rank | `[(item: T, rank: Int)]` |

---

## アーキテクチャ概要

```
┌─────────────────────────────────────────────────────────────────────┐
│                         User Code                                    │
│  import FullTextIndex                                                │
│  let results = try await context.search(Article.self)               │
│      .fullText(\.content)                                            │
│      .terms(["swift"], mode: .all)                                   │
│      .execute()                                                      │
└────────────────────────────┬────────────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────────┐
│                    FDBContext Extension                              │
│                 (各モジュールが提供)                                   │
│  extension FDBContext {                                              │
│      func search<T>(_ type: T.Type) -> FullTextEntryPoint<T>        │
│  }                                                                   │
└────────────────────────────┬────────────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────────┐
│                    Index-Specific Query Builder                      │
│              FullTextQueryBuilder<T>                                 │
│  - terms, matchMode, limit                                           │
│  - func execute() async throws -> [T]                               │
└────────────────────────────┬────────────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────────┐
│                       IndexSearcher                                  │
│               FullTextIndexSearcher                                  │
│  - Subspace構造の理解                                                 │
│  - クエリ実行ロジック                                                  │
└────────────────────────────┬────────────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────────┐
│                       StorageReader                                  │
│  - scanSubspace, scanRange, getValue                                │
│  - 低レベルKVアクセス                                                 │
└─────────────────────────────────────────────────────────────────────┘
```

---

## 各インデックスのクエリAPI

### 1. Scalar Index (DatabaseEngine - 標準)

基本的な値ベースのフィルタリング。`Query<T>`で提供。

```swift
let users = try await context.fetch(User.self)
    .where(\.age > 18)
    .where(\.status == "active")
    .orderBy(\.name)
    .limit(100)
    .execute()
```

### 2. FullText Index

テキスト検索。用語ベースのマッチング。

```swift
let articles = try await context.search(Article.self)
    .fullText(\.content)
    .terms(["swift", "concurrency"], mode: .all)
    .limit(20)
    .execute()
```

**Query Builder API**:
```swift
public struct FullTextQueryBuilder<T: Persistable> {
    func terms(_ terms: [String], mode: TextMatchMode) -> Self
    func limit(_ count: Int) -> Self
    func execute() async throws -> [T]
}
```

### 3. Vector Index

ベクトル類似度検索。k近傍探索。

```swift
let similar = try await context.findSimilar(Product.self)
    .vector(\.embedding, dimensions: 128)
    .query(queryVector, k: 10)
    .metric(.cosine)
    .execute()
// Returns: [(item: Product, distance: Double)]
```

**Query Builder API**:
```swift
public struct VectorQueryBuilder<T: Persistable> {
    func query(_ vector: [Float], k: Int) -> Self
    func metric(_ metric: VectorDistanceMetric) -> Self
    func filter(_ predicate: Predicate<T>) -> Self
    func execute() async throws -> [(item: T, distance: Double)]
}
```

### 4. Spatial Index

地理空間検索。バウンディングボックス、半径検索。

```swift
let stores = try await context.findNearby(Store.self)
    .location(\.geoPoint)
    .within(radiusKm: 5.0, of: currentLocation)
    .orderByDistance()
    .limit(10)
    .execute()
// Returns: [(item: Store, distance: Double)]
```

**Query Builder API**:
```swift
public struct SpatialQueryBuilder<T: Persistable> {
    func within(bounds: BoundingBox) -> Self
    func within(radiusKm: Double, of center: GeoPoint) -> Self
    func orderByDistance() -> Self
    func limit(_ count: Int) -> Self
    func execute() async throws -> [(item: T, distance: Double)]
}
```

### 5. Aggregation Index

集約クエリ。GROUP BY、COUNT、SUM、AVG、MIN、MAX。

```swift
let stats = try await context.aggregate(Order.self)
    .groupBy(\.region)
    .sum(\.amount, as: "totalSales")
    .count(as: "orderCount")
    .having { $0.count > 10 }
    .execute()
// Returns: [AggregateResult<Order>]
```

**Query Builder API**:
```swift
public struct AggregationQueryBuilder<T: Persistable> {
    func groupBy<V>(_ keyPath: KeyPath<T, V>) -> Self
    func count(as name: String) -> Self
    func sum<N: Numeric>(_ keyPath: KeyPath<T, N>, as name: String) -> Self
    func avg<N: Numeric>(_ keyPath: KeyPath<T, N>, as name: String) -> Self
    func min<V: Comparable>(_ keyPath: KeyPath<T, V>, as name: String) -> Self
    func max<V: Comparable>(_ keyPath: KeyPath<T, V>, as name: String) -> Self
    func having(_ predicate: (AggregateResult<T>) -> Bool) -> Self
    func execute() async throws -> [AggregateResult<T>]
}
```

### 6. Rank Index

ランキングクエリ。Top-N、パーセンタイル。

```swift
let leaderboard = try await context.rank(Player.self)
    .by(\.score)
    .top(100)
    .execute()
// Returns: [(item: Player, rank: Int)]

let median = try await context.rank(Employee.self)
    .by(\.salary)
    .percentile(0.5)
    .execute()
// Returns: T?
```

**Query Builder API**:
```swift
public struct RankQueryBuilder<T: Persistable> {
    func by<V: Comparable>(_ keyPath: KeyPath<T, V>) -> Self
    func top(_ n: Int) -> Self
    func bottom(_ n: Int) -> Self
    func range(from: Int, to: Int) -> Self
    func execute() async throws -> [(item: T, rank: Int)]
}

public struct PercentileQueryBuilder<T: Persistable> {
    func by<V: Comparable>(_ keyPath: KeyPath<T, V>) -> Self
    func percentile(_ p: Double) -> Self
    func execute() async throws -> T?
}
```

---

## モジュール依存関係

```
┌─────────────────────────────────────────────────────────────────────┐
│                          Database                                    │
│                    (全モジュール再エクスポート)                         │
└─────────────────────────────────────────────────────────────────────┘
                                 │
        ┌────────────┬───────────┼───────────┬────────────┐
        ▼            ▼           ▼           ▼            ▼
┌───────────┐ ┌───────────┐ ┌───────────┐ ┌───────────┐ ┌───────────┐
│FullText   │ │ Vector    │ │ Spatial   │ │Aggregation│ │  Rank     │
│Index      │ │ Index     │ │ Index     │ │Index      │ │  Index    │
│           │ │           │ │           │ │           │ │           │
│+Query.swift│ │+Query.swift│ │+Query.swift│ │+Query.swift│ │+Query.swift│
└─────┬─────┘ └─────┬─────┘ └─────┬─────┘ └─────┬─────┘ └─────┬─────┘
      │             │             │             │             │
      └─────────────┴──────┬──────┴─────────────┴─────────────┘
                           ▼
                ┌─────────────────┐
                │ DatabaseEngine  │
                │ - FDBContext    │
                │ - StorageReader │
                │ - IndexSearcher │
                │ - Query<T>      │
                └─────────────────┘
                           │
                           ▼
                    ┌───────────┐
                    │   Core    │
                    │(database- │
                    │   kit)    │
                    └───────────┘
```

---

## 実装ガイドライン

### 1. Query Builderの構造

```swift
public struct XXXQueryBuilder<T: Persistable>: Sendable {
    // 依存関係
    private let context: FDBContext
    private let fieldName: String

    // クエリパラメータ
    private var param1: Type1?
    private var param2: Type2?

    // Internal initializer
    internal init(context: FDBContext, fieldName: String) {
        self.context = context
        self.fieldName = fieldName
    }

    // Fluent API methods (イミュータブル)
    public func method1(_ value: Type1) -> Self {
        var copy = self
        copy.param1 = value
        return copy
    }

    // Execute
    public func execute() async throws -> [ResultType] {
        // 1. IndexSearcherを作成
        // 2. Index固有のQueryを構築
        // 3. 検索実行
        // 4. 結果をフェッチして返す
    }
}
```

### 2. FDBContext Extension

```swift
// Sources/XXXIndex/XXXQuery.swift

import DatabaseEngine
import Core

extension FDBContext {
    /// ドキュメント
    public func entryPointMethod<T: Persistable>(_ type: T.Type) -> XXXEntryPoint<T> {
        XXXEntryPoint(context: self)
    }
}

public struct XXXEntryPoint<T: Persistable>: Sendable {
    private let context: FDBContext

    internal init(context: FDBContext) {
        self.context = context
    }

    public func field(_ keyPath: KeyPath<T, FieldType>) -> XXXQueryBuilder<T> {
        XXXQueryBuilder(
            context: context,
            fieldName: T.fieldName(for: keyPath)
        )
    }
}
```

### 3. エラーハンドリング

各モジュールは固有のエラー型を定義：

```swift
public enum XXXQueryError: Error, CustomStringConvertible {
    case missingRequiredParameter(String)
    case indexNotFound(String)
    case invalidConfiguration(String)

    public var description: String {
        switch self {
        case .missingRequiredParameter(let param):
            return "Missing required parameter: \(param)"
        case .indexNotFound(let name):
            return "Index not found: \(name)"
        case .invalidConfiguration(let message):
            return "Invalid configuration: \(message)"
        }
    }
}
```

---

## 移行ガイド

### 既存コードからの移行

```swift
// 旧 (動作しない)
let results = try await context.fetch(Article.self)
    .groupBy()
    .by(\.category)
    .count()
    .build()
    .execute()  // ❌ execute()が存在しない

// 新
import AggregationIndex

let results = try await context.aggregate(Article.self)
    .groupBy(\.category)
    .count()
    .execute()  // ✅ 動作する
```

### 非推奨API

以下のAPIは非推奨となり、将来のバージョンで削除予定：

- `DatabaseEngine/Query/GroupByQuery.swift`
- `DatabaseEngine/Query/RankQuery.swift`
- `DatabaseEngine/Query/SpatialQuery.swift`

これらは各インデックスモジュールの専用Query APIに置き換えられます。

---

## テスト戦略

各モジュールは以下のテストを含む：

1. **Query Builder単体テスト**: パラメータ設定の検証
2. **IndexSearcher統合テスト**: モックStorageReaderを使用
3. **End-to-End テスト**: FDBContextを使用した実行テスト

```swift
// Tests/FullTextIndexTests/FullTextQueryTests.swift

@Suite("FullText Query Tests")
struct FullTextQueryTests {
    @Test("Search with single term returns matching documents")
    func testSingleTermSearch() async throws {
        // ...
    }

    @Test("Search with AND mode requires all terms")
    func testAndModeSearch() async throws {
        // ...
    }
}
```
