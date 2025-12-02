# Query Refactoring Plan

## 課題

### 1. 既存の古いDSLの問題
以下のファイルは`execute()`メソッドがなく、実行できない：
- `Sources/DatabaseEngine/Query/GroupByQuery.swift`
- `Sources/DatabaseEngine/Query/RankQuery.swift`
- `Sources/DatabaseEngine/Query/SpatialQuery.swift`

### 2. 事前計算インデックスの未活用
- `AggregationIndex`の事前計算値（Count/Sum/Avg/MinMax）を使用していない
- `RankIndexMaintainer`の`getTopK()`等を直接活用していない

---

## 解決策

### Phase 1: 既存DSLの非推奨化と削除

**方針**: 古いDSLを削除し、新しいモジュール別Query Extensionに統一

| 古いファイル | 新しい代替 |
|-------------|-----------|
| `DatabaseEngine/Query/GroupByQuery.swift` | `AggregationIndex/AggregationQuery.swift` |
| `DatabaseEngine/Query/RankQuery.swift` | `RankIndex/RankQuery.swift` |
| `DatabaseEngine/Query/SpatialQuery.swift` | `SpatialIndex/SpatialQuery.swift` |

**アクション**:
1. 古いDSLファイルを削除
2. `Query<T>`のextension（`.groupBy()`, `.top()`, `.bottom()`, `.within()`, `.nearBy()`）を削除
3. READMEを更新し、新しいAPIへの移行方法を明記

### Phase 2: IndexQueryContextの拡張

**目的**: IndexMaintainerのクエリメソッドに直接アクセスできるようにする

```swift
// IndexQueryContext.swift に追加

extension IndexQueryContext {

    // MARK: - Rank Index Operations

    /// Execute a rank-based query using RankIndexMaintainer
    public func executeRankQuery<T: Persistable>(
        type: T.Type,
        indexName: String,
        mode: RankQueryMode,
        limit: Int
    ) async throws -> [(item: T, rank: Int)]

    // MARK: - Aggregation Index Operations

    /// Get count from precomputed CountIndex
    public func getPrecomputedCount<T: Persistable>(
        type: T.Type,
        indexName: String,
        groupingValues: [any TupleElement]
    ) async throws -> Int64

    /// Get sum from precomputed SumIndex
    public func getPrecomputedSum<T: Persistable>(
        type: T.Type,
        indexName: String,
        groupingValues: [any TupleElement]
    ) async throws -> Double
}
```

### Phase 3: Query Extension の最適化

**方針**: 事前計算インデックスがあれば使用、なければフォールバック

#### RankQuery.swift の改善

```swift
// RankIndex/RankQuery.swift

public struct RankQueryBuilder<T: Persistable>: Sendable {

    public func execute() async throws -> [(item: T, rank: Int)] {
        // 1. RankIndexが存在するかチェック
        if let indexInfo = try await findRankIndex() {
            // 2. RankIndexMaintainerを使用（O(k)またはO(n log k)）
            return try await executeWithIndex(indexInfo)
        } else {
            // 3. フォールバック: インメモリ計算（O(n log n)）
            return try await executeInMemory()
        }
    }

    private func executeWithIndex(_ indexInfo: IndexInfo) async throws -> [(item: T, rank: Int)] {
        // RankIndexMaintainerを作成して直接クエリ
        let maintainer = createRankIndexMaintainer(indexInfo)

        switch queryMode {
        case .top(let k):
            let topK = try await maintainer.getTopK(k: k, transaction: transaction)
            return try await fetchItemsWithRanks(topK)
        case .percentile(let p):
            // ...
        }
    }
}
```

#### AggregationQuery.swift の改善

```swift
// AggregationIndex/AggregationQuery.swift

public struct AggregationQueryBuilder<T: Persistable> {

    public func execute() async throws -> [AggregateResult<T>] {
        // GROUP BYなしの単純集約かチェック
        if groupByFieldNames.isEmpty {
            // 事前計算インデックスを使用可能
            return try await executeWithPrecomputedIndexes()
        } else {
            // GROUP BY: インメモリ計算にフォールバック
            return try await executeInMemory()
        }
    }

    private func executeWithPrecomputedIndexes() async throws -> [AggregateResult<T>] {
        var results: [String: AnySendable] = [:]

        for agg in aggregations {
            switch agg.type {
            case .count:
                if let countIndex = findCountIndex() {
                    results[agg.name] = try await queryContext.getPrecomputedCount(...)
                }
            case .sum(let field):
                if let sumIndex = findSumIndex(field) {
                    results[agg.name] = try await queryContext.getPrecomputedSum(...)
                }
            // ...
            }
        }

        return [AggregateResult(groupKey: [:], aggregates: results, count: ...)]
    }
}
```

---

## 実装順序

1. **Phase 1a**: 古いDSLファイルの削除
   - `GroupByQuery.swift` を削除
   - `RankQuery.swift` を削除
   - `SpatialQuery.swift` を削除

2. **Phase 1b**: Query<T>のextensionから関連メソッドを削除
   - `.groupBy()` 系メソッド削除
   - `.top()`, `.bottom()`, `.ranked()`, `.percentile()` 削除
   - `.within()`, `.nearBy()` 削除

3. **Phase 2**: IndexQueryContextの拡張
   - `executeRankQuery()` 追加
   - `getPrecomputedCount()` 追加
   - `getPrecomputedSum()` 追加

4. **Phase 3**: Query Extensionの最適化
   - `RankQuery.swift` をRankIndexMaintainer対応に更新
   - `AggregationQuery.swift` を事前計算インデックス対応に更新

5. **Phase 4**: テストと検証
   - 既存テストの更新
   - 新しいAPIのテスト追加
   - パフォーマンス検証

---

## 移行ガイド（ユーザー向け）

### Before (旧API - 動作しない)
```swift
// ❌ execute()がない
let results = context.fetch(Order.self)
    .groupBy()
    .by(\.region)
    .count()
    .build()
    // .execute() がない！

// ❌ execute()がない
let top10 = context.fetch(Player.self)
    .top(10, by: \.score)
    // .execute() がない！
```

### After (新API)
```swift
// ✅ AggregationIndex モジュールを使用
import AggregationIndex

let results = try await context.aggregate(Order.self)
    .groupBy(\.region)
    .count()
    .execute()

// ✅ RankIndex モジュールを使用
import RankIndex

let top10 = try await context.rank(Player.self)
    .by(\.score)
    .top(10)
    .execute()
```

---

## 考慮事項

### 後方互換性
- 古いAPIを使用しているコードはコンパイルエラーになる
- これは意図的：動作しないAPIを使い続けるよりも良い

### パフォーマンス
- 事前計算インデックスがある場合: O(1) または O(k)
- フォールバック: O(n log n) または O(n)

### インデックスの検出
- Schema から Index 情報を取得
- IndexKind の identifier でマッチング
- 対象フィールドとの一致を確認
