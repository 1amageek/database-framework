# Fusion API 設計ドキュメント

## 概要

Fusion は複数の検索クエリ（Vector, FullText, Spatial, Rank 等）を組み合わせて、
統合されたランキング結果を返す機能です。

### 主な特徴

- **ResultBuilder** による宣言的な構文
- **パイプライン実行**: 順次実行で候補を絞り込み、後段のクエリを高速化
- **並列実行**: `Parallel { }` で複数クエリを同時実行
- **context 不要**: Query 定義時に `context.` の記述が不要
- **自動スコア変換**: 各クエリの結果を自動的に正規化スコアに変換

## アーキテクチャ原則

### 責務の分離

**重要**: DatabaseEngine はプロトコルと汎用インフラのみを提供し、
具体的な Index の知識を持たない。

```
DatabaseEngine (プロトコル・インフラ)
├── FusionQuery プロトコル
├── FusionStage プロトコル
├── FusionBuilder (実行エンジン)
├── ScoredResult (共通型)
└── IndexQueryContext (汎用メソッドのみ)

各 Index モジュール (具体的な実装)
├── VectorIndex/Similar<T>: FusionQuery
├── FullTextIndex/Search<T>: FusionQuery
├── SpatialIndex/Nearby<T>: FusionQuery
├── RankIndex/Rank<T>: FusionQuery
└── ScalarIndex/Filter<T>: FusionQuery
```

### 禁止事項

```swift
// ❌ DatabaseEngine 内で特定の IndexKind を参照してはいけない
// IndexQueryContext.swift
if let vectorKind = descriptor.kind as? VectorIndexKind<T> { ... }  // NG

// ❌ DatabaseEngine に Index 固有のロジックを実装してはいけない
func executeVectorSearchForFusion(...) { ... }  // NG
func computeDistancesForCandidates(...) { ... }  // NG
```

### 許可される実装

```swift
// ✅ DatabaseEngine は汎用メソッドのみ提供
extension IndexQueryContext {
    // アイテム取得（型に依存しない）
    public func fetchItemsByStringIds<T: Persistable>(type: T.Type, ids: [String]) async throws -> [T]

    // トランザクション（汎用）
    public func withTransaction<R>(_ body: ...) async throws -> R

    // サブスペース取得（汎用）
    public func indexSubspace<T: Persistable>(for type: T.Type) async throws -> Subspace
}

// ✅ 各 Index モジュールが FusionQuery を実装
// VectorIndex/Queries/Similar.swift
public struct Similar<T: Persistable>: FusionQuery {
    // VectorIndexKind の知識はここにある（適切な場所）
    // 候補フィルタロジックもここに実装
}
```

## API 設計

### 基本的な使用例

```swift
let results = try await context.fuse(Product.self) {
    // Stage 1: FullText 検索
    Search(\.description)
        .terms(["organic", "coffee"])

    // Stage 2: Vector + Spatial を並列実行
    Parallel {
        Similar(\.embedding, dimensions: 384)
            .query(queryVector, k: 100)

        Nearby(\.location)
            .within(radiusKm: 5, of: userLocation)
    }

    // Stage 3: Rank でリランク
    Rank(\.popularity)
}
.algorithm(.rrf())
.limit(10)
```

### 実行フロー

```
Stage 1: Search(\.description)
         → 結果: 1000件
         → candidates = {Stage 1 の ID}

Stage 2: Parallel { Similar, Nearby }
         → Similar: 候補内のみ検索 (ACORN)
         → Nearby: 候補内のみ検索
         → 結果をマージ
         → candidates = candidates ∩ {Stage 2 の ID}

Stage 3: Rank(\.popularity)
         → 候補内のみランク取得
         → candidates = candidates ∩ {Stage 3 の ID}

Fusion: RRF([Stage1, Stage2a, Stage2b, Stage3])
         → 最終スコア計算
         → limit(10) 適用
```

## Query 定義

### Search (FullText)

```swift
Search(\.description)
    .terms(["swift", "concurrency"])
    .mode(.all)        // .all (AND), .any (OR), .phrase

Search(\.title)
    .terms(["database"])
    .bm25(k1: 1.5, b: 0.8)
```

### Similar (Vector)

```swift
Similar(\.embedding, dimensions: 384)
    .query(queryVector, k: 100)
    .metric(.cosine)   // .cosine, .euclidean, .dotProduct
```

### Nearby (Spatial)

```swift
Nearby(\.location)
    .within(radiusKm: 10, of: centerPoint)

Nearby(\.location)
    .within(bounds: boundingBox)
```

### Rank

```swift
Rank(\.salesRank)
    .order(.ascending)   // 低いrank = 高いスコア

Rank(\.rating)
    .order(.descending)  // 高いrating = 高いスコア
```

### Filter (Scalar)

```swift
Filter(\.category, equals: "electronics")

Filter(\.price)
    .range(100...500)
```

## Fusion アルゴリズム

### Reciprocal Rank Fusion (RRF)

```swift
.algorithm(.rrf(k: 60))
```

各ソースのランクを組み合わせる。複数ソースに出現するドキュメントが高スコア。

```
score(d) = Σ 1/(k + rank_i(d))
```

**Reference**: Cormack et al., "Reciprocal Rank Fusion outperforms Condorcet and
individual Rank Learning Methods" (SIGIR 2009)

### Sum

```swift
.algorithm(.sum)
```

各ソースの正規化スコアを合計。

```
score(d) = Σ normalized_score_i(d)
```

### Max

```swift
.algorithm(.max)
```

各ソースの最大スコアを採用。

```
score(d) = max(score_i(d))
```

### Weighted Sum

```swift
.algorithm(.weighted([0.3, 0.3, 0.2, 0.2]))
```

重み付き合計。**クエリ順**（ステージ順ではない）に重みを適用。

**重要**: Parallel 内の各クエリは別々のソースとしてカウントされる。

```swift
context.fuse(Product.self) {
    Search(...)      // source 0
    Parallel {
        Similar(...) // source 1
        Nearby(...)  // source 2
    }
    Rank(...)        // source 3
}
.algorithm(.weighted([0.3, 0.3, 0.2, 0.2]))  // 4 weights for 4 sources
```

```
score(d) = Σ weight_i × normalized_score_i(d)
```

## コアプロトコル

### FusionQuery

```swift
/// Fusion に参加できるクエリのプロトコル
///
/// **設計原則**:
/// - context は各 Query の初期化時に渡す（execute 時ではない）
/// - execute() は candidates のみを受け取る
/// - 各 Index モジュールが具体的な実装を提供
///
/// **実装場所**: 各 Index モジュール内（DatabaseEngine には含まれない）
public protocol FusionQuery<Item>: Sendable {
    associatedtype Item: Persistable

    /// クエリを実行してスコア付き結果を返す
    /// - Parameter candidates: 前ステージからの候補ID（nil = 制限なし）
    /// - Returns: スコア付き結果の配列（スコアは 0.0〜1.0、高いほど良い）
    func execute(candidates: Set<String>?) async throws -> [ScoredResult<Item>]
}
```

### FusionStage

```swift
/// 実行ステージのプロトコル
///
/// **設計原則**:
/// - FusionQuery と同様、context は初期化時に渡される
/// - DatabaseEngine はプロトコルのみ定義
public protocol FusionStage<Item>: Sendable {
    associatedtype Item: Persistable

    /// ステージを実行
    /// - Parameter candidates: 前ステージからの候補ID（nil = 制限なし）
    /// - Returns: 各クエリの結果配列（並列の場合は複数）
    func execute(candidates: Set<String>?) async throws -> [[ScoredResult<Item>]]
}
```

### IndexQueryContext の役割

```swift
/// DatabaseEngine が提供する汎用インフラ
///
/// **提供するもの**:
/// - アイテム取得（型に依存しない）
/// - トランザクション管理
/// - サブスペース取得
/// - スキーマアクセス
///
/// **提供しないもの**:
/// - Index 固有の検索ロジック（各モジュールで実装）
/// - IndexKind の知識（VectorIndexKind, FullTextIndexKind 等）
public struct IndexQueryContext: Sendable {
    // ✅ 汎用メソッドのみ
    public func fetchItemsByStringIds<T: Persistable>(type: T.Type, ids: [String]) async throws -> [T]
    public func withTransaction<R>(_ body: ...) async throws -> R
    public func indexSubspace<T: Persistable>(for type: T.Type) async throws -> Subspace
    public var schema: Schema { get }

    // ❌ Index 固有のメソッドは含まない
    // func executeVectorSearchForFusion(...) // NG: VectorIndex に移動
    // func computeDistancesForCandidates(...) // NG: VectorIndex に移動
}
```

### ScoredResult

```swift
/// スコア付き検索結果
public struct ScoredResult<T: Persistable>: Sendable {
    /// 検索にマッチしたアイテム
    public let item: T

    /// 正規化スコア（0.0〜1.0、高いほど良い）
    public let score: Double
}
```

### ScoredQuery

```swift
/// スコア付きクエリをラップする型
public struct ScoredQuery<T: Persistable>: Sendable {
    private let _execute: @Sendable (IndexQueryContext, Set<String>?) async throws -> [ScoredResult<T>]

    public init(_ execute: @escaping @Sendable (IndexQueryContext, Set<String>?) async throws -> [ScoredResult<T>]) {
        self._execute = execute
    }

    public func execute(context: IndexQueryContext, candidates: Set<String>? = nil) async throws -> [ScoredResult<T>] {
        try await _execute(context, candidates)
    }
}
```

## ResultBuilder

### FusionStageBuilder

```swift
@resultBuilder
public struct FusionStageBuilder<T: Persistable> {

    /// 複数ステージをビルド
    public static func buildBlock(_ stages: (any FusionStage<T>)...) -> [any FusionStage<T>] {
        stages
    }

    /// 単一クエリを SingleStage に変換
    public static func buildExpression(_ query: some FusionQuery<T>) -> any FusionStage<T> {
        SingleStage(query: query)
    }

    /// Parallel をそのまま通す
    public static func buildExpression(_ parallel: Parallel<T>) -> any FusionStage<T> {
        parallel
    }

    /// 条件分岐対応
    public static func buildOptional(_ stage: (any FusionStage<T>)?) -> [any FusionStage<T>] {
        stage.map { [$0] } ?? []
    }

    public static func buildEither(first stage: any FusionStage<T>) -> any FusionStage<T> {
        stage
    }

    public static func buildEither(second stage: any FusionStage<T>) -> any FusionStage<T> {
        stage
    }
}
```

### FusionQueryBuilder

```swift
@resultBuilder
public struct FusionQueryBuilder<T: Persistable> {

    /// 複数クエリをビルド（Parallel 内部用）
    public static func buildBlock(_ queries: (any FusionQuery<T>)...) -> [any FusionQuery<T>] {
        queries
    }

    public static func buildExpression(_ query: some FusionQuery<T>) -> any FusionQuery<T> {
        query
    }
}
```

## ステージ実装

### SingleStage

```swift
/// 単一クエリのステージ
public struct SingleStage<T: Persistable>: FusionStage {
    public typealias Item = T

    let query: any FusionQuery<T>

    public func execute(
        context: IndexQueryContext,
        candidates: Set<String>?
    ) async throws -> [[ScoredResult<T>]] {
        let results = try await query.execute(context: context, candidates: candidates)
        return [results]
    }
}
```

### Parallel

```swift
/// 複数クエリを並列実行するステージ
public struct Parallel<T: Persistable>: FusionStage {
    public typealias Item = T

    let queries: [any FusionQuery<T>]

    public init(@FusionQueryBuilder<T> _ content: () -> [any FusionQuery<T>]) {
        self.queries = content()
    }

    public func execute(
        context: IndexQueryContext,
        candidates: Set<String>?
    ) async throws -> [[ScoredResult<T>]] {
        try await withThrowingTaskGroup(of: [ScoredResult<T>].self) { group in
            for query in queries {
                group.addTask {
                    try await query.execute(context: context, candidates: candidates)
                }
            }

            var results: [[ScoredResult<T>]] = []
            for try await result in group {
                results.append(result)
            }
            return results
        }
    }
}
```

## FusionBuilder

```swift
public struct FusionBuilder<T: Persistable>: Sendable {

    private let queryContext: IndexQueryContext
    private let stages: [any FusionStage<T>]
    private var algorithm: Algorithm = .rrf()
    private var limitCount: Int?

    public enum Algorithm: Sendable {
        case rrf(k: Int = 60)
        case sum
        case max
        case weighted([Double])
    }

    internal init(queryContext: IndexQueryContext, stages: [any FusionStage<T>]) {
        self.queryContext = queryContext
        self.stages = stages
    }

    public func algorithm(_ algorithm: Algorithm) -> Self {
        var copy = self
        copy.algorithm = algorithm
        return copy
    }

    public func limit(_ count: Int) -> Self {
        var copy = self
        copy.limitCount = count
        return copy
    }

    public func execute() async throws -> [ScoredResult<T>] {
        guard !stages.isEmpty else { return [] }

        var candidateIds: Set<String>? = nil
        var allResults: [[ScoredResult<T>]] = []

        for (index, stage) in stages.enumerated() {
            // Stage 0 は候補制限なし、Stage 1以降は前ステージの候補に限定
            let stageResults = try await stage.execute(
                context: queryContext,
                candidates: index > 0 ? candidateIds : nil
            )

            // 候補ID を更新（交差）
            var stageIds: Set<String> = []
            for results in stageResults {
                stageIds.formUnion(results.map { "\($0.item.id)" })
            }
            candidateIds = candidateIds.map { $0.intersection(stageIds) } ?? stageIds

            allResults.append(contentsOf: stageResults)
        }

        // Fusion アルゴリズム適用
        var fused = applyAlgorithm(algorithm, to: allResults)

        if let limit = limitCount {
            fused = Array(fused.prefix(limit))
        }

        return fused
    }

    private func applyAlgorithm(
        _ algorithm: Algorithm,
        to sources: [[ScoredResult<T>]]
    ) -> [ScoredResult<T>] {
        var scores: [String: (item: T, score: Double)] = [:]

        switch algorithm {
        case .rrf(let k):
            for source in sources {
                for (rank, result) in source.enumerated() {
                    let id = "\(result.item.id)"
                    let rrfScore = 1.0 / Double(k + rank + 1)
                    scores[id] = (result.item, (scores[id]?.score ?? 0) + rrfScore)
                }
            }

        case .sum:
            for source in sources {
                for result in source {
                    let id = "\(result.item.id)"
                    scores[id] = (result.item, (scores[id]?.score ?? 0) + result.score)
                }
            }

        case .max:
            for source in sources {
                for result in source {
                    let id = "\(result.item.id)"
                    let current = scores[id]?.score ?? 0
                    scores[id] = (result.item, max(current, result.score))
                }
            }

        case .weighted(let weights):
            for (sourceIndex, source) in sources.enumerated() {
                let weight = sourceIndex < weights.count ? weights[sourceIndex] : 1.0
                for result in source {
                    let id = "\(result.item.id)"
                    scores[id] = (result.item, (scores[id]?.score ?? 0) + result.score * weight)
                }
            }
        }

        return scores.values
            .sorted { $0.score > $1.score }
            .map { ScoredResult(item: $0.item, score: $0.score) }
    }
}
```

## FDBContext Extension

```swift
extension FDBContext {

    /// Fusion クエリを開始
    ///
    /// **Usage**:
    /// ```swift
    /// let results = try await context.fuse(Product.self) {
    ///     Search(\.description).terms(["coffee"])
    ///     Similar(\.embedding, dimensions: 384).query(vector, k: 100)
    /// }
    /// .algorithm(.rrf())
    /// .limit(10)
    /// ```
    public func fuse<T: Persistable>(
        _ type: T.Type,
        @FusionStageBuilder<T> _ content: () -> [any FusionStage<T>]
    ) -> FusionBuilder<T> {
        FusionBuilder(queryContext: indexQueryContext, stages: content())
    }
}
```

## Query 実装例

**注意**: 以下の実装は各 Index モジュールに配置される。DatabaseEngine には含まれない。

### Search (FullTextIndex/Fusion/Search.swift)

```swift
// FullTextIndex モジュール内に配置
import DatabaseEngine
import FullText

public struct Search<T: Persistable>: FusionQuery {
    public typealias Item = T

    private let queryContext: IndexQueryContext
    private let fieldName: String
    private var searchTerms: [String] = []
    private var matchMode: TextMatchMode = .all
    private var bm25Params: BM25Parameters = .default

    public init(_ keyPath: KeyPath<T, String>, context: IndexQueryContext) {
        self.queryContext = context
        self.fieldName = T.fieldName(for: keyPath)
    }

    public func terms(_ terms: [String], mode: TextMatchMode = .all) -> Self {
        var copy = self
        copy.searchTerms = terms
        copy.matchMode = mode
        return copy
    }

    public func bm25(k1: Float = 1.2, b: Float = 0.75) -> Self {
        var copy = self
        copy.bm25Params = BM25Parameters(k1: k1, b: b)
        return copy
    }

    public func execute(candidates: Set<String>?) async throws -> [ScoredResult<T>] {
        guard !searchTerms.isEmpty else { return [] }

        // FullTextIndex モジュール内の既存ロジックを活用
        let builder = FullTextQueryBuilder(queryContext: queryContext, fieldName: fieldName)
            .terms(searchTerms, mode: matchMode)
            .bm25(k1: bm25Params.k1, b: bm25Params.b)

        var results = try await builder.executeWithScores()

        // 候補フィルタ（このモジュールで処理）
        if let candidateIds = candidates {
            results = results.filter { candidateIds.contains("\($0.item.id)") }
        }

        // スコア正規化
        guard let maxScore = results.map(\.score).max(), maxScore > 0 else {
            return results.map { ScoredResult(item: $0.item, score: 1.0) }
        }

        return results.map {
            ScoredResult(item: $0.item, score: $0.score / maxScore)
        }
    }
}
```

### Similar (VectorIndex/Fusion/Similar.swift)

```swift
// VectorIndex モジュール内に配置
import DatabaseEngine
import Vector

public struct Similar<T: Persistable>: FusionQuery {
    public typealias Item = T

    private let queryContext: IndexQueryContext
    private let fieldName: String
    private let dimensions: Int
    private var queryVector: [Float]?
    private var k: Int = 10
    private var metric: VectorDistanceMetric = .cosine

    public init(_ keyPath: KeyPath<T, [Float]>, dimensions: Int, context: IndexQueryContext) {
        self.queryContext = context
        self.fieldName = T.fieldName(for: keyPath)
        self.dimensions = dimensions
    }

    public func query(_ vector: [Float], k: Int) -> Self {
        var copy = self
        copy.queryVector = vector
        copy.k = k
        return copy
    }

    public func metric(_ metric: VectorDistanceMetric) -> Self {
        var copy = self
        copy.metric = metric
        return copy
    }

    public func execute(candidates: Set<String>?) async throws -> [ScoredResult<T>] {
        guard let vector = queryVector else { return [] }

        let results: [(item: T, distance: Double)]

        if let candidateIds = candidates {
            // 候補フィルタ付き検索（VectorIndex モジュール内で完結）
            results = try await executeWithCandidates(candidateIds)
        } else {
            // 通常の kNN 検索
            results = try await executeStandardSearch()
        }

        // Distance → Score (低い距離 = 高いスコア)
        guard let maxDist = results.map(\.distance).max(), maxDist > 0 else {
            return results.map { ScoredResult(item: $0.item, score: 1.0) }
        }

        return results.map {
            ScoredResult(item: $0.item, score: 1.0 - $0.distance / maxDist)
        }
    }

    // MARK: - Private Methods (VectorIndex モジュール内に閉じた実装)

    private func executeStandardSearch() async throws -> [(item: T, distance: Double)] {
        let builder = VectorQueryBuilder(queryContext: queryContext, fieldName: fieldName, dimensions: dimensions)
            .query(queryVector!, k: k)
            .metric(metric)

        return try await builder.execute()
    }

    private func executeWithCandidates(_ candidateIds: Set<String>) async throws -> [(item: T, distance: Double)] {
        let bruteForceThreshold = 1000

        if candidateIds.count <= bruteForceThreshold {
            // 小規模候補: ブルートフォース
            return try await computeDistancesForCandidates(candidateIds)
        } else {
            // 大規模候補: 拡張 kNN + フィルタ
            return try await executeExpandedSearch(candidateIds)
        }
    }

    private func computeDistancesForCandidates(_ candidateIds: Set<String>) async throws -> [(item: T, distance: Double)] {
        // VectorIndex モジュールは fieldName を知っている
        let items = try await queryContext.fetchItemsByStringIds(type: T.self, ids: Array(candidateIds))

        var results: [(item: T, distance: Double)] = []

        for item in items {
            // fieldName は初期化時に設定済み（IndexDescriptor 不要）
            guard let vector = item[dynamicMember: fieldName] as? [Float],
                  vector.count == dimensions else {
                continue
            }

            let distance = computeDistance(queryVector!, vector, metric: metric)
            results.append((item: item, distance: distance))
        }

        results.sort { $0.distance < $1.distance }
        return Array(results.prefix(k))
    }

    private func executeExpandedSearch(_ candidateIds: Set<String>) async throws -> [(item: T, distance: Double)] {
        // k 拡張公式
        let sqrtScaled = Int(Double(candidateIds.count).squareRoot()) * k
        let expandedK = min(
            candidateIds.count,
            max(k * 10, candidateIds.count / 2, k + 2000, sqrtScaled)
        )

        let builder = VectorQueryBuilder(queryContext: queryContext, fieldName: fieldName, dimensions: dimensions)
            .query(queryVector!, k: expandedK)
            .metric(metric)

        var results = try await builder.execute()

        // 候補フィルタ
        results = results.filter { candidateIds.contains("\($0.item.id)") }

        return Array(results.prefix(k))
    }

    private func computeDistance(_ a: [Float], _ b: [Float], metric: VectorDistanceMetric) -> Double {
        // 距離計算（VectorIndex モジュール内のユーティリティ）
        switch metric {
        case .euclidean:
            var sum: Float = 0
            for i in 0..<a.count {
                let diff = a[i] - b[i]
                sum += diff * diff
            }
            return Double(sum.squareRoot())

        case .cosine:
            var dot: Float = 0, normA: Float = 0, normB: Float = 0
            for i in 0..<a.count {
                dot += a[i] * b[i]
                normA += a[i] * a[i]
                normB += b[i] * b[i]
            }
            let denom = normA.squareRoot() * normB.squareRoot()
            return denom == 0 ? 1.0 : Double(1.0 - dot / denom)

        case .dotProduct:
            var dot: Float = 0
            for i in 0..<a.count { dot += a[i] * b[i] }
            return Double(-dot)
        }
    }
}
```

### Nearby (SpatialIndex/Fusion/Nearby.swift)

```swift
// SpatialIndex モジュール内に配置
import DatabaseEngine
import Spatial

public struct Nearby<T: Persistable>: FusionQuery {
    public typealias Item = T

    private let queryContext: IndexQueryContext
    private let fieldName: String
    private var constraint: SpatialConstraint?
    private var referencePoint: GeoPoint?

    public init(_ keyPath: KeyPath<T, GeoPoint>, context: IndexQueryContext) {
        self.queryContext = context
        self.fieldName = T.fieldName(for: keyPath)
    }

    public func within(radiusKm: Double, of center: GeoPoint) -> Self {
        var copy = self
        copy.constraint = SpatialConstraint(
            type: .withinDistance(
                center: (latitude: center.latitude, longitude: center.longitude),
                radiusMeters: radiusKm * 1000.0
            )
        )
        copy.referencePoint = center
        return copy
    }

    public func execute(candidates: Set<String>?) async throws -> [ScoredResult<T>] {
        guard let constraint = constraint else { return [] }

        // SpatialIndex モジュール内の既存ロジックを活用
        let builder = SpatialQueryBuilder(queryContext: queryContext, fieldName: fieldName)

        var results = try await builder.execute()

        // 候補フィルタ（このモジュールで処理）
        if let candidateIds = candidates {
            results = results.filter { candidateIds.contains("\($0.item.id)") }
        }

        // 距離スコア計算
        guard let ref = referencePoint else {
            return results.map { ScoredResult(item: $0.item, score: 1.0) }
        }

        let itemsWithDistance: [(item: T, distance: Double)] = results.compactMap { result in
            guard let location = result.item[dynamicMember: fieldName] as? GeoPoint else {
                return nil
            }
            return (item: result.item, distance: ref.distance(to: location))
        }

        guard let maxDist = itemsWithDistance.map(\.distance).max(), maxDist > 0 else {
            return results.map { ScoredResult(item: $0.item, score: 1.0) }
        }

        return itemsWithDistance.map {
            ScoredResult(item: $0.item, score: 1.0 - $0.distance / maxDist)
        }
    }
}
```

### Rank (RankIndex/Fusion/Rank.swift)

```swift
// RankIndex モジュール内に配置
import DatabaseEngine
import Rank

public struct Rank<T: Persistable>: FusionQuery {
    public typealias Item = T

    public enum Order: Sendable {
        case ascending   // 低い値 = 高いスコア (例: salesRank)
        case descending  // 高い値 = 高いスコア (例: rating)
    }

    private let queryContext: IndexQueryContext
    private let fieldName: String
    private var order: Order = .ascending

    public init<V: Comparable>(_ keyPath: KeyPath<T, V>, context: IndexQueryContext) {
        self.queryContext = context
        self.fieldName = T.fieldName(for: keyPath)
    }

    public func order(_ order: Order) -> Self {
        var copy = self
        copy.order = order
        return copy
    }

    public func execute(candidates: Set<String>?) async throws -> [ScoredResult<T>] {
        // 候補のアイテムを取得（Rank は他のクエリの後に使う前提）
        guard let candidateIds = candidates else {
            return []
        }

        let items = try await queryContext.fetchItemsByStringIds(
            type: T.self,
            ids: Array(candidateIds)
        )

        // フィールド値でソート
        let sorted: [(item: T, value: Double)] = items.compactMap { item in
            guard let value = item[dynamicMember: fieldName] else { return nil }
            let doubleValue: Double
            if let v = value as? Double { doubleValue = v }
            else if let v = value as? Int { doubleValue = Double(v) }
            else if let v = value as? Float { doubleValue = Double(v) }
            else { return nil }
            return (item: item, value: doubleValue)
        }

        let ranked: [(item: T, value: Double)]
        switch order {
        case .ascending:
            ranked = sorted.sorted { $0.value < $1.value }
        case .descending:
            ranked = sorted.sorted { $0.value > $1.value }
        }

        // ランク → スコア (1位 = 1.0, 最下位 = 0.0)
        let count = Double(ranked.count)
        return ranked.enumerated().map { index, item in
            let score = count > 1 ? 1.0 - Double(index) / (count - 1) : 1.0
            return ScoredResult(item: item.item, score: score)
        }
    }
}
```

### Filter (ScalarIndex/Fusion/Filter.swift)

```swift
// ScalarIndex モジュール内に配置
import DatabaseEngine

public struct Filter<T: Persistable>: FusionQuery {
    public typealias Item = T

    private let queryContext: IndexQueryContext
    private let fieldName: String
    private var predicate: (@Sendable (T) -> Bool)?

    public init<V: Equatable & Sendable>(
        _ keyPath: KeyPath<T, V>,
        equals value: V,
        context: IndexQueryContext
    ) {
        self.queryContext = context
        self.fieldName = T.fieldName(for: keyPath)
        self.predicate = { item in item[keyPath: keyPath] == value }
    }

    public func execute(candidates: Set<String>?) async throws -> [ScoredResult<T>] {
        // ScalarIndex モジュール内の既存ロジックを活用
        let indexName = "\(T.persistableType)_\(fieldName)"

        var results = try await queryContext.executeScalarIndexSearch(
            type: T.self,
            indexName: indexName,
            fieldName: fieldName,
            value: /* extracted from predicate */
        )

        // 候補フィルタ（このモジュールで処理）
        if let candidateIds = candidates {
            results = results.filter { candidateIds.contains("\($0.id)") }
        }

        // フィルタはスコア 1.0（通過 or 不通過）
        return results.map { ScoredResult(item: $0, score: 1.0) }
    }
}
```

## 使用パターン

### パターン 1: シンプルな Hybrid Search

```swift
// Vector + FullText
let results = try await context.fuse(Article.self) {
    Search(\.content).terms(["machine", "learning"])
    Similar(\.embedding, dimensions: 768).query(queryVector, k: 50)
}
.algorithm(.rrf())
.limit(10)
```

### パターン 2: フィルタ → スコアリング

```swift
// カテゴリフィルタ → Vector + FullText 並列
let results = try await context.fuse(Product.self) {
    Filter(\.category, equals: "electronics")

    Parallel {
        Search(\.description).terms(["wireless", "headphones"])
        Similar(\.embedding, dimensions: 384).query(queryVector, k: 100)
    }
}
.algorithm(.sum)
.limit(20)
```

### パターン 3: 地理 + テキスト + 人気度

```swift
// 近くの店舗 → テキスト検索 → 人気順
let results = try await context.fuse(Store.self) {
    Nearby(\.location).within(radiusKm: 5, of: userLocation)
    Search(\.description).terms(["coffee", "wifi"])
    Rank(\.rating).order(.descending)
}
.algorithm(.weighted([0.3, 0.4, 0.3]))
.limit(10)
```

### パターン 4: 複雑なパイプライン

```swift
let results = try await context.fuse(Product.self) {
    // Stage 1: 高速フィルタ
    Filter(\.isActive, equals: true)

    // Stage 2: テキストで候補生成
    Search(\.title).terms(userQuery.split(separator: " ").map(String.init))

    // Stage 3: Vector + Spatial 並列
    Parallel {
        Similar(\.embedding, dimensions: 384)
            .query(queryEmbedding, k: 200)

        Nearby(\.warehouseLocation)
            .within(radiusKm: 50, of: userLocation)
    }

    // Stage 4: 価格・在庫でリランク
    Rank(\.price).order(.ascending)
}
.algorithm(.rrf(k: 60))
.limit(20)
```

## 性能考慮事項

### パイプライン最適化

1. **高速クエリを先に**: Scalar Filter → FullText → Vector の順が効率的
2. **候補絞り込み**: 各ステージで候補が減るため、後段のクエリが高速化
3. **ACORN 活用**: Vector 検索は `candidates` を受け取ると ACORN フィルタ検索を使用

### 並列実行

- `Parallel { }` 内のクエリは `TaskGroup` で同時実行
- I/O バウンドなクエリ（FDB アクセス）は並列化の恩恵が大きい

### メモリ使用量

- 各ステージの結果はメモリに保持される
- 大量結果の場合は早めに `limit()` を設定

## ファイル構成

### DatabaseEngine（プロトコル・インフラのみ）

```
Sources/DatabaseEngine/Fusion/
├── FusionDesign.md           # この設計ドキュメント
├── FusionQuery.swift         # FusionQuery プロトコル
├── FusionStage.swift         # FusionStage プロトコル、SingleStage、Parallel
├── FusionBuilder.swift       # FusionBuilder、ResultBuilder
└── ScoredResult.swift        # ScoredResult 型
```

### 各 Index モジュール（具体的な FusionQuery 実装）

```
Sources/VectorIndex/
├── VectorQuery.swift         # 既存の VectorQueryBuilder
└── Fusion/
    └── Similar.swift         # FusionQuery 実装（候補フィルタ含む）

Sources/FullTextIndex/
├── FullTextQuery.swift       # 既存の FullTextQueryBuilder
└── Fusion/
    └── Search.swift          # FusionQuery 実装（BM25 スコアリング含む）

Sources/SpatialIndex/
├── SpatialQuery.swift        # 既存の SpatialQueryBuilder
└── Fusion/
    └── Nearby.swift          # FusionQuery 実装

Sources/RankIndex/
├── RankQuery.swift           # 既存の RankQueryBuilder
└── Fusion/
    └── Rank.swift            # FusionQuery 実装

Sources/ScalarIndex/
└── Fusion/
    └── Filter.swift          # FusionQuery 実装
```

### モジュール依存関係

```
Database (re-export)
    │
    ├── VectorIndex ──────┐
    ├── FullTextIndex ────┼──→ DatabaseEngine (Fusion protocols)
    ├── SpatialIndex ─────┤
    ├── RankIndex ────────┤
    └── ScalarIndex ──────┘
```

**重要**: 各 Index モジュールは DatabaseEngine の Fusion プロトコルに依存するが、
DatabaseEngine は各 Index モジュールに依存しない（循環依存なし）。

## 参考文献

- Cormack, G. V., Clarke, C. L., & Buettcher, S. (2009). "Reciprocal Rank Fusion outperforms Condorcet and individual Rank Learning Methods". SIGIR '09.
- Robertson, S., & Zaragoza, H. (2009). "The Probabilistic Relevance Framework: BM25 and Beyond". Foundations and Trends in Information Retrieval.
- Patel, L., et al. (2024). "ACORN: Performant and Predicate-Agnostic Search Over Vector Embeddings and Structured Data". SIGMOD '24.
