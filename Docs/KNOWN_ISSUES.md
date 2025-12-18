# Known Issues

このドキュメントは、database-framework の既知の設計上の問題と修正方針を記録します。

## Issue #1: IndexMaintenanceService が IndexKindMaintainable を使用していない

### 重要度
**高** - コアアーキテクチャの整合性に関わる問題

### 概要

`IndexMaintenanceService.updateIndexes()` が `IndexKindMaintainable` プロトコルを使用せず、`kindIdentifier` に基づくswitch文をハードコードしている。これにより、プロトコルベースの拡張性という設計目標が損なわれている。

### 問題の詳細

**設計意図** (CLAUDE.md記載):
```
IndexKind ──▶ IndexKindMaintainable ──▶ IndexMaintainer
(metadata)        (bridge)              (runtime)
```

**実際の実装**:
```
OnlineIndexer ───▶ IndexKindMaintainable ✅ (正しく使用)
Migration     ───▶ IndexKindMaintainable ✅ (正しく使用)
FDBDataStore  ───▶ IndexMaintenanceService ───▶ switch文 ❌
                   (count, sum, min/max のみ対応)
                   (Vector, Graph, FullText は default へ)
```

**問題のコード** (`IndexMaintenanceService.swift:88-129`):
```swift
switch kindIdentifier {
case "count":
    try await updateCountIndex(...)
case "sum":
    try await updateSumIndex(...)
case "min", "max":
    try await updateMinMaxIndex(...)
default:
    // Vector, Graph, FullText はここに落ちる！
    try await updateScalarIndex(...)  // ❌ 正しく維持されない
}
```

### 影響

1. **Vector/Graph/FullTextインデックスが正しく維持されない可能性**
   - 通常のCRUD操作（insert/update/delete）で `updateScalarIndex` が呼ばれる
   - これらのインデックスは独自のキー構造を持つため、正しく維持されない

2. **拡張性の欠如**
   - 新しいインデックス型を追加する際、`IndexMaintenanceService` の修正が必要
   - これはプロトコルベースの拡張性という設計目標に反する

3. **コードの重複**
   - `EntityIndexBuilder` (Migration用) と `IndexMaintenanceService` (CRUD用) で異なるパスが存在
   - 同じインデックス型に対して2箇所でメンテナンスロジックを維持する必要がある

### 修正方針

**Option A: IndexMaintenanceService を IndexKindMaintainable ベースに書き換え**

```swift
// IndexMaintenanceService.swift (修正後)
func updateIndexes<T: Persistable>(
    oldModel: T?,
    newModel: T?,
    id: Tuple,
    transaction: any TransactionProtocol
) async throws {
    let indexDescriptors = T.indexDescriptors
    guard !indexDescriptors.isEmpty else { return }

    for descriptor in indexDescriptors {
        let state = indexStates[descriptor.name] ?? .disabled
        guard state.shouldMaintain else { continue }

        let indexSubspaceForIndex = indexSubspace.subspace(descriptor.name)

        // IndexKindMaintainable を使用してメンテナを取得
        guard let maintainable = descriptor.kind as? any IndexKindMaintainable else {
            logger.warning("Index '\(descriptor.name)' does not conform to IndexKindMaintainable")
            continue
        }

        let maintainer = maintainable.makeIndexMaintainer(
            index: descriptor,
            subspace: indexSubspaceForIndex,
            configurations: [:] // TODO: IndexConfiguration の取得方法を検討
        )

        try await maintainer.updateIndex(
            oldItem: oldModel,
            newItem: newModel,
            transaction: transaction
        )
    }
}
```

**Option B: IndexMaintenanceService を廃止し、EntityIndexBuilder のパターンに統一**

- `FDBDataStore` が直接 `IndexKindMaintainable` を使用
- `IndexMaintenanceService` の責務を `FDBDataStore` に統合

### 関連ファイル

- `Sources/DatabaseEngine/Internal/IndexMaintenanceService.swift`
- `Sources/DatabaseEngine/Internal/FDBDataStore.swift`
- `Sources/DatabaseEngine/Index/IndexKindMaintainable.swift`
- `Sources/DatabaseEngine/Migration/EntityIndexBuilder.swift`

### テスト計画

修正後、以下のテストを追加する必要がある：

1. **VectorIndex CRUD テスト**
   - insert後にVectorインデックスが正しく構築されることを確認
   - update後にVectorインデックスが正しく更新されることを確認
   - delete後にVectorインデックスが正しくクリアされることを確認

2. **GraphIndex CRUD テスト**
   - 同様にGraph/RDFインデックスの維持を確認

3. **FullTextIndex CRUD テスト**
   - 同様にFullTextインデックスの維持を確認

---

## Issue #2: Fusion API のトランザクション一貫性

### 重要度
**中** - データ一貫性に関わる問題

### 概要

Fusion APIの各ステージがそれぞれ独自のトランザクションを開始しており、Fusion全体が単一トランザクション内で実行されていない。

### 問題の詳細

**問題のコード** (`IndexQueryContext.swift:148-152`):
```swift
public func withTransaction<R: Sendable>(
    _ body: @Sendable @escaping (any TransactionProtocol) async throws -> R
) async throws -> R {
    // 毎回新しいトランザクションを開始
    return try await context.container.database.withTransaction(body)
}
```

各FusionQueryの `execute()` メソッドが `queryContext.withTransaction()` を呼び出すと、それぞれ別のトランザクションが開始される。

### 影響

1. **スナップショットの不一致**
   - Stage 1 が読み取った時点と Stage 2 が読み取った時点で、データが異なる可能性
   - 例: Stage 1 で取得した候補IDが、Stage 2 実行時には削除されている

2. **RAGパイプラインでの問題**
   - Vector検索とFullText検索が異なる時点のデータを参照
   - 結果の整合性が保証されない

### 修正方針

**Option A: FusionBuilder でトランザクションを管理**

```swift
// FusionBuilder.swift (修正後)
public func execute() async throws -> [ScoredResult<T>] {
    return try await queryContext.context.container.database.withTransaction { transaction in
        // トランザクションをステージに渡す
        let transactionContext = queryContext.withTransaction(transaction)

        var candidateIds: Set<String>? = nil
        var allResults: [[ScoredResult<T>]] = []

        for (index, stage) in stages.enumerated() {
            let stageResults = try await stage.execute(
                context: transactionContext,
                candidates: index > 0 ? candidateIds : nil
            )
            // ...
        }
        // ...
    }
}
```

**Option B: ReadVersionCache を活用**

- Fusionクエリ開始時に ReadVersion を取得
- 各ステージで同じ ReadVersion を使用してスナップショット一貫性を確保

### 関連ファイル

- `Sources/DatabaseEngine/QueryPlanner/IndexQueryContext.swift`
- `Sources/DatabaseEngine/Fusion/FusionBuilder.swift`
- `Sources/DatabaseEngine/Fusion/FusionQuery.swift`

### テスト計画

1. **並行更新テスト**
   - Fusionクエリ実行中にデータを更新
   - すべてのステージが同じスナップショットを参照することを確認

---

## Issue #3: RAG パイプラインのテスト不足

### 重要度
**低** - 機能は実装済みだがテストが不足

### 概要

Vector + FullText の組み合わせ（典型的なRAGパイプライン）のテストが存在しない。

### 現状

- Fusion API の設計ドキュメント (`FusionDesign.md`) には明確にパターンが記載
- 個別のFusionステージ（Graph, Bitmap, Leaderboard）のテストは存在
- Vector + FullText の統合テストが欠如

### 修正方針

以下のテストを追加：

```swift
// Tests/DatabaseEngineTests/Fusion/RAGPipelineTests.swift

@Suite("RAG Pipeline Tests")
struct RAGPipelineTests {

    @Test func testVectorPlusFullTextFusion() async throws {
        // Setup: Vector + FullText インデックスを持つモデル
        // Execute: context.fuse() で両方を組み合わせ
        // Verify: RRF スコアが正しく計算されることを確認
    }

    @Test func testHybridSearchWithCandidateFiltering() async throws {
        // FullText で候補を絞り込み
        // Vector で類似度計算（ACORN パターン）
    }
}
```

### 関連ファイル

- `Sources/VectorIndex/Fusion/Similar.swift`
- `Sources/FullTextIndex/Fusion/Search.swift`
- `Tests/DatabaseEngineTests/Fusion/` (新規追加)
