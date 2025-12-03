# FDB Feature Integration Design

## 概要

fdb-swift-bindings が提供する全機能を database-framework で活用するための設計。

## 統合する機能

| 機能 | 用途 | 統合箇所 |
|------|------|---------|
| `TransactionConfiguration` | 優先度・タイムアウト制御 | 全コンポーネント |
| `ReadVersionCache` + `setReadVersion` | GRV キャッシュ | FDBContainer |
| `getCommittedVersion` | キャッシュ更新 | FDBContainer |
| `StreamingMode` | 範囲クエリ最適化 | IndexSearcher |
| `getRangeSplitPoints` | 並列スキャン | OnlineIndexer |
| `getEstimatedRangeSizeBytes` | 統計・サイズ推定 | StatisticsProvider |
| `getApproximateSize` | トランザクション監視 | FDBContainer |

---

## 1. TransactionConfiguration の全面活用

### 1.1 DatabaseProtocol 拡張

```swift
// fdb-swift-bindings/Sources/FoundationDB/FoundationdDB.swift に追加

extension DatabaseProtocol {
    /// Execute transaction with configuration
    public func withTransaction<T: Sendable>(
        configuration: TransactionConfiguration,
        _ operation: @Sendable (any TransactionProtocol) async throws -> T
    ) async throws -> T
}
```

### 1.2 用途別デフォルト設定

| コンポーネント | 設定 | 理由 |
|--------------|------|------|
| OnlineIndexer | `.batch` | バックグラウンド処理、低優先度 |
| Migration | `.system` | スキーマ変更、最高優先度 |
| PlanExecutor (読み取り) | `.readOnly` | GRV キャッシュ有効 |
| FDBContext.save | `.default` | 通常の書き込み |
| StatisticsProvider | `.batch` | バックグラウンド統計収集 |

---

## 2. ReadVersionCache 統合

### 2.1 FDBContainer への統合

```swift
public final class FDBContainer {
    /// Read version cache for GRV optimization
    private let readVersionCache = ReadVersionCache()

    public func withTransaction<T: Sendable>(
        configuration: TransactionConfiguration = .default,
        _ operation: @Sendable (any TransactionProtocol) async throws -> T
    ) async throws -> T {
        let transaction = try database.createTransaction()
        try transaction.apply(configuration)

        // GRV キャッシュ適用
        if configuration.useGrvCache {
            if let cachedVersion = readVersionCache.getCachedVersion(
                semantics: .bounded(seconds: 5.0)
            ) {
                transaction.setReadVersion(cachedVersion)
            }
        }

        // ... 実行 ...

        // コミット後にキャッシュ更新
        if committed {
            let commitVersion = try transaction.getCommittedVersion()
            readVersionCache.recordCommitVersion(commitVersion)
        }

        return result
    }
}
```

---

## 3. StreamingMode 自動選択

### 3.1 選択ロジック

```swift
extension FDB.StreamingMode {
    /// クエリ特性に基づいて最適な StreamingMode を選択
    static func forQuery(
        estimatedRows: Int?,
        hasLimit: Bool,
        isFullScan: Bool
    ) -> FDB.StreamingMode {
        // 件数制限あり → exact で効率化
        if hasLimit, let rows = estimatedRows, rows <= 100 {
            return .exact
        }

        // フルスキャン → wantAll で全データ取得
        if isFullScan {
            return .wantAll
        }

        // 大量データ → serial で最大スループット
        if let rows = estimatedRows, rows > 10000 {
            return .serial
        }

        // デフォルト
        return .iterator
    }
}
```

### 3.2 IndexSearcher への統合

```swift
// IndexSearcher.swift
func scanRange(..., streamingMode: FDB.StreamingMode? = nil) -> AsyncStream<...> {
    let mode = streamingMode ?? .forQuery(
        estimatedRows: statistics?.estimatedRows,
        hasLimit: limit != nil,
        isFullScan: isFullTableScan
    )

    return transaction.getRange(
        from: beginSelector,
        to: endSelector,
        limit: limit ?? 0,
        streamingMode: mode
    )
}
```

---

## 4. 並列スキャン (getRangeSplitPoints)

### 4.1 OnlineIndexer での活用

```swift
// OnlineIndexer.swift
func buildIndexParallel() async throws {
    let (beginKey, endKey) = recordSubspace.range()

    // サイズ推定
    let estimatedSize = try await transaction.getEstimatedRangeSizeBytes(
        beginKey: beginKey,
        endKey: endKey
    )

    // 小さい場合は単一スキャン
    guard estimatedSize > 10_000_000 else {  // 10MB
        return try await buildIndexSequential()
    }

    // 分割点取得
    let splitPoints = try await transaction.getRangeSplitPoints(
        beginKey: beginKey,
        endKey: endKey,
        chunkSize: 5_000_000  // 5MB chunks
    )

    // 並列処理
    try await withThrowingTaskGroup(of: Int.self) { group in
        for i in 0..<(splitPoints.count - 1) {
            let chunkBegin = splitPoints[i]
            let chunkEnd = splitPoints[i + 1]

            group.addTask {
                try await self.buildIndexChunk(
                    from: chunkBegin,
                    to: chunkEnd,
                    configuration: .batch
                )
            }
        }

        var totalProcessed = 0
        for try await count in group {
            totalProcessed += count
        }
        return totalProcessed
    }
}
```

---

## 5. トランザクションサイズ監視

### 5.1 自動分割

```swift
// FDBContext.swift または BatchWriter
func saveWithAutoSplit<T: Persistable>(items: [T]) async throws {
    var batch: [T] = []

    for item in items {
        batch.append(item)

        // バッチサイズチェック
        let size = try await transaction.getApproximateSize()
        if size > 9_000_000 {  // 9MB (FDB limit は 10MB)
            try await commitBatch(batch)
            batch = []
        }
    }

    if !batch.isEmpty {
        try await commitBatch(batch)
    }
}
```

### 5.2 警告ログ

```swift
extension FDBContainer {
    func withTransaction<T>(...) async throws -> T {
        // ... 実行後 ...

        let size = try await transaction.getApproximateSize()
        if size > 5_000_000 {  // 5MB 警告閾値
            logger.warning("Large transaction: \(size) bytes")
        }

        // ...
    }
}
```

---

## 6. 統計収集での活用

### 6.1 getEstimatedRangeSizeBytes

```swift
// StatisticsProvider.swift
func estimateTableSize<T: Persistable>(for type: T.Type) async throws -> TableSizeEstimate {
    let subspace = try await resolveSubspace(for: type)
    let (beginKey, endKey) = subspace.range()

    let estimatedBytes = try await transaction.getEstimatedRangeSizeBytes(
        beginKey: beginKey,
        endKey: endKey
    )

    return TableSizeEstimate(
        estimatedBytes: estimatedBytes,
        estimatedRows: estimatedBytes / averageRowSize
    )
}
```

---

## 7. 実装ファイル一覧

| ファイル | 変更内容 |
|---------|---------|
| `FDBContainer.swift` | ReadVersionCache 統合、サイズ監視 |
| `DatabaseProtocol` (fdb-swift-bindings) | `withTransaction(configuration:)` 追加 |
| `IndexSearcher.swift` | StreamingMode 自動選択 |
| `OnlineIndexer.swift` | 並列スキャン実装 |
| `Migration.swift` | `.system` 設定使用 |
| `StatisticsProvider.swift` | サイズ推定 API 使用 |
| `FDBContext.swift` | トランザクションサイズ監視 |

---

## 8. 期待効果

| 最適化 | 期待改善 |
|--------|---------|
| GRV キャッシュ | 読み取りレイテンシ 10-20% 削減 |
| 並列スキャン | インデックス構築 2-8x 高速化 |
| StreamingMode | ネットワーク往復削減 |
| 優先度制御 | バックグラウンド処理の分離 |
| サイズ監視 | 10MB 制限違反の防止 |
