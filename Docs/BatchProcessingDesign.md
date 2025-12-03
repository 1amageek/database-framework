# Batch Processing Design

## 問題の根本原因

### 1. RangeSet の設計問題

**現状**: `nextBatch(size:)` がサイズパラメータを無視して全範囲を返す

**根本原因**: キー空間を数学的に分割しようとしている設計が間違い
- FDBのキーはバイト配列であり、均等分割は困難
- キーの分布は不明（疎密がある）
- 推定サイズと実際のレコード数は乖離する

**正しいアプローチ**: Continuation パターン
- FDB Record Layer と同じ手法
- バッチ処理後に「最後に処理したキー」を記録
- 次のバッチは記録したキーから再開
- 実際のバッチサイズはループ内で `batchSize` に達したら `break` で制御
  （FDBのSwiftバインディングには `limit` パラメータがないため）

### 2. トランザクション境界の問題

**現状**: 全レコードを1トランザクションで処理

**根本原因**: バッチ = レンジ分割 という誤解
- トランザクション境界とレンジ分割は別の問題
- バッチごとに別トランザクションが必要

**正しいアプローチ**:
```
Transaction 1: records 0-999 → commit + save progress
Transaction 2: records 1000-1999 → commit + save progress
...
```

### 3. BatchFetcher のスレッドセーフ問題

**現状**: 同一トランザクションを複数タスクで共有

**根本原因**: FDBトランザクションはスレッドセーフではない
- 内部状態がある（read version, write conflict ranges等）
- 並行アクセスで状態が破損する

**正しいアプローチ**:
- 単一トランザクション内では逐次読み取り
- 並列が必要なら独立したスナップショット読み取りを使用

---

## 設計

### RangeContinuation

```swift
/// Range processing state with continuation support
public struct RangeContinuation: Sendable, Codable, Equatable {
    /// Start of the range (inclusive)
    public let rangeBegin: [UInt8]

    /// End of the range (exclusive)
    public let rangeEnd: [UInt8]

    /// Last successfully processed key (nil if not started)
    /// Next batch starts AFTER this key
    public var lastProcessedKey: [UInt8]?

    /// Whether this range is fully processed
    public var isComplete: Bool
}
```

### RangeSet の再設計

```swift
public struct RangeSet: Sendable {
    private var continuations: [RangeContinuation]

    /// Get bounds for next batch (actual limiting done by caller)
    public func nextBatchBounds() -> (begin: [UInt8], end: [UInt8])?

    /// Record progress after processing a batch
    public mutating func recordProgress(
        rangeIndex: Int,
        lastProcessedKey: [UInt8],
        isRangeComplete: Bool
    )
}
```

### OnlineIndexer のバッチ処理

```swift
func buildIndexesInBatches(batchSize: Int) async throws {
    while let bounds = rangeSet.nextBatchBounds() {
        var lastKey: [UInt8]? = nil
        var count = 0

        // 1トランザクション = 1バッチ
        try await database.withTransaction { tx in
            let sequence = tx.getRange(
                beginSelector: .firstGreaterOrEqual(bounds.begin),
                endSelector: .firstGreaterOrEqual(bounds.end),
                snapshot: false
            )

            // batchSize に達したら break でループを抜ける
            for try await (key, value) in sequence {
                try await processItem(key, value, tx)
                lastKey = Array(key)
                count += 1

                if count >= batchSize {
                    break  // ← ここでバッチサイズを制御
                }
            }

            // 進捗を保存（トランザクション内）
            if let lastKey = lastKey {
                let isComplete = count < batchSize
                rangeSet.recordProgress(
                    rangeIndex: bounds.rangeIndex,
                    lastProcessedKey: lastKey,
                    isComplete: isComplete
                )
            } else {
                rangeSet.markRangeComplete(rangeIndex: bounds.rangeIndex)
            }
            saveProgress(rangeSet, tx)
        }
        // トランザクションがコミットされた = 進捗が永続化された
    }
}
```

### BatchFetcher の修正

並列読み取りを削除し、逐次読み取りのみにする：

```swift
public func fetchBatch(
    primaryKeys: [Tuple],
    subspace: Subspace,
    transaction: any TransactionProtocol
) async throws -> [Item] {
    var results: [Item] = []
    results.reserveCapacity(primaryKeys.count)

    // Always sequential - FDB transactions are not thread-safe
    for pk in primaryKeys {
        let key = subspace.pack(pk)
        if let data = try await transaction.getValue(for: key) {
            let item: Item = try DataAccess.deserialize(Array(data))
            results.append(item)
        }
    }

    return results
}
```

並列が必要な場合は、呼び出し側で複数トランザクションを使用：

```swift
// 呼び出し側で並列化する場合
try await withThrowingTaskGroup(of: [Item].self) { group in
    for batch in batches {
        group.addTask {
            // 各タスクで独立したトランザクション
            try await database.withTransaction { tx in
                try await fetcher.fetchBatch(batch, tx)
            }
        }
    }
}
```

### PlanComplexityCalculator の修正

```swift
case .intersection(let op):
    breakdown.intersectionCount += 1

    // 子の複雑度を別途計算
    let beforeTotal = breakdown.totalComplexity
    for child in op.children {
        analyzeBreakdown(child, into: &breakdown)
    }
    let childComplexity = breakdown.totalComplexity - beforeTotal

    // 子の複雑度のみを2倍にする（既に1回加算済みなので、もう1回加算）
    breakdown.totalComplexity += childComplexity
```

---

## 実装順序

1. **RangeContinuation** 構造体を追加
2. **RangeSet** を continuation ベースに修正
3. **MultiTargetOnlineIndexer** のバッチ処理を修正
4. **MutualOnlineIndexer** のバッチ処理を修正
5. **BatchFetcher** から並列読み取りを削除
6. **PlanComplexityCalculator** の計算を修正
7. テストを追加・更新
