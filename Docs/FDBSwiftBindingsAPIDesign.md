# fdb-swift-bindings API 設計

## 概要

FDB C API `fdb_transaction_get_range` のパラメータを Swift API として適切に公開するための設計。

**設計原則**: fdb-swift-bindings はプリミティブ API のみを提供し、高レベル抽象化は database-framework が担当する。

## C API パラメータ

```c
FDBFuture* fdb_transaction_get_range(
    FDBTransaction* tr,
    // Begin KeySelector
    uint8_t const* begin_key_name,
    int begin_key_name_length,
    fdb_bool_t begin_or_equal,
    int begin_offset,
    // End KeySelector
    uint8_t const* end_key_name,
    int end_key_name_length,
    fdb_bool_t end_or_equal,
    int end_offset,
    // Query parameters
    int limit,
    int target_bytes,
    FDBStreamingMode mode,
    int iteration,
    fdb_bool_t snapshot,
    fdb_bool_t reverse
);
```

## パラメータ分類

| パラメータ | 分類 | 公開レベル | 理由 |
|-----------|------|-----------|------|
| `limit` | クエリセマンティクス | 高レベル | 結果件数に影響 |
| `reverse` | クエリセマンティクス | 高レベル | 結果順序に影響 |
| `snapshot` | 分離レベル | 高レベル | 読み取り一貫性に影響 |
| `streamingMode` | 転送最適化 | 高レベル | パフォーマンスチューニング |
| `targetBytes` | 転送最適化 | 低レベルのみ | 高度な最適化用 |
| `iteration` | 内部状態 | 低レベルのみ | AsyncIterator が内部管理 |

## 実装済み API

### 1. TransactionProtocol（低レベル API）

```swift
public protocol TransactionProtocol: Sendable {
    /// Low-level range query with all C API parameters exposed
    func getRangeNative(
        beginSelector: FDB.KeySelector,
        endSelector: FDB.KeySelector,
        limit: Int,
        targetBytes: Int,
        streamingMode: FDB.StreamingMode,
        iteration: Int,
        reverse: Bool,
        snapshot: Bool
    ) async throws -> ResultRange
}
```

### 2. 高レベル API（getRange）

```swift
extension TransactionProtocol {
    /// High-level range query returning an AsyncSequence
    public func getRange(
        from begin: FDB.KeySelector,
        to end: FDB.KeySelector,
        limit: Int = 0,
        reverse: Bool = false,
        snapshot: Bool = false,
        streamingMode: FDB.StreamingMode = .iterator
    ) -> FDB.AsyncKVSequence
}
```

### 3. AsyncKVSequence

バックグラウンドプリフェッチ付きの高性能 AsyncSequence:

```swift
extension FDB {
    public struct AsyncKVSequence: AsyncSequence {
        public typealias Element = (Bytes, Bytes)

        let transaction: TransactionProtocol
        let beginSelector: KeySelector
        let endSelector: KeySelector
        let limit: Int
        let reverse: Bool
        let snapshot: Bool
        let streamingMode: StreamingMode

        public struct AsyncIterator: AsyncIteratorProtocol {
            // Internal state - iteration は内部管理
            private var iteration: Int = 1
            private var preFetchTask: Task<ResultRange?, Error>?
            // ...
        }
    }
}
```

**パフォーマンス特性**:
- **Overlapped I/O**: ネットワークリクエストとデータ処理が並行
- **Reduced Latency**: プリフェッチによりネットワーク往復を隠蔽
- **Memory Efficient**: 1-2 バッチのみメモリに保持

### 4. FDBTransaction 実装

```swift
public final class FDBTransaction: TransactionProtocol {
    public func getRangeNative(
        beginSelector: FDB.KeySelector,
        endSelector: FDB.KeySelector,
        limit: Int,
        targetBytes: Int,
        streamingMode: FDB.StreamingMode,
        iteration: Int,
        reverse: Bool,
        snapshot: Bool
    ) async throws -> ResultRange {
        // fdb_transaction_get_range への直接マッピング
    }
}
```

## 使用例

### 基本的な範囲クエリ

```swift
for try await (key, value) in transaction.getRange(
    from: .firstGreaterOrEqual(startKey),
    to: .firstGreaterOrEqual(endKey)
) {
    print(key, value)
}
```

### 逆順スキャン

```swift
// 降順で最新10件を取得
for try await (key, value) in transaction.getRange(
    from: .firstGreaterOrEqual(startKey),
    to: .firstGreaterOrEqual(endKey),
    limit: 10,
    reverse: true
) {
    print(key, value)
}
```

### バルク読み取り最適化

```swift
// 全データを高速に取得
for try await (key, value) in transaction.getRange(
    from: .firstGreaterOrEqual(startKey),
    to: .firstGreaterOrEqual(endKey),
    streamingMode: .wantAll
) {
    processInBulk(key, value)
}
```

### スナップショット読み取り

```swift
// 長時間の読み取り（コンフリクトなし）
for try await (key, value) in transaction.getRange(
    from: .firstGreaterOrEqual(startKey),
    to: .firstGreaterOrEqual(endKey),
    snapshot: true
) {
    slowProcess(key, value)
}
```

### 低レベル API（上級者向け）

```swift
// 直接 C API パラメータを制御
let result = try await transaction.getRangeNative(
    beginSelector: .firstGreaterOrEqual(startKey),
    endSelector: .firstGreaterOrEqual(endKey),
    limit: 100,
    targetBytes: 1_000_000,  // 1MB
    streamingMode: .exact,
    iteration: 1,
    reverse: false,
    snapshot: false
)
```

## StreamingMode ガイド

| モード | ユースケース |
|--------|-------------|
| `.iterator` | 一般的なイテレーション（デフォルト） |
| `.wantAll` | 全データのバルク読み取り |
| `.exact` | 正確な件数を1バッチで取得 |
| `.small` | メモリ制約のある環境 |
| `.serial` | 単一クライアントの高スループット |

## 関連ファイル

- `Sources/FoundationDB/FoundationdDB.swift` - TransactionProtocol 定義
- `Sources/FoundationDB/Transaction.swift` - FDBTransaction 実装
- `Sources/FoundationDB/Fdb+AsyncKVSequence.swift` - AsyncKVSequence 実装
- `Sources/FoundationDB/Fdb+Options.swift` - StreamingMode 定義
