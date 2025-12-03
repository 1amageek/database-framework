# FDB Transaction Optimization Design

## Overview

database-framework が fdb-swift-bindings の最適化オプションを活用するための設計。

**設計原則**: fdb-swift-bindings はプリミティブ API のみを提供し、database-framework が高レベルの抽象化を担当する。

## アーキテクチャ

```
fdb-swift-bindings (プリミティブ層)     database-framework (高レベル層)
┌────────────────────────────────┐      ┌────────────────────────────────┐
│ setOption(forOption:)          │ ←─── │ TransactionConfiguration       │
│ FDB.TransactionOption          │      │ apply(_:) extension            │
│ FDB.StreamingMode              │      │ withTransaction(configuration:)│
│ getEstimatedRangeSizeBytes()   │      │ ReadVersionCache               │
│ getRangeSplitPoints()          │      │ PriorityRateLimiter            │
└────────────────────────────────┘      └────────────────────────────────┘
      プリミティブ API のみ                   ブリッジ + 高レベル抽象化
```

## fdb-swift-bindings で利用可能な最適化機能

### 1. TransactionOption（トランザクションオプション）

| オプション | Raw Value | 説明 |
|-----------|-----------|------|
| `priorityBatch` | 201 | バッチ処理用の低優先度 |
| `prioritySystemImmediate` | 200 | システム操作用の最高優先度 |
| `useGrvCache` | 1101 | キャッシュされた GRV を使用 |
| `timeout` | 500 | トランザクションタイムアウト（ミリ秒） |
| `retryLimit` | 501 | リトライ上限 |
| `maxRetryDelay` | 502 | 最大リトライ遅延（ミリ秒） |
| `readPriorityLow` | 510 | 低読み取り優先度 |
| `readPriorityNormal` | 509 | 通常読み取り優先度 |
| `readPriorityHigh` | 511 | 高読み取り優先度 |
| `snapshotRywDisable` | 601 | スナップショット RYW 無効化 |
| `tag` | 800 | トランザクションタグ（スロットリング用） |
| `debugTransactionIdentifier` | 403 | デバッグ用トランザクション ID |
| `logTransaction` | 404 | トランザクションログ有効化 |

### 2. StreamingMode（範囲クエリ用）

| モード | Value | 説明 |
|--------|-------|------|
| `wantAll` | -2 | 全データを早期に取得 |
| `iterator` | -1 | デフォルト。段階的にバッチサイズを増加 |
| `exact` | 0 | 指定した行数を1バッチで取得 |
| `small` | 1 | 小さなバッチ |
| `medium` | 2 | 中サイズのバッチ |
| `large` | 3 | 大きなバッチ |
| `serial` | 4 | 最大バッチ。単一クライアント向け |

### 3. サーバーサイド統計 API

| API | 説明 |
|-----|------|
| `getEstimatedRangeSizeBytes` | 範囲のバイトサイズ推定（O(1)） |
| `getRangeSplitPoints` | 並列処理用のスプリットポイント取得 |
| `getApproximateSize` | トランザクションサイズの概算 |

## database-framework の実装

### TransactionConfiguration

FDB の型を直接使用し、変換レイヤーを不要にする設計:

```swift
// Sources/DatabaseEngine/Transaction/TransactionPriority.swift

public struct TransactionConfiguration: Sendable, Equatable {
    /// FDB.TransactionOption を直接使用
    public let priority: FDB.TransactionOption?        // .priorityBatch, .prioritySystemImmediate
    public let readPriority: FDB.TransactionOption?    // .readPriorityLow/Normal/High
    public let timeout: Int?
    public let retryLimit: Int?
    public let maxRetryDelay: Int?
    public let useGrvCache: Bool
    public let snapshotRywDisable: Bool
    public let debugTransactionIdentifier: String?
    public let logTransaction: Bool
    public let tags: [String]

    // プリセット
    public static let `default` = TransactionConfiguration()
    public static let readOnly = TransactionConfiguration(useGrvCache: true)
    public static let batch = TransactionConfiguration(
        priority: .priorityBatch,
        readPriority: .readPriorityLow,
        timeout: 30_000,
        retryLimit: 20,
        maxRetryDelay: 5_000
    )
    public static let system = TransactionConfiguration(
        priority: .prioritySystemImmediate,
        readPriority: .readPriorityHigh,
        timeout: 2_000,
        retryLimit: 5,
        maxRetryDelay: 100
    )
    public static let interactive = TransactionConfiguration(
        timeout: 1_000,
        retryLimit: 3,
        maxRetryDelay: 50
    )
}
```

### apply() Extension

database-framework 側で TransactionProtocol を拡張:

```swift
extension TransactionProtocol {
    public func apply(_ config: TransactionConfiguration) throws {
        if let priority = config.priority {
            try setOption(forOption: priority)
        }
        if let readPriority = config.readPriority {
            try setOption(forOption: readPriority)
        }
        if let timeout = config.timeout {
            try setOption(to: timeout, forOption: .timeout)
        }
        // ... 直接マッピング、変換不要
    }
}
```

### FDBContainer.withTransaction

設定付きトランザクション実行:

```swift
public func withTransaction<T: Sendable>(
    configuration: TransactionConfiguration = .default,
    _ operation: @Sendable (any TransactionProtocol) async throws -> T
) async throws -> T {
    let maxRetries = configuration.retryLimit ?? 100

    for attempt in 0..<maxRetries {
        let transaction = try database.createTransaction()
        try transaction.apply(configuration)

        do {
            let result = try await operation(transaction)
            let committed = try await transaction.commit()
            if committed { return result }
        } catch {
            transaction.cancel()
            if let fdbError = error as? FDBError, fdbError.isRetryable {
                if attempt < maxRetries - 1 {
                    let delay = min(configuration.maxRetryDelay ?? 1000, 10 * (1 << min(attempt, 10)))
                    try await Task.sleep(nanoseconds: UInt64(delay) * 1_000_000)
                    continue
                }
            }
            throw error
        }
    }
    throw FDBError(code: 1020)  // transaction_too_old
}
```

## 使用例

### 1. クエリ実行（PlanExecutor）

```swift
func executeQuery<T: Persistable>(plan: PlanOperator<T>) async throws -> [T] {
    try await container.withTransaction(configuration: .readOnly) { transaction in
        // GRV キャッシュ有効のクエリ実行
    }
}
```

### 2. インデックス構築（OnlineIndexer）

```swift
func buildIndex() async throws {
    try await container.withTransaction(configuration: .batch) { transaction in
        // 低優先度でインデックスエントリを書き込み
    }
}
```

### 3. スキーマ操作（Migration）

```swift
func applyMigration() async throws {
    try await container.withTransaction(configuration: .system) { transaction in
        // 最高優先度でメタデータ更新
    }
}
```

### 4. 大規模スキャンの並列化

```swift
func parallelScan(subspace: Subspace) async throws -> [Record] {
    let splitPoints = try await transaction.getRangeSplitPoints(
        beginKey: subspace.range().0,
        endKey: subspace.range().1,
        chunkSize: 10_000_000  // 10MB
    )

    return try await withTaskGroup(of: [Record].self) { group in
        for chunk in splitPoints.windows(ofCount: 2) {
            group.addTask {
                try await container.withTransaction(configuration: .batch) { tx in
                    // チャンクをスキャン
                }
            }
        }
        return try await group.reduce([], +)
    }
}
```

## パフォーマンス期待値

| 最適化 | 期待される改善 |
|--------|---------------|
| GRV キャッシュ | 読み取りレイテンシ 10-20% 削減 |
| バッチ優先度 | バックグラウンド処理の分離 |
| 並列スキャン | 大規模スキャンのスループット 2-8x |
| StreamingMode 最適化 | ネットワーク往復の削減 |

## 注意事項

1. **GRV キャッシュの制限**
   - `disableClientBypass` ネットワークオプションが必要
   - キャッシュの陳腐化に注意（デフォルト 5 秒）

2. **優先度の使用**
   - `systemImmediate` は控えめに使用
   - 過度の使用は低優先度トランザクションを飢餓させる

3. **タイムアウト設定**
   - 短すぎるタイムアウトは不要なリトライを引き起こす
   - 長すぎるタイムアウトはリソースを無駄にする

## 関連ファイル

- `fdb-swift-bindings/Sources/FoundationDB/Fdb+Options.swift` - FDB オプション定義
- `fdb-swift-bindings/Sources/FoundationDB/FoundationdDB.swift` - プロトコル定義
- `database-framework/Sources/DatabaseEngine/Transaction/TransactionPriority.swift` - 設定型と apply()
- `database-framework/Sources/DatabaseEngine/Transaction/ReadVersionCache.swift` - GRV キャッシュ
- `database-framework/Sources/DatabaseEngine/FDBContainer.swift` - トランザクション実行
