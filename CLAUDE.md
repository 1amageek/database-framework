# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

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

### Core Protocol Bridge

`IndexKindMaintainable` bridges metadata (IndexKind) with runtime (IndexMaintainer):

```swift
// In database-kit: Defines WHAT to index
public struct ScalarIndexKind: IndexKind { ... }

// In database-framework: Defines HOW to maintain index
extension ScalarIndexKind: IndexKindMaintainable {
    func makeIndexMaintainer<Item>(...) -> any IndexMaintainer<Item>
}
```

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
| `FDBContainer` | SwiftData-like container managing schema, migrations, and contexts |
| `FDBContext` | Change tracking and batch operations (insert/delete/fetch/save) |
| `DataStore` | Storage backend protocol (default: `FDBDataStore`) |
| `IndexMaintainer<Item>` | Protocol for index update logic (`updateIndex`, `scanItem`) |
| `IndexKindMaintainable` | Bridge protocol connecting IndexKind to IndexMaintainer |
| `OnlineIndexer` | Background index building for schema migrations |

### Data Layout in FoundationDB

```
[fdb]/R/[PersistableType]/[id]           → Protobuf-encoded record
[fdb]/I/[indexName]/[values...]/[id]     → Index entry (empty value for scalar)
[fdb]/_metadata/schema/version           → Tuple(major, minor, patch)
[fdb]/_metadata/index/[indexName]/state  → IndexState (readable/write_only/disabled)
```

### Index Maintainer Pattern

Each index module follows this pattern:

1. `*IndexKind.swift` - Extends IndexKind with `IndexKindMaintainable` conformance
2. `*IndexMaintainer.swift` - Implements `IndexMaintainer` protocol

```swift
// Example: ScalarIndex/
├── ScalarIndexKind.swift           // extension ScalarIndexKind: IndexKindMaintainable
└── ScalarIndexMaintainer.swift     // struct ScalarIndexMaintainer<Item>: IndexMaintainer
```

### Testing Pattern

Tests use a shared singleton for FDB initialization to avoid "API version may be set only once" errors:

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

Test models are defined in `Tests/Shared/TestModels.swift`.

## Adding a New Index Type

1. Define `IndexKind` in database-kit (e.g., `TimeSeriesIndexKind`)
2. Create new module in `Sources/` (e.g., `TimeSeriesIndex/`)
3. Add `IndexKindMaintainable` extension
4. Implement `IndexMaintainer` struct
5. Add module to `Package.swift` and `Database` target dependencies

## Swift Concurrency Pattern

### final class + Mutex パターン

**重要**: このプロジェクトは `actor` を使用せず、`final class: Sendable` + `Mutex` パターンを採用。

**理由**: スループット最適化
- actor はシリアライズされた実行 → 低スループット
- Mutex は細粒度ロック → 高い並行性
- データベース I/O 中も他のタスクを実行可能

**実装パターン**:
```swift
import Synchronization

public final class ClassName: Sendable {
    // 1. DatabaseProtocol は内部的にスレッドセーフ
    nonisolated(unsafe) private let database: any DatabaseProtocol

    // 2. 可変状態は Mutex で保護（struct にまとめる）
    private struct State: Sendable {
        var counter: Int = 0
        var isRunning: Bool = false
    }
    private let state: Mutex<State>

    public init(database: any DatabaseProtocol) {
        self.database = database
        self.state = Mutex(State())
    }

    // 3. withLock で状態アクセス（ロックスコープは最小限）
    public func operation() async throws {
        let count = state.withLock { state in
            state.counter += 1
            return state.counter
        }

        // I/O 中はロックを保持しない
        try await database.withTransaction { transaction in
            // 他のタスクは getProgress() などを呼べる
        }
    }
}
```

**ガイドライン**:
1. ✅ `final class: Sendable` を使用（actor は使用しない）
2. ✅ `DatabaseProtocol` には `nonisolated(unsafe)` を使用
3. ✅ 可変状態は `Mutex<State>` で保護（State は `Sendable` な struct）
4. ✅ ロックスコープは最小限（I/O を含めない）
5. ❌ `NSLock` は使用しない（async context で問題が発生する）
6. ❌ `@unchecked Sendable` は避ける（Mutex で適切に保護）

## 実装品質ガイドライン

### 基本方針

**実装コストは考慮しない。最適化と完成度が最優先。**

### 必須要件

1. **学術的根拠**: アルゴリズムは論文・教科書に基づく。独自手法は禁止
2. **参照実装の調査**: PostgreSQL, FoundationDB Record Layer, CockroachDB 等を必ず参照
3. **TODO/FIXME の扱い**: 作業継続のマーカーとして使用可。ただし放置せず完了まで実装を続ける
4. **包括的テスト**: 新機能には必ずユニットテストを追加
5. **エッジケース網羅**: 境界値、空入力、大規模データを全てカバー

### 実装プロセス

```
1. 調査フェーズ（省略禁止）
   ├── 学術論文を検索（Google Scholar, ACM DL）
   ├── 成熟したOSS実装を分析
   ├── 計算量・空間量を確認
   └── 既知の限界を把握

2. 設計フェーズ
   ├── 複数アプローチの比較表を作成
   ├── 最適なアルゴリズムを選定
   ├── データ構造を決定
   └── API設計

3. 実装フェーズ
   ├── テストを先に書く（TDD）
   ├── 参照論文・実装をコメントで明記
   ├── 定数には根拠をコメント
   └── 性能特性をドキュメント化

4. 検証フェーズ
   ├── 全テスト通過
   ├── ベンチマーク測定（必要に応じて）
   └── コードレビュー
```

### コーディング規約

```swift
// ✅ 良い例: 根拠を明記
/// Reservoir Sampling (Algorithm R)
/// Reference: Vitter, J.S. "Random Sampling with a Reservoir", ACM TOMS 1985
/// Time: O(n), Space: O(k) where k = reservoir size
public struct ReservoirSampling<T> { ... }

// ✅ 良い例: 定数に根拠
/// HyperLogLog precision parameter
/// - p=14 gives 16384 registers, ~0.8% standard error
/// - Memory: 2^p bytes = 16KB
/// Reference: Flajolet et al., "HyperLogLog: the analysis of a near-optimal cardinality estimation algorithm"
private let precision: Int = 14

// ❌ 悪い例: 根拠不明のマジックナンバー
let base = 256.0  // なぜ256？
let threshold = 0.5  // なぜ0.5？
```

### 参照すべきリソース

| 分野 | 参照先 |
|------|--------|
| クエリ最適化 | PostgreSQL src/backend/optimizer/ |
| 統計・ヒストグラム | PostgreSQL src/backend/utils/adt/selfuncs.c |
| インデックス設計 | FoundationDB Record Layer |
| 分散システム | CockroachDB, TiDB |
| アルゴリズム全般 | CLRS "Introduction to Algorithms" |
| データベース理論 | "Database System Concepts" (Silberschatz) |
