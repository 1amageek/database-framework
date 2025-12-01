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
