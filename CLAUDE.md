# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Vision

### Purpose

**database-framework** is a **protocol-extensible, customizable index database** designed for the AI era.

The rise of Large Language Models (LLMs) and Retrieval-Augmented Generation (RAG) has created new requirements for databases:
- **Vector Search**: Semantic similarity for embeddings
- **Graph Traversal**: Knowledge graphs and entity relationships
- **Full-Text Search**: Hybrid retrieval combining keywords and semantics
- **Multi-Modal Queries**: Combining different index types in a single query

Traditional databases treat these as separate systems. **database-framework unifies them** into a single, coherent database built on FoundationDB's transactional guarantees.

### Design Philosophy

```
┌─────────────────────────────────────────────────────────────────────┐
│                     Protocol-Based Extensibility                     │
│                                                                      │
│   IndexKind (protocol)  ───▶  IndexMaintainer (protocol)            │
│        ↓                            ↓                                │
│   User defines           Framework executes                          │
│   WHAT to index          HOW to maintain                            │
└─────────────────────────────────────────────────────────────────────┘
```

1. **Protocol-Driven**: New index types can be added without modifying core code
2. **Composable**: Combine Vector + Graph + FullText in unified queries (Fusion API)
3. **Transactional**: All operations backed by FoundationDB's ACID guarantees
4. **AI-Native**: First-class support for embeddings, knowledge graphs, and RAG patterns

### Core Use Cases

| Use Case | Index Types | Example |
|----------|-------------|---------|
| **Semantic Search** | Vector | Find similar documents by embedding |
| **Knowledge Graph** | Graph (RDF/Hexastore) | Query entity relationships |
| **RAG Pipeline** | Vector + FullText | Hybrid retrieval for LLM context |
| **Entity Resolution** | Graph + Scalar | Link entities across data sources |
| **Recommendation** | Vector + Rank | Similar items with popularity ranking |

### Why FoundationDB?

- **Horizontal Scalability**: Petabyte-scale with linear performance
- **Strong Consistency**: Serializable transactions across all indexes
- **Operational Simplicity**: Single system for all index types
- **Proven at Scale**: Powers Apple, Snowflake, and other large-scale systems

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
| `FDBContainer` | Application resource manager: database connection, schema, securityDelegate, directory resolution. Does NOT create transactions |
| `FDBContext` | Transaction manager + User-facing API: owns ReadVersionCache, creates transactions via TransactionRunner, change tracking |
| `FDBDataStore` | Data operations within transactions: receives transaction as parameter, does NOT create transactions |
| `DataStore` | Storage backend protocol (default: `FDBDataStore`) |
| `SerializedModel` | Serialized data carrier for dual-write optimization |
| `IndexMaintainer<Item>` | Protocol for index update logic (`updateIndex`, `scanItem`) |
| `IndexMaintenanceService` | Centralized index maintenance: uniqueness checking, index updates, violation tracking |
| `IndexKindMaintainable` | Bridge protocol connecting IndexKind to IndexMaintainer |
| `OnlineIndexer` | Background index building for schema migrations |
| `MultiTargetOnlineIndexer` | Build multiple indexes in single data scan |
| `MutualOnlineIndexer` | Build bidirectional indexes (e.g., followers/following) |
| `OnlineIndexScrubber` | Detect and repair index inconsistencies |
| `IndexFromIndexBuilder` | Build new index from existing index |
| `CascadesOptimizer` | Cascades framework query optimizer |
| `TransactionConfiguration` | Transaction presets (.default/.batch/.interactive/.readOnly/.longRunning) with priority, timeout, retry, cache policy |
| `TransactionRunner` | Retry logic with exponential backoff and jitter, cache policy support |
| `CachePolicy` | Cache policy for read operations (.server/.cached/.stale(N)) |
| `ReadVersionCache` | Caches read versions for CachePolicy (reduces getReadVersion() calls) |
| `LargeValueSplitter` | Handle values exceeding FDB's 100KB limit |
| `TransformingSerializer` | Compression (LZ4/zlib/LZMA/LZFSE) and encryption (AES-256-GCM) |
| `Polymorphable` | Protocol enabling union types with shared directory and polymorphic queries |
| `IndexStateManager` | Manages index lifecycle states (disabled/writeOnly/readable) |
| `ItemStorage` | Envelope-based storage for items (handles large values, blobs) |
| `ItemEnvelope` | Storage format wrapper with magic number validation |
| `DirectoryPath<T>` | Type-safe field values for dynamic directory resolution |
| `AnyDirectoryPath` | Type-erased wrapper for DirectoryPath |

### Core Architecture Design Principles (Context-Centric)

**設計哲学**: SwiftData の ModelContainer/ModelContext パターンに準拠

| コンポーネント | 責務 | トランザクション |
|--------------|------|-----------------|
| `FDBContainer` | リソース管理（DB接続、Schema、Directory） | **作成しない** |
| `FDBContext` | データ操作、トランザクション管理、キャッシュ | **作成する** |
| `FDBDataStore` | 低レベル操作（トランザクション内） | **受け取る** |

**なぜ Context がトランザクションを管理するのか**:

1. **SwiftData との整合性**: ModelContext が Unit of Work のスコープを定義するように、FDBContext がトランザクションの境界を定義する
2. **ReadVersionCache の自然なスコープ**: キャッシュは作業単位（Context）ごとに独立すべき
   - Web アプリ: 1 リクエスト = 1 Context → リクエスト内でキャッシュが有効
   - バッチ処理: 長時間の Context → 処理全体でキャッシュを共有
   - 並列処理: 各 Context が独立したキャッシュ → 干渉なし
3. **明示的なトランザクション API**: ユーザーは `context.withTransaction()` で明示的にトランザクションを制御

**禁止事項**:
- ❌ `FDBContainer` にトランザクション作成メソッドを追加しない
- ❌ `FDBDataStore` が独自にトランザクションを作成しない
- ✅ トランザクション作成は `FDBContext` に集約

### Responsibility Separation

```
┌─────────────────────────────────────────────────────────────────────────┐
│ FDBContainer (Application Resource Manager)                             │
│   - database: DatabaseProtocol (raw FDB connection)                     │
│   - schema: Schema                                                      │
│   - securityDelegate: DataStoreSecurityDelegate?                        │
│   - indexConfigurations: [String: [IndexConfiguration]]                 │
│   - store(for:path:): DataStore instance per type with directory path   │
│   - resolveDirectory(for:path:): Directory resolution for Persistable   │
│   ❌ Does NOT create or manage transactions                              │
│   ❌ Does NOT have ReadVersionCache                                      │
└─────────────────────────────────────────────────────────────────────────┘
                                    │
                                    │ creates
                                    ▼
┌─────────────────────────────────────────────────────────────────────────┐
│ FDBContext (Transaction Manager + User-Facing API)                      │
│   - readVersionCache: ReadVersionCache (per-context, not shared)        │
│   - insert(), delete() → queue changes locally                          │
│   - save() → persist all queued changes                                 │
│   - fetch() → returns ONLY persisted data (no pending mixing)           │
│   - withTransaction(configuration:) → TransactionRunner execution       │
│   - clearReadVersionCache() → reset cache                               │
│   - readVersionCacheInfo() → (version, ageMillis) for debugging         │
│   ✅ OWNS transaction creation via TransactionRunner                     │
│   ✅ OWNS ReadVersionCache for weak read semantics                       │
└─────────────────────────────────────────────────────────────────────────┘
                                    │
                                    │ delegates to (with transaction)
                                    ▼
┌─────────────────────────────────────────────────────────────────────────┐
│ FDBDataStore (Data Operations within Transaction)                       │
│   - executeBatchInTransaction(transaction:) → [SerializedModel]         │
│   - fetchInTransaction(transaction:) → [Item]                           │
│   - fetchByIdInTransaction(transaction:) → Item? (O(1) direct lookup)   │
│   - countInTransaction(transaction:) → Int                              │
│   - Security evaluation via securityDelegate                            │
│   - Index maintenance via IndexMaintainer                               │
│   ❌ Does NOT create transactions                                        │
│   ✅ RECEIVES transaction as parameter                                   │
└─────────────────────────────────────────────────────────────────────────┘
                                    │
                                    │ uses
                                    ▼
┌─────────────────────────────────────────────────────────────────────────┐
│ TransactionRunner (Retry Logic)                                         │
│   - Creates NEW transaction for each retry attempt                      │
│   - Applies TransactionConfiguration options                            │
│   - Applies cached read version (weak read semantics, first attempt)    │
│   - Exponential backoff with jitter (prevents thundering herd)          │
│   - Updates read version cache after successful commit                  │
└─────────────────────────────────────────────────────────────────────────┘
```

**Transaction Execution Flow**:
```
context.withTransaction(configuration: .default) { tx in ... }
       │
       ▼
TransactionRunner.run(database:configuration:readVersionCache:operation:)
       │
       ├── 1. Create NEW transaction for each attempt
       ├── 2. Apply configuration (priority, timeout, etc.)
       ├── 3. Apply cached read version (first attempt only, if weak read semantics)
       ├── 4. Execute operation with transaction
       ├── 5. Commit transaction
       ├── 6. On retryable error: exponential backoff + retry
       └── 7. On success: update context's read version cache
```

### Transaction API の使い分け（設計原則）

本フレームワークには**3つのトランザクション API** があり、それぞれ明確な用途がある：

| API | 戻り値 | ReadVersionCache | アクセスレベル | 用途 |
|-----|--------|------------------|---------------|------|
| `context.withTransaction()` | `TransactionContext` | ✅ 使用 | `public` | ユーザー向け高レベルAPI |
| `context.withRawTransaction()` | `TransactionProtocol` | ✅ 使用 | `internal` | 内部インフラ（キャッシュ必要） |
| `database.withTransaction()` | `TransactionProtocol` | ❌ 不使用 | `public` | システム操作 |

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                     Transaction API の選択基準                               │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  ユーザーデータの読み書き？                                                   │
│       │                                                                     │
│       ├─ YES → context.withTransaction()                                    │
│       │         • TransactionContext（高レベルAPI）                          │
│       │         • get(), set(), delete() でセキュリティ評価                   │
│       │         • Directory 解決、Subspace キャッシュ                         │
│       │                                                                     │
│       └─ NO → ReadVersionCache が必要？                                      │
│                 │                                                           │
│                 ├─ YES → context.withRawTransaction()  [internal]           │
│                 │         • TransactionProtocol（低レベルAPI）               │
│                 │         • IndexQueryContext 等の内部インフラ               │
│                 │         • キャッシュの恩恵を受けつつ直接アクセス              │
│                 │                                                           │
│                 └─ NO → database.withTransaction()                          │
│                           • TransactionProtocol（低レベルAPI）               │
│                           • DirectoryLayer, Migration                       │
│                           • OnlineIndexer, Graph Algorithms                 │
│                           • 長時間バッチ処理（キャッシュが古くなる）            │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

#### 1. `context.withTransaction()` - ユーザー向け高レベルAPI

```swift
// TransactionContext を返す（高レベル）
try await context.withTransaction(configuration: .default) { tx: TransactionContext in
    // 高レベルAPI: セキュリティ評価、Directory解決が自動
    let user = try await tx.get(User.self, id: userID)
    try await tx.set(updatedUser)
    try await tx.delete(User.self, id: userID)
}

// 主な用途: FDBContext.save(), fetch() 等のユーザー向け操作
```

**TransactionContext が提供する機能**:
- `get()` / `set()` / `delete()` - セキュリティ評価付き
- Directory 解決（パーティション対応）
- Subspace キャッシュ
- `transaction` プロパティは `private`（カプセル化）

#### 2. `context.withRawTransaction()` - 内部インフラ用（キャッシュあり）

```swift
// TransactionProtocol を返す（低レベル、internal）
try await context.withRawTransaction(configuration: .default) { tx: TransactionProtocol in
    // 低レベルAPI: 直接キーアクセス
    let value = try await tx.getValue(for: key, snapshot: true)
    tx.setValue(value, for: key)
    for try await (k, v) in tx.getRange(...) { ... }
}

// 主な用途: IndexQueryContext.withTransaction()
```

**使用場面**:
- `IndexQueryContext` - インデックス構造を直接読み取る必要がある
- ReadVersionCache の恩恵を受けつつ、低レベルアクセスが必要な場合
- セキュリティ評価はアイテム取得後に別途行う

#### 3. `database.withTransaction()` - システム操作用（キャッシュなし）

```swift
// TransactionProtocol を返す（低レベル、キャッシュなし）
try await container.database.withTransaction(configuration: .batch) { tx: TransactionProtocol in
    // システム操作: キャッシュの恩恵なし
    try await directoryLayer.createOrOpen(at: path, using: tx)
}

// 主な用途: DirectoryLayer, Migration, OnlineIndexer, Graph Algorithms
```

**使用場面**:
- DirectoryLayer 操作
- Migration
- OnlineIndexer / IndexScrubber
- Graph Algorithms（独立したインフラコンポーネント）
- StatisticsManager
- 長時間バッチ処理（キャッシュが古くなるため無意味）

#### Graph Algorithms が `database.withTransaction()` を使う理由

Graph Algorithms（ShortestPathFinder, CycleDetector, TopologicalSort 等）は**インフラコンポーネント**として設計：

1. **独立性**: FDBContext に依存しない再利用可能なコンポーネント
2. **バッチ処理**: 長時間実行では ReadVersionCache が古くなる
3. **システム統合**: OnlineIndexer や Migration から呼ばれる可能性がある
4. **単純性**: 1回のアルゴリズム実行で ReadVersionCache の恩恵は限定的

```swift
// Graph Algorithm は database を直接受け取る = インフラコンポーネント
public final class CycleDetector<Edge: Persistable>: Sendable {
    nonisolated(unsafe) private let database: any DatabaseProtocol

    public init(database: any DatabaseProtocol, subspace: Subspace) {
        // database を直接保持（Context ではない）
    }
}
```

#### コンポーネント別 API 使用一覧

| コンポーネント | 使用する API | 理由 |
|--------------|-------------|------|
| `FDBContext.save()` | `withTransaction()` | ユーザー向け、高レベルAPI |
| `FDBContext.fetch()` | `withTransaction()` | ユーザー向け、高レベルAPI |
| `IndexQueryContext` | `withRawTransaction()` | 内部インフラ、キャッシュ必要 |
| `FDBDataStore` | トランザクションを**受け取る** | 自身では作成しない |
| `Graph Algorithms` | `database.withTransaction()` | 独立インフラ、キャッシュ不要 |
| `OnlineIndexer` | `database.withTransaction()` | バックグラウンド処理 |
| `DirectoryLayer` | `database.withTransaction()` | システム操作 |
| `Migration` | `database.withTransaction()` | システム操作 |

### FDBContext (Transaction Manager)

`FDBContext` is the central transaction manager that owns `ReadVersionCache` and provides the user-facing API.

**Reference**: While FDB Record Layer's `FDBDatabase.java` maintains `lastSeenFDBVersion` at the database level shared across contexts, our design places the cache at the Context level for better scoping per unit of work.

**Initialization**:
```swift
// Create via FDBContainer (recommended)
let container = try FDBContainer(for: schema)
let context = container.newContext()

// Context owns its own ReadVersionCache
// Each context has independent cache state
```

**Transaction Execution**:
```swift
// Transaction with weak read semantics (uses context's cache)
let result = try await context.withTransaction(configuration: .readOnly) { tx in
    try await tx.getValue(for: key, snapshot: true)
}

// Transaction without cache (strict consistency)
let result = try await context.withTransaction(configuration: .default) { tx in
    try await tx.getValue(for: key, snapshot: false)
}

// System operations bypass context and use raw database
try await container.database.withTransaction(configuration: .batch) { tx in
    // DirectoryLayer, Migration, etc.
}
```

**Cache Management**:
```swift
// Clear cache after schema changes or for testing
context.clearReadVersionCache()

// Get cache info for debugging/metrics
if let (version, ageMillis) = context.readVersionCacheInfo() {
    print("Cached version: \(version), age: \(ageMillis)ms")
}
```

### CachePolicy

Cache policy for read operations that controls whether transactions reuse cached read versions.

**Trade-off**: Slightly stale data vs. reduced latency and load.

**Policies**:

| Policy | Behavior | Use Case |
|--------|----------|----------|
| `.server` | Always fetch from server (no cache) | Read-after-write consistency, critical operations |
| `.cached` | Use cache if available (no time limit) | Dashboard queries, analytics |
| `.stale(N)` | Use cache only if younger than N seconds | Custom staleness tolerance |

**Usage**:
```swift
// Strict consistency (default)
let users = try await context.fetch(User.self)
    .cachePolicy(.server)
    .execute()

// Use cache if available
let users = try await context.fetch(User.self)
    .cachePolicy(.cached)
    .execute()

// Use cache only if younger than 30 seconds
let users = try await context.fetch(User.self)
    .cachePolicy(.stale(30))
    .execute()

// Single item lookup with cache (O(1) direct key lookup)
let user = try await context.model(for: userId, as: User.self, cachePolicy: .cached)

// With TransactionConfiguration
let config = TransactionConfiguration(
    priority: .batch,
    cachePolicy: .stale(60)  // Up to 60 second staleness
)

try await context.withTransaction(configuration: config) { tx in
    // May read slightly stale data, but with lower latency
    // Uses context's ReadVersionCache
}
```

### ReadVersionCache

Caches read versions from successful transactions for CachePolicy.

**Thread Safety**: Uses `Mutex` for thread-safe access.

**Monotonic Time**: Uses `DispatchTime.now().uptimeNanoseconds` for timestamps.

**Update Behavior**:
- **After commit**: Uses committed version (most accurate)
- **After read-only tx**: Uses read version (only if newer than cached)

**Metrics Collection**:
```swift
let cache = MetricsCollectingReadVersionCache()

// Use normally...
if let version = cache.getCachedVersion(policy: .cached) { ... }

// Check metrics
let metrics = cache.metrics
print("Cache hit rate: \(metrics.hitRate * 100)%")
```

### FDBContext Semantics

**Core Operations**:
```swift
let context = container.newContext()

// insert() - queue model for insertion (not persisted yet)
context.insert(model)

// delete() - queue model for deletion (not persisted yet)
context.delete(model)

// save() - persist all queued changes atomically
try await context.save()

// fetch() - returns ONLY persisted data (ignores pending changes)
let results = try await context.fetch(MyType.self).execute()

// model(for:as:) - direct O(1) ID lookup (uses GET security)
let user = try await context.model(for: userId, as: User.self)

// model(for:as:cachePolicy:) - with cache policy for read optimization
let user = try await context.model(for: userId, as: User.self, cachePolicy: .cached)
```

**Design Principle**: `fetch()` returns only persisted data. Pending inserts/deletes are NOT mixed into fetch results.

**Rationale**:
- Clean semantics: fetch always reflects database state
- No complex offset/limit issues with pending mixing
- No sort order conflicts between pending and persisted
- Predictable behavior for cursor-based pagination

**Usage Pattern**:
```swift
let context = container.newContext()

// Insert a model
var user = User(name: "Alice")
context.insert(user)

// fetch() does NOT include the pending insert
let users = try await context.fetch(User.self).execute()
// users.isEmpty == true (assuming empty database)

// After save(), the model is persisted
try await context.save()

// Now fetch() returns the persisted model
let usersAfterSave = try await context.fetch(User.self).execute()
// usersAfterSave.count == 1
```

### Data Operation Flow (FDBContext.save)

```
FDBContext.save()
    │
    ├─ Group models by type
    │
    └─ anyStore.withRawTransaction { transaction in
           │
           ├─ For each type:
           │      store.executeBatchInTransaction(inserts, deletes, transaction)
           │          → Security evaluation
           │          → Serialize with single encoder
           │          → Index maintenance
           │          → Returns [SerializedModel]
           │
           └─ processDualWrites(serializedInserts, deletes, transaction)
                  → Reuse pre-serialized data (no re-serialization)
                  → Write to polymorphic directories
       }
```

**Optimization Points**:
- Single transaction for all operations
- Batch processing per type (not per-model)
- `SerializedModel` avoids re-serialization for Polymorphable dual-write
- Single encoder instance reused across batch

### Data Layout in FoundationDB

```
[fdb]/R/[PersistableType]/[id]           → ItemEnvelope(JSON-encoded item)
[fdb]/B/[blob-key]                       → Large value blob chunks
[fdb]/I/[indexName]/[values...]/[id]     → Index entry (empty value for scalar)
[fdb]/_metadata/schema/version           → Tuple(major, minor, patch)
[fdb]/_metadata/index/[indexName]/state  → IndexState (readable/write_only/disabled)
```

**ItemEnvelope Format**:
- All items are wrapped in `ItemEnvelope` with magic number `ITEM` (0x49 0x54 0x45 0x4D)
- Large values (>100KB) are automatically split into blob chunks in `/B/` subspace
- Reading raw data without `ItemStorage.read()` will fail with "Data is not in ItemEnvelope format"

Subspace keys are single characters for storage efficiency. Use `SubspaceKey.items`, `SubspaceKey.indexes`, `SubspaceKey.blobs`, etc. for semantic clarity in code.

## Implemented Features

### Index Types (12 types)

| Index | Module | Description |
|-------|--------|-------------|
| Scalar | `ScalarIndex` | Equality/range queries on single/compound fields |
| Vector | `VectorIndex` | Semantic search (Flat brute-force, HNSW approximate) |
| FullText | `FullTextIndex` | Text search with stemming, fuzzy matching, highlighting |
| Spatial | `SpatialIndex` | Geographic queries (Geohash, Morton Code, S2 cells) |
| Rank | `RankIndex` | Leaderboard-style ranking with position queries |
| Permuted | `PermutedIndex` | Permutation-based multi-field queries |
| Graph | `GraphIndex` | Unified graph/RDF index with multiple strategies (see below) |
| Aggregation | `AggregationIndex` | Materialized aggregations (Count, Sum, Min/Max, Average) |
| Version | `VersionIndex` | Temporal versioning with FDB versionstamps |
| Bitmap | `BitmapIndex` | Set membership queries using Roaring Bitmaps |
| Leaderboard | `LeaderboardIndex` | Time-windowed leaderboards |
| Relationship | `RelationshipIndex` | Cross-type indexes for relationship queries (requires transaction) |

### GraphIndex (Unified Graph/RDF Index)

GraphIndex provides a unified solution for both general graph edges and RDF triples with configurable storage strategies.

**Terminology Mapping**:
```
Graph terms:  Source  --[Label]------>  Target
RDF terms:    Subject --[Predicate]-->  Object
Unified:      From    --[Edge]------->  To
```

**Storage Strategies** (`GraphIndexStrategy`):

| Strategy | Indexes | Write Cost | Use Case |
|----------|---------|------------|----------|
| `adjacency` | 2 (out/in) | Low | Social graphs, simple traversal |
| `tripleStore` | 3 (SPO/POS/OSP) | Medium | RDF/knowledge graphs, SPARQL-like queries |
| `hexastore` | 6 (all permutations) | High | Read-heavy workloads, all query patterns optimal |

**Usage Examples**:
```swift
// RDF triple store
@Persistable
struct Statement {
    var subject: String
    var predicate: String
    var object: String

    #Index<Statement>(type: GraphIndexKind.rdf(
        subject: \.subject,
        predicate: \.predicate,
        object: \.object,
        strategy: .tripleStore
    ))
}

// Social graph (follows)
@Persistable
struct Follow {
    var follower: String
    var followee: String

    #Index<Follow>(type: GraphIndexKind.adjacency(
        source: \.follower,
        target: \.followee
    ))
}

// High-performance knowledge graph
@Persistable
struct KnowledgeTriple {
    var entity: String
    var relation: String
    var value: String

    #Index<KnowledgeTriple>(type: GraphIndexKind.knowledgeGraph(
        entity: \.entity,
        relation: \.relation,
        value: \.value
    ))
}
```

**Query Patterns by Strategy**:
```
adjacency (2-index):
  [out]/[edge]/[from]/[to]     - outgoing edges
  [in]/[edge]/[to]/[from]      - incoming edges

tripleStore (3-index):
  [spo]/[from]/[edge]/[to]     - S??, SP?, SPO queries
  [pos]/[edge]/[to]/[from]     - ?P?, ?PO queries
  [osp]/[to]/[from]/[edge]     - ??O queries

hexastore (6-index):
  All 6 permutations for optimal single-index scan on any pattern
```

**Key Structure Constraints (Adjacency Strategy)**:

Adjacency strategy has `[direction][edge][from][to]` key structure, where `edge` comes before `from`:

| Query Pattern | Adjacency | TripleStore | Hexastore |
|---------------|-----------|-------------|-----------|
| `(from, edge, ?)` | ✅ Prefix scan | ✅ Prefix scan | ✅ Prefix scan |
| `(from, ?, ?)` | ❌ Full scan + filter | ✅ Prefix scan | ✅ Prefix scan |
| `(?, edge, to)` | ✅ Prefix scan (via `in` index) | ✅ Prefix scan | ✅ Prefix scan |

**API semantics for `edgeLabel` parameter**:
- `edgeLabel = "follows"` → Filter by specific label (efficient prefix scan)
- `edgeLabel = ""` → Filter by empty label (for unlabeled graphs, efficient prefix scan)
- `edgeLabel = nil` → Match ALL labels (wildcard, requires full scan + filter for adjacency)

**Performance Note**: When using adjacency strategy with `edgeLabel=nil` in `ShortestPathFinder`, `PageRankComputer`, or `GraphQuery`, the query must scan the entire edge subspace. For better performance, either specify an `edgeLabel` or use `tripleStore`/`hexastore` strategy.

**Reference**: Weiss, C., Karras, P., & Bernstein, A. (2008). "Hexastore: sextuple indexing for semantic web data management" VLDB Endowment, 1(1), 1008-1019.

### Uniqueness Enforcement

Uniqueness constraints are enforced by `IndexMaintenanceService` before index entries are created.

**Architecture**:
```
FDBDataStore.save()
    │
    ├── 1. Read existing record (if update)
    │
    └── 2. IndexMaintenanceService.updateIndexesUntyped()
            │
            ├── checkUniquenessConstraint()  ← Uniqueness check BEFORE index write
            │       │
            │       ├── Extract field values from model
            │       ├── Build index key prefix
            │       ├── Scan for existing entries with same values
            │       ├── Skip self (update case) using Tuple equality
            │       └── Throw UniquenessViolationError if conflict
            │
            └── IndexMaintainer.updateIndex()  ← Actual index write
```

**Array Field Uniqueness**:

For array-typed fields (e.g., `tags: [String]`), each array element is checked individually:

```swift
// Model
@Persistable
struct Document {
    var tags: [String]  // unique constraint
}

// Index entries created (one per element):
// [subspace]["tag1"][id]
// [subspace]["tag2"][id]

// Uniqueness check: each element checked separately
// If another document has "tag1", violation is thrown
```

**Self-Update Detection**:

When updating an existing record, the system must skip the record's own index entries:

```swift
// Uses Tuple equality (type-agnostic, compares encoded bytes)
// Supports all TupleElement ID types: String, Int64, UUID, etc.

// Key structure: [subspace prefix][values...][id...]
// ID is extracted from the END of the key tuple
let idStartIndex = keyTuple.count - oldId.count
let existingIdElements = keyTuple[idStartIndex..<keyTuple.count]

if oldId == Tuple(existingIdElements) {
    continue  // Skip our own entry
}
```

**Sparse Index Behavior**:

| Field Value | Index Entry | Uniqueness Check |
|-------------|-------------|------------------|
| `nil` | None (sparse) | Skipped |
| `[]` (empty array) | None | Skipped |
| `["a", "b"]` | Two entries | Each checked |

**Index State Behavior**:

| State | Behavior |
|-------|----------|
| `readable` | Throw `UniquenessViolationError` immediately |
| `writeOnly` | Record violation for later resolution |
| `disabled` | Skip uniqueness check |

### Online Indexing

| Component | File | Description |
|-----------|------|-------------|
| `OnlineIndexer` | `OnlineIndexer.swift` | Batch processing with resumable progress via RangeSet |
| `MultiTargetOnlineIndexer` | `MultiTargetOnlineIndexer.swift` | Build multiple indexes in single scan |
| `MutualOnlineIndexer` | `MutualOnlineIndexer.swift` | Build bidirectional indexes simultaneously |
| `OnlineIndexScrubber` | `OnlineIndexScrubber.swift` | Two-phase consistency verification and repair |
| `IndexFromIndexBuilder` | `IndexFromIndexBuilder.swift` | Build index from existing index (reduces I/O) |
| `AdaptiveThrottler` | `AdaptiveThrottler.swift` | Backpressure-aware rate limiting |

### Query Planning (Cascades Framework)

| Component | File | Description |
|-----------|------|-------------|
| `CascadesOptimizer` | `Cascades/CascadesOptimizer.swift` | Top-down rule-based optimizer (Graefe 1995) |
| `Memo` | `Cascades/Memo.swift` | Memoization structure for equivalence classes |
| `Rule` | `Cascades/Rule.swift` | Transformation and implementation rules |
| `Expression` | `Cascades/Expression.swift` | Logical and physical operators |
| `CostEstimator` | `CostEstimator.swift` | Statistics-driven cost model |
| `StatisticsProvider` | `StatisticsProvider.swift` | Column cardinality, histograms, HyperLogLog |

### Serialization & Storage

| Component | File | Description |
|-----------|------|-------------|
| `ItemStorage` | `Serialization/ItemStorage.swift` | Primary storage interface with envelope format |
| `ItemEnvelope` | `Serialization/ItemEnvelope.swift` | Magic-number validated storage format |
| `TransformingSerializer` | `Serialization/TransformingSerializer.swift` | Compression (LZ4/zlib/LZMA/LZFSE) + encryption (AES-256-GCM) |
| `RecordEncryption` | `Serialization/RecordEncryption.swift` | Key providers (Static, Rotating, Derived, Environment) |

#### ItemStorage/ItemEnvelope Format

All items are stored using `ItemStorage.write()` and read using `ItemStorage.read()`. The format uses a magic number header for validation:

```
[ITEM magic (4 bytes: 0x49 0x54 0x45 0x4D)] [payload...]
```

**Important**: Direct `transaction.getValue()` cannot be used to read item data. Always use `ItemStorage.read()` to properly unwrap the envelope:

```swift
// ✅ Correct: Use ItemStorage
let storage = ItemStorage(transaction: tx, blobsSubspace: blobsSubspace)
if let data = try await storage.read(for: key) {
    let item = try decoder.decode(MyType.self, from: Data(data))
}

// ❌ Wrong: Direct transaction access
if let data = try await tx.getValue(for: key) {
    // Error: "Data is not in ItemEnvelope format"
    let item = try decoder.decode(MyType.self, from: Data(data))
}
```

### Transaction Management

| Component | File | Description |
|-----------|------|-------------|
| `TransactionConfiguration` | `Transaction/TransactionConfiguration.swift` | Priority (default/batch/system), timeout, retry |
| `TransactionRunner` | `Transaction/TransactionRunner.swift` | Retry logic with exponential backoff |
| `CommitHook` | `Transaction/CommitHook.swift` | Synchronous callbacks before commit |
| `AsyncCommitHook` | `Transaction/AsyncCommitHook.swift` | Asynchronous callbacks before commit |

#### Transaction Strategy

All database operations are executed through `TransactionRunner`, which provides:
- Exponential backoff with jitter (prevents thundering herd)
- Configurable retry limits and timeouts
- Weak read semantics support (cached read versions)
- Transaction priority control

**Execution Flow**:
```
context.withTransaction(configuration: .X) { tx in ... }
       │
       ▼
TransactionRunner.run(configuration:readVersionCache:operation:)
       │
       ├── 1. Create NEW transaction for each attempt
       ├── 2. Apply configuration (priority, timeout, etc.)
       ├── 3. Apply cached read version (first attempt only, if weak read semantics)
       ├── 4. Execute operation
       ├── 5. Commit transaction  ← 自動コミット
       ├── 6. On retryable error: exponential backoff + retry
       └── 7. On success: update read version cache (commit version)
```

**⚠️ 重要: withTransaction 内で commit() を呼ばないこと**

`withTransaction` はクロージャが正常終了した後、自動的にコミットします。クロージャ内で明示的に `commit()` を呼ぶと二重コミットになり、FDB が "Operation issued while a commit was outstanding" エラーを返します。

```swift
// ❌ 悪い例: 二重コミット
try await database.withTransaction { tx in
    tx.setValue(value, for: key)
    _ = try await tx.commit()  // ← 呼んではいけない
}
// withTransaction が再度 commit() を呼ぶ → エラー

// ✅ 良い例: withTransaction に任せる
try await database.withTransaction { tx in
    tx.setValue(value, for: key)
    // commit() は呼ばない - withTransaction が自動でコミット
}
```

**注意**: `TransactionProtocol.commit()` は public API ですが、`withTransaction` パターンを使う場合は呼び出し不要です。この設計上の制約は fdb-swift-bindings の API 設計に起因します。

**Backoff Algorithm** (AWS exponential backoff pattern):
```
delay = min(initialDelay * 2^attempt, maxDelay) + jitter
```
- `initialDelay`: Configurable via `DATABASE_TRANSACTION_INITIAL_DELAY` env var (default: 300ms)
- `maxDelay`: From `TransactionConfiguration.maxRetryDelay` (default: 1000ms)
- `jitter`: 0-50% of delay (prevents thundering herd)

**Weak Read Semantics Flow**:
```
1st attempt:
   └── Check ReadVersionCache for valid cached version
       └── If valid (age < maxStalenessMillis): setReadVersion(cachedVersion)
       └── If invalid/missing: FDB gets fresh read version

Retry attempts:
   └── Always get fresh read version (avoid repeating transaction_too_old)

After commit:
   └── Update cache with committed version (or read version for read-only tx)
```

#### Configuration Presets

| Preset | Timeout | Retry | Priority | Read Priority | CachePolicy | Use Case |
|--------|---------|-------|----------|---------------|-------------|----------|
| `.default` | ~5s (FDB) | 5 | normal | normal | `.server` | User-facing CRUD, queries |
| `.interactive` | 1s | 3 | normal | normal | `.server` | Lock operations (fail-fast) |
| `.batch` | 30s | 20 | batch | low | `.server` | Background indexing, statistics |
| `.longRunning` | 60s | 50 | batch | low | `.stale(60)` | Full scans, analytics |
| `.readOnly` | 2s | 3 | normal | normal | `.cached` | Dashboard, cached lookups |
| `.system` | 2s | 5 | system | high | `.server` | Reserved for critical operations |

**Additional Configuration Options**:

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `maxRetryDelay` | Int (ms) | 1000 | Maximum delay between retries |
| `readPriority` | ReadPriority | .normal | Priority for read operations (.low/.normal/.high) |
| `disableReadCache` | Bool | false | Disable server-side read caching |
| `cachePolicy` | CachePolicy | .server | Cache policy for read operations |
| `tracing` | Tracing | .disabled | Transaction logging and tracing |

**Tracing Configuration**:
```swift
let config = TransactionConfiguration(
    tracing: .init(
        transactionID: "req-12345",      // For log correlation
        logTransaction: true,             // Detailed FDB logging
        serverRequestTracing: true,       // Server operation tracing
        tags: ["api-v2", "user-request"]  // Categorization
    )
)
```

**Preset Selection Guide**:

```
┌─────────────────────────────────────────────────────────────────┐
│                    Which preset should I use?                   │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  User clicks "Save" button?                                     │
│  User runs a search query?          ──────▶  .default           │
│  API request from client?                                       │
│                                                                 │
│  Lock acquisition/release?                                      │
│  Need to fail fast on contention?   ──────▶  .interactive       │
│                                                                 │
│  Dashboard display?                                             │
│  Cached lookup (stale OK)?          ──────▶  .readOnly          │
│  Analytics read query?                                          │
│                                                                 │
│  Background index building?                                     │
│  Statistics collection?             ──────▶  .batch             │
│  Schema migration?                                              │
│                                                                 │
│  Full table scan for analytics?                                 │
│  Report generation?                 ──────▶  .longRunning       │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

#### Usage Examples

```swift
// User-facing save operation (via context)
let context = container.newContext()
context.insert(user)
try await context.save()  // Uses context.withTransaction internally

// Explicit transaction with custom configuration
try await context.withTransaction(configuration: .default) { tx in
    // Multiple operations in single transaction
    try await store.fetchInTransaction(transaction: tx)
}

// Dashboard query (stale data OK, uses context's cached read version)
try await context.withTransaction(configuration: .readOnly) { tx in
    try await tx.getValue(for: dashboardKey, snapshot: true)
}

// Custom configuration
let customConfig = TransactionConfiguration(
    timeout: 10_000,      // 10 seconds
    retryLimit: 10,
    priority: .default,
    cachePolicy: .stale(30)  // Allow up to 30 seconds stale reads
)
try await context.withTransaction(configuration: customConfig) { tx in
    // ... operation ...
}

// System operations (bypass context, use raw database)
try await container.database.withTransaction(configuration: .batch) { tx in
    try await indexer.buildBatch(items: batch, transaction: tx)
}

// Lock acquisition (system-level, fail fast)
try await container.database.withTransaction(configuration: .interactive) { tx in
    guard try await acquireLock(tx) else { return false }
    // ... do work ...
    return true
}
```

#### Internal Routing

**Application Operations** (use `context.withTransaction()` for ReadVersionCache support):

| Entry Point | Routes Through | Configuration |
|-------------|----------------|---------------|
| `FDBContext.save()` | `context.withTransaction` | `.default` |
| `FDBContext.fetch()` | `context.withTransaction` | `.default` |
| `IndexQueryContext.withTransaction` | `context.withTransaction` | `.default` |
| `Index Maintainer search()` | `context.withTransaction` | `.default` |

**System Operations** (use `container.database.withTransaction()` directly):

| Entry Point | Routes Through | Configuration |
|-------------|----------------|---------------|
| `DirectoryLayer operations` | `database.withTransaction` | `.batch` |
| `Migration operations` | `database.withTransaction` | `.batch` |
| `OnlineIndexer.buildBatch()` | `database.withTransaction` | `.batch` |
| `StatisticsManager.*` | `database.withTransaction` | `.batch` |
| `SynchronizedSession.acquire()` | `database.withTransaction` | `.interactive` |

**Important**:
- Application code should use `context.withTransaction()` to benefit from ReadVersionCache
- System code (DirectoryLayer, Migration) uses `container.database.withTransaction()` directly
- Never call `withTransaction { }` without a configuration parameter

**Environment Variables**:
| Variable | Default | Description |
|----------|---------|-------------|
| `DATABASE_TRANSACTION_RETRY_LIMIT` | 5 | Default retry limit |
| `DATABASE_TRANSACTION_MAX_RETRY_DELAY` | 1000 | Maximum retry delay (ms) |
| `DATABASE_TRANSACTION_TIMEOUT` | nil (FDB default) | Transaction timeout (ms) |
| `DATABASE_TRANSACTION_INITIAL_DELAY` | 300 | Initial backoff delay (ms) |

### Field-Level Security

Field-Level Security provides fine-grained access control at the field level within models.

**Two-Package Split**:
- **database-kit** (Core): `@Restricted` property wrapper, `FieldAccessLevel`, `RestrictedProtocol`
- **database-framework** (DatabaseEngine): `FieldSecurityEvaluator`, `FieldSecurityError`, `FDBContext+FieldSecurity`

**Model Definition** (using database-kit):
```swift
@Persistable
struct Employee {
    var id: String = ULID().ulidString
    var name: String = ""

    // Only HR and managers can read; only HR can write
    @Restricted(read: .roles(["hr", "manager"]), write: .roles(["hr"]))
    var salary: Double = 0

    // Only HR can read/write
    @Restricted(read: .roles(["hr"]), write: .roles(["hr"]))
    var ssn: String = ""

    // Anyone can read; only admin can write
    @Restricted(write: .roles(["admin"]))
    var department: String = ""

    // Only authenticated users can read
    @Restricted(read: .authenticated)
    var internalNotes: String = ""
}
```

**Access Levels** (`FieldAccessLevel`):

| Level | Description |
|-------|-------------|
| `.public` | Everyone can access (default) |
| `.authenticated` | Only authenticated users |
| `.roles(Set<String>)` | Only users with specific roles |
| `.custom((AuthContext) -> Bool)` | Custom predicate |

**Usage with FDBContext** (using database-framework):
```swift
// Set auth context via TaskLocal
try await AuthContextKey.$current.withValue(userAuth) {
    // Secure fetch - restricted fields are masked
    let employees = try await context.fetchSecure(Employee.self).execute()

    // Secure single fetch
    let employee = try await context.modelSecure(for: id, as: Employee.self)

    // Check field access
    let canReadSalary = context.canRead(\.salary, in: employee)
    let canWriteSalary = context.canWrite(\.salary, in: employee)

    // Get list of restricted fields
    let unreadable = context.unreadableFields(in: employee)
    let unwritable = context.unwritableFields(in: employee)

    // Validate before write
    try context.validateFieldWrite(original: existingEmployee, updated: modifiedEmployee)
    context.insert(modifiedEmployee)
    try await context.save()
}
```

**Key Components**:

| Component | Package | Description |
|-----------|---------|-------------|
| `FieldAccessLevel` | database-kit | Access level enum |
| `@Restricted` | database-kit | Property wrapper for field restrictions |
| `RestrictedProtocol` | database-kit | Protocol for runtime type identification |
| `FieldSecurityEvaluator` | database-framework | Evaluation logic (mask, validate) |
| `FieldSecurityError` | database-framework | Error types (readNotAllowed, writeNotAllowed) |
| `SecureQueryExecutor` | database-framework | Query executor with automatic masking |

**Limitation**: Field masking (`mask()`) is currently a no-op because Swift's reflection doesn't support property mutation. Use `unreadableFields(in:auth:)` to identify fields that should be filtered at the serialization or presentation layer.

### Dynamic Directories (Partitioned Data)

Dynamic directories enable multi-tenant and partitioned data patterns by including field values in the directory path.

**Key Components**:

| Type | Description |
|------|-------------|
| `DirectoryPath<T>` | Type-safe struct holding field values for directory resolution |
| `AnyDirectoryPath` | Type-erased version for use when generic type is unknown |
| `DirectoryPathError` | Error type for directory resolution failures |
| `Field<T>` | Directory component that references a model field |
| `Path` | Static directory component |

**Model Definition**:
```swift
@Persistable
struct TenantOrder {
    // Dynamic directory: includes tenantID field value in path
    #Directory<TenantOrder>("app", Field(\.tenantID), "orders")

    var id: String = UUID().uuidString
    var tenantID: String = ""  // Partition key
    var status: String = ""
    var total: Double = 0.0
}
```

**Storage Layout**:
```
[fdb]/app/[tenantID]/orders/R/TenantOrder/[id] → ItemEnvelope
```

**Query API (Fluent)**:
```swift
// Query with partition - required for dynamic directory types
let orders = try await context.fetch(TenantOrder.self)
    .partition(\.tenantID, equals: "tenant_123")
    .where(\.status == "pending")
    .execute()

// Multiple partition fields
let data = try await context.fetch(RegionalData.self)
    .partition(\.region, equals: "us-west")
    .partition(\.year, equals: 2024)
    .execute()
```

**DirectoryPath API (Manual)**:
```swift
// Create DirectoryPath manually
var path = DirectoryPath<TenantOrder>()
path.set(\.tenantID, to: "tenant_123")

// Use with FDBContainer
let subspace = try await container.resolveDirectory(for: TenantOrder.self, path: path)
let store = try await container.store(for: TenantOrder.self, path: path)

// Use with model() for single item fetch
let order = try await context.model(for: orderID, as: TenantOrder.self, partition: path)

// Create from existing model instance
let pathFromModel = DirectoryPath<TenantOrder>.from(existingOrder)
```

**API Design**:
```swift
// FDBContainer - unified API with `path:` parameter
public func resolveDirectory<T: Persistable>(
    for type: T.Type,
    path: DirectoryPath<T> = DirectoryPath()
) async throws -> Subspace

public func resolveDirectory(
    for type: any Persistable.Type,
    path: AnyDirectoryPath? = nil
) async throws -> Subspace

public func store<T: Persistable>(
    for type: T.Type,
    path: DirectoryPath<T> = DirectoryPath()
) async throws -> any DataStore

public func store(
    for type: any Persistable.Type,
    path: AnyDirectoryPath? = nil
) async throws -> any DataStore
```

**Static vs Dynamic Directories**:

| Type | directoryPathComponents | hasDynamicDirectory |
|------|------------------------|---------------------|
| Static | `["app", "users"]` | `false` |
| Dynamic | `["app", Field(\.tenantID), "orders"]` | `true` |

**Error Handling**:
```swift
// Querying dynamic directory without partition throws error
do {
    let orders = try await context.fetch(TenantOrder.self).execute()
} catch DirectoryPathError.dynamicFieldsRequired(let typeName, let fields) {
    // "Type 'TenantOrder' requires field values for directory resolution: tenantID"
}

// Missing required partition field
do {
    var path = DirectoryPath<TenantOrder>()
    try path.validate()  // tenantID not set
} catch DirectoryPathError.missingFields(let fields) {
    // "Missing directory field values: tenantID"
}
```

**TransactionContext Support**:
```swift
// Via FDBContext (recommended - uses ReadVersionCache)
try await context.withTransaction(configuration: .default) { transaction in
    let txContext = TransactionContext(transaction: transaction, container: container)

    // set() extracts partition from model automatically
    try await txContext.set(order)

    // get() requires explicit partition
    var path = DirectoryPath<TenantOrder>()
    path.set(\.tenantID, to: "tenant_123")
    let fetched = try await txContext.get(TenantOrder.self, id: orderID, partition: path)
}

// Via raw database (system operations, no cache)
try await container.database.withTransaction(configuration: .batch) { transaction in
    let txContext = TransactionContext(transaction: transaction, container: container)
    // ...
}
```

### Polymorphable (Union Record Type)

Polymorphable enables multiple `Persistable` types to share a directory and indexes, allowing polymorphic queries across different concrete types (similar to FDB Record Layer's Union Record Type).

**Core Concept**:
```swift
// In database-kit: Define a polymorphic protocol
@Polymorphable
protocol Document: Polymorphable {
    var id: String { get }
    var title: String { get }

    #Directory<Document>("app", "documents")
}

// Conforming types share the protocol's directory
@Persistable
struct Article: Document {
    var id: String = ULID().ulidString
    var title: String
    var content: String

    #Directory<Article>("app", "articles")  // Optional: own directory for dual-write
}

@Persistable
struct Report: Document {
    var id: String = ULID().ulidString
    var title: String
    var data: Data
    // No #Directory: uses default (persistableType)
}
```

**Property Separation** (due to Swift type system limitations):

| Property | Protocol | Description |
|----------|----------|-------------|
| `directoryPathComponents` | `Persistable` | Type-specific directory |
| `polymorphicDirectoryPathComponents` | `Polymorphable` | Shared protocol directory |
| `directoryLayer` | `Persistable` | Type-specific layer |
| `polymorphicDirectoryLayer` | `Polymorphable` | Shared protocol layer |
| `indexDescriptors` | `Persistable` | Type-specific indexes |
| `polymorphicIndexDescriptors` | `Polymorphable` | Shared protocol indexes |

**Dual-Write Behavior**:
When a type conforms to `Polymorphable` AND has its own `#Directory`:
- Save: Data written to both type-specific AND polymorphic directories
- Delete: Data removed from both directories

**Storage Layout**:
```
[polymorphic-directory]/R/[typeCode]/[id] → Protobuf-encoded item
[type-directory]/R/[PersistableType]/[id] → Protobuf-encoded item (if dual-write)
```

`typeCode` is a deterministic Int64 hash (DJB2 algorithm) of the type name.

**Usage**:
```swift
// Save (automatic dual-write if applicable)
context.insert(article)
try await context.save()

// Polymorphic query - returns all conforming types
let allDocuments = try await context.fetchPolymorphic(Article.self)
// Returns [Article, Report, ...] as [any Persistable]

// Fetch by ID from polymorphic directory
let doc = try await context.fetchPolymorphic(Article.self, id: someId)
```

**Swift Type System Limitation**:
Protocol types cannot be passed to generic functions requiring `Polymorphable` conformance:
```swift
// ❌ Compile error: 'any Document' cannot conform to 'Polymorphable'
try await context.fetchPolymorphic(Document.self)

// ✅ Use concrete type (all conforming types share the same polymorphic directory)
try await context.fetchPolymorphic(Article.self)
```

**Schema.Entity Design Principle**:
`Schema.Entity` is pure data type metadata. Storage information (directories, polymorphic settings) is accessed from the type at runtime via `entity.persistableType`:
```swift
// Schema.Entity only contains:
// - name: String
// - allFields: [String]
// - indexDescriptors: [IndexDescriptor]
// - enumMetadata: [String: EnumMetadata]
// - persistableType: any Persistable.Type  (for runtime type access)

// To check if a type is polymorphic:
if let polyType = entity.persistableType as? any Polymorphable.Type {
    let typeCode = polyType.typeCode(for: entity.name)
    let polyDir = polyType.polymorphicDirectoryPathComponents
}
```

### Comparison with FDB Record Layer

| Feature | Record Layer | database-framework | Status |
|---------|--------------|-------------------|--------|
| Index Types | Value, Rank, Lucene, Count, Sum, Spatial | 13 types including Vector, Graph, Triple | ✅ More |
| Online Indexer | ✅ | ✅ Multi-target, Mutual, Index-from-Index | ✅ |
| Index Scrubbing | ✅ | ✅ Two-phase verification | ✅ |
| Query Planner | Cascades | ✅ Cascades framework | ✅ |
| Large Record Splitting | ✅ SplitHelper | ✅ LargeValueSplitter | ✅ |
| Compression | ✅ Deflate | ✅ LZ4/zlib/LZMA/LZFSE | ✅ |
| Encryption | ✅ AES-CBC | ✅ AES-256-GCM | ✅ |
| Transaction Priority | ✅ | ✅ default/batch/system | ✅ |
| Version History | ✅ FDBRecordVersion | ✅ VersionIndex with versionstamps | ✅ |
| Synthetic Records | ✅ JoinedRecordType | ❌ | Not implemented |
| Union Record Type | ✅ | ✅ Polymorphable (dual-write) | ✅ |
| Uniqueness Enforcement | ✅ checkUniqueness | ✅ Index state-based | ✅ |
| SQL Interface | ✅ ANTLR parser | ❌ | Not implemented (SwiftData-like API) |
| Weak Read Semantics | ✅ Cached read version | ✅ FDBContext + ReadVersionCache | ✅ |

### Extension Pattern for Optional Features

**設計原則**: このプロジェクトは SPM dependencies でカスタマイズ可能なデータベースを目指している。オプション機能は extension で提供し、コアを変更しない。

**パターン**:
```swift
// 各機能モジュールが FDBContext に extension で API を追加
// ユーザーは必要なモジュールを import して使う

// VectorIndex モジュール
extension FDBContext {
    public func findSimilar<T: Persistable>(_ type: T.Type) -> VectorQueryBuilder<T>
}

// FullTextIndex モジュール
extension FDBContext {
    public func search<T: Persistable>(_ type: T.Type) -> FullTextQueryBuilder<T>
}

// Relationship (DatabaseEngine 内)
extension FDBContext {
    public func related<T, R>(_ item: T, _ relationship: KeyPath<T, R?>) async throws -> R?
    public func related<T, R>(_ item: T, _ relationship: KeyPath<T, [R]>) async throws -> [R]
}
```

**重要な禁止事項**:
- ❌ コアの `FDBContext.save()` や `delete()` を直接変更してオプション機能を埋め込まない
- ❌ 使わないユーザーにオーバーヘッドを強制しない
- ✅ 新しいメソッドを extension で追加
- ✅ ユーザーが明示的に呼び出す

**Relationship の Delete Rule Enforcement**:
```swift
// 基本の delete (relationship rules なし)
context.delete(customer)
try await context.save()

// Delete rule enforcement が必要な場合は extension メソッドを使用
try await context.deleteEnforcingRelationshipRules(customer)
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

### Index Implementation Pattern (詳細規定)

インデックスモジュールは**書き込み（維持）**と**読み取り（クエリ）**の2つの責務を持つ。

#### モジュール構成

```
Sources/{IndexName}Index/
├── {IndexName}IndexKind+Maintainable.swift  # IndexKindMaintainable conformance
├── {IndexName}IndexMaintainer.swift         # IndexMaintainer 実装 + search()
├── {IndexName}Query.swift                   # FDBContext extension + QueryBuilder
├── {IndexName}IndexConfiguration.swift      # ランタイム設定（オプション）
└── Fusion/                                  # Fusion API 対応（オプション）
    └── {IndexName}FusionQuery.swift
```

#### 1. IndexMaintainer プロトコル（書き込み側）

```swift
public protocol IndexMaintainer<Item>: Sendable {
    associatedtype Item: Persistable

    /// データ変更時にインデックスを更新
    /// - oldItem: 削除/更新前のアイテム (nil = 新規挿入)
    /// - newItem: 挿入/更新後のアイテム (nil = 削除)
    func updateIndex(
        oldItem: Item?,
        newItem: Item?,
        transaction: any TransactionProtocol
    ) async throws

    /// バッチ構築時に単一アイテムをインデックス
    func scanItem(
        _ item: Item,
        id: Tuple,
        transaction: any TransactionProtocol
    ) async throws

    /// スクラバー検証用: 期待されるインデックスキーを計算
    func computeIndexKeys(for item: Item, id: Tuple) async throws -> [FDB.Bytes]

    /// カスタムビルド戦略（HNSW等の特殊なインデックス用）
    var customBuildStrategy: (any IndexBuildStrategy<Item>)? { get }
}
```

**実装規約**:
```swift
public struct MyIndexMaintainer<Item: Persistable>: IndexMaintainer {

    public func updateIndex(oldItem: Item?, newItem: Item?, transaction: any TransactionProtocol) async throws {
        // 1. 旧エントリ削除
        if let old = oldItem {
            let keys = try buildIndexKeys(for: old)
            for key in keys { transaction.clear(key: key) }
        }
        // 2. 新エントリ追加
        if let new = newItem {
            let keys = try buildIndexKeys(for: new)
            for key in keys { transaction.setValue(value, for: key) }
        }
    }

    // search() はプロトコル外だが、各Maintainerで実装必須
    public func search(..., transaction: any TransactionProtocol) async throws -> [Result] {
        // インデックスを読み取り、結果を返す
    }
}
```

#### 2. Query Builder パターン（読み取り側）

**全体フロー**:
```
FDBContext.{queryMethod}()     ← Entry Point (extension で追加)
    ↓
{Index}EntryPoint<T>           ← フィールド/オプション選択
    ↓
{Index}QueryBuilder<T>         ← パラメータ設定 (Fluent API)
    ↓
execute()                      ← IndexQueryContext 経由で実行
    ↓
Maintainer.search()            ← インデックス読み取り
    ↓
queryContext.fetchItems()      ← Primary Key → Item 変換
```

**FDBContext Extension (Entry Point)**:
```swift
// {IndexName}Query.swift

extension FDBContext {
    /// {IndexName} のクエリを開始
    ///
    /// **Usage**:
    /// ```swift
    /// import {IndexName}Index
    ///
    /// let results = try await context.{queryMethod}(MyType.self)
    ///     .{field}(\\.myField)
    ///     .{parameter}(value)
    ///     .execute()
    /// ```
    public func {queryMethod}<T: Persistable>(_ type: T.Type) -> {Index}EntryPoint<T> {
        {Index}EntryPoint(queryContext: indexQueryContext)
    }
}
```

**Entry Point**:
```swift
public struct {Index}EntryPoint<T: Persistable>: Sendable {
    private let queryContext: IndexQueryContext

    internal init(queryContext: IndexQueryContext) {
        self.queryContext = queryContext
    }

    /// フィールドを指定して QueryBuilder を返す
    public func {field}(_ keyPath: KeyPath<T, FieldType>) -> {Index}QueryBuilder<T> {
        {Index}QueryBuilder(
            queryContext: queryContext,
            fieldName: T.fieldName(for: keyPath)
        )
    }
}
```

**Query Builder**:
```swift
public struct {Index}QueryBuilder<T: Persistable>: Sendable {
    private let queryContext: IndexQueryContext
    private let fieldName: String
    private var param1: Type1?
    private var param2: Type2 = defaultValue

    internal init(queryContext: IndexQueryContext, fieldName: String) {
        self.queryContext = queryContext
        self.fieldName = fieldName
    }

    // Fluent API: 各メソッドは Self を返す
    public func {parameter}(_ value: Type) -> Self {
        var copy = self
        copy.param1 = value
        return copy
    }

    /// クエリを実行
    public func execute() async throws -> [T] {
        // 1. インデックスサブスペースを取得
        let indexSubspace = try await queryContext.indexSubspace(for: T.self)
            .subspace(indexName)

        // 2. トランザクション内で検索実行
        let primaryKeys = try await queryContext.withTransaction { transaction in
            // Maintainer の search() を呼び出し
            return try await maintainer.search(..., transaction: transaction)
        }

        // 3. Primary Key から Item をフェッチ
        return try await queryContext.fetchItems(ids: primaryKeys, type: T.self)
    }
}
```

#### 3. IndexQueryContext の使用

`IndexQueryContext` はクエリ実行に必要なストレージアクセスを提供:

```swift
public struct IndexQueryContext: Sendable {
    public let context: FDBContext

    /// インデックスサブスペースを取得
    public func indexSubspace<T: Persistable>(for type: T.Type) async throws -> Subspace

    /// トランザクション内で処理を実行
    public func withTransaction<R: Sendable>(
        _ body: @Sendable @escaping (any TransactionProtocol) async throws -> R
    ) async throws -> R

    /// Primary Key から Item をフェッチ
    public func fetchItems<T: Persistable>(ids: [Tuple], type: T.Type) async throws -> [T]

    /// Schema 情報へのアクセス
    public var schema: Schema { context.container.schema }
}
```

**アクセス方法**:
```swift
// FDBContext から取得
let queryContext = context.indexQueryContext
```

#### 4. 命名規約

| コンポーネント | 命名パターン | 例 |
|--------------|-------------|-----|
| Entry Point メソッド | `context.{動詞}(Type.self)` | `findSimilar`, `search`, `nearby`, `rank` |
| Entry Point 型 | `{Index}EntryPoint<T>` | `VectorEntryPoint`, `FullTextEntryPoint` |
| Query Builder 型 | `{Index}QueryBuilder<T>` | `VectorQueryBuilder`, `SpatialQueryBuilder` |
| Maintainer 型 | `{Algorithm}IndexMaintainer<T>` | `HNSWIndexMaintainer`, `FlatVectorIndexMaintainer` |
| Error 型 | `{Index}QueryError` | `VectorQueryError`, `SpatialQueryError` |

#### 5. 完全な実装例

```swift
// Sources/MyIndex/MyQuery.swift

import Foundation
import DatabaseEngine
import Core
import FoundationDB

// MARK: - Entry Point

public struct MyEntryPoint<T: Persistable>: Sendable {
    private let queryContext: IndexQueryContext

    internal init(queryContext: IndexQueryContext) {
        self.queryContext = queryContext
    }

    public func field(_ keyPath: KeyPath<T, String>) -> MyQueryBuilder<T> {
        MyQueryBuilder(queryContext: queryContext, fieldName: T.fieldName(for: keyPath))
    }
}

// MARK: - Query Builder

public struct MyQueryBuilder<T: Persistable>: Sendable {
    private let queryContext: IndexQueryContext
    private let fieldName: String
    private var searchValue: String?
    private var limit: Int = 100

    internal init(queryContext: IndexQueryContext, fieldName: String) {
        self.queryContext = queryContext
        self.fieldName = fieldName
    }

    public func value(_ v: String) -> Self {
        var copy = self
        copy.searchValue = v
        return copy
    }

    public func limit(_ n: Int) -> Self {
        var copy = self
        copy.limit = n
        return copy
    }

    public func execute() async throws -> [T] {
        guard let value = searchValue else {
            throw MyQueryError.noSearchValue
        }

        let indexName = "\(T.persistableType)_my_\(fieldName)"
        let indexSubspace = try await queryContext.indexSubspace(for: T.self)
            .subspace(indexName)

        let ids: [Tuple] = try await queryContext.withTransaction { tx in
            // インデックスを読み取り
            let range = indexSubspace.subspace(value).range
            var results: [Tuple] = []
            for try await (key, _) in tx.getRange(range: range) {
                if let tuple = try? Tuple(fromPackedBytes: key) {
                    results.append(tuple)
                }
            }
            return results
        }

        return try await queryContext.fetchItems(ids: ids, type: T.self)
    }
}

// MARK: - FDBContext Extension

extension FDBContext {
    public func myQuery<T: Persistable>(_ type: T.Type) -> MyEntryPoint<T> {
        MyEntryPoint(queryContext: indexQueryContext)
    }
}

// MARK: - Error

public enum MyQueryError: Error {
    case noSearchValue
}
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

### @Persistable マクロ必須パターン

**重要**: テストモデルを含む全ての `Persistable` 型は `@Persistable` マクロを使用する必要がある。手動で `Persistable` プロトコルを実装すると、`ItemStorage/ItemEnvelope` 形式との互換性問題が発生する。

**問題**: 手動実装の `Persistable` は `ItemEnvelope` 形式で保存されず、読み取り時に "Data is not in ItemEnvelope format" エラーが発生する。

```swift
// ❌ 悪い例: 手動 Persistable 実装（動作しない）
struct MyModel: Persistable {
    typealias ID = String
    var id: String = UUID().uuidString
    var name: String

    static var persistableType: String { "MyModel" }
    static var directoryPathComponents: [String] { ["test", "models"] }
    static var allFields: [String] { ["id", "name"] }

    // 手動実装のボイラープレート...
    subscript(dynamicMember member: String) -> (any Sendable)? { ... }
    static func fieldName<V>(for keyPath: KeyPath<MyModel, V>) -> String { ... }
}

// ✅ 良い例: @Persistable マクロを使用
@Persistable
struct MyModel {
    #Directory<MyModel>("test", "models")

    var id: String = UUID().uuidString
    var name: String = ""

    #Index<MyModel>(ScalarIndexKind<MyModel>(fields: [\.name]))
}
```

**マクロが生成するもの**:
- `persistableType`
- `directoryPathComponents` (from `#Directory`)
- `allFields`
- `fieldName(for:)` メソッド群
- `dynamicMember` subscript
- `indexDescriptors` (from `#Index`)

**カスタムインデックスの定義**:

```swift
@Persistable
struct GameScore {
    #Directory<GameScore>("game", "scores")

    var id: String = UUID().uuidString
    var playerId: String = ""
    var score: Int64 = 0
    var region: String = "global"

    // Scalar indexes
    #Index<GameScore>(ScalarIndexKind<GameScore>(fields: [\.playerId]))
    #Index<GameScore>(ScalarIndexKind<GameScore>(fields: [\.region]))

    // Leaderboard index
    #Index<GameScore>(TimeWindowLeaderboardIndexKind<GameScore, Int64>(
        scoreField: \.score,
        window: .daily,
        windowCount: 7
    ))

    // Compound index with groupBy
    #Index<GameScore>(TimeWindowLeaderboardIndexKind<GameScore, Int64>(
        scoreField: \.score,
        groupBy: [\.region],
        window: .daily,
        windowCount: 7
    ))
}
```

**Relationship の定義**:

```swift
@Persistable
struct Customer {
    #Directory<Customer>("app", "customers")

    var id: String = ULID().uuidString
    var name: String = ""

    @Relationship(Order.self)
    var orderIDs: [String] = []
}

@Persistable
struct Order {
    #Directory<Order>("app", "orders")

    var id: String = ULID().uuidString
    var total: Double = 0

    @Relationship(Customer.self)
    var customerID: String? = nil
}
```

### テスト分離パターン

**重要**: テストは並列実行されるため、ハードコードされた ID を使用すると他のテストと干渉する。

**問題**:
- `cleanup()` で全データを削除すると、並列実行中の他のテストのデータも削除される
- 固定 ID（例: `"C001"`, `"O-test-001"`）を使用すると、複数のテストが同じデータを操作してしまう

**解決策**: 各テストで UUID ベースのユニーク ID を生成する

```swift
@Suite("My Tests", .serialized)
struct MyTests {

    /// Generate unique test ID to avoid conflicts with parallel tests
    private func uniqueID(_ prefix: String) -> String {
        "\(prefix)-\(UUID().uuidString.prefix(8))"
    }

    @Test func testSomething() async throws {
        let container = try await setupContainer()
        let context = container.newContext()

        // ✅ 良い例: ユニーク ID を使用
        let customerId = uniqueID("C-test")
        let orderId = uniqueID("O-test")

        var customer = Customer(name: "Alice")
        customer.id = customerId
        context.insert(customer)
        try await context.save()

        // テストロジック...
    }
}
```

**ガイドライン**:
1. ✅ `uniqueID()` ヘルパー関数を各テストスイートに追加
2. ✅ 全ての ID を `uniqueID("prefix")` で生成
3. ✅ 検証対象のデータのみを検証（他のテストのデータを想定しない）
4. ❌ `cleanup()` で `clearAll()` を呼ばない（他のテストに影響）
5. ❌ ハードコードされた ID（`"C001"`, `"test-id"` など）を使用しない

**例**: `IndexStateBehaviorTests`, `RelationshipIndexTests` を参照

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

### デフォルト値の扱い

**デフォルト値がある場合、Optional + nil合体演算子ではなく、直接デフォルト値を設定する。**

```swift
// ❌ 悪い例: Optional + nil合体演算子
public let retryLimit: Int?

public init(retryLimit: Int? = nil) {
    self.retryLimit = retryLimit
}

func run() {
    let maxRetries = configuration.retryLimit ?? 10  // 使用側で毎回デフォルト値を指定
}

// ✅ 良い例: 直接デフォルト値を設定
public let retryLimit: Int

public init(retryLimit: Int = 5) {
    self.retryLimit = retryLimit
}

func run() {
    let maxRetries = configuration.retryLimit  // そのまま使用可能
}
```

**理由**:
- デフォルト値が一箇所で定義され、一貫性が保たれる
- 使用側でのnil チェックや `??` が不要になり、コードがシンプルになる
- 型が非Optionalになり、意図が明確になる

### 参照すべきリソース

| 分野 | 参照先 |
|------|--------|
| クエリ最適化 | PostgreSQL src/backend/optimizer/ |
| 統計・ヒストグラム | PostgreSQL src/backend/utils/adt/selfuncs.c |
| インデックス設計 | FoundationDB Record Layer |
| 分散システム | CockroachDB, TiDB |
| アルゴリズム全般 | CLRS "Introduction to Algorithms" |
| データベース理論 | "Database System Concepts" (Silberschatz) |

## 開発方針

### プロジェクト状態

**このプロジェクトは開発中であり、後方互換性を考慮する必要はない。**

### コード管理原則

1. **不要なコードは削除**: 混乱を避けるため、使用されていないコード・互換性レイヤー・デッドコードは即座に削除
2. **破壊的変更を恐れない**: 本番データがないため、API・データフォーマットの変更は自由に行う
3. **最適な設計を優先**: 後方互換性のためのワークアラウンドよりも、正しい設計を選択
4. **マイグレーションコード不要**: 旧フォーマットから新フォーマットへの変換コードは書かない

### 具体例

```swift
// ❌ 不要: 互換性のための両方サポート
func readKey() -> Value {
    if let newFormat = tryNewFormat() { return newFormat }
    return tryOldFormat()  // ← 削除すべき
}

// ✅ 正しい: 新しい設計のみ
func readKey() -> Value {
    return readNewFormat()
}
```

### Subspace キー設計

文字列キーよりも整数キーを使用し、効率性を優先：

```swift
// ❌ 避ける: 文字列キー（デバッグ可読性のため等）
subspace.subspace("nodes")

// ✅ 推奨: 整数キー（効率的）
subspace.subspace(SubspaceKey.nodes.rawValue)  // enum SubspaceKey: Int64
```

### Persistable フィールドアクセス（Mirror禁止）

**重要**: `Persistable`型のフィールドアクセスには`Mirror`を使用しない。`Persistable`プロトコルは動的フィールドアクセスのためのインターフェースを提供している。

**Persistableが提供するフィールドアクセス機能**:

```swift
@dynamicMemberLookup
public protocol Persistable {
    // 動的フィールドアクセス（フィールド名でアクセス）
    subscript(dynamicMember member: String) -> (any Sendable)? { get }

    // KeyPathからフィールド名への変換
    static func fieldName(for keyPath: KeyPath<Self, Value>) -> String
    static func fieldName(for keyPath: PartialKeyPath<Self>) -> String
    static func fieldName(for keyPath: AnyKeyPath) -> String

    // 直接プロパティアクセス
    var id: ID { get }
    static var allFields: [String] { get }
}
```

**正しいパターン**:

```swift
// ✅ 良い例: dynamicMember subscript を使用
func extractField<T: Persistable>(from item: T, fieldName: String) -> Any? {
    return item[dynamicMember: fieldName]
}

// ✅ 良い例: 直接プロパティアクセス
func getCacheKey<T: Persistable>(for item: T) -> String {
    return "\(item.id)"  // Persistable.id を直接使用
}

// ✅ 良い例: DataAccess.extractField を使用（ネストされたフィールドもサポート）
let values = try DataAccess.extractField(from: item, keyPath: "address.city")

// ❌ 悪い例: Mirror を使用
func extractField<T: Persistable>(from item: T, fieldName: String) -> Any? {
    let mirror = Mirror(reflecting: item)
    for child in mirror.children {
        if child.label == fieldName {
            return child.value
        }
    }
    return nil
}
```

**DataAccess の使用**:

`DataAccess`クラスはフィールドアクセスの統一インターフェースを提供する：

```swift
// 単一フィールド抽出（ネストされたフィールドもサポート）
let values = try DataAccess.extractField(from: item, keyPath: "email")
let nested = try DataAccess.extractField(from: item, keyPath: "address.city")

// KeyPath経由のフィールド抽出（型安全）
let values = try DataAccess.extractFieldsUsingKeyPaths(from: item, keyPaths: [keyPath])

// インデックスフィールド評価（keyPaths優先、expression fallback）
let values = try DataAccess.evaluateIndexFields(
    from: item,
    keyPaths: index.keyPaths,
    expression: index.rootExpression
)
```

**Mirrorが許容されるケース**:

以下の場合のみMirrorの使用を許容する：

1. **非Persistable型のネストされた構造体へのアクセス**:
   ```swift
   // 例: User.address.city で Address が Persistable でない場合
   @Persistable struct User {
       var address: Address  // Address は通常の struct
   }
   struct Address {  // Persistable ではない
       var city: String
   }
   ```
   この場合、`address`までは`dynamicMember`でアクセスし、`city`はMirrorでアクセスする必要がある。
   （DataAccess.extractNestedField がこのパターンを実装）

2. **コレクション型の判定**（`displayStyle == .collection`）

3. **Any型からの値抽出**（型情報が完全に失われている場合）

**禁止事項**:
- ❌ `Persistable`型のトップレベルフィールドへのMirrorアクセス
- ❌ `item.id`の代わりにMirrorで"id"フィールドを探す
- ❌ フィールド名がわかっている場合のMirror走査

---

## Known Limitations & Future Work

### Watch機能 (fdb-swift-bindings拡張が必要)

**現状**: `WatchManager`はスタブ実装であり、常にエラーを返す。

**理由**: FoundationDBのネイティブWatch API (`fdb_transaction_watch()`) は、キーの変更を監視するFutureを返すC APIである。しかし、現在の`fdb-swift-bindings`ライブラリはこのAPIを公開していない。

**TransactionProtocolで利用可能なメソッド**:
- getValue, setValue, clear, clearRange
- getKey, getRange, getRangeNative
- commit, cancel
- getReadVersion, getVersionstamp

**不足しているメソッド**:
- `watch(key:)` → `fdb_transaction_watch()` C APIを呼び出すメソッド

**将来の実装に必要な作業**:

1. **fdb-swift-bindings の拡張**:
   ```swift
   // TransactionProtocol に追加が必要
   func watch(key: [UInt8]) async throws -> FDBFuture<Void>
   ```

2. **FDBFuture<Void>の実装**: Watchは値を返さず、キーが変更されたことのみを通知するため、`FDBFuture<Void>`型が必要。

3. **WatchManagerの実装更新**: スタブ実装を実際のwatch APIを使用した実装に置き換え。

**代替アプローチ** (fdb-swift-bindings を変更しない場合):
- ポーリングベースの実装（定期的にキーを読み取り、変更を検出）
- ただし、これはFDBのネイティブWatch機能の効率性を犠牲にする

### インデックス再構築 (簡易実装)

**現状**: `AdminContext.rebuildIndex()`は簡易実装であり、インデックスエントリの実際の再作成は行わない。

**理由**:
1. **IndexMaintainerとの統合が必要**: 各インデックスタイプ（Vector, FullText, Scalar等）には専用のMaintainerがあり、それぞれ異なるエントリ形式を持つ。
2. **OnlineIndexerの使用が望ましい**: 本番環境では、バッチ処理、進捗追跡、再開可能性、スロットリングをサポートする`OnlineIndexer`を使用すべき。
3. **型消去の問題**: `AdminContext`は型消去された`Schema.Entity`で動作するが、`OnlineIndexer<T>`はジェネリック型`T`を必要とする。

**将来の実装に必要な作業**:
- `IndexMaintainer`との統合
- 型消去を解決するための設計変更
- `OnlineIndexer`を使用した本格的な再構築ロジック
