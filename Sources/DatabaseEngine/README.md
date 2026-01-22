# DatabaseEngine

Core engine for FoundationDB-backed persistence with transactional guarantees.

## Overview

DatabaseEngine provides the foundation for all database operations, including:
- Transaction management with configurable retry policies
- Data storage with ItemEnvelope format
- Index maintenance coordination
- Security evaluation
- Cache management for read optimization

## Core Components

### FDBContainer

Application-level resource manager. Does NOT create transactions.

```swift
// Initialize container
let container = try FDBContainer(for: schema)

// Get a new context (for user operations)
let context = container.newContext()

// Access database directly (for system operations)
let database = container.database
```

### FDBContext

Transaction manager and user-facing API. Owns ReadVersionCache.

```swift
let context = container.newContext()

// Queue changes
context.insert(model)
context.delete(model)

// Persist all queued changes atomically
try await context.save()

// Fetch persisted data (ignores pending changes)
let results = try await context.fetch(MyType.self).execute()

// Direct O(1) ID lookup
let user = try await context.model(for: userId, as: User.self)
```

**Design Principle**: `fetch()` returns only persisted data. Pending inserts/deletes are NOT mixed into results.

### FDBDataStore

Low-level data operations within transactions. Receives transaction as parameter.

## Transaction Management

### Transaction Configuration Presets

| Preset | Timeout | Retry | Priority | Use Case |
|--------|---------|-------|----------|----------|
| `.default` | ~5s (FDB) | 5 | normal | User-facing CRUD, queries |
| `.interactive` | 1s | 3 | normal | Lock operations (fail-fast) |
| `.batch` | 30s | 20 | batch | Background indexing, statistics |
| `.longRunning` | 60s | 50 | batch | Full scans, analytics |
| `.readOnly` | 2s | 3 | normal | Dashboard, cached lookups |
| `.system` | 2s | 5 | system | Reserved for critical operations |

### Preset Selection Guide

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

### Usage Examples

```swift
// User-facing save operation
let context = container.newContext()
context.insert(user)
try await context.save()

// Explicit transaction with configuration
try await context.withTransaction(configuration: .default) { tx in
    let user = try await tx.get(User.self, id: userID)
    try await tx.set(updatedUser)
}

// Dashboard query (stale data OK)
try await context.withTransaction(configuration: .readOnly) { tx in
    try await tx.getValue(for: dashboardKey, snapshot: true)
}

// Custom configuration
let customConfig = TransactionConfiguration(
    timeout: 10_000,
    retryLimit: 10,
    priority: .default,
    cachePolicy: .stale(30)
)
try await context.withTransaction(configuration: customConfig) { tx in
    // ...
}
```

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `DATABASE_TRANSACTION_RETRY_LIMIT` | 5 | Default retry limit |
| `DATABASE_TRANSACTION_MAX_RETRY_DELAY` | 1000 | Maximum retry delay (ms) |
| `DATABASE_TRANSACTION_TIMEOUT` | nil (FDB default) | Transaction timeout (ms) |
| `DATABASE_TRANSACTION_INITIAL_DELAY` | 300 | Initial backoff delay (ms) |

## Cache Policy

Control whether transactions reuse cached read versions for reduced latency.

| Policy | Behavior | Use Case |
|--------|----------|----------|
| `.server` | Always fetch from server | Read-after-write consistency |
| `.cached` | Use cache if available | Dashboard queries |
| `.stale(N)` | Use cache if < N seconds old | Custom staleness tolerance |

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
```

## Data Storage

### ItemEnvelope Format

All items are stored with a magic number header for validation:

```
[ITEM magic (4 bytes: 0x49 0x54 0x45 0x4D)] [payload...]
```

**Important**: Use `ItemStorage.read()` to read items, not direct `transaction.getValue()`.

```swift
// Correct: Use ItemStorage
let storage = ItemStorage(transaction: tx, blobsSubspace: blobsSubspace)
if let data = try await storage.read(for: key) {
    let item = try decoder.decode(MyType.self, from: Data(data))
}

// Wrong: Direct access fails
if let data = try await tx.getValue(for: key) {
    // Error: "Data is not in ItemEnvelope format"
}
```

### Large Value Splitting

Values exceeding FDB's 100KB limit are automatically split into blob chunks:

```
[fdb]/R/[type]/[id]     → ItemEnvelope (reference to blobs)
[fdb]/B/[blob-key]/0    → Chunk 0
[fdb]/B/[blob-key]/1    → Chunk 1
...
```

## Serialization

### Compression

`TransformingSerializer` supports multiple compression algorithms:

| Algorithm | Use Case |
|-----------|----------|
| LZ4 | Fast compression, moderate ratio |
| zlib | Balanced speed and ratio |
| LZMA | High compression ratio |
| LZFSE | Apple-optimized |

### Encryption

AES-256-GCM encryption with multiple key providers:

| Provider | Use Case |
|----------|----------|
| `StaticKeyProvider` | Development, testing |
| `RotatingKeyProvider` | Production with key rotation |
| `DerivedKeyProvider` | Per-record keys from master |
| `EnvironmentKeyProvider` | Key from environment variable |

## Field-Level Security

Fine-grained access control at the field level.

```swift
@Persistable
struct Employee {
    var id: String = ULID().uuidString
    var name: String = ""

    @Restricted(read: .roles(["hr", "manager"]), write: .roles(["hr"]))
    var salary: Double = 0

    @Restricted(read: .roles(["hr"]), write: .roles(["hr"]))
    var ssn: String = ""
}

// Secure fetch with auth context
try await AuthContextKey.$current.withValue(userAuth) {
    let employees = try await context.fetchSecure(Employee.self).execute()
}
```

## Dynamic Directories

Multi-tenant and partitioned data patterns.

```swift
@Persistable
struct TenantOrder {
    #Directory<TenantOrder>("app", Field(\.tenantID), "orders")

    var id: String = UUID().uuidString
    var tenantID: String = ""
    var status: String = ""
}

// Query with partition
let orders = try await context.fetch(TenantOrder.self)
    .partition(\.tenantID, equals: "tenant_123")
    .where(\.status == "pending")
    .execute()
```

## Online Indexing

| Component | Description |
|-----------|-------------|
| `OnlineIndexer` | Batch processing with resumable progress |
| `MultiTargetOnlineIndexer` | Build multiple indexes in single scan |
| `MutualOnlineIndexer` | Build bidirectional indexes |
| `OnlineIndexScrubber` | Two-phase consistency verification |
| `IndexFromIndexBuilder` | Build index from existing index |
| `AdaptiveThrottler` | Backpressure-aware rate limiting |

## Query Planning

Cascades framework for cost-based query optimization:

| Component | Description |
|-----------|-------------|
| `CascadesOptimizer` | Top-down rule-based optimizer |
| `Memo` | Memoization structure |
| `Rule` | Transformation rules |
| `CostEstimator` | Statistics-driven cost model |
| `StatisticsProvider` | Cardinality, histograms, HyperLogLog |

## Performance Characteristics

### Transaction Retry

Uses AWS-style exponential backoff:
```
delay = min(initialDelay * 2^attempt, maxDelay) + jitter
```
- Jitter: 0-50% of delay (prevents thundering herd)

### ReadVersionCache

- Reduces `getReadVersion()` calls
- Thread-safe via `Mutex`
- Updated after successful commits

## References

- [FoundationDB Record Layer](https://github.com/FoundationDB/fdb-record-layer)
- [FDB Atomic Operations](https://apple.github.io/foundationdb/api-general.html#atomic-operations)
- [Cascades Optimizer (Graefe 1995)](https://dl.acm.org/doi/10.1145/223784.223785)
