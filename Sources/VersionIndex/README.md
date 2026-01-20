# VersionIndex

Temporal versioning with FDB versionstamps for complete audit trails and point-in-time queries.

## Overview

VersionIndex maintains a complete history of all changes to records using FoundationDB's versionstamps for globally monotonic ordering. Every insert, update, and delete operation creates a new version entry, enabling point-in-time queries, rollbacks, and complete audit trails.

**Algorithm**:
- **Versionstamped Keys**: Uses FDB's `SET_VERSIONSTAMPED_KEY` atomic operation
- **Global Ordering**: Versionstamps provide globally unique, monotonically increasing identifiers
- **Automatic Cleanup**: Configurable retention strategies (keepAll, keepLast, keepForDuration)

**Storage Layout**:
```
Key: [indexSubspace][primaryKey][versionstamp(10 bytes)]
Value: [timestamp(8 bytes)][item data]

Example:
  [I]/Document_version/["doc-123"]/[0x00000000001A...] = [timestamp][JSON data]
  [I]/Document_version/["doc-123"]/[0x00000000001B...] = [timestamp][JSON data]
  [I]/Document_version/["doc-123"]/[0x00000000001C...] = [timestamp]  (deletion marker)
```

**Versionstamp Format**:
- 10 bytes total
- First 8 bytes: Transaction commit version (assigned by FDB)
- Last 2 bytes: Batch number within transaction
- Globally unique across the entire FDB cluster

## Use Cases

### 1. Document Version History

**Scenario**: Track all changes to documents with full rollback capability.

```swift
@Persistable
struct Document {
    var id: String = ULID().ulidString
    var title: String = ""
    var content: String = ""
    var updatedAt: Date = Date()

    #Index<Document>(type: VersionIndexKind(field: \.id, strategy: .keepAll))
}

// Get complete version history
let history = try await context.versions(Document.self)
    .forItem(documentId)
    .execute()

for (version, doc) in history {
    print("Version: \(version.commitVersion)")
    print("Title: \(doc.title)")
    print("Content: \(doc.content)")
}

// Get latest version (O(1) with key selector)
let latest = try await context.versions(Document.self)
    .forItem(documentId)
    .latest()

// Get document at specific version
let atVersion = try await context.versions(Document.self)
    .forItem(documentId)
    .at(version)
```

**Performance**: O(1) for latest version, O(k) for k versions of history.

### 2. Audit Trail with Limited Retention

**Scenario**: Keep last 10 versions for audit compliance without unbounded storage.

```swift
@Persistable
struct AuditedRecord {
    var id: String = ULID().ulidString
    var data: String = ""
    var modifiedBy: String = ""

    // Keep only last 10 versions
    #Index<AuditedRecord>(type: VersionIndexKind(
        field: \.id,
        strategy: .keepLast(10)
    ))
}

// Older versions are automatically cleaned up on each update
context.insert(updatedRecord)
try await context.save()

// Only the 10 most recent versions remain
let history = try await context.versions(AuditedRecord.self)
    .forItem(recordId)
    .execute()
// history.count <= 10
```

### 3. Time-Based Retention

**Scenario**: Keep versions for 30 days for regulatory compliance.

```swift
@Persistable
struct ComplianceRecord {
    var id: String = ULID().ulidString
    var transactionType: String = ""
    var amount: Double = 0.0

    // Keep versions for 30 days
    #Index<ComplianceRecord>(type: VersionIndexKind(
        field: \.id,
        strategy: .keepForDuration(30 * 24 * 60 * 60)  // 30 days in seconds
    ))
}

// Query history - old versions are automatically purged
let history = try await context.versions(ComplianceRecord.self)
    .forItem(recordId)
    .execute()
// Only versions from last 30 days are returned
```

### 4. Point-in-Time Recovery

**Scenario**: Restore system state to a specific point in time.

```swift
// Get versions for all documents
let documentIds: [String] = ...  // List of document IDs

var restoredDocuments: [Document] = []
let targetVersion = Version(...)  // Specific recovery point

for docId in documentIds {
    if let doc = try await context.versions(Document.self)
        .forItem(docId)
        .at(targetVersion) {
        restoredDocuments.append(doc)
    }
}

// Use restoredDocuments for recovery
```

### 5. Change Detection

**Scenario**: Detect what changed between versions.

```swift
let history = try await context.versions(Document.self)
    .forItem(documentId)
    .limit(2)  // Get last 2 versions
    .execute()

if history.count >= 2 {
    let current = history[0].item
    let previous = history[1].item

    if current.title != previous.title {
        print("Title changed: '\(previous.title)' -> '\(current.title)'")
    }
    if current.content != previous.content {
        print("Content was modified")
    }
}
```

## Design Patterns

### FDB Versionstamp Mechanism

```
┌─────────────────────────────────────────────────────────────────┐
│                     Versionstamp Flow                           │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  1. Client prepares key with placeholder:                       │
│     [subspace][primaryKey][0xFF 0xFF 0xFF 0xFF 0xFF 0xFF ...]   │
│                            ↑                                    │
│                     10-byte placeholder                         │
│                                                                 │
│  2. Client calls atomicOp with SET_VERSIONSTAMPED_KEY           │
│                                                                 │
│  3. FDB commits transaction:                                    │
│     - Assigns commit version (8 bytes)                          │
│     - Assigns batch number (2 bytes)                            │
│     - Replaces placeholder with actual versionstamp             │
│                                                                 │
│  4. Final key in database:                                      │
│     [subspace][primaryKey][0x0000 0001 2345 6789 00 01]         │
│                            ↑                   ↑                │
│                     commit version      batch number            │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

**Key Properties**:
- **Globally Unique**: No two transactions get the same versionstamp
- **Monotonically Increasing**: Later commits have higher versionstamps
- **Assigned at Commit**: Cannot be read in the same transaction

### Retention Strategy Selection

| Strategy | Storage | Cleanup Trigger | Use Case |
|----------|---------|-----------------|----------|
| `keepAll` | Unbounded | None | Full audit trail, compliance |
| `keepLast(N)` | O(N) per item | Each update | Recent history, debugging |
| `keepForDuration(T)` | Time-bounded | Each update | Regulatory compliance |

**Decision Matrix**:
```
┌─────────────────────────────────────────────────────────────────┐
│                 Which strategy should I use?                    │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  Full audit trail required?                                     │
│  Legal/regulatory requirement?         ──────▶  .keepAll        │
│  No storage constraints?                                        │
│                                                                 │
│  Need recent history only?                                      │
│  Debugging/rollback purposes?          ──────▶  .keepLast(N)    │
│  Fixed number of versions?                                      │
│                                                                 │
│  Time-based retention policy?                                   │
│  GDPR "right to be forgotten"?         ──────▶  .keepForDuration│
│  Storage cost concerns?                                         │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

### Deletion Markers

When a record is deleted, a deletion marker is stored instead of the full item data:

```
Deletion Marker:
  Key: [subspace][primaryKey][versionstamp]
  Value: [timestamp(8 bytes)]  // No item data = deletion marker

Regular Version:
  Key: [subspace][primaryKey][versionstamp]
  Value: [timestamp(8 bytes)][item data]
```

This enables:
- Tracking when records were deleted
- Distinguishing "no version exists" from "was deleted at this time"
- Complete audit trail including deletions

### Query Patterns

| Query | Method | Complexity |
|-------|--------|------------|
| Latest version | `getLatestVersion()` | O(1) |
| Full history | `getVersionHistory()` | O(k) versions |
| Limited history | `getVersionHistory(limit:)` | O(min(k, limit)) |
| Version at point | `at(version)` | O(k) |
| History for item | `forItem(id)` | Index lookup |

## Implementation Status

| Feature | Status | Notes |
|---------|--------|-------|
| Version creation | ✅ Complete | Uses SET_VERSIONSTAMPED_KEY |
| Version history query | ✅ Complete | Newest-first ordering |
| Latest version query | ✅ Complete | O(1) with lastLessThan selector |
| Deletion markers | ✅ Complete | Empty item data |
| keepAll strategy | ✅ Complete | Default, no cleanup |
| keepLast(N) strategy | ✅ Complete | Maintains N versions |
| keepForDuration strategy | ✅ Complete | Time-based cleanup |
| Batch indexing (scanItem) | ✅ Complete | For OnlineIndexer |
| Cross-item point-in-time | ⚠️ Partial | Requires iteration |
| Diff between versions | ❌ Not implemented | User-side comparison |
| Branching/merging | ❌ Not implemented | Out of scope |

## Performance Characteristics

| Operation | Time Complexity | Notes |
|-----------|----------------|-------|
| Insert (new version) | O(1) | Atomic versionstamped key |
| Update (new version) | O(r) | r = versions to clean up |
| Delete (marker) | O(1) | Single atomic operation |
| Get latest | O(1) | KeySelector: lastLessThan |
| Get history (k versions) | O(k) | Range scan |
| Get at version | O(k) | Scan until version found |

### Storage Overhead

| Strategy | Storage per Item | Notes |
|----------|------------------|-------|
| keepAll | O(updates) | Unbounded, grows with history |
| keepLast(N) | O(N) | Fixed maximum |
| keepForDuration | O(rate * duration) | Bounded by time window |

**Per-Version Storage**:
- Key: ~30-50 bytes (subspace + primaryKey + 10-byte versionstamp)
- Value: 8 bytes (timestamp) + item size

### FDB Considerations

- **Versionstamp Atomic Operation**: Uses `SET_VERSIONSTAMPED_KEY` mutation type
- **Cannot Read Versionstamped Keys in Same Transaction**: Retention cleanup applied BEFORE new version
- **Transaction Size Limit**: 10MB per transaction (affects batch operations)
- **Key Size Limit**: 10KB (primary key must be small enough)

## Benchmark Results

Run with: `swift test --filter VersionIndexPerformanceTests`

### Indexing

| Records | Strategy | Insert Time | Throughput |
|---------|----------|-------------|------------|
| 100 | keepAll | ~50ms | ~2,000/s |
| 100 | keepLast(5) | ~80ms | ~1,250/s |
| 1,000 | keepAll | ~500ms | ~2,000/s |
| 1,000 | keepLast(5) | ~800ms | ~1,250/s |

### Query

| Versions per Item | Query Type | Latency (p50) |
|-------------------|------------|---------------|
| 10 | Latest version | ~1ms |
| 10 | Full history | ~5ms |
| 100 | Latest version | ~1ms |
| 100 | Full history | ~20ms |
| 100 | Limited (10) | ~5ms |

### Retention Cleanup

| Existing Versions | Cleanup Strategy | Cleanup Time |
|-------------------|------------------|--------------|
| 10 | keepLast(5) | ~2ms |
| 100 | keepLast(5) | ~15ms |
| 1,000 | keepLast(5) | ~100ms |

*Benchmarks run on M1 Mac with local FoundationDB cluster.*

## References

- [FDB Versionstamps](https://apple.github.io/foundationdb/developer-guide.html#versionstamps) - FoundationDB documentation
- [FDB Record Layer Versioning](https://github.com/FoundationDB/fdb-record-layer) - Reference implementation
- [Temporal Data Management](https://en.wikipedia.org/wiki/Temporal_database) - Database concepts
- [Event Sourcing](https://martinfowler.com/eaaDev/EventSourcing.html) - Related pattern
