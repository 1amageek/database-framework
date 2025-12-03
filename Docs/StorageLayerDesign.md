# Storage Layer Design

## Overview

`StorageTransaction` is an internal abstraction layer between `database-framework` and `fdb-swift-bindings`. It transparently handles compression and large value splitting for record data.

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        FDBDataStore                              │
│  (save/fetch/delete operations for Persistable models)          │
└─────────────────────────────────────────────────────────────────┘
                              │
                              │ Record Data Only
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                     StorageTransaction                           │
│  ┌─────────────────┐    ┌──────────────────────┐                │
│  │ Compression     │    │ Large Value Splitter │                │
│  │ (zlib)          │    │ (90KB chunks)        │                │
│  └─────────────────┘    └──────────────────────┘                │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                    TransactionProtocol                           │
│  (fdb-swift-bindings: getValue, setValue, clear, getRange)      │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                       FoundationDB                               │
└─────────────────────────────────────────────────────────────────┘
```

## Design Principles

### 1. Always Enabled, No Configuration

Both compression and splitting are **always enabled**. There are no configuration flags:

```swift
// Simple initialization - no configuration needed
let storage = StorageTransaction(transaction)
```

**Rationale**:
- **Compression**: Automatically skipped for small or incompressible data (header indicates no transformation). Zero overhead when not beneficial.
- **Splitting**: Automatically skipped for data ≤ 90KB. Required for large data (>100KB FDB limit).

Configuration flags would serve no practical purpose:
- `compressionEnabled: false` - No benefit; compression auto-skips when not helpful
- `splitEnabled: false` - Would break for large data with no upside

### 2. Internal Implementation

`StorageTransaction` is `internal` - not part of the public API. Users interact only with `FDBContext` and `Persistable` models.

```swift
// User code (public API)
let context = container.newContext()
context.insert(myModel)
try await context.save()

// Internal: FDBDataStore uses StorageTransaction transparently
```

### 3. Scope: Record Data Only

`StorageTransaction` is used **only for record data** (R/ subspace), not for:
- Index entries (I/ subspace) - store empty values `[]`
- Metadata - small, structured data
- Range queries for index scanning

```
[subspace]/R/[type]/[id] → StorageTransaction (compressed, potentially split)
[subspace]/I/[index]/... → Direct transaction (empty value)
```

## Data Flow

### Write Path

```
Model
  │
  ▼ DataAccess.serialize()
Protobuf bytes
  │
  ▼ TransformingSerializer.serialize()
[0x01][compressed data]  (or [0x00][raw data] if compression not beneficial)
  │
  ▼ LargeValueSplitter.save()
  │
  ├─ If ≤ 90KB: Single key-value pair
  │    Key: [baseKey]
  │    Value: [transformed data]
  │
  └─ If > 90KB: Split into multiple pairs
       Key: [baseKey][0x00] → Header (totalSize, partCount)
       Key: [baseKey][0x01] → Part 1
       Key: [baseKey][0x02] → Part 2
       ...
```

### Read Path

```
Keys in FDB
  │
  ▼ LargeValueSplitter.load()
  │
  ├─ Single key: Return value directly
  │
  └─ Split value: Read header, reassemble all parts
  │
  ▼
[0x01][compressed data]
  │
  ▼ TransformingSerializer.deserialize()
Protobuf bytes
  │
  ▼ DataAccess.deserialize()
Model
```

## Key Components

### TransformingSerializer

Handles data transformation with a 1-byte header indicating the transformation type:

| Header Byte | Meaning |
|-------------|---------|
| `0x00` | No transformation (raw data) |
| `0x01` | zlib compression |

**Auto-skip logic**: If compressed size ≥ original size, stores with `0x00` header (no compression).

### LargeValueSplitter

Handles FDB's 100KB value size limit:

| Configuration | Value |
|---------------|-------|
| Max value size | 90KB (leaves room for overhead) |
| Max parts | 254 |
| Max total size | ~22MB |

**Key structure for split values**:
```
[baseKey][0x00] → Header: [totalSize:Int64][partCount:Int32]
[baseKey][0x01] → Part 1 data (up to 90KB)
[baseKey][0x02] → Part 2 data
...
```

## Performance Characteristics

| Operation | Overhead |
|-----------|----------|
| Small data (< 1KB) | Minimal (header byte only if incompressible) |
| Compressible data | CPU for zlib, but reduced I/O |
| Large data (> 90KB) | Multiple FDB operations (unavoidable) |
| Read split value | Sequential reads for parts |

## Error Handling

| Error | Cause | Recovery |
|-------|-------|----------|
| `SplitError.valueTooLarge` | Data > 22MB after compression | Reduce data size or store externally |
| `SplitError.missingPart` | Partial write (crash during save) | Re-save the record |
| `SplitError.sizeMismatch` | Corruption | Re-save the record |

## Usage in FDBDataStore

```swift
// In saveModel
private func saveModel<T: Persistable>(
    _ model: T,
    transaction: any TransactionProtocol
) async throws {
    let data = try DataAccess.serialize(model)
    let key = typeSubspace.pack(idTuple)

    // Use StorageTransaction for record data
    let storage = StorageTransaction(transaction)
    try storage.write(data, for: key)
}

// In fetchAll
func fetchAll<T: Persistable>(_ type: T.Type) async throws -> [T] {
    try await database.withTransaction(configuration: .readOnly) { transaction in
        let storage = StorageTransaction(transaction)

        for try await (key, _) in transaction.getRange(...) {
            // Range scan returns keys; read full value via storage
            if let value = try await storage.read(for: key) {
                let model: T = try DataAccess.deserialize(value)
                results.append(model)
            }
        }
        return results
    }
}
```

## References

- **FDB Record Layer SplitHelper**: Similar approach for handling large records
- **FoundationDB Limits**: 100KB max value size, 10MB max transaction size
- **zlib**: RFC 1950, widely used compression with good balance of speed/ratio
