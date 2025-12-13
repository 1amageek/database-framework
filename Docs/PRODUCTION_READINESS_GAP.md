# Production Readiness Gap Analysis

This document tracks the missing features required for database-framework to be production-ready, compared to FDB Record Layer.

## Overview

| Area | Status | Priority |
|------|--------|----------|
| [1. Transaction Infrastructure](#1-transaction-infrastructure) | ✅ Complete | High |
| [2. Instrumentation & Monitoring](#2-instrumentation--monitoring) | ⚠️ Partial | Medium |
| [3. Error Handling](#3-error-handling) | ⚠️ Partial | High |
| [4. Data Integrity Validation](#4-data-integrity-validation) | ⚠️ Partial | Medium |
| [5. Storage Evolution](#5-storage-evolution) | ❌ Missing | High |

---

## 1. Transaction Infrastructure

### Current Implementation

| Component | File | Status |
|-----------|------|--------|
| TransactionConfiguration | `Transaction/TransactionConfiguration.swift` | ✅ Complete |
| TransactionRunner | `Transaction/TransactionRunner.swift` | ✅ Complete |
| TransactionContext | `Transaction/TransactionContext.swift` | ✅ Complete |
| WeakReadSemantics | `Transaction/WeakReadSemantics.swift` | ✅ Complete |
| ReadVersionCache | `Core/ReadVersionCache.swift` | ✅ Complete |
| InstrumentedTransaction | `Instrumentation/InstrumentedTransaction.swift` | ✅ Complete |
| CommitCheck | `Transaction/CommitCheck.swift` | ✅ Complete |
| PostCommit | `Transaction/PostCommit.swift` | ✅ Complete |
| TransactionListener | `Transaction/TransactionListener.swift` | ✅ Complete |

### Implemented Features

#### 1.1 CommitCheck (Pre-commit Validation)

- [x] **Status**: ✅ Implemented
- **File**: `Sources/DatabaseEngine/Transaction/CommitCheck.swift`

**Description**: Hooks that run before transaction commit for validation (e.g., uniqueness constraints).

**Components**:
- `CommitCheck` protocol - Base protocol for pre-commit validation
- `CommitCheckRegistry` - Registry for managing multiple checks
- `UniquenessCommitCheck` - Built-in uniqueness constraint validation
- `CompositeCommitCheck` - Run multiple checks (with failFast option)
- `ConditionalCommitCheck` - Conditional check execution

**Usage**:
```swift
let registry = CommitCheckRegistry()
registry.add(UniquenessCommitCheck(
    indexSubspace: emailIndex,
    value: user.email,
    fieldName: "email"
))
try await registry.executeAll(transaction: transaction)
```

#### 1.2 PostCommit Hooks

- [x] **Status**: ✅ Implemented
- **File**: `Sources/DatabaseEngine/Transaction/PostCommit.swift`

**Description**: Callbacks executed after successful commit (e.g., cache invalidation, notifications).

**Components**:
- `PostCommit` protocol - Base protocol for post-commit hooks
- `PostCommitRegistry` - Registry with priority-based execution
- `ClosurePostCommit` - Closure-based hooks
- `RetryingPostCommit` - Retry with exponential backoff
- `DelayedPostCommit` - Delayed execution
- `CompositePostCommit` - Multiple hooks (sequential or concurrent)

**Usage**:
```swift
let registry = PostCommitRegistry()
registry.add(name: "cache-invalidation", priority: 10) {
    await cache.invalidate("user:\(userID)")
}
await registry.executeAll()
```

#### 1.3 Transaction ID & Logging

- [x] **Status**: ✅ Implemented
- **File**: `Sources/DatabaseEngine/Transaction/TransactionConfiguration.swift`

**Description**: Transaction ID for log correlation, FDB client trace log integration.

**New Properties in TransactionConfiguration**:
- `tracing: Tracing` - Grouped tracing/logging configuration
  - `transactionID: String?` - Unique ID for log correlation
  - `logTransaction: Bool` - Enable detailed FDB logging
  - `serverRequestTracing: Bool` - Enable server-side tracing
  - `tags: Set<String>` - Tags for categorization and filtering

**Usage**:
```swift
// Simple tracing with transaction ID
let config = TransactionConfiguration(
    tracing: .init(transactionID: "user-request-\(requestID)")
)

// Full debugging configuration
let debugConfig = TransactionConfiguration(
    tracing: .init(
        transactionID: "debug-session",
        logTransaction: true,
        serverRequestTracing: true,
        tags: ["api-v2", "user-service"]
    )
)

// Convenience accessors available
config.transactionID  // via config.tracing.transactionID
config.logTransaction // via config.tracing.logTransaction
```

#### 1.4 TransactionListener

- [x] **Status**: ✅ Implemented
- **File**: `Sources/DatabaseEngine/Transaction/TransactionListener.swift`

**Description**: Database-level listener for transaction lifecycle events.

**Components**:
- `TransactionEvent` enum - created, committing, committed, failed, cancelled, closed
- `TransactionListener` protocol - Event listener interface
- `TransactionListenerRegistry` - Manage multiple listeners
- `TransactionLifecycleTracker` - Track individual transaction lifecycle
- `MetricsTransactionListener` - Built-in metrics collection
- `LoggingTransactionListener` - Built-in logging listener

**Usage**:
```swift
let registry = TransactionListenerRegistry()
registry.add(MetricsTransactionListener())
registry.add { event in
    print("Transaction event: \(event)")
}
```

---

## 2. Instrumentation & Monitoring

### Current Implementation

| Component | Status |
|-----------|--------|
| StoreTimer | ✅ Complete |
| StoreTimerEvent | ✅ Complete |
| TransactionMetrics | ✅ Complete |
| InstrumentedTransaction | ✅ Complete |
| MetricsAggregator | ✅ Complete |

### Missing Features

#### 2.1 Delayed Events

- [ ] **Status**: Not Implemented
- **Priority**: 🟡 Medium
- **Effort**: 1 day

**Description**: Events that are only recorded on successful commit (e.g., bytes_written).

#### 2.2 Database-level Metrics Aggregation

- [ ] **Status**: Not Implemented
- **Priority**: 🟢 Low
- **Effort**: 1 day

**Description**: Automatic metrics aggregation at FDBContainer level.

---

## 3. Error Handling

### Current Implementation

Multiple error types scattered across modules:
- `FDBContextError`
- `FDBLimitError`
- `ItemEnvelopeError`
- `TransformError`
- `FormatVersionError`
- `DirectoryPathError`
- Per-index error types

### Missing Features

#### 3.1 Unified Error Code System

- [ ] **Status**: Not Implemented
- **Priority**: 🔴 High
- **Effort**: 3-4 days

**Description**: Unified error code enum with retry classification.

**Proposed Implementation**: `Sources/DatabaseEngine/Error/DatabaseErrorCode.swift`

#### 3.2 Conflicting Keys Reporting

- [ ] **Status**: Not Implemented
- **Priority**: 🟡 Medium
- **Effort**: 1 day

**Description**: Report conflicting keys on transaction conflict for debugging.

#### 3.3 Retry Classification Protocol

- [ ] **Status**: Not Implemented
- **Priority**: 🟡 Medium
- **Effort**: 1 day

**Description**: `RetryableError` protocol for all error types.

---

## 4. Data Integrity Validation

### Current Implementation

| Component | Status |
|-----------|--------|
| OnlineIndexScrubber | ✅ Complete |
| ItemEnvelope (magic number) | ✅ Complete |
| KeyValidation | ✅ Complete |
| TransformingSerializer (checksum) | ✅ Complete |

### Missing Features

#### 4.1 IndexMaintainer.validateEntries()

- [ ] **Status**: Not Implemented
- **Priority**: 🟡 Medium
- **Effort**: 2-3 days

**Description**: Per-entry index validation API.

#### 4.2 Serialization Round-trip Validation

- [ ] **Status**: Not Implemented
- **Priority**: 🟢 Low
- **Effort**: 1 day

**Description**: Validate serialize/deserialize round-trip.

---

## 5. Storage Evolution

### Current Implementation

| Component | File | Status |
|-----------|------|--------|
| FormatVersion | `Migration/FormatVersion.swift` | ✅ Complete |
| FormatVersionManager | `Migration/FormatVersion.swift` | ✅ Complete |
| FormatVersionError | `Migration/FormatVersion.swift` | ✅ Complete |
| FDBContainer Integration | - | ❌ **Not Implemented** |

### Missing Features

#### 5.1 FDBContainer Format Version Integration

- [ ] **Status**: Not Implemented
- **Priority**: 🔴 **Critical**
- **Effort**: 2 days

**Description**: Check and upgrade format version when opening store.

#### 5.2 Feature Flags per Version

- [ ] **Status**: Not Implemented
- **Priority**: 🟡 Medium
- **Effort**: 1 day

**Description**: Feature flags based on format version.

---

## Implementation Progress

### Phase 1: Transaction Infrastructure (✅ Complete)

| Task | Status | File |
|------|--------|------|
| CommitCheck protocol | ✅ Complete | `Transaction/CommitCheck.swift` |
| PostCommit protocol | ✅ Complete | `Transaction/PostCommit.swift` |
| TransactionListener | ✅ Complete | `Transaction/TransactionListener.swift` |
| Transaction ID & Logging | ✅ Complete | `Transaction/TransactionConfiguration.swift` |
| Tests | ✅ Complete | `Tests/TransactionInfrastructureTests.swift` |

### Phase 2: Error Handling

| Task | Status | PR |
|------|--------|-----|
| DatabaseErrorCode | ⬜ Not Started | - |
| RetryableError protocol | ⬜ Not Started | - |
| Conflicting keys | ⬜ Not Started | - |
| Tests | ⬜ Not Started | - |

### Phase 3: Storage Evolution

| Task | Status | PR |
|------|--------|-----|
| FDBContainer integration | ⬜ Not Started | - |
| Feature flags | ⬜ Not Started | - |
| Tests | ⬜ Not Started | - |

### Phase 4: Instrumentation

| Task | Status | PR |
|------|--------|-----|
| Delayed events | ⬜ Not Started | - |
| Database-level aggregation | ⬜ Not Started | - |
| Tests | ⬜ Not Started | - |

### Phase 5: Data Integrity

| Task | Status | PR |
|------|--------|-----|
| validateEntries | ⬜ Not Started | - |
| Round-trip validation | ⬜ Not Started | - |
| Tests | ⬜ Not Started | - |

---

## References

- [FDB Record Layer Overview](https://deepwiki.com/FoundationDB/fdb-record-layer)
- [FDB Record Layer Transaction Management](https://deepwiki.com/FoundationDB/fdb-record-layer#2.2)
- [FDB Record Layer Serialization](https://deepwiki.com/FoundationDB/fdb-record-layer#2.3)
