# Type-Safe Numeric Index Design

## Status: Implemented (Core)
## Author: Claude
## Date: 2024-12-06

---

## 1. Problem Statement

### 1.1 Current State

94 occurrences of numeric type conversion code are scattered across 20+ files:

```swift
// Pattern repeated everywhere - all converted to Double
if let intValue = value as? Int { return Double(intValue) }
if let doubleValue = value as? Double { return doubleValue }
if let floatValue = value as? Float { return Double(floatValue) }
if let int64Value = value as? Int64 { return Double(int64Value) }
```

### 1.2 Root Cause

**IndexKind lacks value type information:**

```swift
// Current: No type information for value field
public struct SumIndexKind<Root: Persistable>: IndexKind {
    public let valueFieldName: String  // Type is lost!
}
```

### 1.3 Why This Is Wrong

1. **Precision Loss**: `Int64.max` cannot be represented exactly as `Double`
2. **Unnecessary Conversion**: Integer sums don't need floating-point
3. **No Compile-Time Safety**: Type errors only caught at runtime
4. **Violates Generics**: Swift has generics for this purpose

---

## 2. Design Goals

1. **Use Generics**: Each calculation Index has a `Value` type parameter
2. **Compile-Time Safety**: Type mismatches are caught at compile time
3. **No Intermediate Enum**: Direct type parameters, no `NumericStorageKind`
4. **Preserve Precision**: Integer types stay as integers
5. **Codable Support**: Type name stored as String for schema persistence

---

## 3. Affected Index Types

All Index types that perform numeric calculations require type parameters:

| Index | Purpose | Current | Proposed |
|-------|---------|---------|----------|
| `SumIndexKind` | Sum aggregation | `<Root>` | `<Root, Value: Numeric>` |
| `AverageIndexKind` | Average aggregation | `<Root>` | `<Root, Value: Numeric>` |
| `MinIndexKind` | Minimum tracking | `<Root>` | `<Root, Value: Comparable>` |
| `MaxIndexKind` | Maximum tracking | `<Root>` | `<Root, Value: Comparable>` |
| `RankIndexKind` | Leaderboard ranking | `<Root>` | `<Root, Score: Comparable & Numeric>` |
| `TimeWindowLeaderboardIndexKind` | Time-windowed ranking | `<Root>` | `<Root, Score: Comparable & Numeric>` |

---

## 4. Proposed Design

### 4.1 SumIndexKind

```swift
/// Sum aggregation index with type-safe value
public struct SumIndexKind<Root: Persistable, Value: Numeric & Codable & Sendable>: IndexKind {
    public static var identifier: String { "sum" }
    public static var subspaceStructure: SubspaceStructure { .aggregation }

    public let groupByFieldNames: [String]
    public let valueFieldName: String
    public let valueTypeName: String  // For Codable: "Int64", "Double", etc.

    public var fieldNames: [String] {
        groupByFieldNames + [valueFieldName]
    }

    /// Initialize with KeyPaths - type is preserved
    public init(
        groupBy: [PartialKeyPath<Root>],
        value: KeyPath<Root, Value>
    ) {
        self.groupByFieldNames = groupBy.map { Root.fieldName(for: $0) }
        self.valueFieldName = Root.fieldName(for: value)
        self.valueTypeName = String(describing: Value.self)
    }

    /// Initialize from stored schema (Codable reconstruction)
    public init(
        groupByFieldNames: [String],
        valueFieldName: String,
        valueTypeName: String
    ) {
        self.groupByFieldNames = groupByFieldNames
        self.valueFieldName = valueFieldName
        self.valueTypeName = valueTypeName
    }
}
```

**Usage (type parameters are auto-inferred from KeyPath):**
```swift
@Persistable
struct Order {
    var id: String = ULID().ulidString
    var customerId: String
    var amount: Int64  // Integer type

    // No explicit type parameters needed - Value is inferred from \.amount
    #Index<Order>(type: SumIndexKind(groupBy: [\.customerId], value: \.amount))
}
```

**Type Inference:**
```
\.amount → KeyPath<Order, Int64> → Value = Int64 (compiler infers)
\.price  → KeyPath<Order, Double> → Value = Double (compiler infers)

// User writes:
SumIndexKind(groupBy: [\.customerId], value: \.amount)

// Compiler sees:
SumIndexKind<Order, Int64>(groupBy: [\.customerId], value: \.amount)
```

### 4.2 AverageIndexKind

**Note**: Average = sum / count, so result is always `Double` regardless of input type.

```swift
/// Average aggregation index with type-safe value
///
/// **Storage**: sum (Value type) + count (Int64)
/// **Result**: Always Double (division result)
///
/// Example: Average of [1, 2, 4] (Int64)
/// - sum = 7 (Int64, stored as Int64)
/// - count = 3 (Int64)
/// - result = 7.0 / 3.0 = 2.333... (Double)
public struct AverageIndexKind<Root: Persistable, Value: Numeric & Codable & Sendable>: IndexKind {
    public static var identifier: String { "average" }
    public static var subspaceStructure: SubspaceStructure { .aggregation }

    public let groupByFieldNames: [String]
    public let valueFieldName: String
    public let valueTypeName: String

    public var fieldNames: [String] {
        groupByFieldNames + [valueFieldName]
    }

    public init(
        groupBy: [PartialKeyPath<Root>],
        value: KeyPath<Root, Value>
    ) {
        self.groupByFieldNames = groupBy.map { Root.fieldName(for: $0) }
        self.valueFieldName = Root.fieldName(for: value)
        self.valueTypeName = String(describing: Value.self)
    }

    /// Result type is always Double (average = sum / count)
    public typealias ResultType = Double
}
```

**Storage Layout**:
```
[indexSubspace][groupKey]["sum"]   = Value (Int64 bytes or scaled Double)
[indexSubspace][groupKey]["count"] = Int64
```

**Calculation**:
```swift
let average: Double = Double(sum) / Double(count)
```

### 4.3 MinIndexKind / MaxIndexKind

```swift
/// Min aggregation index with type-safe value
public struct MinIndexKind<Root: Persistable, Value: Comparable & Codable & Sendable>: IndexKind {
    public static var identifier: String { "min" }
    public static var subspaceStructure: SubspaceStructure { .flat }

    public let groupByFieldNames: [String]
    public let valueFieldName: String
    public let valueTypeName: String

    public var fieldNames: [String] {
        groupByFieldNames + [valueFieldName]
    }

    public init(
        groupBy: [PartialKeyPath<Root>],
        value: KeyPath<Root, Value>
    ) {
        self.groupByFieldNames = groupBy.map { Root.fieldName(for: $0) }
        self.valueFieldName = Root.fieldName(for: value)
        self.valueTypeName = String(describing: Value.self)
    }
}

/// Max aggregation index with type-safe value
public struct MaxIndexKind<Root: Persistable, Value: Comparable & Codable & Sendable>: IndexKind {
    public static var identifier: String { "max" }
    public static var subspaceStructure: SubspaceStructure { .flat }

    public let groupByFieldNames: [String]
    public let valueFieldName: String
    public let valueTypeName: String

    public var fieldNames: [String] {
        groupByFieldNames + [valueFieldName]
    }

    public init(
        groupBy: [PartialKeyPath<Root>],
        value: KeyPath<Root, Value>
    ) {
        self.groupByFieldNames = groupBy.map { Root.fieldName(for: $0) }
        self.valueFieldName = Root.fieldName(for: value)
        self.valueTypeName = String(describing: Value.self)
    }
}
```

### 4.4 RankIndexKind

```swift
/// Rank index with type-safe score
public struct RankIndexKind<Root: Persistable, Score: Comparable & Numeric & Codable & Sendable>: IndexKind {
    public static var identifier: String { "rank" }
    public static var subspaceStructure: SubspaceStructure { .hierarchical }

    public let fieldNames: [String]
    public let scoreTypeName: String
    public let bucketSize: Int

    public init(
        field: KeyPath<Root, Score>,
        bucketSize: Int = 100
    ) {
        self.fieldNames = [Root.fieldName(for: field)]
        self.scoreTypeName = String(describing: Score.self)
        self.bucketSize = bucketSize
    }
}
```

### 4.5 TimeWindowLeaderboardIndexKind

```swift
/// Time-windowed leaderboard index with type-safe score
public struct TimeWindowLeaderboardIndexKind<Root: Persistable, Score: Comparable & Numeric & Codable & Sendable>: IndexKind {
    public static var identifier: String { "time_window_leaderboard" }
    public static var subspaceStructure: SubspaceStructure { .hierarchical }

    public let scoreFieldName: String
    public let scoreTypeName: String
    public let groupByFieldNames: [String]
    public let window: LeaderboardWindowType
    public let windowCount: Int

    public var fieldNames: [String] {
        groupByFieldNames + [scoreFieldName]
    }

    public init(
        scoreField: KeyPath<Root, Score>,
        groupBy: [PartialKeyPath<Root>] = [],
        window: LeaderboardWindowType = .daily,
        windowCount: Int = 7
    ) {
        self.scoreFieldName = Root.fieldName(for: scoreField)
        self.scoreTypeName = String(describing: Score.self)
        self.groupByFieldNames = groupBy.map { Root.fieldName(for: $0) }
        self.window = window
        self.windowCount = windowCount
    }
}
```

---

## 5. IndexMaintainer Updates

### 5.1 Generic IndexMaintainer Pattern

```swift
/// Sum index maintainer with compile-time type safety
public struct SumIndexMaintainer<Item: Persistable, Value: Numeric & Codable & Sendable>: IndexMaintainer {
    private let index: Index
    private let subspace: Subspace
    private let idExpression: KeyExpression
    private let valueKeyPath: String

    public func updateIndex(
        oldItem: Item?,
        newItem: Item?,
        transaction: any TransactionProtocol
    ) async throws {
        let oldValue: Value? = oldItem.flatMap { extractValue(from: $0) }
        let newValue: Value? = newItem.flatMap { extractValue(from: $0) }

        // Type-safe calculation - no conversion needed
        let delta = computeDelta(old: oldValue, new: newValue)

        if delta != Value.zero {
            let bytes = toBytes(delta)
            transaction.atomicOp(key: key, param: bytes, mutationType: .add)
        }
    }

    /// Extract value using compile-time known type
    private func extractValue(from item: Item) -> Value? {
        item[dynamicMember: valueKeyPath] as? Value
    }

    /// Convert to bytes based on Value type
    private func toBytes(_ value: Value) -> [UInt8] {
        // Specialization at compile time
        if Value.self == Int64.self {
            return ByteConversion.int64ToBytes(value as! Int64)
        } else if Value.self == Int.self {
            return ByteConversion.int64ToBytes(Int64(value as! Int))
        } else if Value.self == Double.self {
            return ByteConversion.doubleToScaledBytes(value as! Double)
        } else if Value.self == Float.self {
            return ByteConversion.doubleToScaledBytes(Double(value as! Float))
        }
        // Handle other numeric types...
    }
}
```

### 5.2 IndexKindMaintainable Bridge

```swift
extension SumIndexKind: IndexKindMaintainable {
    public func makeIndexMaintainer<Item: Persistable>(
        index: Index,
        subspace: Subspace,
        idExpression: KeyExpression,
        configurations: [any IndexConfiguration]
    ) -> any IndexMaintainer<Item> {
        // Create type-specific maintainer based on valueTypeName
        switch valueTypeName {
        case "Int64":
            return SumIndexMaintainer<Item, Int64>(...)
        case "Int":
            return SumIndexMaintainer<Item, Int>(...)
        case "Double":
            return SumIndexMaintainer<Item, Double>(...)
        case "Float":
            return SumIndexMaintainer<Item, Float>(...)
        default:
            fatalError("Unsupported numeric type: \(valueTypeName)")
        }
    }
}
```

---

## 6. Codable Support

### 6.1 Type Name Storage

Since Swift generics are erased at runtime for Codable, we store the type name:

```swift
public struct SumIndexKind<Root: Persistable, Value: Numeric>: IndexKind, Codable {
    public let valueTypeName: String  // "Int64", "Double", etc.

    // Codable implementation uses valueTypeName for reconstruction
}
```

### 6.2 Type Registry (Optional Enhancement)

For safer type reconstruction:

```swift
enum NumericTypeRegistry {
    static func type(for name: String) -> Any.Type? {
        switch name {
        case "Int": return Int.self
        case "Int64": return Int64.self
        case "Int32": return Int32.self
        case "Double": return Double.self
        case "Float": return Float.self
        default: return nil
        }
    }
}
```

---

## 7. FDB Storage Format

### 7.1 Integer Types (Int, Int64, Int32, etc.)

- **Storage**: Direct Int64 bytes (8 bytes, little-endian)
- **Atomic Operation**: FDB `add` on Int64 bytes
- **Precision**: Exact (no conversion loss)

```swift
// Int64 → bytes
ByteConversion.int64ToBytes(value)

// bytes → Int64
ByteConversion.bytesToInt64(bytes)
```

### 7.2 Floating-Point Types (Float, Double)

- **Storage**: Scaled fixed-point Int64 (6 decimal places)
- **Atomic Operation**: FDB `add` on scaled Int64 bytes
- **Precision**: 6 decimal places

```swift
// Double → scaled Int64 bytes
ByteConversion.doubleToScaledBytes(value)

// scaled bytes → Double
ByteConversion.scaledBytesToDouble(bytes)
```

---

## 8. Implementation Checklist

### 8.1 database-kit Changes

- [x] Update `SumIndexKind<Root>` → `SumIndexKind<Root, Value: Numeric>`
- [x] Update `AverageIndexKind<Root>` → `AverageIndexKind<Root, Value: Numeric>`
- [x] Update `MinIndexKind<Root>` → `MinIndexKind<Root, Value: Comparable>`
- [x] Update `MaxIndexKind<Root>` → `MaxIndexKind<Root, Value: Comparable>`
- [x] Update `RankIndexKind<Root>` → `RankIndexKind<Root, Score: Comparable & Numeric>`
- [x] Update `TimeWindowLeaderboardIndexKind<Root>` → `TimeWindowLeaderboardIndexKind<Root, Score>`
- [x] Add `valueTypeName` / `scoreTypeName` properties
- [ ] Update `#Index` macro to infer type parameters (macro already handles this)

### 8.2 database-framework Changes

- [x] Update `SumIndexMaintainer` with type parameter
- [x] Update `AverageIndexMaintainer` with type parameter
- [x] Update `MinMaxIndexMaintainer` with type parameter
- [x] Update `RankIndexMaintainer` with type parameter
- [x] Update `TimeWindowLeaderboardIndexMaintainer` with type parameter
- [x] Update `IndexKindMaintainable` extensions for type dispatch
- [x] Remove `extractNumericValue` / `toDouble` from all maintainers
- [ ] Update `AggregationExecution` for type-safe queries (TODO)

### 8.3 Cleanup

- [x] Remove type conversion code from Maintainers
- [ ] Update tests for type-safe behavior
- [ ] Verify precision preservation for large Int64 values

---

## 9. Migration

### 9.1 API Change

User-facing syntax remains the same - type parameters are inferred:

```swift
// User code (unchanged)
SumIndexKind(groupBy: [\.customerId], value: \.amount)

// Internal expansion (automatic)
// If amount: Int64 → SumIndexKind<Order, Int64>
// If amount: Double → SumIndexKind<Order, Double>
```

**No explicit type parameters needed** - the compiler infers `Value` from the KeyPath.

### 9.2 Schema Migration

Existing indexes without `valueTypeName`:
- Default to `Double` for backward compatibility
- Log warning recommending re-indexing

---

## 10. Testing

### 10.1 Type Safety Tests

```swift
@Test func sumIndexPreservesIntegerType() async throws {
    // Sum of Int64 values should be exact
    let sum = try await context.aggregate(Order.self)
        .sum(\.amount)  // Int64 field
        .execute()

    // No precision loss for large values
    XCTAssertEqual(sum, Int64.max - 1000 + Int64.max - 2000)
}

@Test func sumIndexHandlesFloatingPoint() async throws {
    // Sum of Double values uses fixed-point
    let sum = try await context.aggregate(Transaction.self)
        .sum(\.price)  // Double field
        .execute()

    XCTAssertEqual(sum, 123.456789, accuracy: 0.000001)
}
```

### 10.2 Compile-Time Safety Tests

```swift
// This should NOT compile - type mismatch
@Persistable
struct BadModel {
    var count: String

    // Error: String does not conform to Numeric
    #Index<BadModel>(type: SumIndexKind(groupBy: [], value: \.count))
}
```

---

## 11. Summary

### 11.1 Type Parameters and Result Types

| Index | Type Parameter | Storage | Result Type |
|-------|---------------|---------|-------------|
| `SumIndexKind<Root, Value>` | `Value: Numeric` | Value | `Value` |
| `AverageIndexKind<Root, Value>` | `Value: Numeric` | sum: Value, count: Int64 | `Double` |
| `MinIndexKind<Root, Value>` | `Value: Comparable` | Value | `Value` |
| `MaxIndexKind<Root, Value>` | `Value: Comparable` | Value | `Value` |
| `RankIndexKind<Root, Score>` | `Score: Comparable & Numeric` | Score | `Score` |
| `TimeWindowLeaderboardIndexKind<Root, Score>` | `Score: Comparable & Numeric` | Score | `Score` |

### 11.2 Before vs After

| Aspect | Before | After |
|--------|--------|-------|
| Type Info | Lost (String field name) | Preserved (generic parameter) |
| Precision | Double for all | Native type (except Average result) |
| Safety | Runtime checks | Compile-time |
| Code | 94 conversion sites | Centralized in ByteConversion |
| API | `SumIndexKind<Root>` | `SumIndexKind<Root, Value>` |
