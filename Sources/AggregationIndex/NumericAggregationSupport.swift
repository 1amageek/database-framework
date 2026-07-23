// NumericAggregationSupport.swift
// AggregationIndex - Shared utilities for numeric aggregation indexes
//
// Provides common value extraction and transactional mutation support for
// Sum, Average, and other numeric aggregation maintainers.
//
// Reference: Consolidates duplicate code from SumIndexMaintainer, AverageIndexMaintainer

#if canImport(FoundationEssentials)
import FoundationEssentials
#else
import Foundation
#endif
import Core
import DatabaseEngine
import StorageKit

public enum AggregationStorageError: Error, Sendable, Equatable {
    case integerOverflow
    case integerNotExactlyRepresentableAsDouble(Int64)
    case unsignedIntegerNotExactlyRepresentableAsDouble(UInt64)
    case negativeCount(Int64)
    case nonPositiveStoredCount(Int64)
    case scanLimitExceeded(Int)
    case scanByteLimitExceeded(Int)
    case membershipLimitExceeded(Int)
    case membershipByteLimitExceeded(Int)
    case missingMembershipMetadata
    case invalidMembershipMetadataByteCount(Int)
    case nonPositiveMembershipMetadata
    case membershipMetadataUnderflow
    case membershipMetadataMismatch(
        expectedScanBytes: Int64,
        actualScanBytes: Int64
    )
    case membershipMetadataTooSmall(
        minimumScanBytes: Int64,
        actualScanBytes: Int64
    )
    case membershipCountMismatch(expected: Int64, actual: Int64)
    case membershipScanByteMismatch(expected: Int64, actual: Int64)
    case invalidWideIntegerByteCount(Int)
    case invalidFloatingPointStateByteCount(Int)
    case incompatibleNumericValue(
        expected: AggregationNumericStorageKind,
        actual: AggregationNumericStorageKind
    )
    case nonFiniteFloatingPoint
}

public enum AggregationNumericStorageKind: Sendable, Equatable {
    case signedInteger
    case unsignedInteger
    case floatingPoint
}

public enum AggregationNumericValue: Sendable, Equatable {
    case signedInteger(Int64)
    case unsignedInteger(UInt64)
    case floatingPoint(Double)

    public var storageKind: AggregationNumericStorageKind {
        switch self {
        case .signedInteger:
            return .signedInteger
        case .unsignedInteger:
            return .unsignedInteger
        case .floatingPoint:
            return .floatingPoint
        }
    }

    public var fieldValue: FieldValue {
        switch self {
        case .signedInteger(let value):
            return .int64(value)
        case .unsignedInteger(let value):
            return .uint64(value)
        case .floatingPoint(let value):
            return .double(value)
        }
    }
}

enum AggregationNumericAccumulatorValue: Sendable, Equatable {
    case signedInteger(Int128)
    case unsignedInteger(UInt128)
    case floatingPoint(Double)
}

private enum AggregationWideIntegerCodec {
    static let encodedByteCount = MemoryLayout<UInt64>.size * 2

    static func encode(_ value: Int128) -> Bytes {
        encode(UInt128(bitPattern: value))
    }

    static func encode(_ value: UInt128) -> Bytes {
        Bytes.copying(count: encodedByteCount) { destination in
            guard let baseAddress = destination.baseAddress else {
                preconditionFailure(
                    "Wide integer aggregate state requires storage"
                )
            }
            baseAddress.storeBytes(
                of: UInt64(truncatingIfNeeded: value).littleEndian,
                toByteOffset: 0,
                as: UInt64.self
            )
            baseAddress.storeBytes(
                of: UInt64(truncatingIfNeeded: value >> 64).littleEndian,
                toByteOffset: MemoryLayout<UInt64>.size,
                as: UInt64.self
            )
        }
    }

    static func decodeSigned(_ bytes: Bytes) throws -> Int128 {
        Int128(bitPattern: try decodeUnsigned(bytes))
    }

    static func decodeUnsigned(_ bytes: Bytes) throws -> UInt128 {
        guard bytes.count == encodedByteCount else {
            throw AggregationStorageError.invalidWideIntegerByteCount(
                bytes.count
            )
        }
        return bytes.withUnsafeBytes { buffer in
            let lower = UInt64(littleEndian: buffer.loadUnaligned(
                fromByteOffset: 0,
                as: UInt64.self
            ))
            let upper = UInt64(littleEndian: buffer.loadUnaligned(
                fromByteOffset: MemoryLayout<UInt64>.size,
                as: UInt64.self
            ))
            return UInt128(lower) | (UInt128(upper) << 64)
        }
    }
}

/// Persistent Neumaier accumulator for floating-point SUM and AVERAGE.
///
/// The two finite components are stored instead of repeatedly rounding the
/// materialized total after every mutation. The fixed 16-byte frame is the
/// ownership boundary; decoding borrows the storage buffer and allocates no
/// intermediate byte collection.
private struct AggregationFloatingPointState: Sendable, Equatable {
    static let encodedByteCount = MemoryLayout<UInt64>.size * 2

    var sum: Double = 0
    var compensation: Double = 0

    mutating func add(_ value: Double) throws {
        guard value.isFinite else {
            throw AggregationStorageError.nonFiniteFloatingPoint
        }

        let next = sum + value
        guard next.isFinite else {
            throw AggregationStorageError.nonFiniteFloatingPoint
        }
        let correction: Double
        if abs(sum) >= abs(value) {
            correction = (sum - next) + value
        } else {
            correction = (value - next) + sum
        }
        let nextCompensation = compensation + correction
        guard nextCompensation.isFinite else {
            throw AggregationStorageError.nonFiniteFloatingPoint
        }

        sum = next
        compensation = nextCompensation
        _ = try total()
    }

    func total() throws -> Double {
        let value = sum + compensation
        guard value.isFinite else {
            throw AggregationStorageError.nonFiniteFloatingPoint
        }
        return value
    }

    func encode() throws -> Bytes {
        _ = try total()
        return Bytes.copying(count: Self.encodedByteCount) { destination in
            guard let baseAddress = destination.baseAddress else {
                preconditionFailure(
                    "Floating-point aggregate state requires storage"
                )
            }
            baseAddress.storeBytes(
                of: sum.bitPattern.littleEndian,
                toByteOffset: 0,
                as: UInt64.self
            )
            baseAddress.storeBytes(
                of: compensation.bitPattern.littleEndian,
                toByteOffset: MemoryLayout<UInt64>.size,
                as: UInt64.self
            )
        }
    }

    static func decode(_ bytes: Bytes) throws -> Self {
        guard bytes.count == encodedByteCount else {
            throw AggregationStorageError.invalidFloatingPointStateByteCount(
                bytes.count
            )
        }
        let state = bytes.withUnsafeBytes { buffer in
            Self(
                sum: Double(bitPattern: UInt64(littleEndian:
                    buffer.loadUnaligned(
                        fromByteOffset: 0,
                        as: UInt64.self
                    )
                )),
                compensation: Double(bitPattern: UInt64(littleEndian:
                    buffer.loadUnaligned(
                        fromByteOffset: MemoryLayout<UInt64>.size,
                        as: UInt64.self
                    )
                ))
            )
        }
        guard state.sum.isFinite, state.compensation.isFinite else {
            throw AggregationStorageError.nonFiniteFloatingPoint
        }
        _ = try state.total()
        return state
    }
}

private func checkedAggregationSum(
    _ lhs: Int64,
    _ rhs: Int64
) throws(AggregationStorageError) -> Int64 {
    let (result, overflow) = lhs.addingReportingOverflow(rhs)
    guard !overflow else { throw .integerOverflow }
    return result
}

// MARK: - Numeric Value Extraction

/// Utility for extracting numeric values from tuple elements with type safety
///
/// **Purpose**: Centralizes value extraction logic previously duplicated across
/// SumIndexMaintainer, AverageIndexMaintainer, and MinMaxIndexMaintainer.
///
/// **FDB Tuple Layer Behavior**:
/// - Int, Int32, Int64 → stored as Int64
/// - UInt, UInt32, UInt64 → stored as UInt64
/// - Float, Double → stored as Double
public enum NumericValueExtractor {

    /// Extract numeric value with type-safe conversion
    ///
    /// Handles FDB's type coercion (Int→Int64, Float→Double).
    ///
    /// - Parameters:
    ///   - element: The tuple element to extract from
    ///   - valueType: The expected value type
    /// - Returns: Canonical signed, unsigned, or floating-point value.
    /// - Throws: If extraction fails
    public static func extractNumeric<Value: IndexNumericValue>(
        from element: any TupleElement,
        as valueType: Value.Type
    ) throws -> AggregationNumericValue {
        switch valueType {
        case is Int64.Type, is Int.Type, is Int32.Type, is Int16.Type, is Int8.Type:
            let value = try TypeConversion.int64(from: element)
            return .signedInteger(value)

        case is UInt.Type:
            let value = try TupleDecoder.decode(element, as: UInt.self)
            return .unsignedInteger(UInt64(value))
        case is UInt8.Type:
            let value = try TupleDecoder.decode(element, as: UInt8.self)
            return .unsignedInteger(UInt64(value))
        case is UInt16.Type:
            let value = try TupleDecoder.decode(element, as: UInt16.self)
            return .unsignedInteger(UInt64(value))
        case is UInt32.Type:
            let value = try TupleDecoder.decode(element, as: UInt32.self)
            return .unsignedInteger(UInt64(value))
        case is UInt64.Type:
            let value = try TupleDecoder.decode(element, as: UInt64.self)
            return .unsignedInteger(value)

        case is Double.Type, is Float.Type:
            let value = try TypeConversion.double(from: element)
            guard value.isFinite else {
                throw AggregationStorageError.nonFiniteFloatingPoint
            }
            return .floatingPoint(value)

        default:
            throw IndexError.invalidConfiguration(
                "Unsupported numeric type for aggregation: \(valueType)"
            )
        }
    }

    public static func storageKind<Value: IndexNumericValue>(
        _ type: Value.Type
    ) throws -> AggregationNumericStorageKind {
        switch type.indexScalarType {
        case .uint, .uint8, .uint16, .uint32, .uint64:
            return .unsignedInteger
        case .float, .double:
            return .floatingPoint
        case .int, .int8, .int16, .int32, .int64:
            return .signedInteger
        case .string, .date:
            throw IndexError.invalidConfiguration(
                "Numeric index value declared a non-numeric scalar type"
            )
        }
    }
}

struct AggregationStorageKey {
    let groupingIdentity: Bytes
    let groupingElements: [any TupleElement]
    let marker: String
}

enum AggregationScanLimits {
    static let maximumBytes = 16 * 1_024 * 1_024
}

func checkedAggregationScannedBytes(
    _ current: Int,
    adding additional: Int,
    maximum: Int = AggregationScanLimits.maximumBytes
) throws -> Int {
    let (updated, overflow) = current.addingReportingOverflow(additional)
    guard !overflow, updated <= maximum else {
        throw AggregationStorageError.scanByteLimitExceeded(maximum)
    }
    return updated
}

/// Decodes a physical SUM/AVERAGE key in one pass.
///
/// `groupingIdentity` is a constant-time slice of the original key owner. The
/// scan path therefore does not re-encode every grouping tuple just to build a
/// dictionary key.
func decodeAggregationStorageKey(
    _ key: Bytes,
    in subspace: Subspace
) throws -> AggregationStorageKey {
    var cursor = try subspace.tupleCursor(for: key)
    var groupingElements: [any TupleElement] = []
    var markerStart: Int?
    var marker: String?

    while !cursor.isAtEnd {
        let elementStart = cursor.consumedByteCount
        let element = try cursor.requireNext()
        if cursor.isAtEnd {
            markerStart = elementStart
            marker = element as? String
        } else {
            groupingElements.append(element)
        }
    }

    guard let markerStart, let marker else {
        throw IndexError.invalidStructure(
            "Aggregation index key is missing its string value marker"
        )
    }
    let identityStart = subspace.prefix.count
    let identityEnd = identityStart + markerStart
    return AggregationStorageKey(
        groupingIdentity: key[identityStart..<identityEnd],
        groupingElements: groupingElements,
        marker: marker
    )
}

// MARK: - Transactional Numeric Mutations

/// Protocol providing checked numeric mutations for aggregation maintainers.
///
/// **Purpose**: Centralizes numeric mutations previously duplicated across
/// SumIndexMaintainer and AverageIndexMaintainer.
///
/// Numeric mutations read and replace one aggregate value in the caller's
/// transaction. Integer storage remains exact, while floating-point storage
/// retains its compensation component consistently on every backend.
public protocol AggregationNumericStorageSupport {
    var numericStorageKind: AggregationNumericStorageKind { get throws }
    var storesWideIntegerAccumulator: Bool { get }
}

extension AggregationNumericStorageSupport {
    public var storesWideIntegerAccumulator: Bool { false }
}

public protocol NumericAggregationMutationSupport:
    SubspaceIndexMaintainer,
    AggregationNumericStorageSupport {}

extension NumericAggregationMutationSupport {

    /// Mutates the sum and membership count as one invariant-preserving pair.
    /// Both entries are read and validated before either write is staged.
    func mutateNumericAggregate(
        sumKey: Bytes,
        countKey: Bytes,
        removing oldValue: AggregationNumericValue?,
        adding newValue: AggregationNumericValue?,
        transaction: any TransactionAccess
    ) async throws {
        try validateStorageKind(of: oldValue)
        try validateStorageKind(of: newValue)

        let currentBytes = try await transaction.getValue(for: sumKey)
        let countBytes = try await transaction.getValue(for: countKey)
        guard (currentBytes == nil) == (countBytes == nil) else {
            throw IndexError.invalidStructure(
                "Numeric aggregate requires both sum and count entries"
            )
        }

        var count: Int64
        if let countBytes {
            count = try ByteConversion.bytesToInt64(countBytes)
            guard count > 0 else {
                throw AggregationStorageError.nonPositiveStoredCount(count)
            }
        } else {
            count = 0
        }
        if oldValue != nil {
            guard count > 0 else {
                throw AggregationStorageError.negativeCount(-1)
            }
            count = try checkedAggregationSum(count, -1)
        }
        if newValue != nil {
            count = try checkedAggregationSum(count, 1)
        }

        let encodedResult: Bytes
        switch try numericStorageKind {
        case .signedInteger:
            let current: Int128
            if let currentBytes {
                if storesWideIntegerAccumulator {
                    current = try AggregationWideIntegerCodec.decodeSigned(
                        currentBytes
                    )
                } else {
                    current = Int128(
                        try ByteConversion.bytesToInt64(currentBytes)
                    )
                }
            } else {
                current = 0
            }
            var exact = current
            if case .signedInteger(let oldValue) = oldValue {
                let (updated, overflow) = exact.subtractingReportingOverflow(
                    Int128(oldValue)
                )
                guard !overflow else {
                    throw AggregationStorageError.integerOverflow
                }
                exact = updated
            }
            if case .signedInteger(let newValue) = newValue {
                let (updated, overflow) = exact.addingReportingOverflow(
                    Int128(newValue)
                )
                guard !overflow else {
                    throw AggregationStorageError.integerOverflow
                }
                exact = updated
            }
            if storesWideIntegerAccumulator {
                encodedResult = AggregationWideIntegerCodec.encode(exact)
            } else {
                guard let value = Int64(exactly: exact) else {
                    throw AggregationStorageError.integerOverflow
                }
                encodedResult = ByteConversion.int64ToBytes(value)
            }

        case .unsignedInteger:
            let current: UInt128
            if let currentBytes {
                if storesWideIntegerAccumulator {
                    current = try AggregationWideIntegerCodec.decodeUnsigned(
                        currentBytes
                    )
                } else {
                    current = UInt128(
                        try ByteConversion.bytesToUInt64(currentBytes)
                    )
                }
            } else {
                current = 0
            }
            var exact = current
            if case .unsignedInteger(let oldValue) = oldValue {
                guard exact >= UInt128(oldValue) else {
                    throw AggregationStorageError.integerOverflow
                }
                exact -= UInt128(oldValue)
            }
            if case .unsignedInteger(let newValue) = newValue {
                let (updated, overflow) = exact.addingReportingOverflow(
                    UInt128(newValue)
                )
                guard !overflow else {
                    throw AggregationStorageError.integerOverflow
                }
                exact = updated
            }
            if storesWideIntegerAccumulator {
                encodedResult = AggregationWideIntegerCodec.encode(exact)
            } else {
                guard let value = UInt64(exactly: exact) else {
                    throw AggregationStorageError.integerOverflow
                }
                encodedResult = ByteConversion.uint64ToBytes(value)
            }

        case .floatingPoint:
            var state: AggregationFloatingPointState
            if let currentBytes {
                state = try AggregationFloatingPointState.decode(currentBytes)
            } else {
                state = AggregationFloatingPointState()
            }
            if case .floatingPoint(let oldValue) = oldValue {
                try state.add(-oldValue)
            }
            if case .floatingPoint(let newValue) = newValue {
                try state.add(newValue)
            }
            encodedResult = try state.encode()
        }

        if count == 0 {
            try transaction.clear(key: sumKey)
            try transaction.clear(key: countKey)
        } else {
            try transaction.setValue(
                encodedResult,
                for: sumKey
            )
            try transaction.setValue(
                ByteConversion.int64ToBytes(count),
                for: countKey
            )
        }
    }

    private func validateStorageKind(
        of value: AggregationNumericValue?
    ) throws {
        guard let value else { return }
        let expected = try numericStorageKind
        guard value.storageKind == expected else {
            throw AggregationStorageError.incompatibleNumericValue(
                expected: expected,
                actual: value.storageKind
            )
        }
    }

}

// MARK: - Grouping Key Helpers

/// Protocol for aggregation maintainers that use grouping keys
public protocol GroupingKeySupport: SubspaceIndexMaintainer {
    /// The index definition
    var index: Index { get }
}

extension GroupingKeySupport {

    /// Build a grouping key from values
    ///
    /// - Parameter values: The grouping values
    /// - Returns: Packed key bytes
    /// - Throws: If packing fails
    public func buildGroupingKey(
        _ values: [any TupleElement]
    ) throws -> Bytes {
        try packAndValidate(elements: values)
    }
}

// MARK: - Query Result Helpers

/// Protocol for reading aggregation results
public protocol AggregationQuerySupport:
    SubspaceIndexMaintainer,
    AggregationNumericStorageSupport {}

extension AggregationQuerySupport {

    /// Convert a canonical numeric result to Double without rounding.
    public func exactDouble(from value: FieldValue) throws -> Double {
        switch value {
        case .int64(let integer):
            guard let result = Double(exactly: integer) else {
                throw AggregationStorageError.integerNotExactlyRepresentableAsDouble(
                    integer
                )
            }
            return result
        case .uint64(let integer):
            guard let result = Double(exactly: integer) else {
                throw AggregationStorageError.unsignedIntegerNotExactlyRepresentableAsDouble(
                    integer
                )
            }
            return result
        case .double(let result):
            guard result.isFinite else {
                throw AggregationStorageError.nonFiniteFloatingPoint
            }
            return result
        default:
            throw IndexError.invalidStructure(
                "Aggregation result contains a non-numeric value"
            )
        }
    }

    /// Read an Int64 value from stored bytes
    ///
    /// - Parameter bytes: The stored bytes
    /// - Returns: The Int64 value
    public func readInt64Value(_ bytes: Bytes) throws -> Int64 {
        try ByteConversion.bytesToInt64(bytes)
    }

    public func readUInt64Value(_ bytes: Bytes) throws -> UInt64 {
        try ByteConversion.bytesToUInt64(bytes)
    }

    /// Read the finite total from a persisted compensated accumulator.
    ///
    /// - Parameter bytes: The stored bytes
    /// - Returns: The Double value
    public func readDoubleValue(_ bytes: Bytes) throws -> Double {
        try AggregationFloatingPointState.decode(bytes).total()
    }

    public func readStoredNumericValue(
        _ bytes: Bytes
    ) throws -> AggregationNumericValue {
        switch try readStoredNumericAccumulator(bytes) {
        case .signedInteger(let value):
            guard let narrowed = Int64(exactly: value) else {
                throw AggregationStorageError.integerOverflow
            }
            return .signedInteger(narrowed)
        case .unsignedInteger(let value):
            guard let narrowed = UInt64(exactly: value) else {
                throw AggregationStorageError.integerOverflow
            }
            return .unsignedInteger(narrowed)
        case .floatingPoint(let value):
            return .floatingPoint(value)
        }
    }

    func readStoredNumericAccumulator(
        _ bytes: Bytes
    ) throws -> AggregationNumericAccumulatorValue {
        switch try numericStorageKind {
        case .signedInteger:
            if storesWideIntegerAccumulator {
                return .signedInteger(
                    try AggregationWideIntegerCodec.decodeSigned(bytes)
                )
            }
            return .signedInteger(Int128(try readInt64Value(bytes)))
        case .unsignedInteger:
            if storesWideIntegerAccumulator {
                return .unsignedInteger(
                    try AggregationWideIntegerCodec.decodeUnsigned(bytes)
                )
            }
            return .unsignedInteger(UInt128(try readUInt64Value(bytes)))
        case .floatingPoint:
            return .floatingPoint(try readDoubleValue(bytes))
        }
    }

    /// Read numeric value based on value type
    ///
    /// - Parameter bytes: The stored bytes
    /// - Returns: Double representation of the value
    public func readNumericValue(_ bytes: Bytes) throws -> Double {
        switch try readStoredNumericValue(bytes) {
        case .signedInteger(let integer):
            guard let value = Double(exactly: integer) else {
                throw AggregationStorageError.integerNotExactlyRepresentableAsDouble(
                    integer
                )
            }
            return value
        case .unsignedInteger(let integer):
            guard let value = Double(exactly: integer) else {
                throw AggregationStorageError.unsignedIntegerNotExactlyRepresentableAsDouble(
                    integer
                )
            }
            return value
        case .floatingPoint(let value):
            return value
        }
    }

}

// MARK: - Combined Support Protocol

/// Combined protocol for numeric aggregation maintainers
///
/// Provides all common functionality needed by Sum and Average maintainers.
public protocol NumericAggregationMaintainer:
    NumericAggregationMutationSupport,
    GroupingKeySupport,
    AggregationQuerySupport {}

// MARK: - Count Aggregation Support

/// Protocol for count-based aggregation maintainers (COUNT, COUNT_NOT_NULL)
///
/// Provides common functionality for maintainers that track counts.
public protocol CountAggregationMaintainer: SubspaceIndexMaintainer, GroupingKeySupport {
    var index: Index { get }
    var groupingFieldCount: Int { get }
}

extension CountAggregationMaintainer {

    /// Increment count for a grouping key
    public func incrementCount(
        key: Bytes,
        transaction: any TransactionAccess
    ) async throws {
        let current: Int64
        if let bytes = try await transaction.getValue(for: key) {
            current = try readStoredCount(bytes)
        } else {
            current = 0
        }
        let result = try checkedAggregationSum(current, 1)
        try transaction.setValue(ByteConversion.int64ToBytes(result), for: key)
    }

    /// Decrement count for a grouping key
    public func decrementCount(
        key: Bytes,
        transaction: any TransactionAccess
    ) async throws {
        guard let bytes = try await transaction.getValue(for: key) else {
            throw AggregationStorageError.negativeCount(-1)
        }
        let current = try readStoredCount(bytes)
        let result = try checkedAggregationSum(current, -1)
        guard result >= 0 else { throw AggregationStorageError.negativeCount(result) }
        if result == 0 {
            try transaction.clear(key: key)
        } else {
            try transaction.setValue(ByteConversion.int64ToBytes(result), for: key)
        }
    }

    /// Read a canonical persisted count.
    ///
    /// Zero is represented by the absence of a key so scans cannot surface
    /// empty groups as materialized aggregate results.
    public func readStoredCount(_ bytes: Bytes) throws -> Int64 {
        let count = try ByteConversion.bytesToInt64(bytes)
        guard count > 0 else {
            throw AggregationStorageError.nonPositiveStoredCount(count)
        }
        return count
    }

    /// Get count for a specific grouping
    public func getCountValue(
        groupingValues: [any TupleElement],
        transaction: any TransactionAccess
    ) async throws -> Int64 {
        guard groupingValues.count == groupingFieldCount else {
            throw IndexError.invalidArgument(
                "Grouping value count does not match count index '\(index.name)'"
            )
        }
        let key = try buildGroupingKey(groupingValues)
        guard let bytes = try await transaction.getValue(for: key) else {
            return 0
        }
        return try readStoredCount(bytes)
    }

    /// Maximum number of keys to scan for safety (prevents DoS on large indexes)
    private var maxScanKeys: Int { 100_000 }

    /// Scan all count entries
    ///
    /// **Resource Limit**: Scans at most 100,000 keys to prevent DoS attacks.
    public func scanAllCounts(
        transaction: any TransactionAccess
    ) async throws -> [(grouping: [any TupleElement], count: Int64)] {
        // An empty tuple packs to the subspace prefix itself. Subspace range
        // scans intentionally begin after that prefix, so a global count must
        // be read directly instead of being treated as a grouped range scan.
        if groupingFieldCount == 0 {
            let key = try buildGroupingKey([])
            guard let value = try await transaction.getValue(
                for: key,
                snapshot: true
            ) else {
                return []
            }
            return [(grouping: [], count: try readStoredCount(value))]
        }

        let range = subspace.range()
        var results: [(grouping: [any TupleElement], count: Int64)] = []
        var scannedKeys = 0
        var scannedBytes = 0

        try await transaction.forEachInRange(
            from: .firstGreaterOrEqual(range.begin),
            to: .firstGreaterOrEqual(range.end),
            limit: maxScanKeys + 1,
            snapshot: true,
            streamingMode: .iterator
        ) { key, value in
            scannedKeys += 1
            guard scannedKeys <= maxScanKeys else {
                throw AggregationStorageError.scanLimitExceeded(maxScanKeys)
            }
            scannedBytes = try checkedAggregationScannedBytes(
                scannedBytes,
                adding: key.count + value.count
            )

            var cursor = try subspace.tupleCursor(for: key)
            var elements: [any TupleElement] = []
            while !cursor.isAtEnd {
                elements.append(try cursor.requireNext())
            }
            guard elements.count == groupingFieldCount else {
                throw IndexError.invalidStructure(
                    "Count index key has an invalid grouping field count"
                )
            }
            let count = try readStoredCount(value)

            results.append((grouping: elements, count: count))
        }

        return results
    }
}
