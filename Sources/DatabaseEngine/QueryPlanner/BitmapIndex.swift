// BitmapIndex.swift
// QueryPlanner - Bitmap index support for low-cardinality columns

import Foundation
import Core
import FoundationDB

/// Bitmap index support for efficient filtering on low-cardinality columns
///
/// **What is a Bitmap Index?**
/// A bitmap index stores a bit array for each distinct value in a column.
/// Each bit represents whether a row has that value.
///
/// **Example**:
/// ```
/// Table: users (id, status)
/// status values: 'active', 'inactive', 'suspended'
///
/// Bitmap for 'active':     1 0 1 1 0 1 0 0 1 1
/// Bitmap for 'inactive':   0 1 0 0 1 0 1 0 0 0
/// Bitmap for 'suspended':  0 0 0 0 0 0 0 1 0 0
/// ```
///
/// **Advantages**:
/// - Extremely efficient for AND/OR operations (bitwise AND/OR)
/// - Compact storage for low-cardinality columns
/// - Fast for counting queries
/// - Excellent for multi-column filtering with low cardinality
///
/// **When to use**:
/// - Columns with few distinct values (cardinality < 100)
/// - Frequent AND/OR queries combining multiple columns
/// - Data warehouse style analytics
///
/// **When NOT to use**:
/// - High-cardinality columns (creates huge bitmaps)
/// - Frequently updated tables (bitmap maintenance is expensive)
/// - Single-column equality lookups (B-tree is usually better)
public struct BitmapIndexKind: Sendable, Hashable {
    /// Index name
    public let name: String

    /// Field being indexed
    public let fieldName: String

    /// Expected cardinality (for optimization)
    public let expectedCardinality: Int?

    /// Compression strategy
    public let compression: BitmapCompression

    public init(
        name: String,
        fieldName: String,
        expectedCardinality: Int? = nil,
        compression: BitmapCompression = .runLength
    ) {
        self.name = name
        self.fieldName = fieldName
        self.expectedCardinality = expectedCardinality
        self.compression = compression
    }
}

/// Bitmap compression strategies
public enum BitmapCompression: Sendable, Hashable {
    /// No compression (raw bitmap)
    case none

    /// Run-length encoding (good for sparse bitmaps)
    case runLength

    /// Word-aligned hybrid (WAH) compression
    case wordAligned

    /// Roaring bitmap (hybrid approach, good general choice)
    case roaring
}

// MARK: - Bitmap Data Structure

/// A compressed bitmap for efficient set operations
public struct Bitmap: @unchecked Sendable {

    /// Storage for bitmap data (compressed)
    private var storage: [UInt64]

    /// Total number of bits
    public let bitCount: Int

    /// Compression used
    public let compression: BitmapCompression

    public init(bitCount: Int, compression: BitmapCompression = .runLength) {
        self.bitCount = bitCount
        self.compression = compression
        let wordCount = (bitCount + 63) / 64
        self.storage = [UInt64](repeating: 0, count: wordCount)
    }

    /// Set a bit at position
    public mutating func set(_ position: Int) {
        guard position < bitCount else { return }
        let wordIndex = position / 64
        let bitIndex = position % 64
        storage[wordIndex] |= (1 << bitIndex)
    }

    /// Clear a bit at position
    public mutating func clear(_ position: Int) {
        guard position < bitCount else { return }
        let wordIndex = position / 64
        let bitIndex = position % 64
        storage[wordIndex] &= ~(1 << bitIndex)
    }

    /// Check if bit is set
    public func isSet(_ position: Int) -> Bool {
        guard position < bitCount else { return false }
        let wordIndex = position / 64
        let bitIndex = position % 64
        return (storage[wordIndex] & (1 << bitIndex)) != 0
    }

    /// Count set bits (population count)
    public var popCount: Int {
        storage.reduce(0) { $0 + $1.nonzeroBitCount }
    }

    /// Get all set positions
    public var setPositions: [Int] {
        var positions: [Int] = []
        for (wordIndex, word) in storage.enumerated() {
            if word == 0 { continue }
            for bitIndex in 0..<64 {
                if (word & (1 << bitIndex)) != 0 {
                    let position = wordIndex * 64 + bitIndex
                    if position < bitCount {
                        positions.append(position)
                    }
                }
            }
        }
        return positions
    }

    // MARK: - Set Operations

    /// Bitwise AND
    public func and(_ other: Bitmap) -> Bitmap {
        precondition(bitCount == other.bitCount, "Bitmaps must have same size")
        var result = Bitmap(bitCount: bitCount, compression: compression)
        for i in 0..<storage.count {
            result.storage[i] = storage[i] & other.storage[i]
        }
        return result
    }

    /// Bitwise OR
    public func or(_ other: Bitmap) -> Bitmap {
        precondition(bitCount == other.bitCount, "Bitmaps must have same size")
        var result = Bitmap(bitCount: bitCount, compression: compression)
        for i in 0..<storage.count {
            result.storage[i] = storage[i] | other.storage[i]
        }
        return result
    }

    /// Bitwise NOT
    public func not() -> Bitmap {
        var result = Bitmap(bitCount: bitCount, compression: compression)
        for i in 0..<storage.count {
            result.storage[i] = ~storage[i]
        }
        // Mask out extra bits in last word
        let extraBits = storage.count * 64 - bitCount
        if extraBits > 0 {
            let mask = UInt64.max >> extraBits
            result.storage[storage.count - 1] &= mask
        }
        return result
    }

    /// Bitwise XOR
    public func xor(_ other: Bitmap) -> Bitmap {
        precondition(bitCount == other.bitCount, "Bitmaps must have same size")
        var result = Bitmap(bitCount: bitCount, compression: compression)
        for i in 0..<storage.count {
            result.storage[i] = storage[i] ^ other.storage[i]
        }
        return result
    }

    /// Bitwise AND NOT (A AND (NOT B))
    public func andNot(_ other: Bitmap) -> Bitmap {
        precondition(bitCount == other.bitCount, "Bitmaps must have same size")
        var result = Bitmap(bitCount: bitCount, compression: compression)
        for i in 0..<storage.count {
            result.storage[i] = storage[i] & ~other.storage[i]
        }
        return result
    }
}

// MARK: - Bitmap Index Structure

/// A bitmap index for a single column
public struct BitmapIndexData: @unchecked Sendable {
    /// Index name
    public let indexName: String

    /// Field name
    public let fieldName: String

    /// Total row count
    public let rowCount: Int

    /// Bitmaps per distinct value
    private var valueBitmaps: [String: Bitmap]

    public init(indexName: String, fieldName: String, rowCount: Int) {
        self.indexName = indexName
        self.fieldName = fieldName
        self.rowCount = rowCount
        self.valueBitmaps = [:]
    }

    /// Add a value at row position
    public mutating func add(value: String, at rowPosition: Int) {
        if valueBitmaps[value] == nil {
            valueBitmaps[value] = Bitmap(bitCount: rowCount)
        }
        valueBitmaps[value]?.set(rowPosition)
    }

    /// Get bitmap for a specific value
    public func bitmap(for value: String) -> Bitmap? {
        valueBitmaps[value]
    }

    /// Get all distinct values
    public var distinctValues: [String] {
        Array(valueBitmaps.keys)
    }

    /// Get cardinality
    public var cardinality: Int {
        valueBitmaps.count
    }

    /// Execute an equality condition
    public func evaluate(equals value: String) -> Bitmap? {
        bitmap(for: value)
    }

    /// Execute an IN condition
    public func evaluate(in values: [String]) -> Bitmap? {
        guard !values.isEmpty else { return nil }

        var result = valueBitmaps[values[0]] ?? Bitmap(bitCount: rowCount)
        for value in values.dropFirst() {
            if let bitmap = valueBitmaps[value] {
                result = result.or(bitmap)
            }
        }
        return result
    }

    /// Execute NOT EQUAL condition
    public func evaluate(notEquals value: String) -> Bitmap? {
        guard let bitmap = valueBitmaps[value] else {
            // Value doesn't exist, all rows match
            var all = Bitmap(bitCount: rowCount)
            for i in 0..<rowCount {
                all.set(i)
            }
            return all
        }
        return bitmap.not()
    }
}

// MARK: - Bitmap Scan Operator

/// Operator for bitmap index scan
public struct BitmapScanOperator<T: Persistable>: @unchecked Sendable {
    /// The bitmap index to use
    public let indexName: String

    /// Field being queried
    public let fieldName: String

    /// Operation to perform
    public let operation: BitmapOperation

    /// Estimated matching rows
    public let estimatedRows: Int

    public init(
        indexName: String,
        fieldName: String,
        operation: BitmapOperation,
        estimatedRows: Int
    ) {
        self.indexName = indexName
        self.fieldName = fieldName
        self.operation = operation
        self.estimatedRows = estimatedRows
    }
}

/// Bitmap operations
public enum BitmapOperation: @unchecked Sendable {
    case equals(value: any TupleElement)
    case notEquals(value: any TupleElement)
    case `in`(values: [any TupleElement])
    case notIn(values: [any TupleElement])
}

// MARK: - Bitmap Combine Operator

/// Combines multiple bitmap scans
public struct BitmapCombineOperator<T: Persistable>: @unchecked Sendable {
    /// Child bitmap scans
    public let children: [BitmapScanOperator<T>]

    /// Combine operation
    public let combineOp: BitmapCombineOp

    /// Estimated matching rows
    public let estimatedRows: Int

    public init(
        children: [BitmapScanOperator<T>],
        combineOp: BitmapCombineOp,
        estimatedRows: Int
    ) {
        self.children = children
        self.combineOp = combineOp
        self.estimatedRows = estimatedRows
    }
}

/// How to combine bitmaps
public enum BitmapCombineOp: Sendable {
    case and
    case or
}

// MARK: - Bitmap Index Analyzer

/// Analyzes queries for bitmap index opportunities
public struct BitmapIndexAnalyzer<T: Persistable> {

    private let availableBitmapIndexes: [String: BitmapIndexKind]
    private let statistics: StatisticsProvider

    public init(
        bitmapIndexes: [BitmapIndexKind],
        statistics: StatisticsProvider
    ) {
        var indexMap: [String: BitmapIndexKind] = [:]
        for index in bitmapIndexes {
            indexMap[index.fieldName] = index
        }
        self.availableBitmapIndexes = indexMap
        self.statistics = statistics
    }

    /// Analyze if bitmap indexes can be used
    public func analyze(
        conditions: [any FieldConditionProtocol<T>],
        analysis: QueryAnalysis<T>
    ) -> BitmapIndexAnalysis {
        var usableBitmaps: [UsableBitmapEntry] = []

        for condition in conditions {
            guard let bitmapIndex = availableBitmapIndexes[condition.fieldName] else {
                continue
            }

            // Check if condition type is supported by bitmap
            let isSupported = isBitmapSupportedCondition(condition)
            if isSupported {
                usableBitmaps.append(UsableBitmapEntry(
                    fieldName: condition.fieldName,
                    index: bitmapIndex
                ))
            }
        }

        guard !usableBitmaps.isEmpty else {
            return BitmapIndexAnalysis(
                canUseBitmap: false,
                reason: "No bitmap indexes available for query conditions"
            )
        }

        return BitmapIndexAnalysis(
            canUseBitmap: true,
            usableBitmaps: usableBitmaps,
            reason: "Found \(usableBitmaps.count) usable bitmap index(es)"
        )
    }

    private func isBitmapSupportedCondition(_ condition: any FieldConditionProtocol<T>) -> Bool {
        // Bitmap indexes support equality, IN, and null checks
        return condition.isEquality || condition.isIn || condition.isNullCheck
    }
}

/// A usable bitmap entry
public struct UsableBitmapEntry: Sendable {
    public let fieldName: String
    public let index: BitmapIndexKind

    public init(fieldName: String, index: BitmapIndexKind) {
        self.fieldName = fieldName
        self.index = index
    }
}

/// Result of bitmap index analysis
public struct BitmapIndexAnalysis: Sendable {
    public let canUseBitmap: Bool
    public let usableBitmaps: [UsableBitmapEntry]
    public let reason: String

    public init(
        canUseBitmap: Bool,
        usableBitmaps: [UsableBitmapEntry] = [],
        reason: String
    ) {
        self.canUseBitmap = canUseBitmap
        self.usableBitmaps = usableBitmaps
        self.reason = reason
    }
}

// MARK: - Cost Model Extension

extension CostModel {
    /// Cost weight for bitmap operations (per bitmap word)
    public var bitmapOperationWeight: Double { 0.01 }

    /// Cost weight for bitmap-to-rowid conversion
    public var bitmapToRowIdWeight: Double { 0.1 }

    /// Calculate bitmap scan cost
    public func bitmapScanCost(rowCount: Int, selectivity: Double) -> Double {
        let wordCount = (rowCount + 63) / 64
        let scanCost = Double(wordCount) * bitmapOperationWeight
        let conversionCost = Double(rowCount) * selectivity * bitmapToRowIdWeight
        return scanCost + conversionCost
    }

    /// Calculate bitmap combine cost
    public func bitmapCombineCost(rowCount: Int, numBitmaps: Int) -> Double {
        let wordCount = (rowCount + 63) / 64
        return Double(wordCount) * Double(numBitmaps - 1) * bitmapOperationWeight
    }
}

// MARK: - Bitmap Index Suggester

/// Suggests bitmap indexes for workload
public struct BitmapIndexSuggester<T: Persistable> {

    private let statistics: StatisticsProvider
    private let maxCardinality: Int

    public init(statistics: StatisticsProvider, maxCardinality: Int = 100) {
        self.statistics = statistics
        self.maxCardinality = maxCardinality
    }

    /// Suggest bitmap indexes based on query patterns
    public func suggest(
        frequentConditions: [any FieldConditionProtocol<T>],
        existingIndexes: [IndexDescriptor]
    ) -> [BitmapIndexSuggestion] {
        var suggestions: [BitmapIndexSuggestion] = []

        // Group conditions by field
        var conditionsByField: [String: [any FieldConditionProtocol<T>]] = [:]
        for condition in frequentConditions {
            let field = condition.fieldName
            conditionsByField[field, default: []].append(condition)
        }

        for (fieldName, conditions) in conditionsByField {
            // Check cardinality
            guard let distinctValues = statistics.estimatedDistinctValues(field: fieldName, type: T.self) else {
                continue
            }

            if distinctValues > maxCardinality {
                continue // Too high cardinality
            }

            // Check if already indexed
            let hasIndex = existingIndexes.contains { index in
                index.keyPaths.first.map { T.fieldName(for: $0) == fieldName } ?? false
            }

            if hasIndex {
                continue // Already has efficient index
            }

            // Check condition types
            let equalityCount = conditions.filter { $0.isEquality }.count
            let inCount = conditions.filter { $0.isIn }.count

            if equalityCount + inCount > 0 {
                suggestions.append(BitmapIndexSuggestion(
                    fieldName: fieldName,
                    estimatedCardinality: distinctValues,
                    usageCount: conditions.count,
                    reason: "Low cardinality field (\(distinctValues) values) with \(equalityCount) equality conditions"
                ))
            }
        }

        // Sort by usage count descending
        return suggestions.sorted { $0.usageCount > $1.usageCount }
    }
}

/// Bitmap index suggestion
public struct BitmapIndexSuggestion: Sendable {
    public let fieldName: String
    public let estimatedCardinality: Int
    public let usageCount: Int
    public let reason: String
}

