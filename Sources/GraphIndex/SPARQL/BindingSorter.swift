// BindingSorter.swift
// GraphIndex - ORDER BY sorting for SPARQL query results
//
// Sorts VariableBinding arrays by multiple keys with configurable direction
// and null ordering. Follows SPARQL 1.1 Section 15 ordering semantics.
//
// Reference: W3C SPARQL 1.1 Query Language, Section 15 (Solution Sequences and Modifiers)

import DatabaseKit
import DatabaseTypes
import DatabaseEngine

/// A single ORDER BY sort key for VariableBinding sorting
///
/// Encapsulates the evaluation function, sort direction, and null ordering
/// for one component of a multi-key sort.
///
/// **Usage**:
/// ```swift
/// // Simple variable sort
/// let key = BindingSortKey.variable("?name")
///
/// // Descending with nulls last
/// let key = BindingSortKey.variable("?age", ascending: false, nullsLast: true)
///
/// // Custom evaluation
/// let key = BindingSortKey(ascending: true) { binding in binding["?score"] }
/// ```
public struct BindingSortKey: Sendable {

    /// Evaluates a binding to produce the sort value
    public let evaluate: @Sendable (VariableBinding) throws -> FieldValue?

    /// Sort direction: true = ascending (ASC), false = descending (DESC)
    public let ascending: Bool

    /// Whether null/unbound values sort last (true) or first (false)
    ///
    /// SPARQL 1.1 default: nulls sort as smallest (first in ASC, last in DESC).
    /// This property overrides the default behavior.
    public let nullsLast: Bool

    // MARK: - Initialization

    /// Create a sort key with a custom evaluation function
    ///
    /// - Parameters:
    ///   - ascending: Sort direction (default: true = ASC)
    ///   - nullsLast: Whether nulls sort last (default: false = nulls sort first)
    ///   - evaluate: Function to extract the sort value from a binding
    public init(
        ascending: Bool = true,
        nullsLast: Bool = false,
        evaluate: @escaping @Sendable (VariableBinding) throws -> FieldValue?
    ) {
        self.ascending = ascending
        self.nullsLast = nullsLast
        self.evaluate = evaluate
    }

    // MARK: - Convenience Constructors

    /// Create a sort key from a variable name
    ///
    /// - Parameters:
    ///   - name: Variable name (e.g., "?person")
    ///   - ascending: Sort direction (default: true = ASC)
    ///   - nullsLast: Whether nulls sort last (default: false)
    /// - Returns: A BindingSortKey that extracts the named variable's value
    public static func variable(
        _ name: String,
        ascending: Bool = true,
        nullsLast: Bool = false
    ) -> BindingSortKey {
        BindingSortKey(
            ascending: ascending,
            nullsLast: nullsLast,
            evaluate: { binding in binding[name] }
        )
    }
}

/// Sorts VariableBinding arrays by multiple sort keys
///
/// Implements multi-key sorting following SPARQL 1.1 ORDER BY semantics:
/// 1. Compare by first key; if equal, compare by second key, etc.
/// 2. Null/unbound values are ordered according to `nullsLast` setting.
/// 3. Incomparable types use `FieldValue.Comparable` type ordering.
///
/// **Reference**: W3C SPARQL 1.1 Query Language, Section 15.1
public struct BindingSorter: Sendable {

    private struct DecoratedBinding: Sendable {
        let originalIndex: Int
        let keyOffset: Int
        let fingerprint: ByteString
    }

    private struct ScratchPlan: Sendable {
        let keyStorageCount: Int
        let retainedByteCount: UInt64
    }

    private static let fingerprintStorageByteCount: UInt64 = 32

    // Swift does not expose native Array buffer-header capacity. Charge a
    // conservative fixed overhead for each of the four owned scratch buffers
    // in addition to their stride-based element storage.
    private static let arrayStorageOverheadByteCount: UInt64 = 64
    private static let scratchArrayCount: UInt64 = 4

    /// Sort bindings by multiple keys
    ///
    /// - Parameters:
    ///   - bindings: The bindings to sort
    ///   - keys: Ordered list of sort keys (primary key first)
    /// - Returns: Sorted array of bindings
    public static func sort(
        _ bindings: consuming [VariableBinding],
        by keys: [BindingSortKey],
        workMeter: DatabaseWorkMeter
    ) throws -> [VariableBinding] {
        try workMeter.consume(UInt64(bindings.count), at: .sortInput)
        guard !keys.isEmpty, bindings.count > 1 else {
            return consume bindings
        }

        let scratchPlan = try scratchPlan(
            bindingCount: bindings.count,
            keyCount: keys.count,
            workMeter: workMeter
        )
        let scratchReservation = try workMeter.reserveIntermediate(
            rows: UInt64(bindings.count),
            bytes: scratchPlan.retainedByteCount,
            at: .sortInput
        )
        defer { scratchReservation.release() }

        var ownedBindings = consume bindings
        try decorateSortAndReorder(
            &ownedBindings,
            keys: keys,
            keyStorageCount: scratchPlan.keyStorageCount,
            workMeter: workMeter
        )
        return ownedBindings
    }

    private static func decorateSortAndReorder(
        _ ownedBindings: inout [VariableBinding],
        keys: [BindingSortKey],
        keyStorageCount: Int,
        workMeter: DatabaseWorkMeter
    ) throws {
        var evaluatedKeys: [FieldValue?] = []
        evaluatedKeys.reserveCapacity(keyStorageCount)
        var decorated: [DecoratedBinding] = []
        decorated.reserveCapacity(ownedBindings.count)
        for (index, binding) in ownedBindings.enumerated() {
            let keyOffset = evaluatedKeys.count
            for key in keys {
                evaluatedKeys.append(try key.evaluate(binding))
            }
            decorated.append(
                DecoratedBinding(
                    originalIndex: index,
                    keyOffset: keyOffset,
                    fingerprint: try binding.canonicalFingerprint(
                        workMeter: workMeter
                    )
                )
            )
        }

        try decorated.sort { lhsItem, rhsItem in
            for keyIndex in keys.indices {
                let key = keys[keyIndex]
                try workMeter.consume(2, at: .sortComparison)
                let lVal = evaluatedKeys[lhsItem.keyOffset + keyIndex]
                let rVal = evaluatedKeys[rhsItem.keyOffset + keyIndex]

                let result = try compareValues(
                    lVal,
                    rVal,
                    nullsLast: key.nullsLast
                )

                switch result {
                case .same:
                    continue // Tie on this key, try next
                case .ascending:
                    return key.ascending
                case .descending:
                    return !key.ascending
                }
            }
            try workMeter.consume(2, at: .sortComparison)
            return lhsItem.fingerprint.lexicographicallyPrecedes(
                rhsItem.fingerprint
            )
        }

        reorder(&ownedBindings, accordingTo: decorated)
    }

    package static func sort(
        _ bindings: consuming [VariableBinding],
        by keys: [SPARQLOrderKeyPlan],
        workMeter: DatabaseWorkMeter,
        evaluate: @Sendable (
            SPARQLExpressionPlan,
            VariableBinding
        ) async throws -> FieldValue?
    ) async throws -> [VariableBinding] {
        try workMeter.consume(UInt64(bindings.count), at: .sortInput)
        guard !keys.isEmpty, bindings.count > 1 else {
            return consume bindings
        }

        let scratchPlan = try scratchPlan(
            bindingCount: bindings.count,
            keyCount: keys.count,
            workMeter: workMeter
        )
        let scratchReservation = try workMeter.reserveIntermediate(
            rows: UInt64(bindings.count),
            bytes: scratchPlan.retainedByteCount,
            at: .sortInput
        )
        defer { scratchReservation.release() }

        var ownedBindings = consume bindings
        try await decorateSortAndReorder(
            &ownedBindings,
            keys: keys,
            keyStorageCount: scratchPlan.keyStorageCount,
            workMeter: workMeter,
            evaluate: evaluate
        )
        return ownedBindings
    }

    /// Sorts a retained relation while preserving its request-scoped owner.
    /// Unique input is reordered in place. Shared input is first admitted into
    /// a unique row-header buffer because cache storage is immutable.
    static func sort(
        _ bindings: consuming SPARQLRetainedBindings,
        by keys: [SPARQLOrderKeyPlan],
        workMeter: DatabaseWorkMeter,
        evaluate: @Sendable (
            SPARQLExpressionPlan,
            VariableBinding
        ) async throws -> FieldValue?
    ) async throws -> SPARQLRetainedBindings {
        let bindingCount = bindings.count
        try workMeter.consume(UInt64(bindingCount), at: .sortInput)
        guard !keys.isEmpty, bindingCount > 1 else {
            return consume bindings
        }

        let normalizedBuilder = try SPARQLRetainedBindingBuilder.resuming(
            consume bindings,
            workMeter: workMeter,
            stage: .sortInput
        )
        let normalized = normalizedBuilder.finish()
        let scratchPlan = try scratchPlan(
            bindingCount: bindingCount,
            keyCount: keys.count,
            workMeter: workMeter
        )
        let scratchReservation = try workMeter.reserveIntermediate(
            rows: UInt64(bindingCount),
            bytes: scratchPlan.retainedByteCount,
            at: .sortInput
        )
        defer { scratchReservation.release() }

        var evaluatedKeys: [FieldValue?] = []
        evaluatedKeys.reserveCapacity(scratchPlan.keyStorageCount)
        var decorated: [DecoratedBinding] = []
        decorated.reserveCapacity(bindingCount)
        for index in 0..<bindingCount {
            let keyOffset = evaluatedKeys.count
            let fingerprint = try await normalized.withElement(
                at: index
            ) { binding in
                for key in keys {
                    evaluatedKeys.append(
                        try await evaluate(
                            key.expression,
                            copy binding
                        )
                    )
                }
                return try binding.canonicalFingerprint(
                    workMeter: workMeter
                )
            }
            decorated.append(
                DecoratedBinding(
                    originalIndex: index,
                    keyOffset: keyOffset,
                    fingerprint: fingerprint
                )
            )
        }

        try decorated.sort { lhsItem, rhsItem in
            for keyIndex in keys.indices {
                let key = keys[keyIndex]
                try workMeter.consume(2, at: .sortComparison)
                let lhs = evaluatedKeys[lhsItem.keyOffset + keyIndex]
                let rhs = evaluatedKeys[rhsItem.keyOffset + keyIndex]
                switch try compareValues(
                    lhs,
                    rhs,
                    nullsLast: key.nullsLast
                ) {
                case .same:
                    continue
                case .ascending:
                    return key.ascending
                case .descending:
                    return !key.ascending
                }
            }
            try workMeter.consume(2, at: .sortComparison)
            return lhsItem.fingerprint.lexicographicallyPrecedes(
                rhsItem.fingerprint
            )
        }

        return (consume normalized).reorderingUniqueElements { destination in
            decorated[destination].originalIndex
        }
    }

    private static func decorateSortAndReorder(
        _ ownedBindings: inout [VariableBinding],
        keys: [SPARQLOrderKeyPlan],
        keyStorageCount: Int,
        workMeter: DatabaseWorkMeter,
        evaluate: @Sendable (
            SPARQLExpressionPlan,
            VariableBinding
        ) async throws -> FieldValue?
    ) async throws {
        var evaluatedKeys: [FieldValue?] = []
        evaluatedKeys.reserveCapacity(keyStorageCount)
        var decorated: [DecoratedBinding] = []
        decorated.reserveCapacity(ownedBindings.count)
        for (index, binding) in ownedBindings.enumerated() {
            let keyOffset = evaluatedKeys.count
            for key in keys {
                evaluatedKeys.append(
                    try await evaluate(key.expression, binding)
                )
            }
            decorated.append(
                DecoratedBinding(
                    originalIndex: index,
                    keyOffset: keyOffset,
                    fingerprint: try binding.canonicalFingerprint(
                        workMeter: workMeter
                    )
                )
            )
        }

        try decorated.sort { lhsItem, rhsItem in
            for keyIndex in keys.indices {
                let key = keys[keyIndex]
                try workMeter.consume(2, at: .sortComparison)
                let lhs = evaluatedKeys[lhsItem.keyOffset + keyIndex]
                let rhs = evaluatedKeys[rhsItem.keyOffset + keyIndex]
                switch try compareValues(
                    lhs,
                    rhs,
                    nullsLast: key.nullsLast
                ) {
                case .same:
                    continue
                case .ascending:
                    return key.ascending
                case .descending:
                    return !key.ascending
                }
            }
            try workMeter.consume(2, at: .sortComparison)
            return lhsItem.fingerprint.lexicographicallyPrecedes(
                rhsItem.fingerprint
            )
        }

        reorder(&ownedBindings, accordingTo: decorated)
    }

    // MARK: - Private

    /// Compare two optional FieldValues with null handling
    ///
    /// - Parameters:
    ///   - lhs: Left value (nil = unbound/null)
    ///   - rhs: Right value (nil = unbound/null)
    ///   - nullsLast: Whether nulls sort after all non-null values
    /// - Returns: Comparison result
    private static func compareValues(
        _ lhs: FieldValue?,
        _ rhs: FieldValue?,
        nullsLast: Bool
    ) throws -> SPARQLComparisonOrder {
        switch (lhs, rhs) {
        case (.none, .none):
            return .same
        case (.none, .some):
            return nullsLast ? .descending : .ascending
        case (.some, .none):
            return nullsLast ? .ascending : .descending
        case (.some(.null), .some(.null)):
            return .same
        case (.some(.null), .some):
            return nullsLast ? .descending : .ascending
        case (.some, .some(.null)):
            return nullsLast ? .ascending : .descending
        case (.some(let l), .some(let r)):
            return try SPARQLTermOrdering.compare(l, r)
        }
    }

    private static func reorder(
        _ bindings: inout [VariableBinding],
        accordingTo decorated: [DecoratedBinding]
    ) {
        var originalAtPosition = Array(bindings.indices)
        var positionOfOriginal = Array(bindings.indices)
        for destination in bindings.indices {
            let desiredOriginal = decorated[destination].originalIndex
            let currentPosition = positionOfOriginal[desiredOriginal]
            guard currentPosition != destination else { continue }

            let displacedOriginal = originalAtPosition[destination]
            bindings.swapAt(destination, currentPosition)
            originalAtPosition.swapAt(destination, currentPosition)
            positionOfOriginal[desiredOriginal] = destination
            positionOfOriginal[displacedOriginal] = currentPosition
        }
    }

    private static func scratchPlan(
        bindingCount: Int,
        keyCount: Int,
        workMeter: DatabaseWorkMeter
    ) throws -> ScratchPlan {
        let (keyStorageCount, keyCountOverflow) = bindingCount
            .multipliedReportingOverflow(by: keyCount)
        guard !keyCountOverflow else {
            throw scratchByteOverflow(workMeter: workMeter)
        }

        let rowCount = UInt64(bindingCount)
        let keySlotCount = UInt64(keyStorageCount)
        var retainedByteCount: UInt64 = 0
        retainedByteCount = try checkedAdd(
            retainedByteCount,
            checkedMultiply(
                keySlotCount,
                UInt64(MemoryLayout<FieldValue?>.stride),
                workMeter: workMeter
            ),
            workMeter: workMeter
        )
        retainedByteCount = try checkedAdd(
            retainedByteCount,
            checkedMultiply(
                rowCount,
                UInt64(MemoryLayout<DecoratedBinding>.stride),
                workMeter: workMeter
            ),
            workMeter: workMeter
        )
        retainedByteCount = try checkedAdd(
            retainedByteCount,
            checkedMultiply(
                rowCount,
                fingerprintStorageByteCount
                    + arrayStorageOverheadByteCount,
                workMeter: workMeter
            ),
            workMeter: workMeter
        )
        let reorderSlotCount = try checkedMultiply(
            rowCount,
            2,
            workMeter: workMeter
        )
        retainedByteCount = try checkedAdd(
            retainedByteCount,
            checkedMultiply(
                reorderSlotCount,
                UInt64(MemoryLayout<Int>.stride),
                workMeter: workMeter
            ),
            workMeter: workMeter
        )
        retainedByteCount = try checkedAdd(
            retainedByteCount,
            checkedMultiply(
                scratchArrayCount,
                arrayStorageOverheadByteCount,
                workMeter: workMeter
            ),
            workMeter: workMeter
        )
        return ScratchPlan(
            keyStorageCount: keyStorageCount,
            retainedByteCount: retainedByteCount
        )
    }

    private static func checkedAdd(
        _ left: UInt64,
        _ right: UInt64,
        workMeter: DatabaseWorkMeter
    ) throws -> UInt64 {
        let (result, overflow) = left.addingReportingOverflow(right)
        guard !overflow else {
            throw scratchByteOverflow(workMeter: workMeter)
        }
        return result
    }

    private static func checkedMultiply(
        _ left: UInt64,
        _ right: UInt64,
        workMeter: DatabaseWorkMeter
    ) throws -> UInt64 {
        let (result, overflow) = left.multipliedReportingOverflow(by: right)
        guard !overflow else {
            throw scratchByteOverflow(workMeter: workMeter)
        }
        return result
    }

    private static func scratchByteOverflow(
        workMeter: DatabaseWorkMeter
    ) -> DatabaseWorkLimitError {
        DatabaseWorkLimitError.maximumIntermediateBytes(
            stage: .sortInput,
            consumed: workMeter.retainedIntermediateBytes,
            requested: UInt64.max,
            maximum: workMeter.budget.maximumIntermediateBytes
        )
    }
}
