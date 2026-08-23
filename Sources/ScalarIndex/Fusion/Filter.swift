@_spi(DatabaseExecution) import DatabaseEngine
import DatabaseKit
// Filter.swift
// ScalarIndex - Scalar filter query for Fusion
//
import DatabaseTypes
import StorageKit

// MARK: - FilterError

/// Errors that can occur during filter execution
public enum FilterError: Error, Sendable, Equatable {
    case missingFieldSelection
    case incomparableValues(
        fieldName: String,
        valueType: String,
        boundType: String
    )
    case unorderedFloatingPoint(fieldName: String)
    case missingPersistedField(
        entity: String,
        field: FieldIdentity
    )
    case malformedIndexEntry(
        fieldName: String,
        indexedFieldCount: Int,
        elementCount: Int
    )
}

// MARK: - Filter

/// Scalar filter query for Fusion
///
/// Filters items based on scalar field values using index.
/// All matching items receive a score of 1.0.
///
/// **Usage**:
/// ```swift
/// let results = try await context.fuse(Product.self) {
///     Filter(#field(\Product.category), equals: "electronics")
///     Search(\.description).terms(["wireless"])
/// }
/// .execute()
/// ```
public struct Filter<T: Persistable>: FusionQuery, Sendable {
    public typealias Item = T

    private let queryContext: IndexQueryContext!
    private let field: FieldIdentity?
    private let predicate: FilterPredicate

    private enum FilterPredicate: Sendable {
        case equals(FieldValue)
        case `in`([FieldValue])
        case range(
            min: FieldValue?,
            max: FieldValue?,
            minInclusive: Bool,
            maxInclusive: Bool
        )
        case custom(@Sendable (T) -> Bool)
    }

    private var fieldName: String {
        field?.name ?? ""
    }

    // MARK: - Initialization (FusionContext - Equals)

    /// Create a Filter for equality comparison
    ///
    /// Uses FusionContext.current for context (automatically set by `context.fuse { }`).
    ///
    /// **Usage**:
    /// ```swift
    /// context.fuse(Product.self) {
    ///     Filter(#field(\Product.category), equals: "electronics")
    ///     Search(\.description).terms(["wireless"])
    /// }
    /// ```
    public init<V: FieldValueRepresentable>(
        _ field: Field<T, V>,
        equals value: V
    ) {
        let context = FusionContext.current
        self.field = field.identity
        self.predicate = .equals(value.fieldValue)
        self.queryContext = context
    }

    /// Create a Filter for optional field equality
    public init<V: FieldValueRepresentable>(
        _ field: Field<T, V?>,
        equals value: V
    ) {
        let context = FusionContext.current
        self.field = field.identity
        self.predicate = .equals(value.fieldValue)
        self.queryContext = context
    }

    // MARK: - Initialization (FusionContext - In)

    /// Create a Filter for set membership
    public init<V: FieldValueRepresentable>(
        _ field: Field<T, V>,
        in values: [V]
    ) {
        let context = FusionContext.current
        self.field = field.identity
        self.predicate = .in(values.map { $0.fieldValue })
        self.queryContext = context
    }

    // MARK: - Initialization (FusionContext - Range)

    /// Create a Filter for range comparison
    public init<V: FieldValueRepresentable & Comparable>(
        _ field: Field<T, V>,
        range: ClosedRange<V>
    ) {
        let context = FusionContext.current
        self.field = field.identity
        self.predicate = .range(
            min: range.lowerBound.fieldValue,
            max: range.upperBound.fieldValue,
            minInclusive: true,
            maxInclusive: true
        )
        self.queryContext = context
    }

    /// Create a Filter for half-open range
    public init<V: FieldValueRepresentable & Comparable>(
        _ field: Field<T, V>,
        range: Range<V>
    ) {
        let context = FusionContext.current
        self.field = field.identity
        self.predicate = .range(
            min: range.lowerBound.fieldValue,
            max: range.upperBound.fieldValue,
            minInclusive: true,
            maxInclusive: false
        )
        self.queryContext = context
    }

    /// Create a Filter for greater than
    public init<V: FieldValueRepresentable & Comparable>(
        _ field: Field<T, V>,
        greaterThan value: V
    ) {
        let context = FusionContext.current
        self.field = field.identity
        self.predicate = .range(
            min: value.fieldValue,
            max: nil,
            minInclusive: false,
            maxInclusive: false
        )
        self.queryContext = context
    }

    /// Create a Filter for greater than or equal
    public init<V: FieldValueRepresentable & Comparable>(
        _ field: Field<T, V>,
        greaterThanOrEqual value: V
    ) {
        let context = FusionContext.current
        self.field = field.identity
        self.predicate = .range(
            min: value.fieldValue,
            max: nil,
            minInclusive: true,
            maxInclusive: false
        )
        self.queryContext = context
    }

    /// Create a Filter for less than
    public init<V: FieldValueRepresentable & Comparable>(
        _ field: Field<T, V>,
        lessThan value: V
    ) {
        let context = FusionContext.current
        self.field = field.identity
        self.predicate = .range(
            min: nil,
            max: value.fieldValue,
            minInclusive: false,
            maxInclusive: false
        )
        self.queryContext = context
    }

    /// Create a Filter for less than or equal
    public init<V: FieldValueRepresentable & Comparable>(
        _ field: Field<T, V>,
        lessThanOrEqual value: V
    ) {
        let context = FusionContext.current
        self.field = field.identity
        self.predicate = .range(
            min: nil,
            max: value.fieldValue,
            minInclusive: false,
            maxInclusive: true
        )
        self.queryContext = context
    }

    // MARK: - Initialization (FusionContext - Custom)

    /// Create a Filter with custom predicate
    public init(_ predicate: @escaping @Sendable (T) -> Bool) {
        let context = FusionContext.current
        self.field = nil
        self.predicate = .custom(predicate)
        self.queryContext = context
    }

    // MARK: - Initialization (Explicit Context - Equals)

    /// Create a Filter for equality comparison with explicit context
    public init<V: FieldValueRepresentable>(
        _ field: Field<T, V>,
        equals value: V,
        context: IndexQueryContext
    ) {
        self.field = field.identity
        self.predicate = .equals(value.fieldValue)
        self.queryContext = context
    }

    /// Create a Filter for optional field equality with explicit context
    public init<V: FieldValueRepresentable>(
        _ field: Field<T, V?>,
        equals value: V,
        context: IndexQueryContext
    ) {
        self.field = field.identity
        self.predicate = .equals(value.fieldValue)
        self.queryContext = context
    }

    // MARK: - Initialization (Explicit Context - In)

    /// Create a Filter for set membership with explicit context
    public init<V: FieldValueRepresentable>(
        _ field: Field<T, V>,
        in values: [V],
        context: IndexQueryContext
    ) {
        self.field = field.identity
        self.predicate = .in(values.map { $0.fieldValue })
        self.queryContext = context
    }

    // MARK: - Initialization (Explicit Context - Range)

    /// Create a Filter for range comparison with explicit context
    public init<V: FieldValueRepresentable & Comparable>(
        _ field: Field<T, V>,
        range: ClosedRange<V>,
        context: IndexQueryContext
    ) {
        self.field = field.identity
        self.predicate = .range(
            min: range.lowerBound.fieldValue,
            max: range.upperBound.fieldValue,
            minInclusive: true,
            maxInclusive: true
        )
        self.queryContext = context
    }

    /// Create a Filter for half-open range with explicit context
    public init<V: FieldValueRepresentable & Comparable>(
        _ field: Field<T, V>,
        range: Range<V>,
        context: IndexQueryContext
    ) {
        self.field = field.identity
        self.predicate = .range(
            min: range.lowerBound.fieldValue,
            max: range.upperBound.fieldValue,
            minInclusive: true,
            maxInclusive: false
        )
        self.queryContext = context
    }

    /// Create a Filter for greater than with explicit context
    public init<V: FieldValueRepresentable & Comparable>(
        _ field: Field<T, V>,
        greaterThan value: V,
        context: IndexQueryContext
    ) {
        self.field = field.identity
        self.predicate = .range(
            min: value.fieldValue,
            max: nil,
            minInclusive: false,
            maxInclusive: false
        )
        self.queryContext = context
    }

    /// Create a Filter for greater than or equal with explicit context
    public init<V: FieldValueRepresentable & Comparable>(
        _ field: Field<T, V>,
        greaterThanOrEqual value: V,
        context: IndexQueryContext
    ) {
        self.field = field.identity
        self.predicate = .range(
            min: value.fieldValue,
            max: nil,
            minInclusive: true,
            maxInclusive: false
        )
        self.queryContext = context
    }

    /// Create a Filter for less than with explicit context
    public init<V: FieldValueRepresentable & Comparable>(
        _ field: Field<T, V>,
        lessThan value: V,
        context: IndexQueryContext
    ) {
        self.field = field.identity
        self.predicate = .range(
            min: nil,
            max: value.fieldValue,
            minInclusive: false,
            maxInclusive: false
        )
        self.queryContext = context
    }

    /// Create a Filter for less than or equal with explicit context
    public init<V: FieldValueRepresentable & Comparable>(
        _ field: Field<T, V>,
        lessThanOrEqual value: V,
        context: IndexQueryContext
    ) {
        self.field = field.identity
        self.predicate = .range(
            min: nil,
            max: value.fieldValue,
            minInclusive: false,
            maxInclusive: true
        )
        self.queryContext = context
    }

    // MARK: - Initialization (Explicit Context - Custom)

    /// Create a Filter with custom predicate and explicit context
    public init(_ predicate: @escaping @Sendable (T) -> Bool, context: IndexQueryContext) {
        self.field = nil
        self.predicate = .custom(predicate)
        self.queryContext = context
    }

    // MARK: - Index Discovery

    /// Find the index descriptor that can efficiently answer this query
    ///
    /// For scalar indexes, only the leftmost field can be used for efficient
    /// equality/range queries. This follows B-tree index semantics:
    ///
    /// - Composite index `[a, b, c]` has key structure: `[a][b][c][primaryKey]`
    /// - Efficient queries: `a` alone, `(a, b)`, or `(a, b, c)` (left-to-right)
    /// - Inefficient queries: `b` alone, `c` alone (requires full index scan)
    ///
    /// **Reference**: "Database System Concepts" (Silberschatz) - Chapter 14.3
    // MARK: - FusionQuery

    public var fusionQueryPlan: FusionQueryPlan<T> {
        guard let queryContext else {
            return FusionQueryPlan(
                configurationError: .invalidConfiguration(
                    "Filter requires an IndexQueryContext or context.fuse"
                )
            )
        }
        return FusionQueryPlan(
            context: queryContext,
            authorization: IndexReadAuthorization(
                limit: nil,
                offset: nil,
                orderBy: nil
            ),
            fieldNames: field.map { Set([$0.name]) } ?? Set(T.allFields),
            operation: { [self] candidates, execution in
                try await executeBound(
                    candidates: candidates,
                    execution: execution
                )
            }
        )
    }

    private func executeBound(
        candidates: Set<T.ID>?,
        execution: ReadExecutionContext
    ) async throws -> FusionQueryResult<T> {
        if case .in(let values) = predicate, values.isEmpty {
            return try FusionQueryResultBuilder<T>(
                execution: execution
            ).finish()
        }
        let rows = try await queryContext.context
            .executeRetainedCanonicalQueryRows(
                SelectQuery(
                    projection: .all,
                    source: .table(TableRef(table: T.persistableType)),
                    filter: try canonicalPredicate()
                ),
                execution: execution,
                graphPartitions: queryContext.partitionValues
            )
        try rows.validateWorkMeter(
            execution.workMeter,
            sourceName: "Scalar Fusion Filter"
        )
        var output = try FusionQueryResultBuilder<T>(
            execution: execution,
            expectedCount: min(rows.count, candidates?.count ?? rows.count)
        )
        for index in 0..<rows.count {
            try execution.workMeter.consume(at: .filterEvaluation)
            let retainedRow = rows.row(at: index)
            try output.appendDecodedRow(retainedRow, score: 1.0) { item in
                if let candidates, !candidates.contains(item.id) {
                    return false
                }
                if case .custom(let predicate) = self.predicate {
                    return predicate(item)
                }
                return true
            }
        }
        try execution.workMeter.consume(
            UInt64(output.count),
            at: .sortInput
        )
        return try output.finish()
    }

    /// Builds the canonical SQL predicate so scalar-index selection, fallback,
    /// authorization, and typed comparison failures remain owned by the single
    /// relational planner.
    private func canonicalPredicate() throws -> Expression? {
        let column = Expression.column(ColumnRef(column: fieldName))
        switch predicate {
        case .equals(let value):
            return .equal(column, .literal(try value.toLiteral()))
        case .in(let values):
            return .inList(
                column,
                values: try values.map {
                    .literal(try $0.toLiteral())
                }
            )
        case .range(
            let minimum,
            let maximum,
            let minimumInclusive,
            let maximumInclusive
        ):
            let lower = try minimum.map { value in
                minimumInclusive
                    ? Expression.greaterThanOrEqual(
                        column,
                        .literal(try value.toLiteral())
                    )
                    : Expression.greaterThan(
                        column,
                        .literal(try value.toLiteral())
                    )
            }
            let upper = try maximum.map { value in
                maximumInclusive
                    ? Expression.lessThanOrEqual(
                        column,
                        .literal(try value.toLiteral())
                    )
                    : Expression.lessThan(
                        column,
                        .literal(try value.toLiteral())
                    )
            }
            switch (lower, upper) {
            case (.some(let lower), .some(let upper)):
                return .and(lower, upper)
            case (.some(let lower), .none):
                return lower
            case (.none, .some(let upper)):
                return upper
            case (.none, .none):
                return nil
            }
        case .custom:
            return nil
        }
    }

}
