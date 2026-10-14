import DatabaseKit
import DatabaseTypes
import StorageKit

/// Opaque continuation produced and consumed by the engine read pipeline.
public struct QueryContinuation: Sendable, Hashable {
    public let bytes: ByteString

    public init(_ bytes: ByteString) {
        self.bytes = bytes
    }
}

/// Consistency policy requested for a read execution.
public enum ReadConsistency: Sendable, Hashable {
    case serializable
    case snapshot
}

/// Container-internal execution options for a canonical read.
public struct ReadExecutionOptions: Sendable, Hashable {
    public let consistency: ReadConsistency?
    public let pageSize: Int?
    public let continuation: QueryContinuation?
    public let budget: ExecutionBudget
    public let continuationScope: ByteString
    /// Indicates that the caller has pinned every page to the same immutable
    /// storage read point. Callers without such a lease keep
    /// result-fingerprint validation enabled.
    package let continuationSnapshotIsStable: Bool
    /// Controls only the client-facing page window. Nested SQL sources disable
    /// this while retaining their own logical LIMIT/OFFSET and the request's
    /// shared work and memory budgets.
    package let appliesExternalPageWindow: Bool

    public init(
        consistency: ReadConsistency? = nil,
        pageSize: Int? = nil,
        continuation: QueryContinuation? = nil,
        budget: ExecutionBudget = ExecutionBudget(),
        continuationScope: ByteString = [],
        continuationSnapshotIsStable: Bool = false
    ) {
        self.init(
            consistency: consistency,
            pageSize: pageSize,
            continuation: continuation,
            budget: budget,
            continuationScope: continuationScope,
            continuationSnapshotIsStable: continuationSnapshotIsStable,
            appliesExternalPageWindow: true
        )
    }

    private init(
        consistency: ReadConsistency?,
        pageSize: Int?,
        continuation: QueryContinuation?,
        budget: ExecutionBudget,
        continuationScope: ByteString,
        continuationSnapshotIsStable: Bool,
        appliesExternalPageWindow: Bool
    ) {
        self.consistency = consistency
        self.pageSize = pageSize
        self.continuation = continuation
        self.budget = budget
        self.continuationScope = continuationScope
        self.continuationSnapshotIsStable = continuationSnapshotIsStable
        self.appliesExternalPageWindow = appliesExternalPageWindow
    }

    package func withoutExternalPageWindow() -> Self {
        Self(
            consistency: consistency,
            pageSize: nil,
            continuation: nil,
            budget: budget,
            continuationScope: continuationScope,
            continuationSnapshotIsStable: false,
            appliesExternalPageWindow: false
        )
    }

    public static var `default`: ReadExecutionOptions {
        ReadExecutionOptions()
    }
}

/// Request-scoped state for one canonical read execution.
public struct ReadExecutionContext: Sendable {
    public let options: ReadExecutionOptions
    public let workMeter: DatabaseWorkMeter
    public let queryStructuralLimits: QueryStructuralLimits

    public init(
        options: ReadExecutionOptions = .default,
        monotonicClock: any StorageMonotonicClock,
        workMeter: DatabaseWorkMeter? = nil,
        queryStructuralLimits: QueryStructuralLimits = .default
    ) {
        self.options = options
        self.workMeter = workMeter ?? DatabaseWorkMeter(
            budget: options.budget,
            monotonicClock: monotonicClock
        )
        self.queryStructuralLimits = queryStructuralLimits
    }

    public var consistency: ReadConsistency? { options.consistency }
    public var continuation: QueryContinuation? { options.continuation }

    package func withoutExternalPageWindow() -> Self {
        Self(
            options: options.withoutExternalPageWindow(),
            workMeter: workMeter,
            queryStructuralLimits: queryStructuralLimits
        )
    }

    private init(
        options: ReadExecutionOptions,
        workMeter: DatabaseWorkMeter,
        queryStructuralLimits: QueryStructuralLimits
    ) {
        self.options = options
        self.workMeter = workMeter
        self.queryStructuralLimits = queryStructuralLimits
    }

    public func resolvePageSize() throws -> Int? {
        guard options.appliesExternalPageWindow else {
            return nil
        }
        if let pageSize = options.pageSize {
            return pageSize
        }
        guard let maximumRows = Int(exactly: options.budget.maximumRows) else {
            throw CanonicalReadError.unsupportedSelectQuery(
                "The row budget cannot be represented on the current platform"
            )
        }
        return maximumRows
    }
}

/// Opaque entity version used by optimistic concurrency checks.
public struct PersistableVersionToken: Sendable, Hashable {
    public let value: String

    public init(_ value: String) {
        self.value = value
    }
}

/// A caller-owned positional row interpreted only with an explicit column schema.
public struct QueryRow: Sendable, Hashable {
    public let values: [QueryValue]
    public let annotations: [String: FieldValue]
    public let version: PersistableVersionToken?

    public init(
        values: [QueryValue],
        columns: [QueryColumn],
        annotations: [String: FieldValue] = [:],
        version: PersistableVersionToken? = nil
    ) throws {
        self.init(validatedValues: values, annotations: annotations, version: version)
        try validate(columns: columns)
    }

    /// The canonical owner has already validated arity, type and nullability
    /// against its retained relation schema before admitted construction.
    package init(
        validatedValues: [QueryValue],
        annotations: [String: FieldValue] = [:],
        version: PersistableVersionToken? = nil
    ) {
        self.values = validatedValues
        self.annotations = annotations
        self.version = version
    }

    public func validate(columns: [QueryColumn]) throws {
        try QueryColumn.validate(columns)
        guard values.count == columns.count else {
            throw QueryRowCodecError.arityMismatch(expected: columns.count, actual: values.count)
        }
        for (column, value) in zip(columns, values) {
            guard QueryValueValidator.accepts(value, type: column.type) else {
                throw QueryRowCodecError.incompatibleValue(column: column.number)
            }
            guard column.isNullable || !value.isNull else {
                throw QueryRowCodecError.nullInRequiredColumn(column: column.number)
            }
        }
    }

    public func namedValues(columns: [QueryColumn]) throws -> [String: QueryValue] {
        try validate(columns: columns)
        var result: [String: QueryValue] = [:]
        result.reserveCapacity(columns.count)
        for (column, value) in zip(columns, values) {
            guard result.updateValue(value, forKey: column.name) == nil else {
                throw QueryRowCodecError.duplicateColumn(column.name)
            }
        }
        return result
    }

    public func primitiveFields(columns: [QueryColumn]) throws -> [String: FieldValue] {
        let named = try namedValues(columns: columns)
        var result: [String: FieldValue] = [:]
        result.reserveCapacity(named.count)
        for (name, value) in named {
            guard case .primitive(let primitive) = value else {
                throw QueryRowCodecError.richValueRequiresDeclaredAdapter(field: name)
            }
            result[name] = primitive
        }
        return result
    }
}

/// A final caller-owned result preserving its schema even when it has no rows.
public struct QueryResponse: Sendable {
    public let columns: [QueryColumn]
    public let rows: [QueryRow]
    public let continuation: QueryContinuation?
    public let metadata: [String: FieldValue]
    public let affectedRows: Int?

    public init(
        columns: [QueryColumn],
        rows: [QueryRow] = [],
        continuation: QueryContinuation? = nil,
        metadata: [String: FieldValue] = [:],
        affectedRows: Int? = nil
    ) throws {
        try QueryColumn.validate(columns)
        for row in rows { try row.validate(columns: columns) }
        self.columns = columns
        self.rows = rows
        self.continuation = continuation
        self.metadata = metadata
        self.affectedRows = affectedRows
    }
}
