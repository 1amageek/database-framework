import DatabaseKit
import DatabaseTypes
import DatabaseWire
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
    /// storage read point. Only server-owned historical snapshots may enable
    /// this; local callers keep result-fingerprint validation.
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

/// Canonical row produced by the engine before transport projection.
public struct QueryRow: Sendable, Hashable {
    public let fields: [String: FieldValue]
    public let annotations: [String: FieldValue]
    public let version: PersistableVersionToken?

    public init(
        fields: [String: FieldValue],
        annotations: [String: FieldValue] = [:],
        version: PersistableVersionToken? = nil
    ) {
        self.fields = fields
        self.annotations = annotations
        self.version = version
    }
}

/// Canonical engine result before it is encoded as a DatabaseWire operation response.
public struct QueryResponse: Sendable {
    public let rows: [QueryRow]
    public let continuation: QueryContinuation?
    public let metadata: [String: FieldValue]
    public let affectedRows: Int?

    public init(
        rows: [QueryRow] = [],
        continuation: QueryContinuation? = nil,
        metadata: [String: FieldValue] = [:],
        affectedRows: Int? = nil
    ) {
        self.rows = rows
        self.continuation = continuation
        self.metadata = metadata
        self.affectedRows = affectedRows
    }
}
