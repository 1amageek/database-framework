// FusionQuery.swift
// DatabaseEngine - Protocol for fusion-compatible queries

import DatabaseKit
import DatabaseTypes

/// Protocol for queries that can participate in fusion operations
///
/// Conforming types can be combined using `FusionBuilder` to create
/// hybrid search queries that merge results from multiple sources.
///
/// **Design Principle**:
/// Each Index module (VectorIndex, FullTextIndex, etc.) provides its own
/// FusionQuery implementation. DatabaseEngine does not know about specific
/// index types - it only knows this protocol.
///
/// **Implementing a FusionQuery** (in Index module):
/// ```swift
/// // In VectorIndex/Fusion/Similar.swift
/// public struct Similar<T: Persistable>: FusionQuery {
///     private let queryContext: IndexQueryContext
///     // ... other properties
///
///     public func execute(candidates: Set<T.ID>?) async throws -> [ScoredResult<T>] {
///         // Use IndexDescriptor to find index name
///         guard let descriptor = findIndexDescriptor() else { throw ... }
///         // Execute query using queryContext
///     }
/// }
/// ```
///
/// **Usage in Fusion**:
/// ```swift
/// let results = try await context.fuse(Product.self) {
///     Search(\.description).terms(["coffee"])
///     Similar(\.embedding, dimensions: 384).query(vector, k: 100)
/// }
/// .execute()
/// ```
public struct FusionQueryPlan<Item: Persistable>: Sendable {
    public typealias Operation = @Sendable (
        Set<Item.ID>?,
        ReadExecutionContext
    ) async throws -> FusionQueryResult<Item>

    private let queryContext: IndexQueryContext?
    private let authorization: IndexReadAuthorization?
    private let fieldReadAuthorization: FieldReadAuthorization?
    private let operation: Operation
    private let configurationError: FusionQueryError?

    package var executionContext: IndexQueryContext? { queryContext }

    /// Creates a context-free source. Use this only when the operation does
    /// not perform database reads.
    public init(operation: @escaping Operation) {
        self.queryContext = nil
        self.authorization = nil
        self.fieldReadAuthorization = nil
        self.operation = operation
        self.configurationError = nil
    }

    /// Creates a database-backed source whose complete execution is bound to
    /// one read transaction. Nested index and model reads reuse that snapshot.
    public init(
        context: IndexQueryContext,
        authorization: IndexReadAuthorization,
        indexDescriptor: @escaping @Sendable () throws -> IndexDescriptor,
        operation: @escaping Operation
    ) {
        self.queryContext = context
        self.authorization = authorization
        self.fieldReadAuthorization = .index(indexDescriptor)
        self.operation = operation
        self.configurationError = nil
    }

    /// Creates a database-backed source that reads explicit model fields
    /// without consulting an index descriptor.
    public init(
        context: IndexQueryContext,
        authorization: IndexReadAuthorization,
        fieldNames: Set<String>,
        operation: @escaping Operation
    ) {
        self.queryContext = context
        self.authorization = authorization
        self.fieldReadAuthorization = .fields(fieldNames)
        self.operation = operation
        self.configurationError = nil
    }

    package init(configurationError: FusionQueryError) {
        self.queryContext = nil
        self.authorization = nil
        self.fieldReadAuthorization = nil
        self.operation = { _, _ in throw configurationError }
        self.configurationError = configurationError
    }

    public func execute(
        candidates: Set<Item.ID>?,
        execution: ReadExecutionContext
    ) async throws -> FusionQueryResult<Item> {
        if let configurationError { throw configurationError }
        guard let queryContext else {
            let result = try await operation(candidates, execution)
            try result.validateWorkMeter(execution.workMeter)
            return result
        }
        let canonicalRead = CanonicalReadExecution.resolve(
            requested: execution.options.consistency,
            default: .snapshot
        )
        guard let authorization else {
            throw FusionQueryError.invalidConfiguration(
                "A database-backed Fusion source requires exact LIST authorization"
            )
        }
        return try await queryContext.context.withDataOperation {
            let fieldPlan = try fieldReadPlan(using: queryContext)
            let admission = try queryContext.context.admitLogicalRead(
                listAuthorization: authorization,
                fieldPlan: fieldPlan,
                restrictingTo: [Item.persistableType]
            )
            return try await queryContext.context
                .withReadAuthorizationAdmission(admission) {
                    try await queryContext.withReadSnapshot(
                        configuration: canonicalRead.transactionConfiguration
                    ) {
                        let result = try await operation(
                            candidates,
                            execution
                        )
                        try result.validateWorkMeter(execution.workMeter)
                        return result
                    }
                }
        }
    }

    private func fieldReadPlan(
        using queryContext: IndexQueryContext
    ) throws -> DatabaseFieldReadAuthorizationPlan {
        guard let entity = queryContext.schema.entity(
            named: Item.persistableType
        ) else {
            throw FusionQueryError.invalidConfiguration(
                "Entity '\(Item.persistableType)' is not present in the active schema"
            )
        }
        let additionalFields: Set<String>
        switch fieldReadAuthorization {
        case .index(let resolve):
            let descriptor = try resolve()
            additionalFields = Set(descriptor.fieldNames).union(
                descriptor.includedFieldNames
            )
        case .fields(let names):
            additionalFields = names
        case nil:
            additionalFields = []
        }
        return .fullEntity(
            entity,
            including: additionalFields
        )
    }

    private enum FieldReadAuthorization: Sendable {
        case index(@Sendable () throws -> IndexDescriptor)
        case fields(Set<String>)
    }
}

public protocol FusionQuery<Item>: Sendable {
    /// The item type this query returns
    associatedtype Item: Persistable

    /// Execute the query and return scored results
    ///
    /// - Parameter candidates: Optional set of candidate IDs to restrict results to.
    ///                         When provided, the query should only return items whose
    ///                         identifier is in this set. This enables pipeline
    ///                         optimization where later stages only search within
    ///                         candidates from earlier stages.
    /// - Returns: Request-accounted scored results, sorted by score descending.
    ///            Scores should be normalized to [0, 1] where higher is better.
    var fusionQueryPlan: FusionQueryPlan<Item> { get }
}

public extension FusionQuery {
    func execute(
        candidates: Set<Item.ID>?,
        execution: ReadExecutionContext
    ) async throws -> FusionQueryResult<Item> {
        try await fusionQueryPlan.execute(
            candidates: candidates,
            execution: execution
        )
    }
}

/// Error type for FusionQuery implementations
public enum FusionQueryError: Error, CustomStringConvertible, Sendable {
    /// Index not found for the specified field
    case indexNotFound(
        entity: String, field: String,
        indexType: IndexType
    )

    /// Query not properly configured
    case invalidConfiguration(String)

    /// A reranking stage was invoked without a candidate set from an earlier
    /// source stage. This differs from an explicitly empty candidate set.
    case missingCandidates(stage: String)

    /// A custom source or stage returned ownership charged to another request.
    case workMeterMismatch

    /// A candidate selected by a prior physical source no longer has a
    /// persisted entity on the same read snapshot.
    case danglingCandidate(entity: String, primaryKey: ByteString)

    public var description: String {
        switch self {
        case .indexNotFound(let entity, let field, let indexType):
            return "No \(indexType.diagnosticName) index found for field "
                + "'\(field)' on entity '\(entity)'"
        case .invalidConfiguration(let reason):
            return "Invalid query configuration: \(reason)"
        case .missingCandidates(let stage):
            return "Fusion stage '\(stage)' requires candidates from an earlier stage"
        case .workMeterMismatch:
            return "Fusion output belongs to a different execution work meter"
        case .danglingCandidate(let entity, let primaryKey):
            return "Fusion candidate for entity '\(entity)' references missing item \(primaryKey)"
        }
    }
}
