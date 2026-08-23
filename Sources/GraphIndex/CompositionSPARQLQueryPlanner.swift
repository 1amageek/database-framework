#if DATABASE_MULTI_BASE
@_spi(DatabaseExecution) import DatabaseEngine
import DatabaseKit
import DatabaseTypes
import StorageKit

private struct CompositionSPARQLMemberQueryExecutor:
    CompositionMemberQueryExecutor
{
    let sourceExecutor: any SPARQLSourceExecutor
    let graphPartitions: FieldObject

    func validate(_ query: SelectQuery) throws {
        try CompositionSPARQLPlanValidator.validate(query)
    }

    func admitLogicalRead(
        context: DatabaseContext,
        query: SelectQuery,
        restrictingTo entityNames: Set<String>?
    ) throws -> DatabaseReadAuthorizationAdmission {
        let resolution = try RDFDatasetReadResolver.resolve(
            schema: context.container.schema
        )
        let selectedEntities = entityNames ?? Set(
            resolution.map { [$0.entity.name] } ?? []
        )
        return try context.admitLogicalRead(
            listAuthorization: try IndexReadAuthorization(
                sparqlSelectQuery: query
            ),
            fieldPlan: .rdfDataset(schema: context.container.schema),
            restrictingTo: selectedEntities
        )
    }

    func execute(
        context: DatabaseContext,
        query: SelectQuery,
        execution: ReadExecutionContext,
        transaction: any TransactionReadAccess
    ) async throws -> DatabaseRetainedQueryResponse {
        try await sourceExecutor.executeInTransaction(
            context: context,
            selectQuery: query,
            options: execution,
            partitions: graphPartitions,
            transaction: transaction
        )
    }

    func prepare(
        _ row: DatabaseRetainedQueryRow,
        sourceBaseID: Base.ID
    ) throws -> DatabaseEngine.QueryRow {
        try CompositionRDFIdentity.qualifyBlankNodes(
            in: row.materializeForRetainedTransfer(),
            baseID: sourceBaseID
        )
    }
}

/// Owns Base-local SPARQL SELECT validation and execution for a Composition.
/// DatabaseEngine supplies only the domain-neutral global row merger.
@_spi(DatabaseExecution)
public struct CompositionSPARQLQueryPlanner: Sendable {
    private let structuralLimits: QueryStructuralLimits

    public init(structuralLimits: QueryStructuralLimits) {
        self.structuralLimits = structuralLimits
    }

    public func execute(
        _ query: SelectQuery,
        source: CompositionDataSource,
        graphPartitions: FieldObject,
        pageSize: Int,
        readContext: ReadExecutionContext,
        emit: @Sendable @escaping (
            CompositionQueryEvent
        ) async throws -> Bool
    ) async throws {
        // Reject unsupported SPARQL semantics before runtime capability
        // lookup so plan errors do not depend on adapter registration.
        try CompositionSPARQLPlanValidator.validate(query)
        guard let sourceExecutor = source.container.runtimeConfiguration
            .logicalSourceExecutors.sparqlExecutor else {
            throw CanonicalReadError.unsupportedSource(
                "SPARQL source executor is not registered"
            )
        }
        try await CompositionQueryPlanner(
            structuralLimits: structuralLimits
        ).executeBaseLocal(
            query,
            source: source,
            options: CompositionQueryExecutionOptions(
                pageSize: pageSize,
                readContext: readContext
            ),
            memberExecutor: CompositionSPARQLMemberQueryExecutor(
                sourceExecutor: sourceExecutor,
                graphPartitions: graphPartitions
            ),
            emit: emit
        )
    }
}
#endif
