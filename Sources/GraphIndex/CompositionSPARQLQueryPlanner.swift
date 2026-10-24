#if DATABASE_MULTI_BASE
@_spi(DatabaseExecution) import DatabaseEngine
import DatabaseKit
import DatabaseTypes
import StorageKit

private struct CompositionSPARQLMemberQueryExecutor:
    CompositionMemberQueryExecutor
{
    let graphPartitions: FieldObject

    func validate(_ query: SelectQuery) throws {
        try CompositionSPARQLPlanValidator.validate(query)
    }

    func columns(session: DatabaseReadSession, query: SelectQuery) throws -> DatabaseRetainedQueryColumns {
        try session.boundQueryColumns(query)
    }

    func execute(
        session: DatabaseReadSession,
        query: SelectQuery,
        execution: ReadExecutionContext
    ) async throws -> DatabaseRetainedQueryPage {
        let authorizedSession = try session.admittingRDFDatasetRead()
        return try await authorizedSession.retainedCanonicalPage(
            query,
            execution: execution,
            graphPartitions: graphPartitions
        )
    }

    func preparedFootprint(
        of row: borrowing DatabaseEngine.ExecutionQueryRow,
        sourceBaseID: Base.ID,
        workMeter: DatabaseWorkMeter
    ) throws -> (rows: UInt64, bytes: UInt64) {
        let footprint = try CanonicalRelationalFootprintMeter.queryRowAliasFootprint(
            of: row,
            prefixingRDFBlankNodeIdentifiersWith:
                CompositionRDFIdentity.qualificationPrefix(
                    baseID: sourceBaseID
                ),
            workMeter: workMeter,
            stage: .resultMaterialization
        )
        return (footprint.rows, footprint.bytes)
    }

    func observedPreparedFootprint(
        of row: borrowing DatabaseEngine.ExecutionQueryRow,
        workMeter: DatabaseWorkMeter
    ) throws -> (rows: UInt64, bytes: UInt64) {
        let footprint = try CanonicalRelationalFootprintMeter.queryRowAliasFootprint(
            of: row, prefixingRDFBlankNodeIdentifiersWith: "",
            workMeter: workMeter, stage: .resultMaterialization)
        return (footprint.rows, footprint.bytes)
    }

    func prepare(
        _ row: DatabaseEngine.ExecutionQueryRow,
        columns: [QueryColumn],
        sourceBaseID: Base.ID
    ) throws -> DatabaseEngine.ExecutionQueryRow {
        try CompositionRDFIdentity.qualifyBlankNodes(
            in: row,
            columns: columns,
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
        guard source.container.runtimeConfiguration.logicalSourceExecutors
            .sparqlExecutor != nil else {
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
                graphPartitions: graphPartitions
            ),
            emit: emit
        )
    }
}
#endif
