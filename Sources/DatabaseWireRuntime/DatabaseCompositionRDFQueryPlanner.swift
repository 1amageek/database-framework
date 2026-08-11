#if DATABASE_WIRE_RUNTIME_MULTIPLE_BASES
#if DATABASE_WIRE_RUNTIME_GRAPH_INDEXES
import DatabaseEngine
import DatabaseKit
import DatabaseTypes
@_spi(DatabaseWireRuntime) import DatabaseWire
import StorageKit

/// Executes Base-local RDF graph forms and builds one exact federated union.
package struct DatabaseCompositionRDFQueryPlanner: Sendable {
    package enum Statement: Sendable {
        case construct(ConstructQuery)
        case describe(DescribeQuery)

        var queryStatement: QueryStatement {
            switch self {
            case .construct(let query): .construct(query)
            case .describe(let query): .describe(query)
            }
        }
    }

    private let structuralLimits: QueryStructuralLimits

    package init(structuralLimits: QueryStructuralLimits) {
        self.structuralLimits = structuralLimits
    }

    package func execute(
        _ statement: Statement,
        request: QueryExecuteOperation.Request,
        context: DatabaseOperationContext,
        workMeter: DatabaseWorkMeter,
        queryFingerprint: ByteString,
        snapshotStore: DatabaseQuerySnapshotStore
    ) async throws -> RDFGraphPage {
        switch statement {
        case .construct(let query):
            try DatabaseCompositionSPARQLPlanValidator.validate(query)
        case .describe(let query):
            try DatabaseCompositionSPARQLPlanValidator.validate(query)
        }
        guard request.page.limit <= request.budget.maximumRows,
              let pageLimit = Int(exactly: request.page.limit) else {
            throw DatabaseGraphQueryError.pageLimitExceedsMaximum(
                requested: request.page.limit,
                maximum: request.budget.maximumRows
            )
        }
        guard let executor = context.executor.runtimeConfiguration
            .logicalSourceExecutors.sparqlExecutor else {
            throw CanonicalReadError.unsupportedSource(
                "SPARQL source executor is not registered"
            )
        }
        let source = try context.requireCompositionExecutor()
        return try await source.withReadSnapshot { snapshot in
            let record = snapshot.lease.record
            let reservation = try await snapshotStore.beginWrite(
                compositionID: record.composition.id,
                compositionGeneration: record.generation,
                schemaGeneration: context.executor.schemaGeneration,
                queryFingerprint: queryFingerprint,
                authorization: context.authorization
            )
            do {
                let spill = DatabaseCompositionRDFDistinctSpill(
                    snapshotStore: snapshotStore,
                    reservation: reservation,
                    maximumIntermediateBytes: request.budget
                        .maximumIntermediateBytes,
                    workMeter: workMeter
                )
                let options = ReadExecutionContext(
                    options: ReadExecutionOptions(budget: request.budget),
                    monotonicClock: context.executor.monotonicClock,
                    workMeter: workMeter,
                    queryStructuralLimits: structuralLimits
                )
                for member in snapshot.lease.members {
                    let transaction = try snapshot.transaction(for: member)
                    try await source.withMemberContext(
                        member,
                        in: snapshot
                    ) { databaseContext in
                        let graph: DatabaseRetainedRDFGraph
                        switch statement {
                        case .construct(let query):
                            graph = try await executor
                                .executeConstructInTransaction(
                                    context: databaseContext,
                                    constructQuery: query,
                                    nodeNamespace: try GraphResultNodeNamespace(
                                        queryFingerprint
                                    ),
                                    options: options,
                                    partitions: request.graphPartitions,
                                    transaction: transaction
                                )
                        case .describe(let query):
                            graph = try await executor
                                .executeDescribeInTransaction(
                                    context: databaseContext,
                                    describeQuery: query,
                                    options: options,
                                    partitions: request.graphPartitions,
                                    transaction: transaction
                                )
                        }
                        for index in 0..<graph.count {
                            let quad = try graph.withElement(at: index) {
                                quad in
                                try DatabaseCompositionRDFIdentity
                                    .qualifyBlankNodes(
                                        in: copy quad,
                                        baseID: member.baseID
                                    )
                            }
                            try await spill.insert(
                                quad,
                                origin: .source(member.baseID)
                            )
                        }
                    }
                }

                let builder = try DatabaseCompositionRDFResultBuilder(
                    compositionID: record.composition.id,
                    compositionGeneration: record.generation,
                    baseIDs: record.composition.bases,
                    consistency: .federated(try await snapshot.readPoints()),
                    pageLimit: pageLimit,
                    maximumIntermediateBytes: request.budget
                        .maximumIntermediateBytes,
                    snapshotStore: snapshotStore,
                    reservation: reservation,
                    initialPayloadBytes: await spill.payloadByteCount,
                    workMeter: workMeter
                )
                do {
                    try await spill.forEachResult(batchSize: 64) { result in
                        try await builder.append(
                            result.quad,
                            origin: result.origin
                        )
                        return true
                    }
                    return try await builder.finish()
                } catch {
                    let operationError = error
                    do {
                        try await builder.abort()
                    } catch {
                        throw StorageTransactionCleanupError(
                            operationError: operationError,
                            cancellationError: error
                        )
                    }
                    throw operationError
                }
            } catch {
                let operationError = error
                do {
                    try await snapshotStore.abortWrite(reservation)
                } catch {
                    throw StorageTransactionCleanupError(
                        operationError: operationError,
                        cancellationError: error
                    )
                }
                throw operationError
            }
        }
    }
}
#endif

#endif
