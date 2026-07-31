#if DATABASE_SERVER_GRAPH_INDEXES
import DatabaseEngine
import DatabaseTypes
@_spi(DatabaseServer) import DatabaseWire
import DatabaseKit
import StorageKit

struct DatabaseGraphQueryService: Sendable {
    private let wireLimits: DatabaseWireLimits
    private let queryStructuralLimits: QueryStructuralLimits

    init(
        wireLimits: DatabaseWireLimits = .default,
        queryStructuralLimits: QueryStructuralLimits = .default
    ) {
        self.wireLimits = wireLimits
        self.queryStructuralLimits = queryStructuralLimits
    }

    func executeConstruct(
        _ query: ConstructQuery,
        request: QueryExecuteOperation.Request,
        context: DatabaseOperationContext,
        workMeter: DatabaseWorkMeter
    ) async throws -> RDFGraphPage {
        try await execute(
            kind: .construct,
            statement: .construct(query),
            request: request,
            context: context,
            workMeter: workMeter
        ) { transaction, requestFingerprint in
            let executor = try sparqlExecutor(context: context)
            return try await executor.executeConstructInTransaction(
                context: context.container.newContext(),
                constructQuery: query,
                resultScope: try DatabaseGraphResultScope(
                    requestFingerprint
                ),
                options: readExecution(
                    for: request,
                    workMeter: workMeter,
                    context: context
                ),
                partitions: request.graphPartitions,
                transaction: transaction
            )
        }
    }

    func executeDescribe(
        _ query: DescribeQuery,
        request: QueryExecuteOperation.Request,
        context: DatabaseOperationContext,
        workMeter: DatabaseWorkMeter
    ) async throws -> RDFGraphPage {
        try await execute(
            kind: .describe,
            statement: .describe(query),
            request: request,
            context: context,
            workMeter: workMeter
        ) { transaction, _ in
            let executor = try sparqlExecutor(context: context)
            return try await executor.executeDescribeInTransaction(
                context: context.container.newContext(),
                describeQuery: query,
                options: readExecution(
                    for: request,
                    workMeter: workMeter,
                    context: context
                ),
                partitions: request.graphPartitions,
                transaction: transaction
            )
        }
    }

    private func execute(
        kind: DatabaseGraphQueryPageCursor.Kind,
        statement: QueryStatement,
        request: QueryExecuteOperation.Request,
        context: DatabaseOperationContext,
        workMeter: DatabaseWorkMeter,
        materialize: @escaping @Sendable (
            _ transaction: any TransactionAccess,
            _ requestFingerprint: ByteString
        ) async throws -> DatabaseRetainedRDFGraph
    ) async throws -> RDFGraphPage {
        guard request.page.limit <= request.budget.maximumRows else {
            throw DatabaseGraphQueryError.pageLimitExceedsMaximum(
                requested: request.page.limit,
                maximum: request.budget.maximumRows
            )
        }
        let requestFingerprint = try fingerprint(
            statement: statement,
            request: request
        )
        let cursor = try request.page.continuation.map {
            try DatabaseGraphQueryPageCursor.decode($0, limits: wireLimits)
        }
        guard cursor?.kind == nil || cursor?.kind == kind else {
            throw DatabaseGraphQueryError.invalidContinuation
        }
        guard cursor?.requestFingerprint == nil
                || cursor?.requestFingerprint == requestFingerprint else {
            throw DatabaseGraphQueryError.continuationDoesNotMatchRequest
        }

        do {
            return try await context.container.transactionExecutor.withTransaction(
                configuration: .default,
                clock: context.container.monotonicClock
            ) { transaction in
                if let cursor {
                    do {
                        try transaction.setReadVersion(cursor.snapshotVersion)
                    } catch let error as StorageError
                            where error.code == .unsupportedOperation
                                && !transaction.capabilities
                                    .historicalReadVersion {
                        throw DatabaseGraphQueryError
                            .continuationSnapshotChanged
                    }
                }
                let readVersion = try await transaction.getReadVersion()
                guard cursor?.snapshotVersion == nil
                        || cursor?.snapshotVersion == readVersion else {
                    throw DatabaseGraphQueryError.continuationSnapshotChanged
                }

                var graph = try await materialize(
                    transaction,
                    requestFingerprint
                )
                graph = try canonicalize(
                    consume graph,
                    workMeter: workMeter
                )
                let resultFingerprint = try DatabaseGraphQueryResultFingerprint
                    .compute(
                        graph: graph,
                        wireLimits: wireLimits,
                        workMeter: workMeter
                    )
                guard cursor?.resultFingerprint == nil
                        || cursor?.resultFingerprint == resultFingerprint else {
                    throw DatabaseGraphQueryError
                        .continuationSnapshotChanged
                }

                let offsetValue = cursor?.tripleOffset ?? 0
                guard let offset = Int(exactly: offsetValue),
                      offset <= graph.count,
                      cursor == nil || offset < graph.count else {
                    throw DatabaseGraphQueryError
                        .continuationOffsetOutOfRange(
                            offset: offsetValue,
                            count: graph.count
                        )
                }
                guard let pageLimit = Int(exactly: request.page.limit) else {
                    throw DatabaseGraphQueryError
                        .pageLimitExceedsPlatformCapacity(
                            requested: request.page.limit
                        )
                }
                let end = offset + min(pageLimit, graph.count - offset)
                guard let emittedRows = UInt32(exactly: end - offset) else {
                    throw DatabaseGraphQueryError
                        .pageLimitExceedsPlatformCapacity(
                            requested: request.page.limit
                        )
                }
                try workMeter.recordOutputRows(emittedRows)

                let continuation: ByteString?
                if end < graph.count {
                    continuation = try DatabaseGraphQueryPageCursor(
                        kind: kind,
                        requestFingerprint: requestFingerprint,
                        snapshotVersion: readVersion,
                        resultFingerprint: resultFingerprint,
                        tripleOffset: UInt64(end)
                    ).encode(limits: wireLimits)
                } else {
                    continuation = nil
                }
                let page = graph.promotePage(offset..<end)
                return RDFGraphPage(
                    quads: consume page,
                    continuation: continuation,
                    snapshotVersion: readVersion
                )
            }
        } catch let error as DatabaseGraphQueryError {
            throw error
        } catch let error as StorageError where cursor != nil
                && (error.code == .transactionConflict
                    || error.code == .transactionTooOld
                    || error.code == .transactionFutureVersion) {
            throw DatabaseGraphQueryError.continuationSnapshotChanged
        }
    }

    private func canonicalize(
        _ graph: consuming DatabaseRetainedRDFGraph,
        workMeter: DatabaseWorkMeter
    ) throws -> DatabaseRetainedRDFGraph {
        try workMeter.consume(UInt64(graph.count), at: .sortInput)
        let sorted = try graph.sorting { lhs, rhs in
            try workMeter.consume(2, at: .sortComparison)
            return Self.isCanonicalPredecessor(lhs, rhs)
        }
        return try sorted.removingAdjacentDuplicates { lhs, rhs in
            try workMeter.consume(at: .deduplication)
            return lhs == rhs
        }
    }

    private static func isCanonicalPredecessor(
        _ lhs: borrowing RDFQuad,
        _ rhs: borrowing RDFQuad
    ) -> Bool {
        if lhs.subject != rhs.subject {
            return lhs.subject < rhs.subject
        }
        if lhs.predicate != rhs.predicate {
            return lhs.predicate < rhs.predicate
        }
        if lhs.object != rhs.object {
            return lhs.object < rhs.object
        }
        switch (lhs.graph, rhs.graph) {
        case (.none, .some):
            return true
        case (.some, .none), (.none, .none):
            return false
        case (.some(let left), .some(let right)):
            return left < right
        }
    }

    private func fingerprint(
        statement: QueryStatement,
        request: QueryExecuteOperation.Request
    ) throws -> ByteString {
        let normalized = QueryExecuteOperation.Request(
            input: .ir(statement),
            graphPartitions: request.graphPartitions,
            page: QueryExecuteOperation.Page(
                limit: request.page.limit,
                continuation: nil
            ),
            budget: request.budget
        )
        let payload = try DatabaseWireEncoder(
            limits: wireLimits
        ).encodeRequestPayload(
            DatabaseOperations.queryExecute,
            request: normalized
        )
        return DatabaseRequestDigest.compute(
            operation: .queryExecute,
            prefix: [0x47, 0x51, 0x01],
            payload: payload
        )
    }

    private func readExecution(
        for request: QueryExecuteOperation.Request,
        workMeter: DatabaseWorkMeter,
        context: DatabaseOperationContext
    ) -> ReadExecutionContext {
        ReadExecutionContext(
            options: ReadExecutionOptions(budget: request.budget),
            monotonicClock: context.container.monotonicClock,
            workMeter: workMeter,
            queryStructuralLimits: queryStructuralLimits
        )
    }

    private func sparqlExecutor(
        context: DatabaseOperationContext
    ) throws -> any SPARQLSourceExecutor {
        guard let executor = context.container.runtimeConfiguration
            .logicalSourceExecutors.sparqlExecutor else {
            throw CanonicalReadError.unsupportedSource(
                "SPARQL source executor is not registered"
            )
        }
        return executor
    }
}

#endif
