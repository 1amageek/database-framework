import DatabaseMaintenanceOperations
import DatabaseSchemaOperations
import DatabaseJobRuntime
import DatabaseGraphOperations
import DatabaseMutationOperations
import DatabaseQueryOperations
import DatabaseCommandOperations
import DatabaseOperationCore
#if DATABASE_OPERATIONS_GRAPH_INDEXES
import DatabaseEngine
import DatabaseTypes
@_spi(DatabaseOperations) import DatabaseWire
import DatabaseKit
import StorageKit

struct DatabaseGraphQueryService: Sendable {
    private let wireLimits: DatabaseWireLimits
    private let queryStructuralLimits: QueryStructuralLimits
    private let querySnapshotStore: DatabaseQuerySnapshotStore?

    init(
        wireLimits: DatabaseWireLimits = .default,
        queryStructuralLimits: QueryStructuralLimits = .default,
        querySnapshotStore: DatabaseQuerySnapshotStore? = nil
    ) {
        self.wireLimits = wireLimits
        self.queryStructuralLimits = queryStructuralLimits
        self.querySnapshotStore = querySnapshotStore
    }

    func executeConstruct(
        _ query: ConstructQuery,
        request: QueryExecuteOperation.Request,
        context: DatabaseOperationContext,
        workMeter: DatabaseWorkMeter
    ) async throws -> RDFGraphPage {
        let databaseContext = try context.requireDataContext()
        return try await execute(
            kind: .construct,
            statement: .construct(query),
            request: request,
            context: context,
            workMeter: workMeter
        ) { transaction, requestFingerprint in
            let executor = try sparqlExecutor(context: context)
            return try await executor.executeConstructInTransaction(
                context: databaseContext,
                constructQuery: query,
                nodeNamespace: try GraphResultNodeNamespace(
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
        let databaseContext = try context.requireDataContext()
        return try await execute(
            kind: .describe,
            statement: .describe(query),
            request: request,
            context: context,
            workMeter: workMeter
        ) { transaction, _ in
            let executor = try sparqlExecutor(context: context)
            return try await executor.executeDescribeInTransaction(
                context: databaseContext,
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
        let databaseContext = try context.requireDataContext()
        let lease = try databaseContext.requireOperationDataRoot()
        let queryFingerprint = try DatabaseQuerySnapshotStore.queryFingerprint(
            statement: statement,
            request: request,
            limits: wireLimits
        )
        if let continuation = request.page.continuation,
           continuation.count == DatabaseQuerySnapshotStore
            .continuationByteCount,
           continuation.first == DatabaseQuerySnapshotStore
            .continuationMarker {
            guard let querySnapshotStore else {
                throw DatabaseQueryExecutionError.invalidContinuation
            }
            let page = try await querySnapshotStore.loadRDFGraph(
                continuation: continuation,
                resource: lease.resource,
                dataGeneration: lease.generation,
                schemaGeneration: context.executor.schemaGeneration,
                queryFingerprint: queryFingerprint,
                authorization: context.authorization
            )
            try Self.recordOutput(page, workMeter: workMeter)
            return page
        }
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
        guard cursor?.resource == nil || cursor?.resource == lease.resource,
              cursor?.dataGeneration == nil
                || cursor?.dataGeneration == lease.generation
        else {
            throw DatabaseGraphQueryError.invalidContinuation
        }

        do {
            return try await databaseContext.executeCanonicalRead {
                transaction in
                if let restorableReadPosition = cursor?
                    .restorableReadPosition {
                    do {
                        guard try DatabaseTransactionReadPoint.restore(
                            restorableReadPosition,
                            transaction: transaction
                        ) else {
                            throw DatabaseGraphQueryError
                                .continuationSnapshotChanged
                        }
                    } catch let error as StorageError
                            where error.code == .unsupportedOperation
                                && !transaction.capabilities
                                    .historicalReadVersion {
                        throw DatabaseGraphQueryError
                            .continuationSnapshotChanged
                    }
                }
                let readPoint = try await DatabaseTransactionReadPoint.capture(
                    domainID: lease.domain.id.value,
                    transaction: transaction
                )
                if case .version(let expectedVersion)? = cursor?
                    .restorableReadPosition {
                    guard readPoint.position == .version(expectedVersion) else {
                        throw DatabaseGraphQueryError.continuationSnapshotChanged
                    }
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

                guard let pageLimit = Int(exactly: request.page.limit) else {
                    throw DatabaseGraphQueryError
                        .pageLimitExceedsPlatformCapacity(
                            requested: request.page.limit
                        )
                }
                if cursor == nil,
                   !transaction.capabilities.historicalReadVersion,
                   graph.count > pageLimit {
                    return try await spoolNonHistoricalGraph(
                        consume graph,
                        pageLimit: pageLimit,
                        queryFingerprint: queryFingerprint,
                        request: request,
                        context: context,
                        lease: lease,
                        transaction: transaction,
                        readPoint: readPoint,
                        workMeter: workMeter
                    )
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
                        resource: lease.resource,
                        dataGeneration: lease.generation,
                        requestFingerprint: requestFingerprint,
                        restorableReadPosition: transaction.capabilities
                            .historicalReadVersion
                            ? readPoint.position
                            : nil,
                        resultFingerprint: resultFingerprint,
                        tripleOffset: UInt64(end)
                    ).encode(limits: wireLimits)
                } else {
                    continuation = nil
                }
                let page = graph.promotePage(offset..<end)
                return try RDFGraphPage(
                    quads: consume page,
                    continuation: continuation,
                    provenance: nil,
                    consistency: .transactional(readPoint)
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

    private func spoolNonHistoricalGraph(
        _ graph: consuming DatabaseRetainedRDFGraph,
        pageLimit: Int,
        queryFingerprint: ByteString,
        request: QueryExecuteOperation.Request,
        context: DatabaseOperationContext,
        lease: DatabaseDataRootLease,
        transaction: any TransactionAccess,
        readPoint: DomainReadPoint,
        workMeter: DatabaseWorkMeter
    ) async throws -> RDFGraphPage {
        guard let querySnapshotStore else {
            throw DatabaseQueryExecutionError.querySnapshotUnavailable(
                "the host did not provide durable query snapshot storage"
            )
        }
        let snapshotTransaction = querySnapshotStore.controlWriteTransaction(
            for: lease,
            active: transaction
        )
        let reservation = try await querySnapshotStore.beginWrite(
            resource: lease.resource,
            dataGeneration: lease.generation,
            schemaGeneration: context.executor.schemaGeneration,
            queryFingerprint: queryFingerprint,
            authorization: context.authorization,
            transaction: snapshotTransaction
        )
        do {
            let firstContinuationPageID = try await querySnapshotStore
                .reservePage(
                    in: reservation,
                    transaction: snapshotTransaction
                )
            var currentPageID = firstContinuationPageID
            var continuationPageCount: UInt32 = 0
            var totalPayloadBytes: UInt64 = 0
            var offset = pageLimit

            while offset < graph.count {
                let remaining = graph.count - offset
                let end = remaining <= pageLimit
                    ? graph.count
                    : offset + pageLimit
                let nextPageID = end < graph.count
                    ? try await querySnapshotStore.reservePage(
                        in: reservation,
                        transaction: snapshotTransaction
                    )
                    : nil
                let page = try makePage(
                    graph,
                    range: offset..<end,
                    continuation: nextPageID.map {
                        reservation.continuation(pageID: $0)
                    },
                    consistency: .transactional(readPoint)
                )
                totalPayloadBytes = try await querySnapshotStore.appendPage(
                    page,
                    pageID: currentPageID,
                    to: reservation,
                    consumedPayloadBytes: totalPayloadBytes,
                    maximumIntermediateBytes: request.budget
                        .maximumIntermediateBytes,
                    transaction: snapshotTransaction
                )
                let nextCount = continuationPageCount
                    .addingReportingOverflow(1)
                guard !nextCount.overflow else {
                    throw DatabaseQueryExecutionError.querySnapshotCorrupted
                }
                continuationPageCount = nextCount.partialValue
                offset = end
                if let nextPageID {
                    currentPageID = nextPageID
                }
            }

            try await querySnapshotStore.commitWrite(
                reservation,
                pageCount: continuationPageCount,
                totalPayloadBytes: totalPayloadBytes,
                transaction: snapshotTransaction
            )
            let firstPage = try makePage(
                graph,
                range: 0..<pageLimit,
                continuation: reservation.continuation(
                    pageID: firstContinuationPageID
                ),
                consistency: .transactional(readPoint)
            )
            try Self.recordOutput(firstPage, workMeter: workMeter)
            return firstPage
        } catch {
            let operationError = error
            guard snapshotTransaction == nil else {
                throw operationError
            }
            do {
                try await querySnapshotStore.abortWrite(reservation)
            } catch {
                throw StorageTransactionCleanupError(
                    operationError: operationError,
                    cancellationError: error
                )
            }
            throw operationError
        }
    }

    private func makePage(
        _ graph: borrowing DatabaseRetainedRDFGraph,
        range: Range<Int>,
        continuation: ByteString?,
        consistency: DatabaseKit.DatabaseReadConsistency
    ) throws -> RDFGraphPage {
        var quads: [RDFQuad] = []
        quads.reserveCapacity(range.count)
        for index in range {
            graph.withElement(at: index) { quad in
                quads.append(copy quad)
            }
        }
        return try RDFGraphPage(
            quads: consume quads,
            continuation: continuation,
            provenance: nil,
            consistency: consistency
        )
    }

    private static func recordOutput(
        _ page: RDFGraphPage,
        workMeter: DatabaseWorkMeter
    ) throws {
        guard let quadCount = UInt32(exactly: page.quadCount) else {
            throw DatabaseWorkLimitError.maximumRows(
                stage: .resultMaterialization,
                consumed: workMeter.consumedRows,
                requested: UInt32.max,
                maximum: workMeter.budget.maximumRows
            )
        }
        try workMeter.recordOutputRows(quadCount)
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
            DatabaseOperationCatalog.queryExecute,
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
            monotonicClock: context.executor.monotonicClock,
            workMeter: workMeter,
            queryStructuralLimits: queryStructuralLimits
        )
    }

    private func sparqlExecutor(
        context: DatabaseOperationContext
    ) throws -> any SPARQLSourceExecutor {
        guard let executor = context.executor.runtimeConfiguration
            .logicalSourceExecutors.sparqlExecutor else {
            throw CanonicalReadError.unsupportedSource(
                "SPARQL source executor is not registered"
            )
        }
        return executor
    }
}

#endif
