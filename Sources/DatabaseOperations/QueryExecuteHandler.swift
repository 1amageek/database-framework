import DatabaseMaintenanceOperations
import DatabaseSchemaOperations
import DatabaseJobRuntime
import DatabaseGraphOperations
import DatabaseMutationOperations
import DatabaseQueryOperations
import DatabaseCommandOperations
import DatabaseOperationCore
import DatabaseEngine
import DatabaseTypes
@_spi(DatabaseOperations) import DatabaseWire
import DatabaseKit
import StorageKit

public struct QueryExecuteHandler: DatabaseOperationHandler {
    public typealias Operation = QueryExecuteOperation

    private let runtimeLimits: DatabaseOperationLimits
    private let querySnapshotStore: DatabaseQuerySnapshotStore?

    public init(runtimeLimits: DatabaseOperationLimits = .default) {
        self.runtimeLimits = runtimeLimits
        self.querySnapshotStore = nil
    }

    package init(
        runtimeLimits: DatabaseOperationLimits,
        querySnapshotStore: DatabaseQuerySnapshotStore?
    ) {
        self.runtimeLimits = runtimeLimits
        self.querySnapshotStore = querySnapshotStore
    }

    public func handle(
        _ request: QueryExecuteOperation.Request,
        context: DatabaseOperationContext
    ) async throws -> QueryExecuteOperation.Response {
        try runtimeLimits.validate(request.budget)
        guard request.page.limit > 0 else {
            throw DatabaseQueryExecutionError.pageLimitMustBePositive
        }
        return try await DatabaseExecutionTimeout.run(
            milliseconds: request.budget.timeoutMilliseconds,
            clock: context.executor.monotonicClock
        ) {
            let workMeter = DatabaseWorkMeter(
                budget: request.budget,
                monotonicClock: context.executor.monotonicClock
            )
            let admittedStatement = try DatabaseStatementAdmission(
                structuralLimits: runtimeLimits.queryStructuralLimits
            ).admit(
                request.input,
                parameters: request.parameters
            )
            return try await execute(
                admittedStatement.statement,
                request: request,
                context: context,
                workMeter: workMeter,
                structuralLimits: admittedStatement.structuralLimits
            )
        }
    }

    private func execute(
        _ statement: QueryStatement,
        request: QueryExecuteOperation.Request,
        context: DatabaseOperationContext,
        workMeter: DatabaseWorkMeter,
        structuralLimits: QueryStructuralLimits
    ) async throws -> QueryExecuteOperation.Response {
        #if DATABASE_OPERATIONS_MULTIPLE_BASES
        if case .composition = context.target {
            switch statement {
            case .select, .ask, .construct, .describe:
                break
            default:
                throw DatabaseQueryExecutionError.compositionPlanUnsupported(
                    "this query statement is not advertised for Composition targets"
                )
            }
        }
        #endif
        switch statement {
        case .select(let query):
            return .rows(
                try await executeSelect(
                    query,
                    request: request,
                    context: context,
                    workMeter: workMeter,
                    structuralLimits: structuralLimits
                )
            )
        case .ask(let query):
            #if DATABASE_OPERATIONS_GRAPH_INDEXES
            return .boolean(
                try await Self.executeAsk(
                    query,
                    request: request,
                    context: context,
                    workMeter: workMeter,
                    structuralLimits: structuralLimits
                )
            )
            #else
            _ = query
            throw DatabaseQueryExecutionError.featureUnavailable(
                "ASK requires the GraphIndexes package trait"
            )
            #endif
        case .construct(let query):
            #if DATABASE_OPERATIONS_GRAPH_INDEXES
            #if DATABASE_OPERATIONS_MULTIPLE_BASES
            if case .composition = context.target {
                return .rdfGraph(
                    try await executeCompositionRDFGraph(
                        .construct(query),
                        request: request,
                        context: context,
                        workMeter: workMeter,
                        structuralLimits: structuralLimits
                    )
                )
            }
            #endif
            return .rdfGraph(
                try await DatabaseGraphQueryService(
                    queryStructuralLimits: structuralLimits,
                    querySnapshotStore: querySnapshotStore
                ).executeConstruct(
                    query,
                    request: request,
                    context: context,
                    workMeter: workMeter
                )
            )
            #else
            _ = query
            throw DatabaseQueryExecutionError.featureUnavailable(
                "CONSTRUCT requires the GraphIndexes package trait"
            )
            #endif
        case .describe(let query):
            #if DATABASE_OPERATIONS_GRAPH_INDEXES
            #if DATABASE_OPERATIONS_MULTIPLE_BASES
            if case .composition = context.target {
                return .rdfGraph(
                    try await executeCompositionRDFGraph(
                        .describe(query),
                        request: request,
                        context: context,
                        workMeter: workMeter,
                        structuralLimits: structuralLimits
                    )
                )
            }
            #endif
            return .rdfGraph(
                try await DatabaseGraphQueryService(
                    queryStructuralLimits: structuralLimits,
                    querySnapshotStore: querySnapshotStore
                ).executeDescribe(
                    query,
                    request: request,
                    context: context,
                    workMeter: workMeter
                )
            )
            #else
            _ = query
            throw DatabaseQueryExecutionError.featureUnavailable(
                "DESCRIBE requires the GraphIndexes package trait"
            )
            #endif
        default:
            throw DatabaseQueryExecutionError.mutationRequiresMutationOperation
        }
    }

    private func executeSelect(
        _ query: SelectQuery,
        request: QueryExecuteOperation.Request,
        context: DatabaseOperationContext,
        workMeter: DatabaseWorkMeter,
        structuralLimits: QueryStructuralLimits
    ) async throws -> QueryRowPage {
        #if DATABASE_OPERATIONS_MULTIPLE_BASES
        if case .composition = context.target {
            let queryFingerprint = try DatabaseQuerySnapshotStore
                .queryFingerprint(
                    query: query,
                    request: request,
                    limits: context.wireLimits
            )
            if let continuation = request.page.continuation {
                guard let querySnapshotStore else {
                    throw DatabaseQueryExecutionError
                        .querySnapshotUnavailable(
                            "the host did not provide an opaque identifier generator"
                        )
                }
                let source = try context.requireCompositionExecutor()
                let lease = try await source.acquireReadLease()
                let page = try await querySnapshotStore.load(
                    continuation: continuation,
                    composition: lease.record,
                    schemaGeneration: context.executor.schemaGeneration,
                    queryFingerprint: queryFingerprint,
                    authorization: context.authorization
                )
                try Self.recordOutput(page, workMeter: workMeter)
                return page
            }
            let page = try await DatabaseCompositionQueryPlanner(
                structuralLimits: structuralLimits
            ).execute(
                query,
                request: request,
                context: context,
                workMeter: workMeter,
                queryFingerprint: queryFingerprint,
                snapshotStore: querySnapshotStore
            )
            try Self.recordOutput(page, workMeter: workMeter)
            return page
        }
        #endif
        let databaseContext = try context.requireDataContext()
        let lease = try databaseContext.requireOperationDataRoot()
        if let continuation = request.page.continuation,
           continuation.count == DatabaseQuerySnapshotStore
            .continuationByteCount,
           continuation.first == DatabaseQuerySnapshotStore
            .continuationMarker {
            guard let querySnapshotStore else {
                throw DatabaseQueryExecutionError.invalidContinuation
            }
            let queryFingerprint = try DatabaseQuerySnapshotStore
                .queryFingerprint(
                    query: query,
                    request: request,
                    limits: context.wireLimits
                )
            let page = try await querySnapshotStore.load(
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
            try DatabaseQueryPageCursor.decode(
                $0,
                limits: context.wireLimits
            )
        }
        guard cursor?.resource == nil || cursor?.resource == lease.resource,
              cursor?.dataGeneration == nil
                || cursor?.dataGeneration == lease.generation
        else {
            throw DatabaseQueryExecutionError.invalidContinuation
        }
        let page = try await databaseContext.executeCanonicalRead {
            transaction in
            if let restorableReadPosition = cursor?.restorableReadPosition {
                guard try DatabaseTransactionReadPoint.restore(
                    restorableReadPosition,
                    transaction: transaction
                ) else {
                    throw DatabaseQueryExecutionError.invalidContinuation
                }
            }
            let readPoint = try await DatabaseTransactionReadPoint.capture(
                domainID: lease.domain.id.value,
                transaction: transaction
            )
            if case .version(let expectedVersion)? = cursor?
                .restorableReadPosition {
                guard readPoint.position == .version(expectedVersion) else {
                    throw DatabaseQueryExecutionError.invalidContinuation
                }
            }
            if !transaction.capabilities.historicalReadVersion {
                guard cursor == nil else {
                    throw DatabaseQueryExecutionError.invalidContinuation
                }
                return try await executeDurableSnapshotSelect(
                    query,
                    request: request,
                    context: context,
                    databaseContext: databaseContext,
                    lease: lease,
                    transaction: transaction,
                    readPoint: readPoint,
                    workMeter: workMeter,
                    structuralLimits: structuralLimits
                )
            }
            let response = try await databaseContext.query(
                query,
                execution: try Self.readExecution(
                    for: request,
                    continuation: cursor?.continuation,
                    workMeter: workMeter,
                    structuralLimits: structuralLimits,
                    monotonicClock: context.executor.monotonicClock,
                    continuationSnapshotIsStable: transaction.capabilities
                        .historicalReadVersion
                ),
                graphPartitions: request.graphPartitions
            )
            let continuation = try response.continuation.map {
                try DatabaseQueryPageCursor(
                    resource: lease.resource,
                    restorableReadPosition: transaction.capabilities
                        .historicalReadVersion
                        ? readPoint.position
                        : nil,
                    dataGeneration: lease.generation,
                    continuation: $0.bytes
                ).encode(limits: context.wireLimits)
            }
            return try Self.rowPage(
                response,
                continuation: continuation,
                consistency: .transactional(readPoint)
            )
        }
        try Self.recordOutput(page, workMeter: workMeter)
        return page
    }

    private func executeDurableSnapshotSelect(
        _ query: SelectQuery,
        request: QueryExecuteOperation.Request,
        context: DatabaseOperationContext,
        databaseContext: DatabaseContext,
        lease: DatabaseDataRootLease,
        transaction: any TransactionAccess,
        readPoint: DomainReadPoint,
        workMeter: DatabaseWorkMeter,
        structuralLimits: QueryStructuralLimits
    ) async throws -> QueryRowPage {
        let firstResponse = try await databaseContext.query(
            query,
            execution: try Self.readExecution(
                for: request,
                continuation: nil,
                workMeter: workMeter,
                structuralLimits: structuralLimits,
                monotonicClock: context.executor.monotonicClock,
                continuationSnapshotIsStable: true
            ),
            graphPartitions: request.graphPartitions,
            transaction: transaction
        )
        guard var engineContinuation = firstResponse.continuation else {
            return try Self.rowPage(
                firstResponse,
                continuation: nil,
                consistency: .transactional(readPoint)
            )
        }
        guard let querySnapshotStore else {
            throw DatabaseQueryExecutionError.querySnapshotUnavailable(
                "the host did not provide durable query snapshot storage"
            )
        }
        let queryFingerprint = try DatabaseQuerySnapshotStore.queryFingerprint(
            query: query,
            request: request,
            limits: context.wireLimits
        )
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

            while true {
                let response = try await databaseContext.query(
                    query,
                    execution: try Self.readExecution(
                        for: request,
                        continuation: engineContinuation.bytes,
                        workMeter: workMeter,
                        structuralLimits: structuralLimits,
                        monotonicClock: context.executor.monotonicClock,
                        continuationSnapshotIsStable: true
                    ),
                    graphPartitions: request.graphPartitions,
                    transaction: transaction
                )
                let nextPageID: ByteString?
                if response.continuation != nil {
                    nextPageID = try await querySnapshotStore.reservePage(
                        in: reservation,
                        transaction: snapshotTransaction
                    )
                } else {
                    nextPageID = nil
                }
                let page = try Self.rowPage(
                    response,
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
                let incremented = continuationPageCount
                    .addingReportingOverflow(1)
                guard !incremented.overflow else {
                    throw DatabaseQueryExecutionError.querySnapshotCorrupted
                }
                continuationPageCount = incremented.partialValue
                guard let continuation = response.continuation,
                      let nextPageID else {
                    break
                }
                engineContinuation = continuation
                currentPageID = nextPageID
            }

            try await querySnapshotStore.commitWrite(
                reservation,
                pageCount: continuationPageCount,
                totalPayloadBytes: totalPayloadBytes,
                transaction: snapshotTransaction
            )
            return try Self.rowPage(
                firstResponse,
                continuation: reservation.continuation(
                    pageID: firstContinuationPageID
                ),
                consistency: .transactional(readPoint)
            )
        } catch {
            let operationError = error
            // Co-located control/data writes are uncommitted mutations in the
            // caller-owned transaction; its rollback is the sole cleanup
            // authority. Starting another control transaction here would be a
            // nested runner and could not observe those uncommitted writes.
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

    private static func recordOutput(
        _ page: QueryRowPage,
        workMeter: DatabaseWorkMeter
    ) throws {
        guard let rowCount = UInt32(exactly: page.rowCount) else {
            throw DatabaseWorkLimitError.maximumRows(
                stage: .resultMaterialization,
                consumed: workMeter.consumedRows,
                requested: UInt32.max,
                maximum: workMeter.budget.maximumRows
            )
        }
        try workMeter.recordOutputRows(rowCount)
    }

    #if DATABASE_OPERATIONS_GRAPH_INDEXES && DATABASE_OPERATIONS_MULTIPLE_BASES
    private func executeCompositionRDFGraph(
        _ statement: DatabaseCompositionRDFQueryPlanner.Statement,
        request: QueryExecuteOperation.Request,
        context: DatabaseOperationContext,
        workMeter: DatabaseWorkMeter,
        structuralLimits: QueryStructuralLimits
    ) async throws -> RDFGraphPage {
        guard let querySnapshotStore else {
            throw DatabaseQueryExecutionError.querySnapshotUnavailable(
                "the host did not provide durable Composition snapshot storage"
            )
        }
        let queryFingerprint = try DatabaseQuerySnapshotStore
            .queryFingerprint(
                statement: statement.queryStatement,
                request: request,
                limits: context.wireLimits
            )
        if let continuation = request.page.continuation {
            let source = try context.requireCompositionExecutor()
            let lease = try await source.acquireReadLease()
            let page = try await querySnapshotStore.loadRDFGraph(
                continuation: continuation,
                composition: lease.record,
                schemaGeneration: context.executor.schemaGeneration,
                queryFingerprint: queryFingerprint,
                authorization: context.authorization
            )
            try Self.recordOutput(page, workMeter: workMeter)
            return page
        }
        let page = try await DatabaseCompositionRDFQueryPlanner(
            structuralLimits: structuralLimits
        ).execute(
            statement,
            request: request,
            context: context,
            workMeter: workMeter,
            queryFingerprint: queryFingerprint,
            snapshotStore: querySnapshotStore
        )
        try Self.recordOutput(page, workMeter: workMeter)
        return page
    }

    #endif

    #if DATABASE_OPERATIONS_GRAPH_INDEXES
    private static func executeAsk(
        _ query: AskQuery,
        request: QueryExecuteOperation.Request,
        context: DatabaseOperationContext,
        workMeter: DatabaseWorkMeter,
        structuralLimits: QueryStructuralLimits
    ) async throws -> QueryBooleanResult {
        guard request.page.continuation == nil else {
            throw DatabaseQueryExecutionError.continuationNotSupported("ASK")
        }
        guard let executor = context.executor.runtimeConfiguration
            .logicalSourceExecutors.sparqlExecutor else {
            throw CanonicalReadError.unsupportedSource(
                "SPARQL source executor is not registered"
            )
        }
        #if DATABASE_OPERATIONS_MULTIPLE_BASES
        if case .composition = context.target {
            try DatabaseCompositionSPARQLPlanValidator.validate(query)
            let source = try context.requireCompositionExecutor()
            let result = try await source.withReadSnapshot { snapshot in
                var matchingBases: [Base.ID] = []
                matchingBases.reserveCapacity(snapshot.lease.members.count)
                let options = ReadExecutionContext(
                    options: ReadExecutionOptions(budget: request.budget),
                    monotonicClock: context.executor.monotonicClock,
                    workMeter: workMeter,
                    queryStructuralLimits: structuralLimits
                )
                for member in snapshot.lease.members {
                    let transaction = try snapshot.transaction(for: member)
                    let matched = try await source.withMemberContext(
                        member,
                        in: snapshot
                    ) { databaseContext in
                        try await executor.executeAskInTransaction(
                            context: databaseContext,
                            askQuery: query,
                            options: options,
                            partitions: request.graphPartitions,
                            transaction: transaction
                        )
                    }
                    if matched { matchingBases.append(member.baseID) }
                }
                let contributors = matchingBases.isEmpty
                    ? snapshot.lease.record.composition.bases
                    : matchingBases
                return try QueryBooleanResult(
                    value: !matchingBases.isEmpty,
                    provenance: CompositionPageProvenance(
                        compositionID: snapshot.lease.record.composition.id,
                        generation: snapshot.lease.record.generation,
                        baseIDs: snapshot.lease.record.composition.bases,
                        origins: [.derived(contributors: contributors)]
                    ),
                    consistency: .federated(try await snapshot.readPoints())
                )
            }
            try workMeter.recordOutputRows(1)
            return result
        }
        #endif
        let databaseContext = try context.requireDataContext()
        let lease = try databaseContext.requireOperationDataRoot()
        let response = try await databaseContext.executeCanonicalRead {
            transaction in
            let value = try await executor.executeAskInTransaction(
                context: databaseContext,
                askQuery: query,
                options: ReadExecutionContext(
                    options: ReadExecutionOptions(budget: request.budget),
                    monotonicClock: context.executor.monotonicClock,
                    workMeter: workMeter,
                    queryStructuralLimits: structuralLimits
                ),
                partitions: request.graphPartitions,
                transaction: transaction
            )
            let readPoint = try await DatabaseTransactionReadPoint.capture(
                domainID: lease.domain.id.value,
                transaction: transaction
            )
            return try QueryBooleanResult(
                value: value,
                provenance: nil,
                consistency: .transactional(readPoint)
            )
        }
        try workMeter.recordOutputRows(1)
        return response
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

    #endif

    private static func readExecution(
        for request: QueryExecuteOperation.Request,
        continuation continuationBytes: ByteString?,
        workMeter: DatabaseWorkMeter,
        structuralLimits: QueryStructuralLimits,
        monotonicClock: any StorageMonotonicClock,
        continuationSnapshotIsStable: Bool
    ) throws -> ReadExecutionContext {
        let continuation: QueryContinuation?
        if let bytes = continuationBytes {
            continuation = QueryContinuation(bytes)
        } else {
            continuation = nil
        }
        let boundedPageSize = min(
            request.page.limit,
            request.budget.maximumRows
        )
        guard let pageSize = Int(exactly: boundedPageSize) else {
            throw DatabaseOperationConfigurationError
                .unsupportedOnCurrentPlatform(
                    limit: .maximumRows,
                    actual: UInt64(boundedPageSize),
                    maximum: UInt64(Int.max)
                )
        }
        return ReadExecutionContext(
            options: ReadExecutionOptions(
                pageSize: pageSize,
                continuation: continuation,
                budget: request.budget,
                continuationScope: try continuationScope(for: request),
                continuationSnapshotIsStable: continuationSnapshotIsStable
            ),
            monotonicClock: monotonicClock,
            workMeter: workMeter,
            queryStructuralLimits: structuralLimits
        )
    }

    private static func continuationScope(
        for request: QueryExecuteOperation.Request
    ) throws -> ByteString {
        try DatabaseWireWriter.encode {
            (
                writer: inout DatabaseWireWriter
            ) throws(DatabaseWireError) in
            try request.graphPartitions.encode(into: &writer)
        }
    }

    private static func rowPage(
        _ response: QueryResponse,
        continuation: ByteString?,
        consistency: DatabaseKit.DatabaseReadConsistency
    ) throws -> QueryRowPage {
        let columnNames = Set(
            response.rows.flatMap { $0.fields.keys }
        ).sorted()
        let columns = try columnNames.enumerated().map {
            offset,
            name -> QueryColumn in
            guard let number = UInt32(exactly: offset + 1) else {
                throw DatabaseWireError.byteCountOverflow
            }
            return QueryColumn(number: number, name: name)
        }
        return try QueryRowPage(
            columns: columns,
            rows: try response.rows.map {
                try DatabaseQueryRowEncoder.encode(
                    $0,
                    columnNames: columnNames
                )
            },
            continuation: continuation,
            provenance: nil,
            consistency: consistency
        )
    }

}
