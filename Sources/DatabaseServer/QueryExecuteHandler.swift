import DatabaseEngine
import DatabaseTypes
@_spi(DatabaseServer) import DatabaseWire
import DatabaseKit
import StorageKit

public struct QueryExecuteHandler: DatabaseOperationHandler {
    public typealias Operation = QueryExecuteOperation

    private let runtimeLimits: DatabaseRuntimeLimits
    private let compositionSnapshotStore: DatabaseCompositionSnapshotStore?

    public init(runtimeLimits: DatabaseRuntimeLimits = .default) {
        self.runtimeLimits = runtimeLimits
        self.compositionSnapshotStore = nil
    }

    package init(
        runtimeLimits: DatabaseRuntimeLimits,
        compositionSnapshotStore: DatabaseCompositionSnapshotStore?
    ) {
        self.runtimeLimits = runtimeLimits
        self.compositionSnapshotStore = compositionSnapshotStore
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
            return try await Self.execute(
                admittedStatement.statement,
                request: request,
                context: context,
                workMeter: workMeter,
                structuralLimits: admittedStatement.structuralLimits,
                compositionSnapshotStore: compositionSnapshotStore
            )
        }
    }

    private static func execute(
        _ statement: QueryStatement,
        request: QueryExecuteOperation.Request,
        context: DatabaseOperationContext,
        workMeter: DatabaseWorkMeter,
        structuralLimits: QueryStructuralLimits,
        compositionSnapshotStore: DatabaseCompositionSnapshotStore?
    ) async throws -> QueryExecuteOperation.Response {
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
        switch statement {
        case .select(let query):
            return .rows(
                try await executeSelect(
                    query,
                    request: request,
                    context: context,
                    workMeter: workMeter,
                    structuralLimits: structuralLimits,
                    compositionSnapshotStore: compositionSnapshotStore
                )
            )
        case .ask(let query):
            #if DATABASE_SERVER_GRAPH_INDEXES
            return .boolean(
                try await executeAsk(
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
            #if DATABASE_SERVER_GRAPH_INDEXES
            if case .composition = context.target {
                return .rdfGraph(
                    try await executeCompositionRDFGraph(
                        .construct(query),
                        request: request,
                        context: context,
                        workMeter: workMeter,
                        structuralLimits: structuralLimits,
                        compositionSnapshotStore: compositionSnapshotStore
                    )
                )
            }
            return .rdfGraph(
                try await DatabaseGraphQueryService(
                    queryStructuralLimits: structuralLimits
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
            #if DATABASE_SERVER_GRAPH_INDEXES
            if case .composition = context.target {
                return .rdfGraph(
                    try await executeCompositionRDFGraph(
                        .describe(query),
                        request: request,
                        context: context,
                        workMeter: workMeter,
                        structuralLimits: structuralLimits,
                        compositionSnapshotStore: compositionSnapshotStore
                    )
                )
            }
            return .rdfGraph(
                try await DatabaseGraphQueryService(
                    queryStructuralLimits: structuralLimits
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

    private static func executeSelect(
        _ query: SelectQuery,
        request: QueryExecuteOperation.Request,
        context: DatabaseOperationContext,
        workMeter: DatabaseWorkMeter,
        structuralLimits: QueryStructuralLimits,
        compositionSnapshotStore: DatabaseCompositionSnapshotStore?
    ) async throws -> QueryRowPage {
        if case .composition = context.target {
            let queryFingerprint = try DatabaseCompositionSnapshotStore
                .queryFingerprint(
                    query: query,
                    request: request,
                    limits: context.wireLimits
            )
            if let continuation = request.page.continuation {
                guard let compositionSnapshotStore else {
                    throw DatabaseQueryExecutionError
                        .compositionSnapshotUnavailable(
                            "the host did not provide an opaque identifier generator"
                        )
                }
                let source = try context.requireCompositionExecutor()
                let lease = try await source.acquireReadLease()
                let page = try await compositionSnapshotStore.load(
                    continuation: continuation,
                    composition: lease.record,
                    schemaGeneration: context.executor.schemaGeneration,
                    queryFingerprint: queryFingerprint,
                    authorization: context.authorization
                )
                try recordOutput(page, workMeter: workMeter)
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
                snapshotStore: compositionSnapshotStore
            )
            try recordOutput(page, workMeter: workMeter)
            return page
        }
        let databaseContext = try context.requireBaseContext()
        let lease = try databaseContext.requireOperationBaseLease()
        let cursor = try request.page.continuation.map {
            try DatabaseQueryPageCursor.decode(
                $0,
                limits: context.wireLimits
            )
        }
        guard cursor?.baseID == nil || cursor?.baseID == lease.baseID,
              cursor?.placementGeneration == nil
                || cursor?.placementGeneration == lease.placementGeneration
        else {
            throw DatabaseQueryExecutionError.invalidContinuation
        }
        let page = try await databaseContext.executeCanonicalRead {
            transaction in
            if let cursor {
                guard try DatabaseTransactionReadPoint.restore(
                    cursor.readPosition,
                    transaction: transaction
                ) else {
                    throw DatabaseQueryExecutionError.invalidContinuation
                }
            }
            let readPoint = try await DatabaseTransactionReadPoint.capture(
                domainID: lease.domainID,
                transaction: transaction
            )
            if case .version(let expectedVersion)? = cursor?.readPosition {
                guard readPoint.position == .version(expectedVersion) else {
                    throw DatabaseQueryExecutionError.invalidContinuation
                }
            }
            let response = try await databaseContext.query(
                query,
                execution: try readExecution(
                    for: request,
                    continuation: cursor?.continuation,
                    workMeter: workMeter,
                    structuralLimits: structuralLimits,
                    monotonicClock: context.executor.monotonicClock
                ),
                graphPartitions: request.graphPartitions
            )
            let continuation = try response.continuation.map {
                try DatabaseQueryPageCursor(
                    baseID: lease.baseID,
                    readPosition: readPoint.position,
                    placementGeneration: lease.placementGeneration,
                    continuation: $0.bytes
                ).encode(limits: context.wireLimits)
            }
            return try rowPage(
                response,
                continuation: continuation,
                consistency: .transactional(readPoint)
            )
        }
        try recordOutput(page, workMeter: workMeter)
        return page
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

    #if DATABASE_SERVER_GRAPH_INDEXES
    private static func executeCompositionRDFGraph(
        _ statement: DatabaseCompositionRDFQueryPlanner.Statement,
        request: QueryExecuteOperation.Request,
        context: DatabaseOperationContext,
        workMeter: DatabaseWorkMeter,
        structuralLimits: QueryStructuralLimits,
        compositionSnapshotStore: DatabaseCompositionSnapshotStore?
    ) async throws -> RDFGraphPage {
        guard let compositionSnapshotStore else {
            throw DatabaseQueryExecutionError.compositionSnapshotUnavailable(
                "the host did not provide durable Composition snapshot storage"
            )
        }
        let queryFingerprint = try DatabaseCompositionSnapshotStore
            .queryFingerprint(
                statement: statement.queryStatement,
                request: request,
                limits: context.wireLimits
            )
        if let continuation = request.page.continuation {
            let source = try context.requireCompositionExecutor()
            let lease = try await source.acquireReadLease()
            let page = try await compositionSnapshotStore.loadRDFGraph(
                continuation: continuation,
                composition: lease.record,
                schemaGeneration: context.executor.schemaGeneration,
                queryFingerprint: queryFingerprint,
                authorization: context.authorization
            )
            try recordOutput(page, workMeter: workMeter)
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
            snapshotStore: compositionSnapshotStore
        )
        try recordOutput(page, workMeter: workMeter)
        return page
    }

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
        let databaseContext = try context.requireBaseContext()
        let lease = try databaseContext.requireOperationBaseLease()
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
                domainID: lease.domainID,
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
        monotonicClock: any StorageMonotonicClock
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
            throw DatabaseRuntimeConfigurationError
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
                continuationScope: try continuationScope(for: request)
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
