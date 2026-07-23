import DatabaseEngine
import DatabaseValue
import DatabaseWire
import QueryIR

public struct QueryExecuteHandler: DatabaseOperationHandler {
    public typealias Operation = QueryExecuteOperation

    private let runtimeLimits: DatabaseRuntimeLimits

    public init(runtimeLimits: DatabaseRuntimeLimits = .default) {
        self.runtimeLimits = runtimeLimits
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
            clock: context.container.engine.monotonicClock
        ) {
            let workMeter = DatabaseWorkMeter(budget: request.budget)
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
                structuralLimits: admittedStatement.structuralLimits
            )
        }
    }

    private static func execute(
        _ statement: QueryStatement,
        request: QueryExecuteOperation.Request,
        context: DatabaseOperationContext,
        workMeter: DatabaseWorkMeter,
        structuralLimits: QueryStructuralLimits
    ) async throws -> QueryExecuteOperation.Response {
        try validateSolutionModifiers(in: statement)
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
            return .boolean(
                try await executeAsk(
                    query,
                    request: request,
                    context: context,
                    workMeter: workMeter,
                    structuralLimits: structuralLimits
                )
            )
        case .construct(let query):
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
        case .describe(let query):
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
        default:
            throw DatabaseQueryExecutionError.mutationRequiresMutationOperation
        }
    }

    private static func validateSolutionModifiers(
        in statement: QueryStatement
    ) throws {
        switch statement {
        case .select(let query):
            try validateNonNegative(
                limit: query.limit,
                offset: query.offset
            )
        case .ask(let query):
            try validateNonNegative(
                limit: query.modifiers.limit,
                offset: query.modifiers.offset
            )
        case .construct(let query):
            try validateNonNegative(
                limit: query.modifiers.limit,
                offset: query.modifiers.offset
            )
        case .describe(let query):
            try validateNonNegative(
                limit: query.modifiers.limit,
                offset: query.modifiers.offset
            )
        default:
            return
        }
    }

    private static func validateNonNegative(
        limit: Int?,
        offset: Int?
    ) throws {
        if let limit, limit < 0 {
            throw DatabaseQueryExecutionError
                .solutionModifierMustBeNonNegative(
                    name: "LIMIT",
                    value: limit
                )
        }
        if let offset, offset < 0 {
            throw DatabaseQueryExecutionError
                .solutionModifierMustBeNonNegative(
                    name: "OFFSET",
                    value: offset
                )
        }
    }

    private static func executeSelect(
        _ query: SelectQuery,
        request: QueryExecuteOperation.Request,
        context: DatabaseOperationContext,
        workMeter: DatabaseWorkMeter,
        structuralLimits: QueryStructuralLimits
    ) async throws -> QueryExecuteOperation.RowPage {
        let response = try await context.container.newContext().query(
            query,
            execution: try readExecution(
                for: request,
                workMeter: workMeter,
                structuralLimits: structuralLimits
            ),
            graphPartitions: request.graphPartitions
        )
        let page = try rowPage(response)
        guard let rowCount = UInt32(exactly: page.rows.count) else {
            throw DatabaseWorkLimitError.maximumRows(
                stage: .resultMaterialization,
                consumed: workMeter.consumedRows,
                requested: UInt32.max,
                maximum: workMeter.budget.maximumRows
            )
        }
        try workMeter.recordOutputRows(rowCount)
        return page
    }

    private static func executeAsk(
        _ query: AskQuery,
        request: QueryExecuteOperation.Request,
        context: DatabaseOperationContext,
        workMeter: DatabaseWorkMeter,
        structuralLimits: QueryStructuralLimits
    ) async throws -> Bool {
        guard request.page.continuation == nil else {
            throw DatabaseQueryExecutionError.continuationNotSupported("ASK")
        }
        guard let executor = context.container.runtimeConfiguration
            .logicalSourceExecutors.sparqlExecutor else {
            throw CanonicalReadError.unsupportedSource(
                "SPARQL source executor is not registered"
            )
        }
        let response = try await context.container.engine.withTransaction(
            configuration: .default
        ) { transaction in
            try await executor.executeAskInTransaction(
                context: context.container.newContext(),
                askQuery: query,
                options: ReadExecutionContext(
                    options: ReadExecutionOptions(budget: request.budget),
                    workMeter: workMeter,
                    queryStructuralLimits: structuralLimits
                ),
                partitions: request.graphPartitions,
                transaction: transaction
            )
        }
        try workMeter.recordOutputRows(1)
        return response
    }

    private static func readExecution(
        for request: QueryExecuteOperation.Request,
        workMeter: DatabaseWorkMeter,
        structuralLimits: QueryStructuralLimits
    ) throws -> ReadExecutionContext {
        let continuation: QueryContinuation?
        if let bytes = request.page.continuation {
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
            workMeter: workMeter,
            queryStructuralLimits: structuralLimits
        )
    }

    private static func continuationScope(
        for request: QueryExecuteOperation.Request
    ) throws -> DatabaseBytes {
        let partitions = request.graphPartitions.sorted {
            ($0.number, $0.name) < ($1.number, $1.name)
        }
        var writer = DatabaseWireWriter()
        try writer.writeCount(partitions.count)
        for partition in partitions {
            try partition.encode(into: &writer)
        }
        return DatabaseBytes(writer.bytes)
    }

    private static func rowPage(
        _ response: QueryResponse
    ) throws -> QueryExecuteOperation.RowPage {
        let snapshotVersion = response.metadata["snapshotVersion"]?.int64Value.flatMap {
            UInt64(exactly: $0)
        }
        return QueryExecuteOperation.RowPage(
            rows: try response.rows.map(DatabaseQueryRowEncoder.encode),
            continuation: response.continuation.map(\.bytes),
            snapshotVersion: snapshotVersion
        )
    }
}
