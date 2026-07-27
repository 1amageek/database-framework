import DatabaseEngine
import DatabaseTypes
@_spi(DatabaseServer) import DatabaseWire
import DatabaseKit

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

    private static func executeSelect(
        _ query: SelectQuery,
        request: QueryExecuteOperation.Request,
        context: DatabaseOperationContext,
        workMeter: DatabaseWorkMeter,
        structuralLimits: QueryStructuralLimits
    ) async throws -> QueryRowPage {
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
        guard let rowCount = UInt32(exactly: page.rowCount) else {
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
    ) throws -> ByteString {
        try DatabaseWireWriter.encode {
            (
                writer: inout DatabaseWireWriter
            ) throws(DatabaseWireError) in
            try request.graphPartitions.encode(into: &writer)
        }
    }

    private static func rowPage(
        _ response: QueryResponse
    ) throws -> QueryRowPage {
        let snapshotVersion = response.metadata["snapshotVersion"]?.int64Value.flatMap {
            UInt64(exactly: $0)
        }
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
            continuation: response.continuation.map(\.bytes),
            snapshotVersion: snapshotVersion
        )
    }
}
