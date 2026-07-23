import Core
import DatabaseEngine
import DatabaseValue
import DatabaseWire
import QueryIR
import StorageKit

struct RuntimeSPARQLSourceExecutor: SPARQLSourceExecutor {
    private let functionRegistry: SPARQLFunctionRegistry

    init(functionRegistry: SPARQLFunctionRegistry) {
        self.functionRegistry = functionRegistry
    }

    func execute(
        context: DatabaseContext,
        selectQuery: SelectQuery,
        options: ReadExecutionContext,
        partitions: [DatabaseObjectField]
    ) async throws -> QueryResponse {
        try validate(selectQuery)
        let execution = CanonicalReadExecution.resolve(
            requested: options.consistency,
            default: .serializable
        )
        return try await context.container.engine.withTransaction(
            configuration: execution.transactionConfiguration
        ) { transaction in
            let runtime = try await makeRuntime(
                context: context,
                partitions: partitions,
                transaction: transaction
            )
            return try await executeSelect(
                context: context,
                selectQuery: selectQuery,
                options: options,
                storedFieldNames: runtime.storedFieldNames,
                datasetScanner: runtime.scanner,
                transaction: transaction
            )
        }
    }

    func executeInTransaction(
        context: DatabaseContext,
        selectQuery: SelectQuery,
        options: ReadExecutionContext,
        partitions: [DatabaseObjectField],
        transaction: any TransactionAccess
    ) async throws -> QueryResponse {
        try validate(selectQuery)
        let runtime = try await makeRuntime(
            context: context,
            partitions: partitions,
            transaction: transaction
        )
        return try await executeSelect(
            context: context,
            selectQuery: selectQuery,
            options: options,
            storedFieldNames: runtime.storedFieldNames,
            datasetScanner: runtime.scanner,
            transaction: transaction
        )
    }

    func executeAskInTransaction(
        context: DatabaseContext,
        askQuery: AskQuery,
        options: ReadExecutionContext,
        partitions: [DatabaseObjectField],
        transaction: any TransactionAccess
    ) async throws -> Bool {
        let scanner = try await makeRuntime(
            context: context,
            partitions: partitions,
            transaction: transaction
        ).scanner
        return try await makeExecutor(
            context: context,
            scanner: scanner,
            dataset: askQuery.dataset
        ).executeAskInTransaction(
            askQuery,
            structuralLimits: options.queryStructuralLimits,
            transaction: transaction,
            workMeter: options.workMeter
        )
    }

    func executeConstructInTransaction(
        context: DatabaseContext,
        constructQuery: ConstructQuery,
        resultScope: DatabaseGraphResultScope,
        options: ReadExecutionContext,
        partitions: [DatabaseObjectField],
        transaction: any TransactionAccess
    ) async throws -> DatabaseRetainedRDFGraph {
        let scanner = try await makeRuntime(
            context: context,
            partitions: partitions,
            transaction: transaction
        ).scanner
        return try await makeExecutor(
            context: context,
            scanner: scanner,
            dataset: constructQuery.dataset
        ).executeConstructInTransaction(
            constructQuery,
            resultScope: resultScope,
            structuralLimits: options.queryStructuralLimits,
            transaction: transaction,
            workMeter: options.workMeter
        )
    }

    func executeDescribeInTransaction(
        context: DatabaseContext,
        describeQuery: DescribeQuery,
        options: ReadExecutionContext,
        partitions: [DatabaseObjectField],
        transaction: any TransactionAccess
    ) async throws -> DatabaseRetainedRDFGraph {
        let scanner = try await makeRuntime(
            context: context,
            partitions: partitions,
            transaction: transaction
        ).scanner
        return try await makeExecutor(
            context: context,
            scanner: scanner,
            dataset: describeQuery.dataset
        ).executeDescribeInTransaction(
            describeQuery,
            structuralLimits: options.queryStructuralLimits,
            transaction: transaction,
            workMeter: options.workMeter
        )
    }

    private func validate(_ selectQuery: SelectQuery) throws {
        guard selectQuery.subqueries == nil
                || selectQuery.subqueries?.isEmpty == true else {
            throw CanonicalReadError.unsupportedSelectQuery(
                "SPARQL canonical execution does not yet support WITH bindings"
            )
        }
        switch selectQuery.source {
        case .service(let endpoint, _, _):
            throw CanonicalReadError.unsupportedSource(
                "SERVICE source '\(endpoint)' is not supported on the canonical RPC"
            )
        case .graphPattern, .namedGraph:
            break
        default:
            throw CanonicalReadError.unsupportedSource("Expected SPARQL source")
        }
    }

    private func makeRuntime(
        context: DatabaseContext,
        partitions: [DatabaseObjectField],
        transaction: any TransactionAccess
    ) async throws -> (
        scanner: CanonicalRDFDatasetScanner,
        storedFieldNames: [String]
    ) {
        let resolution = try RDFDatasetReadResolver.resolve(
            schema: context.container.schema
        )
        let projectedSources: [RDFDatasetSource]
        if let resolution,
           let projectedSource = try await projectedSource(
               context: context,
               resolution: resolution,
               partitions: partitions,
               transaction: transaction
           ) {
            projectedSources = [projectedSource]
        } else {
            projectedSources = []
        }
        return (
            scanner: CanonicalRDFDatasetScanner(
                authoritativeStore: CanonicalRDFGraphStore(),
                projectedSources: projectedSources
            ),
            storedFieldNames: resolution?.indexDescriptor.storedFieldNames ?? []
        )
    }

    private func projectedSource(
        context: DatabaseContext,
        resolution: RDFDatasetReadResolution,
        partitions: [DatabaseObjectField],
        transaction: any TransactionAccess
    ) async throws -> RDFDatasetSource? {
        guard let type = resolution.entity.persistableType else {
            throw CanonicalReadError.unsupportedSource(
                try RDFDatasetReadResolver.errorMessage(
                    schema: context.container.schema
                )
            )
        }
        return try await projectedSource(
            context: context,
            type: type,
            resolution: resolution,
            partitions: partitions,
            transaction: transaction
        )
    }

    private func projectedSource<T: Persistable>(
        context: DatabaseContext,
        type: T.Type,
        resolution: RDFDatasetReadResolution,
        partitions: [DatabaseObjectField],
        transaction: any TransactionAccess
    ) async throws -> RDFDatasetSource? {
        let queryContext = try context.indexQueryContext.withPartitions(
            partitions,
            for: type
        )
        guard let indexSubspace = try await queryContext
            .readableIndexSubspace(
                named: resolution.indexDescriptor.name,
                for: type,
                transaction: transaction
            ) else {
            return nil
        }

        return RDFDatasetSource(
            entityName: resolution.entity.name,
            indexName: resolution.indexDescriptor.name,
            indexSubspace: indexSubspace,
            coverage: try resolution.metadata.graphScope.sourceCoverage
        )
    }

    private func makeExecutor(
        context: DatabaseContext,
        scanner: any RDFDatasetScanner,
        dataset: SPARQLDataset
    ) throws -> SPARQLQueryExecutor {
        SPARQLQueryExecutor(
            database: context.container.engine,
            datasetScanner: scanner,
            datasetScope: try SPARQLDatasetExecutionScope(dataset),
            functionRegistry: functionRegistry
        )
    }

    private func executeSelect(
        context: DatabaseContext,
        selectQuery: SelectQuery,
        options: ReadExecutionContext,
        storedFieldNames: [String],
        datasetScanner: any RDFDatasetScanner,
        transaction: (any TransactionAccess)?
    ) async throws -> QueryResponse {
        let selectPlan = try SPARQLSelectPlanCompiler
            .compileForCanonicalPagination(
                selectQuery,
                additionalProjectionVariables: storedFieldNames,
                structuralLimits: options.queryStructuralLimits
            )
        let projectedVariables = selectPlan.projectionVariables
        let executor = try makeExecutor(
            context: context,
            scanner: datasetScanner,
            dataset: selectQuery.dataset
        )

        var bindings: [VariableBinding]
        if let transaction {
            (bindings, _) = try await executor.executeInTransaction(
                selectPlan: selectPlan,
                transaction: transaction,
                workMeter: options.workMeter
            )
        } else {
            (bindings, _) = try await executor.execute(
                selectPlan: selectPlan,
                workMeter: options.workMeter
            )
        }

        var rows: [QueryRow] = []
        rows.reserveCapacity(bindings.count)
        for binding in bindings {
            let row = QueryRow(
                fields: rowFields(
                    from: binding,
                    projectedVariables: projectedVariables
                )
            )
            try options.workMeter.consume(at: .resultMaterialization)
            rows.append(row)
        }
        let page = try CanonicalQueryPagination.window(
            rows: consume rows,
            selectQuery: selectQuery,
            options: options
        )
        return QueryResponse(rows: page.items, continuation: page.continuation)
    }

    private func rowFields(
        from binding: VariableBinding,
        projectedVariables: [String]
    ) -> [String: DatabaseValue] {
        var fields: [String: DatabaseValue] = [:]
        for variable in projectedVariables {
            guard let value = binding[variable] else { continue }
            fields[unprefixedVariable(variable)] = value.asDatabaseValue
        }
        return fields
    }

    private func unprefixedVariable(_ name: String) -> String {
        if name.hasPrefix("?") || name.hasPrefix("$") {
            return String(name.dropFirst())
        }
        return name
    }
}
