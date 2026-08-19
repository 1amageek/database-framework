import DatabaseEngine
import DatabaseKit
import DatabaseTypes
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
        partitions: FieldObject
    ) async throws -> QueryResponse {
        try validate(selectQuery)
        try context.authorizeRDFDatasetFieldRead()
        let execution = CanonicalReadExecution.resolve(
            requested: options.consistency,
            default: .serializable
        )
        return try await context.container.transactionExecutor.withTransaction(
            configuration: execution.transactionConfiguration,
            clock: context.container.monotonicClock
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
                includedFieldNames: runtime.includedFieldNames,
                datasetScanner: runtime.scanner,
                transaction: transaction
            )
        }
    }

    func executeInTransaction(
        context: DatabaseContext,
        selectQuery: SelectQuery,
        options: ReadExecutionContext,
        partitions: FieldObject,
        transaction: any TransactionAccess
    ) async throws -> QueryResponse {
        try validate(selectQuery)
        try context.authorizeRDFDatasetFieldRead()
        let runtime = try await makeRuntime(
            context: context,
            partitions: partitions,
            transaction: transaction
        )
        return try await executeSelect(
            context: context,
            selectQuery: selectQuery,
            options: options,
            includedFieldNames: runtime.includedFieldNames,
            datasetScanner: runtime.scanner,
            transaction: transaction
        )
    }

    func executeAskInTransaction(
        context: DatabaseContext,
        askQuery: AskQuery,
        options: ReadExecutionContext,
        partitions: FieldObject,
        transaction: any TransactionAccess
    ) async throws -> Bool {
        try context.authorizeRDFDatasetFieldRead()
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
        nodeNamespace: GraphResultNodeNamespace,
        options: ReadExecutionContext,
        partitions: FieldObject,
        transaction: any TransactionAccess
    ) async throws -> DatabaseRetainedRDFGraph {
        try context.authorizeRDFDatasetFieldRead()
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
            nodeNamespace: nodeNamespace,
            structuralLimits: options.queryStructuralLimits,
            transaction: transaction,
            workMeter: options.workMeter
        )
    }

    func executeDescribeInTransaction(
        context: DatabaseContext,
        describeQuery: DescribeQuery,
        options: ReadExecutionContext,
        partitions: FieldObject,
        transaction: any TransactionAccess
    ) async throws -> DatabaseRetainedRDFGraph {
        try context.authorizeRDFDatasetFieldRead()
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
        partitions: FieldObject,
        transaction: any TransactionAccess
    ) async throws -> (
        scanner: CanonicalRDFDatasetScanner,
        includedFieldNames: [String]
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
                authoritativeStore: CanonicalRDFGraphStore(
                    rootSubspace: CanonicalRDFGraphStore.rootSubspace(
                        forBaseRoot: try context.operationDataRoot()
                    )
                ),
                projectedSources: projectedSources
            ),
            includedFieldNames: resolution?.indexDescriptor.includedFieldNames ?? []
        )
    }

    private func projectedSource(
        context: DatabaseContext,
        resolution: RDFDatasetReadResolution,
        partitions: FieldObject,
        transaction: any TransactionAccess
    ) async throws -> RDFDatasetSource? {
        let queryContext = context.indexQueryContext
        guard let index = try await queryContext
            .readableIndex(
                named: resolution.indexDescriptor.name,
                    indexType: resolution.indexDescriptor.type,
                    forEntityName: resolution.entity.name,
                partitions: partitions,
                transaction: transaction
            ) else {
            return nil
        }

        return RDFDatasetSource(
            entityName: resolution.entity.name,
            indexName: resolution.indexDescriptor.name,
            indexSubspace: index.subspace,
            coverage: try resolution.metadata.graphMapping.sourceCoverage,
            includedFieldNames: resolution.indexDescriptor.includedFieldNames
        )
    }

    private func makeExecutor(
        context: DatabaseContext,
        scanner: any RDFDatasetScanner,
        dataset: SPARQLDataset
    ) throws -> SPARQLQueryExecutor {
        SPARQLQueryExecutor(
            database: context.container.engine,
            monotonicClock: context.container.monotonicClock,
            wallClock: context.container.wallClock,
            datasetScanner: scanner,
            dataset: try SPARQLExecutionDataset(dataset),
            functionRegistry: functionRegistry
        )
    }

    private func executeSelect(
        context: DatabaseContext,
        selectQuery: SelectQuery,
        options: ReadExecutionContext,
        includedFieldNames: [String],
        datasetScanner: any RDFDatasetScanner,
        transaction: (any TransactionAccess)?
    ) async throws -> QueryResponse {
        let selectPlan = try SPARQLSelectPlanCompiler
            .compileForCanonicalPagination(
                selectQuery,
                additionalProjectionVariables: includedFieldNames,
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

        var rows: [DatabaseEngine.QueryRow] = []
        rows.reserveCapacity(bindings.count)
        for binding in bindings {
            let row = DatabaseEngine.QueryRow(
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
    ) -> [String: FieldValue] {
        var fields: [String: FieldValue] = [:]
        for variable in projectedVariables {
            guard let value = binding[variable] else { continue }
            fields[unprefixedVariable(variable)] = value
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
