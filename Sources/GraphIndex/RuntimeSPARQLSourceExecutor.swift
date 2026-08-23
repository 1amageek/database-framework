@_spi(DatabaseExecution) import DatabaseEngine
import DatabaseKit
import DatabaseTypes
import StorageKit

struct RuntimeSPARQLSourceExecutor: SPARQLSourceExecutor {
    private let functionRegistry: SPARQLFunctionRegistry

    init(functionRegistry: SPARQLFunctionRegistry) {
        self.functionRegistry = functionRegistry
    }

    func executeInTransaction(
        context: DatabaseContext,
        selectQuery: SelectQuery,
        options: ReadExecutionContext,
        partitions: FieldObject,
        transaction: any TransactionReadAccess
    ) async throws -> DatabaseRetainedQueryResponse {
        try validate(selectQuery)
        try context.authorizeRDFDatasetFieldRead()
        let runtime = try await makeRuntime(
            context: context,
            partitions: partitions,
            authorization: try IndexReadAuthorization(
                selectQuery: selectQuery
            ),
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
        transaction: any TransactionReadAccess
    ) async throws -> Bool {
        try context.authorizeRDFDatasetFieldRead()
        let scanner = try await makeRuntime(
            context: context,
            partitions: partitions,
            authorization: try IndexReadAuthorization(
                modifiers: askQuery.modifiers
            ),
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
        transaction: any TransactionReadAccess
    ) async throws -> DatabaseRetainedRDFGraph {
        try context.authorizeRDFDatasetFieldRead()
        let scanner = try await makeRuntime(
            context: context,
            partitions: partitions,
            authorization: try IndexReadAuthorization(
                modifiers: constructQuery.modifiers
            ),
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
        transaction: any TransactionReadAccess
    ) async throws -> DatabaseRetainedRDFGraph {
        try context.authorizeRDFDatasetFieldRead()
        let scanner = try await makeRuntime(
            context: context,
            partitions: partitions,
            authorization: try IndexReadAuthorization(
                modifiers: describeQuery.modifiers
            ),
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
        authorization: IndexReadAuthorization,
        transaction: any TransactionReadAccess
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
               authorization: authorization,
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
        authorization: IndexReadAuthorization,
        transaction: any TransactionReadAccess
    ) async throws -> RDFDatasetSource? {
        let queryContext = context.indexQueryContext
        guard let index = try await queryContext
            .readableIndex(
                named: resolution.indexDescriptor.name,
                    indexType: resolution.indexDescriptor.type,
                forEntityName: resolution.entity.name,
                partitions: partitions,
                authorization: authorization,
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
        transaction: any TransactionReadAccess
    ) async throws -> DatabaseRetainedQueryResponse {
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

        let bindings = try await executor.executeRetainedInTransaction(
            selectPlan: selectPlan,
            transaction: transaction,
            workMeter: options.workMeter
        )

        var rows = try DatabaseRetainedArrayBuilder<DatabaseEngine.QueryRow>(
            workMeter: options.workMeter,
            stage: .resultMaterialization,
            layout: try CanonicalRelationalFootprintMeter.retainedArrayLayout(
                for: DatabaseEngine.QueryRow.self
            ),
            expectedCount: bindings.count
        )
        let footprintMeter = try SPARQLBindingFootprintMeter.make(
            workMeter: options.workMeter,
            stage: .resultMaterialization
        )
        defer { footprintMeter.shutdown() }
        for index in 0..<bindings.count {
            try options.workMeter.consume(at: .resultMaterialization)
            try bindings.withBinding(
                at: index,
                workMeter: options.workMeter
            ) { binding in
                let footprint = try footprintMeter.footprint(of: binding)
                try rows.append(
                    footprint: footprint,
                    make: {
                        DatabaseEngine.QueryRow(
                            fields: rowFields(
                                from: binding,
                                projectedVariables: projectedVariables
                            )
                        )
                    }
                )
            }
        }
        let retainedRows = try rows.finish().moveToSharedOwnership(
            at: .resultMaterialization
        )
        let page = try CanonicalQueryPagination.retainedWindow(
            rows: retainedRows,
            selectQuery: selectQuery,
            options: options
        )
        return DatabaseRetainedQueryResponse(
            rows: retainedRows,
            visibleRange: page.range,
            continuation: page.continuation
        )
    }

    private func rowFields(
        from binding: borrowing VariableBinding,
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
