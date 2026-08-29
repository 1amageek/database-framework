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
        session: DatabaseReadSession,
        selectQuery: SelectQuery,
        options: ReadExecutionContext,
        partitions: FieldObject
    ) async throws -> DatabaseRetainedQueryRows {
        try validate(selectQuery)
        try session.requireRDFDatasetReadAuthorization()
        let runtime = try await makeRuntime(
            session: session,
            partitions: partitions
        )
        return try await executeSelect(
            session: session,
            selectQuery: selectQuery,
            options: options,
            includedFieldNames: runtime.includedFieldNames,
            datasetScanner: runtime.scanner,
            transaction: session.transaction
        )
    }

    func executeAskInTransaction(
        session: DatabaseReadSession,
        askQuery: AskQuery,
        options: ReadExecutionContext,
        partitions: FieldObject
    ) async throws -> Bool {
        try session.requireRDFDatasetReadAuthorization()
        let scanner = try await makeRuntime(
            session: session,
            partitions: partitions
        ).scanner
        return try await makeExecutor(
            session: session,
            scanner: scanner,
            dataset: askQuery.dataset
        ).executeAskInTransaction(
            askQuery,
            structuralLimits: options.queryStructuralLimits,
            transaction: session.transaction,
            workMeter: options.workMeter
        )
    }

    func executeConstructInTransaction(
        session: DatabaseReadSession,
        constructQuery: ConstructQuery,
        nodeNamespace: GraphResultNodeNamespace,
        options: ReadExecutionContext,
        partitions: FieldObject
    ) async throws -> DatabaseRetainedRDFGraph {
        try session.requireRDFDatasetReadAuthorization()
        let scanner = try await makeRuntime(
            session: session,
            partitions: partitions
        ).scanner
        return try await makeExecutor(
            session: session,
            scanner: scanner,
            dataset: constructQuery.dataset
        ).executeConstructInTransaction(
            constructQuery,
            nodeNamespace: nodeNamespace,
            structuralLimits: options.queryStructuralLimits,
            transaction: session.transaction,
            workMeter: options.workMeter
        )
    }

    func executeDescribeInTransaction(
        session: DatabaseReadSession,
        describeQuery: DescribeQuery,
        options: ReadExecutionContext,
        partitions: FieldObject
    ) async throws -> DatabaseRetainedRDFGraph {
        try session.requireRDFDatasetReadAuthorization()
        let scanner = try await makeRuntime(
            session: session,
            partitions: partitions
        ).scanner
        return try await makeExecutor(
            session: session,
            scanner: scanner,
            dataset: describeQuery.dataset
        ).executeDescribeInTransaction(
            describeQuery,
            structuralLimits: options.queryStructuralLimits,
            transaction: session.transaction,
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
        session: DatabaseReadSession,
        partitions: FieldObject
    ) async throws -> (
        scanner: CanonicalRDFDatasetScanner,
        includedFieldNames: [String]
    ) {
        let resolution = try RDFDatasetReadResolver.resolveOptional(
            schema: session.schema
        )
        let projectedSources: [RDFDatasetSource]
        if let resolution,
           let projectedSource = try await projectedSource(
               session: session,
               resolution: resolution,
               partitions: partitions
           ) {
            projectedSources = [projectedSource]
        } else {
            projectedSources = []
        }
        return (
            scanner: CanonicalRDFDatasetScanner(
                authoritativeStore: CanonicalRDFGraphStore(
                    rootSubspace: CanonicalRDFGraphStore.rootSubspace(
                        forDataRoot: session.operationDataRoot
                    )
                ),
                projectedSources: projectedSources
            ),
            includedFieldNames: resolution?.indexDescriptor.includedFieldNames ?? []
        )
    }

    private func projectedSource(
        session: DatabaseReadSession,
        resolution: RDFDatasetReadResolution,
        partitions: FieldObject
    ) async throws -> RDFDatasetSource? {
        guard let index = try await session.readableIndex(
                named: resolution.indexDescriptor.name,
                indexType: resolution.indexDescriptor.type,
                forEntityName: resolution.entity.name,
                partitions: partitions,
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
        session: DatabaseReadSession,
        scanner: any RDFDatasetScanner,
        dataset: SPARQLDataset
    ) throws -> SPARQLQueryExecutor {
        SPARQLQueryExecutor(
            monotonicClock: session.monotonicClock,
            wallClock: session.wallClock,
            datasetScanner: scanner,
            dataset: try SPARQLExecutionDataset(dataset),
            functionRegistry: functionRegistry
        )
    }

    private func executeSelect(
        session: DatabaseReadSession,
        selectQuery: SelectQuery,
        options: ReadExecutionContext,
        includedFieldNames: [String],
        datasetScanner: any RDFDatasetScanner,
        transaction: DatabaseReadTransaction
    ) async throws -> DatabaseRetainedQueryRows {
        let selectPlan = try SPARQLSelectPlanCompiler
            .compileForCanonicalPagination(
                selectQuery,
                additionalProjectionVariables: includedFieldNames,
                structuralLimits: options.queryStructuralLimits
            )
        let executor = try makeExecutor(
            session: session,
            scanner: datasetScanner,
            dataset: selectQuery.dataset
        )
        let result = try await executor.executeRetainedInTransaction(
            selectPlan: selectPlan,
            transaction: transaction,
            workMeter: options.workMeter
        )
        return try result.retainedQueryRows(workMeter: options.workMeter)
    }
}
