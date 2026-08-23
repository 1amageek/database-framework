import DatabaseKit
import DatabaseTypes
import DatabaseEngine
import StorageKit

extension SPARQLQueryExecutor {
    /// Returns an executor over the same dataset with ontology-aware paths.
    public func withOntology(_ context: OntologyContext?) -> Self {
        Self(
            monotonicClock: monotonicClock,
            wallClock: wallClock,
            datasetScanner: datasetScanner,
            readMode: readMode,
            dataset: dataset,
            functionRegistry: functionRegistry,
            ontologyContext: context,
            propertyPathConfiguration: propertyPathConfiguration,
            workMeter: workMeter,
            expressionContext: expressionContext,
            subqueryCache: subqueryCache,
            nestedExpressionStatistics: nestedExpressionStatistics
        )
    }

    package func scanDatasetInTransaction(
        graphTarget: RDFGraphScanTarget,
        transaction: any TransactionReadAccess,
        workMeter: DatabaseWorkMeter
    ) async throws -> RDFDatasetScanResult {
        try await datasetScanner.scan(
            subject: nil,
            predicate: nil,
            object: nil,
            graphTarget: graphTarget,
            limit: nil,
            readMode: readMode,
            transaction: transaction,
            workMeter: workMeter
        )
    }

    func scoped(to dataset: SPARQLExecutionDataset) -> Self {
        Self(
            monotonicClock: monotonicClock,
            wallClock: wallClock,
            datasetScanner: datasetScanner,
            readMode: readMode,
            dataset: dataset,
            functionRegistry: functionRegistry,
            ontologyContext: ontologyContext,
            propertyPathConfiguration: propertyPathConfiguration,
            workMeter: workMeter,
            expressionContext: expressionContext,
            subqueryCache: subqueryCache,
            nestedExpressionStatistics: nestedExpressionStatistics
        )
    }

    func requestScoped(by workMeter: DatabaseWorkMeter) throws -> Self {
        Self(
            monotonicClock: monotonicClock,
            wallClock: wallClock,
            datasetScanner: datasetScanner,
            readMode: readMode,
            dataset: dataset,
            functionRegistry: functionRegistry,
            ontologyContext: ontologyContext,
            propertyPathConfiguration: propertyPathConfiguration,
            workMeter: workMeter,
            expressionContext: try SPARQLQueryExpressionContext(
                now: wallClock.now,
                functionRegistry: functionRegistry,
                workMeter: workMeter
            ),
            subqueryCache: nil,
            nestedExpressionStatistics: nil
        )
    }

    func transactionAttemptScoped() throws -> Self {
        guard let workMeter else {
            throw SPARQLQueryError.executionFailed(
                "SPARQL execution requires a request-scoped work meter"
            )
        }
        guard let expressionContext else {
            throw SPARQLExpressionEvaluationError.runtimeInvariant(
                "query-scoped expression context is unavailable"
            )
        }
        return Self(
            monotonicClock: monotonicClock,
            wallClock: wallClock,
            datasetScanner: datasetScanner,
            readMode: readMode,
            dataset: dataset,
            functionRegistry: functionRegistry,
            ontologyContext: ontologyContext,
            propertyPathConfiguration: propertyPathConfiguration,
            workMeter: workMeter,
            expressionContext: expressionContext,
            subqueryCache: try SPARQLSubqueryResultCache.make(
                workMeter: workMeter
            ),
            nestedExpressionStatistics: SPARQLNestedExpressionStatistics()
        )
    }

    init(
        monotonicClock: any StorageMonotonicClock,
        wallClock: any WallClock,
        datasetScanner: any RDFDatasetScanner,
        readMode: RDFDatasetReadMode,
        dataset: SPARQLExecutionDataset,
        functionRegistry: SPARQLFunctionRegistry,
        ontologyContext: OntologyContext?,
        propertyPathConfiguration: ExecutionPropertyPathConfiguration,
        workMeter: DatabaseWorkMeter?,
        expressionContext: SPARQLQueryExpressionContext?,
        subqueryCache: SPARQLSubqueryResultCache?,
        nestedExpressionStatistics: SPARQLNestedExpressionStatistics?
    ) {
        self.monotonicClock = monotonicClock
        self.wallClock = wallClock
        self.datasetScanner = datasetScanner
        self.readMode = readMode
        self.dataset = dataset
        self.functionRegistry = functionRegistry
        self.ontologyContext = ontologyContext
        self.propertyPathConfiguration = propertyPathConfiguration
        self.workMeter = workMeter
        self.expressionContext = expressionContext
        self.subqueryCache = subqueryCache
        self.nestedExpressionStatistics = nestedExpressionStatistics
    }

    var initialActiveGraph: ActiveGraph {
        ActiveGraph(graphTarget: dataset.defaultGraphTarget)
    }

    func requiredWorkMeter() throws -> DatabaseWorkMeter {
        guard let workMeter else {
            throw SPARQLQueryError.executionFailed(
                "SPARQL execution requires a request-scoped work meter"
            )
        }
        return workMeter
    }

    func includingNestedExpressionStatistics(
        _ result: consuming EvaluationResult
    ) -> EvaluationResult {
        guard let nestedExpressionStatistics else {
            return consume result
        }
        return (consume result).mergedStats(
            with: nestedExpressionStatistics.snapshot()
        )
    }

}
