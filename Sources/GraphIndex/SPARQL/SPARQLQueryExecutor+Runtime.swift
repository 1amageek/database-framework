#if canImport(FoundationEssentials)
import FoundationEssentials
#else
import Foundation
#endif
import DatabaseKit
import DatabaseTypes
import DatabaseEngine
import StorageKit

extension SPARQLQueryExecutor {
    /// Returns an executor over the same dataset with ontology-aware paths.
    public func withOntology(_ context: OntologyContext?) -> Self {
        Self(
            database: database,
            datasetScanner: datasetScanner,
            readMode: readMode,
            datasetScope: datasetScope,
            functionRegistry: functionRegistry,
            ontologyContext: context,
            propertyPathConfiguration: propertyPathConfiguration
        )
    }

    package func scanDatasetInTransaction(
        graphScope: RDFGraphScanScope,
        transaction: any TransactionAccess,
        workMeter: DatabaseWorkMeter
    ) async throws -> RDFDatasetScanResult {
        try await datasetScanner.scan(
            subject: nil,
            predicate: nil,
            object: nil,
            graphScope: graphScope,
            limit: nil,
            readMode: readMode,
            transaction: transaction,
            workMeter: workMeter
        )
    }

    func scoped(to datasetScope: SPARQLDatasetExecutionScope) -> Self {
        Self(
            database: database,
            datasetScanner: datasetScanner,
            readMode: readMode,
            datasetScope: datasetScope,
            functionRegistry: functionRegistry,
            ontologyContext: ontologyContext,
            propertyPathConfiguration: propertyPathConfiguration
        )
    }

    func requestScoped(by workMeter: DatabaseWorkMeter) throws -> Self {
        Self(
            database: database,
            datasetScanner: datasetScanner,
            readMode: readMode,
            datasetScope: datasetScope,
            functionRegistry: functionRegistry,
            ontologyContext: ontologyContext,
            propertyPathConfiguration: propertyPathConfiguration,
            workMeter: workMeter,
            expressionContext: try SPARQLQueryExpressionContext(
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
            database: database,
            datasetScanner: datasetScanner,
            readMode: readMode,
            datasetScope: datasetScope,
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
        database: any StorageEngine,
        datasetScanner: any RDFDatasetScanner,
        readMode: RDFDatasetReadMode,
        datasetScope: SPARQLDatasetExecutionScope,
        functionRegistry: SPARQLFunctionRegistry,
        ontologyContext: OntologyContext?,
        propertyPathConfiguration: ExecutionPropertyPathConfiguration,
        workMeter: DatabaseWorkMeter,
        expressionContext: SPARQLQueryExpressionContext,
        subqueryCache: SPARQLSubqueryResultCache?,
        nestedExpressionStatistics: SPARQLNestedExpressionStatistics?
    ) {
        self.database = database
        self.datasetScanner = datasetScanner
        self.readMode = readMode
        self.datasetScope = datasetScope
        self.functionRegistry = functionRegistry
        self.ontologyContext = ontologyContext
        self.propertyPathConfiguration = propertyPathConfiguration
        self.workMeter = workMeter
        self.expressionContext = expressionContext
        self.subqueryCache = subqueryCache
        self.nestedExpressionStatistics = nestedExpressionStatistics
    }

    var initialActiveGraph: ActiveGraph {
        ActiveGraph(scanScope: datasetScope.defaultGraphScope)
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
