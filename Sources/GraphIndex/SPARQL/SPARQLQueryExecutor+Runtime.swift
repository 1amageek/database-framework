#if canImport(FoundationEssentials)
import FoundationEssentials
#else
import Foundation
#endif
import Core
import DatabaseValue
import Graph
import DatabaseEngine
import QueryIR
import StorageKit

extension SPARQLQueryExecutor {
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

    func requestScoped(by workMeter: DatabaseWorkMeter) -> Self {
        Self(
            database: database,
            datasetScanner: datasetScanner,
            readMode: readMode,
            datasetScope: datasetScope,
            functionRegistry: functionRegistry,
            ontologyContext: ontologyContext,
            propertyPathConfiguration: propertyPathConfiguration,
            workMeter: workMeter,
            expressionContext: SPARQLQueryExpressionContext(
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
