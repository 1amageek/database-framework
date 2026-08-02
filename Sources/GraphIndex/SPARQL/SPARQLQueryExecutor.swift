// SPARQLQueryExecutor.swift
// GraphIndex - SPARQL-like query execution engine
//
// Executes graph patterns against hexastore indexes.

import DatabaseKit
import DatabaseTypes
import DatabaseEngine
import StorageKit

/// SPARQL query execution engine
///
/// Evaluates graph patterns against hexastore indexes with:
/// - Greedy join order optimization
/// - Variable substitution for efficient index lookups
/// - OPTIONAL (left outer join), UNION, and FILTER support
///
/// Non-generic: accepts pre-resolved metadata instead of using `T.self`.
/// Can be used from both generic (DatabaseContext) and dynamic (CLI) code paths.
///
/// **Reference**: W3C SPARQL 1.1 Query Language, Section 18.5 (SPARQL Algebra Evaluation)
public struct SPARQLQueryExecutor: Sendable {

    // MARK: - Properties

    let database: any StorageEngine
    let monotonicClock: any StorageMonotonicClock
    let wallClock: any WallClock
    let datasetScanner: any RDFDatasetScanner
    let readMode: RDFDatasetReadMode
    let propertyPathConfiguration: ExecutionPropertyPathConfiguration
    let workMeter: DatabaseWorkMeter?
    let expressionContext: SPARQLQueryExpressionContext?
    let datasetScope: SPARQLDatasetExecutionScope
    let functionRegistry: SPARQLFunctionRegistry
    let subqueryCache: SPARQLSubqueryResultCache?
    let nestedExpressionStatistics: SPARQLNestedExpressionStatistics?

    /// Optional ontology context for property hierarchy-aware evaluation.
    /// When provided, `.iri(predicate)` in property paths expands to include sub-properties,
    /// `.inverse()` consults owl:inverseOf, and transitive properties use BFS.
    let ontologyContext: OntologyContext?

    /// Reusable scan signature for substituted triple patterns.
    ///
    /// Two patterns can reuse a single index scan only when they produce the same scan range
    /// and have the same graph constraint semantics.
    struct ScanSignature: Hashable {
        let subject: ExecutionTerm
        let predicate: ExecutionTerm
        let object: ExecutionTerm
        let graphScope: RDFGraphScanScope
    }

    struct ActiveGraph: Sendable, Hashable {
        let scanScope: RDFGraphScanScope

        static let defaultGraph = ActiveGraph(scanScope: .defaultGraph)

        static func named(_ graph: RDFGraphName) -> ActiveGraph {
            ActiveGraph(scanScope: .named(graph))
        }
    }

    /// Hash join key for a set of join variables.
    ///
    /// Unbound variables are represented as `.null` to keep arity stable.
    struct JoinKey: Hashable {
        let values: [FieldValue]

        init(
            binding: borrowing VariableBinding,
            variables: [String]
        ) {
            self.values = variables.map { binding[$0] ?? .null }
        }
    }

    enum HashJoinEvaluation: ~Copyable, Sendable {
        case executed(
            results: SPARQLRetainedBindings,
            stats: ExecutionStatistics
        )
        case fallback(reason: JoinFallbackReason, precheckStats: ExecutionStatistics)
    }

    static let nestedLoopThreshold = 64
    static let hashJoinMinStaticBound = 2
    static let hashJoinRightSideScanCap = 64
    static let hashJoinEnabled = true

    // MARK: - Initialization

    /// Initialize with an abstract scanner for one logical RDF dataset.
    public init(
        database: any StorageEngine,
        monotonicClock: any StorageMonotonicClock,
        wallClock: any WallClock,
        datasetScanner: any RDFDatasetScanner,
        readMode: RDFDatasetReadMode = .snapshot,
        datasetScope: SPARQLDatasetExecutionScope = .implicit,
        functionRegistry: SPARQLFunctionRegistry = .empty,
        ontologyContext: OntologyContext? = nil,
        propertyPathConfiguration: ExecutionPropertyPathConfiguration = .default
    ) {
        self.database = database
        self.monotonicClock = monotonicClock
        self.wallClock = wallClock
        self.datasetScanner = datasetScanner
        self.readMode = readMode
        self.datasetScope = datasetScope
        self.functionRegistry = functionRegistry
        self.ontologyContext = ontologyContext
        self.propertyPathConfiguration = propertyPathConfiguration
        self.workMeter = nil
        self.expressionContext = nil
        self.subqueryCache = nil
        self.nestedExpressionStatistics = nil
    }

    /// Initialize with canonical physical RDF dataset sources.
    public init(
        database: any StorageEngine,
        monotonicClock: any StorageMonotonicClock,
        wallClock: any WallClock,
        sources: [RDFDatasetSource],
        readMode: RDFDatasetReadMode = .snapshot,
        datasetScope: SPARQLDatasetExecutionScope = .implicit,
        functionRegistry: SPARQLFunctionRegistry = .empty,
        ontologyContext: OntologyContext? = nil,
        propertyPathConfiguration: ExecutionPropertyPathConfiguration = .default
    ) {
        self.init(
            database: database,
            monotonicClock: monotonicClock,
            wallClock: wallClock,
            datasetScanner: IndexedRDFDatasetScanner(sources: sources),
            readMode: readMode,
            datasetScope: datasetScope,
            functionRegistry: functionRegistry,
            ontologyContext: ontologyContext,
            propertyPathConfiguration: propertyPathConfiguration
        )
    }

    // MARK: - Result Type

    struct EvaluationResult: ~Copyable, Sendable {
        var bindings: SPARQLRetainedBindings
        var stats: ExecutionStatistics

        static func empty(
            stats: ExecutionStatistics = ExecutionStatistics()
        ) -> EvaluationResult {
            EvaluationResult(bindings: .empty, stats: stats)
        }

        consuming func mergedStats(
            with other: ExecutionStatistics
        ) -> EvaluationResult {
            var owned = consume self
            owned.stats = owned.stats.merged(with: other)
            return consume owned
        }
    }

}
