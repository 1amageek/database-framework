/// QueryPlan.swift
/// Execution plan types for the unified query AST
///
/// Reference:
/// - PostgreSQL Query Planner (src/backend/optimizer/)
/// - FoundationDB Record Layer Query Planning

import Foundation

/// Executable query plan
public struct QueryPlan: Sendable {
    /// The plan node
    public let node: QueryPlanNode

    /// Estimated cost
    public let cost: QueryCost

    /// Indexes used
    public let indexes: [IndexUsage]

    /// Plan statistics
    public let statistics: PlanStatistics?

    public init(
        node: QueryPlanNode,
        cost: QueryCost,
        indexes: [IndexUsage],
        statistics: PlanStatistics? = nil
    ) {
        self.node = node
        self.cost = cost
        self.indexes = indexes
        self.statistics = statistics
    }
}

/// Query plan node types
public indirect enum QueryPlanNode: Sendable, Equatable {
    // MARK: - Scan Operations

    /// Sequential table scan
    case tableScan(TableScanPlan)

    /// Index scan
    case indexScan(IndexScanPlan)

    /// Index-only scan (covering index)
    case indexOnlyScan(IndexScanPlan)

    /// Bitmap index scan
    case bitmapScan(BitmapScanPlan)

    // MARK: - Join Operations

    /// Nested loop join
    case nestedLoopJoin(JoinPlan)

    /// Hash join
    case hashJoin(HashJoinPlan)

    /// Merge join (sort-merge)
    case mergeJoin(MergeJoinPlan)

    // MARK: - Graph Operations

    /// Graph traversal
    case graphTraversal(GraphTraversalPlan)

    /// Shortest path computation
    case shortestPath(ShortestPathPlan)

    // MARK: - Triple Pattern Operations

    /// Triple pattern scan
    case triplePatternScan(TriplePatternScanPlan)

    /// Property path evaluation
    case propertyPathEval(PropertyPathPlan)

    // MARK: - Transformation Operations

    /// Filter (WHERE/FILTER)
    case filter(FilterPlan)

    /// Project (SELECT columns)
    case project(ProjectPlan)

    /// Sort (ORDER BY)
    case sort(SortPlan)

    /// Limit (LIMIT/OFFSET)
    case limit(LimitPlan)

    /// Distinct (DISTINCT)
    case distinct(DistinctPlan)

    /// Aggregate (GROUP BY)
    case aggregate(AggregatePlan)

    // MARK: - Set Operations

    /// Union
    case union(SetOperationPlan)

    /// Union All
    case unionAll(SetOperationPlan)

    /// Intersect
    case intersect(SetOperationPlan)

    /// Except
    case except(SetOperationPlan)

    // MARK: - Special Operations

    /// Vector similarity search
    case vectorSearch(VectorSearchPlan)

    /// Full-text search
    case fullTextSearch(FullTextSearchPlan)

    /// Spatial search
    case spatialSearch(SpatialSearchPlan)

    /// Values (inline data)
    case values(ValuesPlan)

    /// Subquery
    case subquery(SubqueryPlan)

    /// Materialize (cache intermediate results)
    case materialize(MaterializePlan)
}

// MARK: - Scan Plans

/// Table scan plan
public struct TableScanPlan: Sendable, Equatable {
    public let schema: String
    public let filter: Expression?

    public init(schema: String, filter: Expression? = nil) {
        self.schema = schema
        self.filter = filter
    }
}

/// Index scan plan
public struct IndexScanPlan: Sendable, Equatable {
    public let schema: String
    public let indexName: String
    public let bounds: IndexBounds
    public let filter: Expression?

    public init(
        schema: String,
        indexName: String,
        bounds: IndexBounds,
        filter: Expression? = nil
    ) {
        self.schema = schema
        self.indexName = indexName
        self.bounds = bounds
        self.filter = filter
    }
}

/// Index bounds for range scans
public struct IndexBounds: Sendable, Equatable {
    public let lower: [Literal]?
    public let upper: [Literal]?
    public let lowerInclusive: Bool
    public let upperInclusive: Bool

    public init(
        lower: [Literal]? = nil,
        upper: [Literal]? = nil,
        lowerInclusive: Bool = true,
        upperInclusive: Bool = true
    ) {
        self.lower = lower
        self.upper = upper
        self.lowerInclusive = lowerInclusive
        self.upperInclusive = upperInclusive
    }

    /// Exact match bounds
    public static func exact(_ values: [Literal]) -> IndexBounds {
        IndexBounds(lower: values, upper: values)
    }

    /// Range bounds
    public static func range(
        from lower: [Literal]?,
        to upper: [Literal]?,
        inclusive: Bool = true
    ) -> IndexBounds {
        IndexBounds(
            lower: lower,
            upper: upper,
            lowerInclusive: inclusive,
            upperInclusive: inclusive
        )
    }

    /// Prefix bounds
    public static func prefix(_ values: [Literal]) -> IndexBounds {
        IndexBounds(lower: values, upper: values)
    }
}

/// Bitmap scan plan
public struct BitmapScanPlan: Sendable, Equatable {
    public let schema: String
    public let scans: [IndexScanPlan]
    public let operation: BitmapOperation

    public init(schema: String, scans: [IndexScanPlan], operation: BitmapOperation) {
        self.schema = schema
        self.scans = scans
        self.operation = operation
    }
}

/// Bitmap operation type
public enum BitmapOperation: Sendable, Equatable {
    case and
    case or
}

// MARK: - Join Plans

/// Base join plan
public struct JoinPlan: Sendable, Equatable {
    public let left: QueryPlanNode
    public let right: QueryPlanNode
    public let condition: Expression?
    public let joinType: JoinType

    public init(
        left: QueryPlanNode,
        right: QueryPlanNode,
        condition: Expression? = nil,
        joinType: JoinType = .inner
    ) {
        self.left = left
        self.right = right
        self.condition = condition
        self.joinType = joinType
    }
}

/// Hash join plan
public struct HashJoinPlan: Sendable, Equatable {
    public let build: QueryPlanNode
    public let probe: QueryPlanNode
    public let buildKeys: [Expression]
    public let probeKeys: [Expression]
    public let joinType: JoinType

    public init(
        build: QueryPlanNode,
        probe: QueryPlanNode,
        buildKeys: [Expression],
        probeKeys: [Expression],
        joinType: JoinType = .inner
    ) {
        self.build = build
        self.probe = probe
        self.buildKeys = buildKeys
        self.probeKeys = probeKeys
        self.joinType = joinType
    }
}

/// Merge join plan
public struct MergeJoinPlan: Sendable, Equatable {
    public let left: QueryPlanNode
    public let right: QueryPlanNode
    public let leftKeys: [Expression]
    public let rightKeys: [Expression]
    public let joinType: JoinType

    public init(
        left: QueryPlanNode,
        right: QueryPlanNode,
        leftKeys: [Expression],
        rightKeys: [Expression],
        joinType: JoinType = .inner
    ) {
        self.left = left
        self.right = right
        self.leftKeys = leftKeys
        self.rightKeys = rightKeys
        self.joinType = joinType
    }
}

// MARK: - Graph Plans

/// Graph traversal plan
public struct GraphTraversalPlan: Sendable, Equatable {
    public let start: QueryPlanNode
    public let pattern: MatchPattern
    public let strategy: TraversalStrategy

    public init(start: QueryPlanNode, pattern: MatchPattern, strategy: TraversalStrategy) {
        self.start = start
        self.pattern = pattern
        self.strategy = strategy
    }
}

/// Traversal strategy
public enum TraversalStrategy: Sendable, Equatable {
    case depthFirst
    case breadthFirst
    case bidirectional
}

/// Shortest path plan
public struct ShortestPathPlan: Sendable, Equatable {
    public let start: QueryPlanNode
    public let end: QueryPlanNode
    public let pattern: PathPattern
    public let algorithm: ShortestPathAlgorithm

    public init(
        start: QueryPlanNode,
        end: QueryPlanNode,
        pattern: PathPattern,
        algorithm: ShortestPathAlgorithm
    ) {
        self.start = start
        self.end = end
        self.pattern = pattern
        self.algorithm = algorithm
    }
}

/// Shortest path algorithm
public enum ShortestPathAlgorithm: Sendable, Equatable {
    case dijkstra
    case bellmanFord
    case bfs  // Unweighted
    case bidirectionalBFS
}

// MARK: - Triple Pattern Plans

/// Triple pattern scan plan
public struct TriplePatternScanPlan: Sendable, Equatable {
    public let pattern: TriplePattern
    public let index: TripleIndex
    public let bindings: [String: SPARQLTerm]

    public init(
        pattern: TriplePattern,
        index: TripleIndex,
        bindings: [String: SPARQLTerm] = [:]
    ) {
        self.pattern = pattern
        self.index = index
        self.bindings = bindings
    }
}

/// Property path evaluation plan
public struct PropertyPathPlan: Sendable, Equatable {
    public let subject: QueryPlanNode?
    public let path: PropertyPath
    public let object: QueryPlanNode?
    public let algorithm: PathEvalAlgorithm

    public init(
        subject: QueryPlanNode?,
        path: PropertyPath,
        object: QueryPlanNode?,
        algorithm: PathEvalAlgorithm = .iterative
    ) {
        self.subject = subject
        self.path = path
        self.object = object
        self.algorithm = algorithm
    }
}

/// Path evaluation algorithm
public enum PathEvalAlgorithm: Sendable, Equatable {
    case iterative
    case recursive
    case automaton
}

// MARK: - Transformation Plans

/// Filter plan
public struct FilterPlan: Sendable, Equatable {
    public let input: QueryPlanNode
    public let condition: Expression

    public init(input: QueryPlanNode, condition: Expression) {
        self.input = input
        self.condition = condition
    }
}

/// Project plan
public struct ProjectPlan: Sendable, Equatable {
    public let input: QueryPlanNode
    public let columns: [ProjectionItem]

    public init(input: QueryPlanNode, columns: [ProjectionItem]) {
        self.input = input
        self.columns = columns
    }
}

/// Sort plan
public struct SortPlan: Sendable, Equatable {
    public let input: QueryPlanNode
    public let keys: [SortKey]
    public let limit: Int?

    public init(input: QueryPlanNode, keys: [SortKey], limit: Int? = nil) {
        self.input = input
        self.keys = keys
        self.limit = limit
    }
}

/// Limit plan
public struct LimitPlan: Sendable, Equatable {
    public let input: QueryPlanNode
    public let count: Int
    public let offset: Int?

    public init(input: QueryPlanNode, count: Int, offset: Int? = nil) {
        self.input = input
        self.count = count
        self.offset = offset
    }
}

/// Distinct plan
public struct DistinctPlan: Sendable, Equatable {
    public let input: QueryPlanNode
    public let columns: [Expression]?

    public init(input: QueryPlanNode, columns: [Expression]? = nil) {
        self.input = input
        self.columns = columns
    }
}

/// Aggregate plan
public struct AggregatePlan: Sendable, Equatable {
    public let input: QueryPlanNode
    public let groupBy: [Expression]
    public let aggregates: [AggregateFunction]

    public init(
        input: QueryPlanNode,
        groupBy: [Expression],
        aggregates: [AggregateFunction]
    ) {
        self.input = input
        self.groupBy = groupBy
        self.aggregates = aggregates
    }
}

// MARK: - Set Operation Plans

/// Set operation plan
public struct SetOperationPlan: Sendable, Equatable {
    public let inputs: [QueryPlanNode]

    public init(inputs: [QueryPlanNode]) {
        self.inputs = inputs
    }
}

// MARK: - Special Plans

/// Vector search plan
public struct VectorSearchPlan: Sendable, Equatable {
    public let schema: String
    public let field: String
    public let query: [Double]
    public let k: Int
    public let metric: VectorMetric
    public let filter: Expression?

    public init(
        schema: String,
        field: String,
        query: [Double],
        k: Int,
        metric: VectorMetric,
        filter: Expression? = nil
    ) {
        self.schema = schema
        self.field = field
        self.query = query
        self.k = k
        self.metric = metric
        self.filter = filter
    }
}

/// Vector distance metric
public enum VectorMetric: String, Sendable, Equatable {
    case cosine
    case euclidean
    case dotProduct
    case manhattan
}

/// Full-text search plan
public struct FullTextSearchPlan: Sendable, Equatable {
    public let schema: String
    public let field: String
    public let query: String
    public let mode: FullTextSearchMode
    public let filter: Expression?

    public init(
        schema: String,
        field: String,
        query: String,
        mode: FullTextSearchMode,
        filter: Expression? = nil
    ) {
        self.schema = schema
        self.field = field
        self.query = query
        self.mode = mode
        self.filter = filter
    }
}

/// Full-text search mode
public enum FullTextSearchMode: String, Sendable, Equatable {
    case match
    case phrase
    case prefix
    case fuzzy
    case boolean
}

/// Spatial search plan
public struct SpatialSearchPlan: Sendable, Equatable {
    public let schema: String
    public let field: String
    public let query: SpatialQuery
    public let filter: Expression?

    public init(
        schema: String,
        field: String,
        query: SpatialQuery,
        filter: Expression? = nil
    ) {
        self.schema = schema
        self.field = field
        self.query = query
        self.filter = filter
    }
}

/// Spatial query types
public enum SpatialQuery: Sendable, Equatable {
    case withinRadius(lat: Double, lon: Double, radiusMeters: Double)
    case withinBounds(minLat: Double, minLon: Double, maxLat: Double, maxLon: Double)
    case nearestK(lat: Double, lon: Double, k: Int)
    case intersects(geometry: String)  // WKT format
}

/// Values plan (inline data)
public struct ValuesPlan: Sendable, Equatable {
    public let columns: [String]
    public let rows: [[Literal]]

    public init(columns: [String], rows: [[Literal]]) {
        self.columns = columns
        self.rows = rows
    }
}

/// Subquery plan
public struct SubqueryPlan: Sendable, Equatable {
    public let plan: QueryPlanNode
    public let alias: String?
    public let lateral: Bool

    public init(plan: QueryPlanNode, alias: String? = nil, lateral: Bool = false) {
        self.plan = plan
        self.alias = alias
        self.lateral = lateral
    }
}

/// Materialize plan
public struct MaterializePlan: Sendable, Equatable {
    public let input: QueryPlanNode
    public let hint: MaterializeHint

    public init(input: QueryPlanNode, hint: MaterializeHint) {
        self.input = input
        self.hint = hint
    }
}

/// Materialize hints
public enum MaterializeHint: Sendable, Equatable {
    case always
    case onReuse
    case never
}
