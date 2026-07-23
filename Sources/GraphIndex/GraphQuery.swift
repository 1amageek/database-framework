// GraphQuery.swift
// GraphIndex - Query extension for graph/RDF indexes
//
// Provides FDBContext extension and query builder following the standard pattern.

#if canImport(FoundationEssentials)
import FoundationEssentials
#else
import Foundation
#endif
import Core
import Graph
import DatabaseEngine
import StorageKit

// MARK: - Graph Entry Point

/// Entry point for graph queries
///
/// **Usage**:
/// ```swift
/// import GraphIndex
///
/// // Find all edges from "Alice"
/// let edges = try await context.graph(Statement.self)
///     .index(\.subject, \.predicate, \.object)
///     .from("Alice")
///     .execute()
///
/// // Find "knows" relationships to "Bob"
/// let whoKnowsBob = try await context.graph(Statement.self)
///     .index(\.subject, \.predicate, \.object)
///     .edge("knows")
///     .to("Bob")
///     .execute()
/// ```
public struct GraphEntryPoint<T: Persistable>: Sendable {
    private let queryContext: IndexQueryContext
    private let ontologyContext: OntologyContext?

    internal init(queryContext: IndexQueryContext, ontologyContext: OntologyContext? = nil) {
        self.queryContext = queryContext
        self.ontologyContext = ontologyContext
    }

    /// Attach an ontology context for OWL-aware queries
    ///
    /// When provided, graph queries will:
    /// - Expand predicates to include sub-properties
    /// - Consult owl:inverseOf for inverse paths
    /// - Use functional property hints for optimization
    ///
    /// - Parameter context: The ontology context
    /// - Returns: Entry point with ontology support
    public func withOntology(_ context: OntologyContext) -> Self {
        GraphEntryPoint(queryContext: queryContext, ontologyContext: context)
    }

    /// Specify the graph index fields
    ///
    /// - Parameters:
    ///   - from: KeyPath to the source/subject field
    ///   - edge: KeyPath to the edge/predicate field
    ///   - to: KeyPath to the target/object field
    /// - Returns: Graph query builder
    public func index<V1, V2, V3>(
        _ from: KeyPath<T, V1>,
        _ edge: KeyPath<T, V2>,
        _ to: KeyPath<T, V3>
    ) throws -> GraphQueryBuilder<T> {
        return GraphQueryBuilder(
            queryContext: queryContext,
            index: try PropertyGraphIndexResolver.exact(
                signature: PropertyGraphIndexSignature(
                    sourceFieldName: T.fieldName(for: from),
                    labelFieldName: T.fieldName(for: edge),
                    targetFieldName: T.fieldName(for: to)
                ),
                for: T.self,
                in: queryContext
            ),
            ontologyContext: ontologyContext
        )
    }

    /// Select an entity-owned graph index by its exact declared name.
    public func index(named indexName: String) throws -> GraphQueryBuilder<T> {
        GraphQueryBuilder(
            queryContext: queryContext,
            index: try PropertyGraphIndexResolver.exact(
                named: indexName,
                for: T.self,
                in: queryContext
            ),
            ontologyContext: ontologyContext
        )
    }

    /// Use the default graph index (first GraphIndexKind found)
    ///
    /// - Returns: Graph query builder configured with the default index
    public func defaultIndex() throws -> GraphQueryBuilder<T> {
        return GraphQueryBuilder(
            queryContext: queryContext,
            index: try PropertyGraphIndexResolver.unique(
                for: T.self,
                in: queryContext
            ),
            ontologyContext: ontologyContext
        )
    }
}

// MARK: - Non-generic Graph Query Executor

/// Non-generic graph query executor
///
/// Can be used from both generic (FDBContext) and dynamic (CLI) code paths.
/// Accepts pre-resolved index metadata instead of using `T.self`.
///
/// **Query Pattern Optimization**:
/// Automatically selects the optimal index based on bound variables:
/// - `(from, edge, to)` → any index (point lookup)
/// - `(from, edge, ?)` → SPO index
/// - `(from, ?, to)` → SOP index (hexastore) or OSP (tripleStore)
/// - `(?, edge, to)` → POS index
/// - `(from, ?, ?)` → SPO index
/// - `(?, edge, ?)` → POS/PSO index
/// - `(?, ?, to)` → OSP index
public struct GraphQueryExecutor: Sendable {

    // MARK: - Types

    /// Query pattern for a single element
    public enum Pattern: Sendable {
        /// Match any value (wildcard)
        case any
        /// Match exact value
        case exact(String)

        /// Extract exact value if available
        var exactValue: String? {
            switch self {
            case .any: return nil
            case .exact(let value): return value
            }
        }
    }

    /// Query result containing matched edge components
    public struct GraphEdge: Sendable {
        public let from: String
        public let edge: String
        public let to: String

        public init(from: String, edge: String, to: String) {
            self.from = from
            self.edge = edge
            self.to = to
        }
    }

    // MARK: - Properties

    private let database: any StorageEngine
    private let indexSubspace: Subspace
    private let strategy: GraphIndexStrategy
    private let fromFieldName: String
    private let edgeFieldName: String
    private let toFieldName: String

    private var fromPattern: Pattern = .any
    private var edgePattern: Pattern = .any
    private var toPattern: Pattern = .any
    private var limitCount: Int?

    // MARK: - Initialization

    /// Initialize with pre-resolved index metadata
    ///
    /// - Parameters:
    ///   - database: Database for transaction execution
    ///   - indexSubspace: Pre-resolved `[I]/[indexName]` subspace
    ///   - strategy: Graph index strategy (adjacency/tripleStore/hexastore)
    ///   - fromFieldName: Name of the from/subject field
    ///   - edgeFieldName: Name of the edge/predicate field
    ///   - toFieldName: Name of the to/object field
    public init(
        database: any StorageEngine,
        indexSubspace: Subspace,
        strategy: GraphIndexStrategy,
        fromFieldName: String,
        edgeFieldName: String,
        toFieldName: String
    ) {
        self.database = database
        self.indexSubspace = indexSubspace
        self.strategy = strategy
        self.fromFieldName = fromFieldName
        self.edgeFieldName = edgeFieldName
        self.toFieldName = toFieldName
    }

    // MARK: - Pattern Setters

    /// Set from/subject pattern
    ///
    /// - Parameter value: Exact value to match
    /// - Returns: New executor with pattern set
    public func from(_ value: String) -> Self {
        var copy = self
        copy.fromPattern = .exact(value)
        return copy
    }

    /// Set edge/predicate pattern
    ///
    /// - Parameter value: Exact value to match
    /// - Returns: New executor with pattern set
    public func edge(_ value: String) -> Self {
        var copy = self
        copy.edgePattern = .exact(value)
        return copy
    }

    /// Set to/object pattern
    ///
    /// - Parameter value: Exact value to match
    /// - Returns: New executor with pattern set
    public func to(_ value: String) -> Self {
        var copy = self
        copy.toPattern = .exact(value)
        return copy
    }

    /// Set result limit
    ///
    /// - Parameter count: Maximum number of results
    /// - Returns: New executor with limit set
    public func limit(_ count: Int) -> Self {
        var copy = self
        copy.limitCount = count
        return copy
    }

    // MARK: - Execution

    /// Execute query and return matching edges
    ///
    /// Automatically selects the optimal index based on the query pattern
    /// and performs a range scan.
    ///
    /// - Returns: Array of matching graph edges
    public func execute() async throws -> [GraphEdge] {
        let ordering = selectOptimalOrdering(strategy: strategy)

        return try await database.withTransaction(configuration: .default) { transaction in
            try await self.scanIndex(
                ordering: ordering,
                indexSubspace: self.indexSubspace,
                transaction: transaction
            )
        }
    }

    // MARK: - Private Methods

    /// Select optimal index ordering based on query pattern
    private func selectOptimalOrdering(strategy: GraphIndexStrategy) -> GraphIndexOrdering {
        GraphIndexScanPlanner.ordering(
            strategy: strategy,
            subjectBound: isBound(fromPattern),
            predicateBound: isBound(edgePattern),
            objectBound: isBound(toPattern),
            graphBound: false
        )
    }

    private func isBound(_ pattern: Pattern) -> Bool {
        switch pattern {
        case .any: return false
        case .exact: return true
        }
    }

    private func scanIndex(
        ordering: GraphIndexOrdering,
        indexSubspace: Subspace,
        transaction: any Transaction
    ) async throws -> [GraphEdge] {
        var results: [GraphEdge] = []
        let orderingSubspace = subspaceForOrdering(ordering, base: indexSubspace)
        let (beginKey, endKey) = buildScanRange(ordering: ordering, subspace: orderingSubspace)

        let stream = try await transaction.collectRange(
            from: .firstGreaterOrEqual(beginKey),
            to: .firstGreaterOrEqual(endKey),
            snapshot: true
        )

        for (key, _) in stream {
            if let edge = try parseKey(key, ordering: ordering, subspace: orderingSubspace) {
                if matchesPatterns(edge) {
                    results.append(edge)
                    if let limit = limitCount, results.count >= limit {
                        break
                    }
                }
            }
        }

        return results
    }

    private func subspaceForOrdering(_ ordering: GraphIndexOrdering, base: Subspace) -> Subspace {
        let key: Int64
        switch ordering {
        case .out: key = 0
        case .in: key = 1
        case .spo: key = 2
        case .pos: key = 3
        case .osp: key = 4
        case .sop: key = 5
        case .pso: key = 6
        case .ops: key = 7
        case .gspo: key = 8
        case .gpos: key = 9
        case .gosp: key = 10
        }
        return base.subspace(key)
    }

    private func buildScanRange(ordering: GraphIndexOrdering, subspace: Subspace) -> (begin: Bytes, end: Bytes) {
        var prefixElements: [any TupleElement] = []
        let elementOrder = ordering.elementOrder
        let patterns = [fromPattern, edgePattern, toPattern]

        if ordering.isGraphFirst {
            // This query builder has no graph parameter, so namedGraphStore scans
            // all named graphs and applies only triple-position filters.
            return subspace.range()
        }

        for idx in elementOrder {
            let pattern = patterns[idx]
            switch pattern {
            case .exact(let value):
                prefixElements.append(value)
            case .any:
                break
            }
            if case .exact = pattern {
                continue
            } else {
                break
            }
        }

        if prefixElements.isEmpty {
            return subspace.range()
        } else {
            let prefixSubspace = Self.buildPrefixSubspace(from: subspace, elements: prefixElements)
            return prefixSubspace.range()
        }
    }

    /// Build a nested subspace from an array of tuple elements
    private static func buildPrefixSubspace(
        from base: Subspace,
        elements: [any TupleElement]
    ) -> Subspace {
        var result = base
        for element in elements {
            result = result.subspace(element)
        }
        return result
    }

    private func parseKey(_ key: Bytes, ordering: GraphIndexOrdering, subspace: Subspace) throws -> GraphEdge? {
        let tuple = try subspace.unpack(key)

        guard tuple.count >= 3 else {
            return nil
        }

        let elementOrder = ordering.elementOrder
        var fromValue: String?
        var edgeValue: String?
        var toValue: String?
        let tupleOffset = ordering.isGraphFirst ? 1 : 0

        for (tupleIdx, componentIdx) in elementOrder.enumerated() {
            let actualTupleIndex = tupleIdx + tupleOffset
            guard actualTupleIndex < tuple.count, let element = tuple[actualTupleIndex] else {
                continue
            }

            guard let stringValue = element as? String else {
                throw GraphIndexError.unexpectedElementType(
                    expected: "String",
                    actual: String(describing: type(of: element))
                )
            }

            switch componentIdx {
            case 0: fromValue = stringValue
            case 1: edgeValue = stringValue
            case 2: toValue = stringValue
            default: break
            }
        }

        guard let from = fromValue, let edge = edgeValue, let to = toValue else {
            return nil
        }

        return GraphEdge(from: from, edge: edge, to: to)
    }

    private func matchesPatterns(_ edge: GraphEdge) -> Bool {
        return matchesPattern(edge.from, pattern: fromPattern) &&
               matchesPattern(edge.edge, pattern: edgePattern) &&
               matchesPattern(edge.to, pattern: toPattern)
    }

    private func matchesPattern(_ value: String, pattern: Pattern) -> Bool {
        switch pattern {
        case .any:
            return true
        case .exact(let expected):
            return value == expected
        }
    }
}

// MARK: - Graph Query Builder (Generic Wrapper)

/// Graph query builder with SPARQL-like pattern matching
///
/// Generic wrapper around `GraphQueryExecutor`. Resolves `T.self` to index
/// metadata, then delegates execution to the non-generic executor.
///
/// **Query Pattern Optimization**:
/// The builder automatically selects the optimal index based on bound variables:
/// - `(from, edge, to)` → any index (point lookup)
/// - `(from, edge, ?)` → SPO index
/// - `(from, ?, to)` → SOP index (hexastore) or OSP (tripleStore)
/// - `(?, edge, to)` → POS index
/// - `(from, ?, ?)` → SPO index
/// - `(?, edge, ?)` → POS/PSO index
/// - `(?, ?, to)` → OSP index
public struct GraphQueryBuilder<T: Persistable>: Sendable {
    // MARK: - Type Aliases

    /// Query pattern for a single element
    public typealias Pattern = GraphQueryExecutor.Pattern

    /// Query result containing matched edge components
    public typealias GraphEdge = GraphQueryExecutor.GraphEdge

    // MARK: - Properties

    private let queryContext: IndexQueryContext
    private let index: DeclaredPropertyGraphIndex
    private let ontologyContext: OntologyContext?  // F-6: ontology context

    private var fromPattern: Pattern = .any
    private var edgePattern: Pattern = .any
    private var toPattern: Pattern = .any
    private var limitCount: Int?
    private var propertyFilters: [PropertyFilter] = []

    // MARK: - Initialization

    internal init(
        queryContext: IndexQueryContext,
        index: DeclaredPropertyGraphIndex,
        ontologyContext: OntologyContext? = nil
    ) {
        self.queryContext = queryContext
        self.index = index
        self.ontologyContext = ontologyContext
    }

    // MARK: - Pattern Setters

    /// Set from/subject pattern
    ///
    /// - Parameter value: Exact value to match
    /// - Returns: New builder with pattern set
    public func from(_ value: String) -> Self {
        var copy = self
        copy.fromPattern = .exact(value)
        return copy
    }

    /// Set edge/predicate pattern
    ///
    /// - Parameter value: Exact value to match
    /// - Returns: New builder with pattern set
    public func edge(_ value: String) -> Self {
        var copy = self
        copy.edgePattern = .exact(value)
        return copy
    }

    /// Set to/object pattern
    ///
    /// - Parameter value: Exact value to match
    /// - Returns: New builder with pattern set
    public func to(_ value: String) -> Self {
        var copy = self
        copy.toPattern = .exact(value)
        return copy
    }

    /// Set result limit
    ///
    /// - Parameter count: Maximum number of results
    /// - Returns: New builder with limit set
    public func limit(_ count: Int) -> Self {
        var copy = self
        copy.limitCount = count
        return copy
    }

    /// Add a type-safe property filter
    ///
    /// - Parameters:
    ///   - keyPath: KeyPath to the property field
    ///   - op: Comparison operator
    ///   - value: Value to compare against
    /// - Returns: New builder with filter added
    public func `where`<Value: Sendable>(
        _ keyPath: KeyPath<T, Value>,
        _ op: ComparisonOperator,
        _ value: Value
    ) throws -> Self {
        let fieldName = T.fieldName(for: keyPath)
        guard let fieldValue = FieldValue(value) else {
            throw GraphQueryError.unsupportedFilterValue(fieldName)
        }
        let filter = PropertyFilter(
            fieldName: fieldName,
            op: op,
            value: fieldValue
        )

        var copy = self
        copy.propertyFilters.append(filter)
        return copy
    }

    /// Add a type-erased property filter (for dynamic queries)
    ///
    /// - Parameters:
    ///   - fieldName: Name of the property field
    ///   - op: Comparison operator
    ///   - value: Value to compare against
    /// - Returns: New builder with filter added
    public func whereRaw(
        fieldName: String,
        _ op: ComparisonOperator,
        _ value: any Sendable
    ) throws -> Self {
        guard let fieldValue = FieldValue(value) else {
            throw GraphQueryError.unsupportedFilterValue(fieldName)
        }
        let filter = PropertyFilter(
            fieldName: fieldName,
            op: op,
            value: fieldValue
        )

        var copy = self
        copy.propertyFilters.append(filter)
        return copy
    }

    // MARK: - Execution

    /// Execute query and return matching edges
    ///
    /// Uses GraphPropertyScanner for property-aware edge scanning with early filtering.
    ///
    /// - Returns: Array of matching graph edges
    public func execute() async throws -> [GraphEdge] {
        if let limitCount, limitCount <= 0 {
            throw GraphQueryError.invalidLimit(limitCount)
        }

        let resolvedIndex = try await PropertyGraphIndexResolver.resolve(
            index,
            for: T.self,
            in: queryContext
        )

        // Extract exact values from patterns
        let fromValue = fromPattern.exactValue
        let edgeValue = edgePattern.exactValue
        let toValue = toPattern.exactValue

        // Execute scan with property filters
        return try await queryContext.withTransaction { transaction in
            let snapshot = GraphReadSnapshot(transaction: transaction)
            let scanner = GraphPropertyScanner(
                indexSubspace: resolvedIndex.indexSubspace,
                strategy: resolvedIndex.metadata.strategy,
                storedFieldNames: resolvedIndex.storedFieldNames,
                snapshot: snapshot
            )
            let stream = scanner.scanEdges(
                from: fromValue.map(GraphIdentity.identifier),
                edge: edgeValue.map(GraphIdentity.identifier),
                to: toValue.map(GraphIdentity.identifier),
                scope: .all,
                propertyFilters: propertyFilters.isEmpty ? nil : propertyFilters,
                transaction: transaction
            )

            var results: [GraphEdge] = []
            var count = 0
            for try await edgeWithProps in stream {
                results.append(GraphEdge(
                    from: try edgeWithProps.source.requirePropertyGraphIdentifier(),
                    edge: try edgeWithProps.edgeLabel.requirePropertyGraphIdentifier(),
                    to: try edgeWithProps.target.requirePropertyGraphIdentifier()
                ))
                count += 1
                if let limit = limitCount, count >= limit {
                    break
                }
            }
            return results
        }
    }

    /// Execute query and return matching items
    ///
    /// - Throws: `GraphQueryError.executeItemsNotSupported` always.
    ///
    /// Graph indexes store edges `(from, edge, to)` without item IDs,
    /// making it impossible to look up the original items from edges.
    ///
    /// **Alternative**: Use `execute()` to get edges, then query items
    /// by their field values using `context.filter()` or `context.query()`.
    public func executeItems() async throws -> [T] {
        throw GraphQueryError.executeItemsNotSupported
    }

}

// MARK: - FDBContext Extension

extension FDBContext {
    /// Start a graph query
    ///
    /// This method is available when you import `GraphIndex`.
    ///
    /// **Usage**:
    /// ```swift
    /// import GraphIndex
    ///
    /// // Find all edges from "Alice"
    /// let edges = try await context.graph(Statement.self)
    ///     .index(\.subject, \.predicate, \.object)
    ///     .from("Alice")
    ///     .execute()
    ///
    /// // Using default index
    /// let edges = try await context.graph(Statement.self)
    ///     .defaultIndex()
    ///     .edge("knows")
    ///     .execute()
    /// ```
    ///
    /// - Parameter type: The Persistable type to query
    /// - Returns: Entry point for configuring the graph query
    public func graph<T: Persistable>(_ type: T.Type) -> GraphEntryPoint<T> {
        GraphEntryPoint(queryContext: indexQueryContext)
    }
}

// MARK: - Graph Query Error

/// Errors for graph query operations
public enum GraphQueryError: Error, Sendable, CustomStringConvertible {
    case unsupportedFilterValue(String)
    case invalidLimit(Int)

    /// executeItems() is not supported for graph indexes
    ///
    /// Graph indexes store edges (from, edge, to) without item IDs.
    /// To fetch items, query by the edge field values directly using
    /// `context.filter()` or `context.query()` instead.
    case executeItemsNotSupported

    public var description: String {
        switch self {
        case .unsupportedFilterValue(let field):
            return "Graph property filter has an unsupported value for field \(field)"
        case .invalidLimit(let limit):
            return "Graph query result limit must be positive: \(limit)"
        case .executeItemsNotSupported:
            return "executeItems() is not supported for graph indexes. Graph indexes store edges without item IDs. Use execute() to get edges, or query by field values to fetch items."
        }
    }
}
