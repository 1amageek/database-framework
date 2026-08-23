// GraphQuery.swift
// GraphIndex - Query extension for graph/RDF indexes
//
// Provides DatabaseContext extension and query builder following the standard pattern.

import DatabaseEngine
import DatabaseKit
import DatabaseTypes
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
        _ from: Field<T, V1>,
        _ edge: Field<T, V2>,
        _ to: Field<T, V3>
    ) throws -> GraphQueryBuilder<T> {
        return GraphQueryBuilder(
            queryContext: queryContext,
            index: try PropertyGraphIndexResolver.exact(
                signature: PropertyGraphIndexSignature(
                    sourceFieldName: from.name,
                    labelFieldName: edge.name,
                    targetFieldName: to.name
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

    /// Use the first declared property-graph index.
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

/// One bound or wildcard component in a property-graph query.
public enum GraphQueryPattern: Sendable {
    case any
    case exact(String)

    var exactValue: String? {
        switch self {
        case .any: return nil
        case .exact(let value): return value
        }
    }
}

/// One property-graph edge returned by `GraphQueryBuilder`.
public struct GraphQueryEdge: Sendable, Equatable {
    public let from: String
    public let edge: String
    public let to: String

    public init(from: String, edge: String, to: String) {
        self.from = from
        self.edge = edge
        self.to = to
    }
}

// MARK: - Graph Query Builder (Generic Wrapper)

/// Graph query builder with SPARQL-like pattern matching
///
/// Resolves the entity-owned index and executes through the context-bound
/// authorization, transaction, and lifecycle scopes.
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
    public typealias Pattern = GraphQueryPattern

    /// Query result containing matched edge components
    public typealias GraphEdge = GraphQueryEdge

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
    public func `where`<Value: FieldValueRepresentable>(
        _ field: Field<T, Value>,
        _ op: ComparisonOperator,
        _ value: Value
    ) -> Self {
        let filter = PropertyFilter(
            fieldName: field.name,
            op: op,
            value: value.fieldValue
        )

        var copy = self
        copy.propertyFilters.append(filter)
        return copy
    }

    /// Add a property filter using the canonical field value model.
    ///
    /// - Parameters:
    ///   - fieldName: Name of the property field
    ///   - op: Comparison operator
    ///   - value: Value to compare against
    /// - Returns: New builder with filter added
    public func whereField(
        named fieldName: String,
        _ op: ComparisonOperator,
        _ value: FieldValue
    ) -> Self {
        let filter = PropertyFilter(
            fieldName: fieldName,
            op: op,
            value: value
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

        // Extract exact values from patterns
        let fromValue = fromPattern.exactValue
        let edgeValue = edgePattern.exactValue
        let toValue = toPattern.exactValue

        // Execute scan with property filters
        return try await PropertyGraphIndexResolver.withResolved(
            index,
            for: T.self,
            in: queryContext,
            authorization: IndexReadAuthorization(
                limit: limitCount,
                offset: nil,
                orderBy: nil
            )
        ) { resolvedIndex, transaction in
            guard let resolvedIndex else {
                return []
            }
            let snapshot = GraphReadSnapshot(
                transaction: transaction,
                monotonicClock: queryContext.context.container.monotonicClock
            )
            let scanner = GraphPropertyScanner(
                indexSubspace: resolvedIndex.indexSubspace,
                strategy: resolvedIndex.configuration.strategy,
                includedFieldNames: resolvedIndex.includedFieldNames,
                snapshot: snapshot
            )
            let stream = scanner.scanEdges(
                from: fromValue.map(GraphIdentity.identifier),
                edge: edgeValue.map(GraphIdentity.identifier),
                to: toValue.map(GraphIdentity.identifier),
                graphTarget: .all,
                propertyFilters: propertyFilters.isEmpty ? nil : propertyFilters,
                transaction: transaction
            )

            var results: [GraphEdge] = []
            var count = 0
            var edgeCursor = stream.makeCursor()
            while let edgeWithProps = try await edgeCursor.next() {
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

// MARK: - DatabaseContext Extension

extension DatabaseContext {
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
    case invalidLimit(Int)

    /// executeItems() is not supported for graph indexes
    ///
    /// Graph indexes store edges (from, edge, to) without item IDs.
    /// To fetch items, query by the edge field values directly using
    /// `context.filter()` or `context.query()` instead.
    case executeItemsNotSupported

    public var description: String {
        switch self {
        case .invalidLimit(let limit):
            return "Graph query result limit must be positive: \(limit)"
        case .executeItemsNotSupported:
            return "executeItems() is not supported for graph indexes. Graph indexes store edges without item IDs. Use execute() to get edges, or query by field values to fetch items."
        }
    }
}
