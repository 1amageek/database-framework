// GraphQuery.swift
// GraphIndex - Query extension for graph/RDF indexes
//
// Provides FDBContext extension and query builder following the standard pattern.

import Foundation
import Core
import Graph
import DatabaseEngine
import FoundationDB

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

    internal init(queryContext: IndexQueryContext) {
        self.queryContext = queryContext
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
    ) -> GraphQueryBuilder<T> {
        let fromField = T.fieldName(for: from)
        let edgeField = T.fieldName(for: edge)
        let toField = T.fieldName(for: to)
        return GraphQueryBuilder(
            queryContext: queryContext,
            fromFieldName: fromField,
            edgeFieldName: edgeField,
            toFieldName: toField
        )
    }

    /// Use the default graph index (first GraphIndexKind found)
    ///
    /// - Returns: Graph query builder configured with the default index
    public func defaultIndex() -> GraphQueryBuilder<T> {
        // Find the first GraphIndexKind for this type
        let descriptor = T.indexDescriptors.first { desc in
            desc.kindIdentifier == GraphIndexKind<T>.identifier
        }

        guard let desc = descriptor,
              let kind = desc.kind as? GraphIndexKind<T> else {
            // Return a builder that will fail on execute
            return GraphQueryBuilder(
                queryContext: queryContext,
                fromFieldName: "",
                edgeFieldName: "",
                toFieldName: ""
            )
        }

        return GraphQueryBuilder(
            queryContext: queryContext,
            fromFieldName: kind.fromField,
            edgeFieldName: kind.edgeField,
            toFieldName: kind.toField
        )
    }
}

// MARK: - Graph Query Builder

/// Graph query builder with SPARQL-like pattern matching
///
/// Supports triple pattern queries using the graph index with automatic
/// selection of the optimal index ordering based on the query pattern.
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
    // MARK: - Types

    /// Query pattern for a single element
    public enum Pattern: Sendable {
        /// Match any value (wildcard)
        case any
        /// Match exact value
        case exact(String)
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

    private let queryContext: IndexQueryContext
    private let fromFieldName: String
    private let edgeFieldName: String
    private let toFieldName: String

    private var fromPattern: Pattern = .any
    private var edgePattern: Pattern = .any
    private var toPattern: Pattern = .any
    private var limitCount: Int?

    // MARK: - Initialization

    internal init(
        queryContext: IndexQueryContext,
        fromFieldName: String,
        edgeFieldName: String,
        toFieldName: String
    ) {
        self.queryContext = queryContext
        self.fromFieldName = fromFieldName
        self.edgeFieldName = edgeFieldName
        self.toFieldName = toFieldName
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

    // MARK: - Execution

    /// Execute query and return matching edges
    ///
    /// Automatically selects the optimal index based on the query pattern
    /// and performs a range scan.
    ///
    /// - Returns: Array of matching graph edges
    public func execute() async throws -> [GraphEdge] {
        guard !fromFieldName.isEmpty else {
            throw GraphQueryError.indexNotConfigured
        }

        let indexName = buildIndexName()

        guard let indexDescriptor = queryContext.schema.indexDescriptor(named: indexName),
              let kind = indexDescriptor.kind as? GraphIndexKind<T> else {
            throw GraphQueryError.indexNotFound(indexName)
        }

        let strategy = kind.strategy
        let ordering = selectOptimalOrdering(strategy: strategy)

        let typeSubspace = try await queryContext.indexSubspace(for: T.self)
        let indexSubspace = typeSubspace.subspace(indexName)

        return try await queryContext.withTransaction { transaction in
            try await self.scanIndex(
                ordering: ordering,
                indexSubspace: indexSubspace,
                transaction: transaction
            )
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

    // MARK: - Private Methods

    private func buildIndexName() -> String {
        "\(T.persistableType)_graph_\(fromFieldName)_\(edgeFieldName)_\(toFieldName)"
    }

    /// Select optimal index ordering based on query pattern
    private func selectOptimalOrdering(strategy: GraphIndexStrategy) -> GraphIndexOrdering {
        let fromBound = isBound(fromPattern)
        let edgeBound = isBound(edgePattern)
        let toBound = isBound(toPattern)

        switch (fromBound, edgeBound, toBound) {
        case (true, true, true):
            return strategy == .adjacency ? .out : .spo
        case (true, true, false):
            return strategy == .adjacency ? .out : .spo
        case (true, false, true):
            return strategy == .hexastore ? .sop : .osp
        case (false, true, true):
            return strategy == .adjacency ? .in : .pos
        case (true, false, false):
            return strategy == .adjacency ? .out : .spo
        case (false, true, false):
            return strategy == .hexastore ? .pso : .pos
        case (false, false, true):
            return strategy == .adjacency ? .in : .osp
        case (false, false, false):
            return strategy == .adjacency ? .out : .spo
        }
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
        transaction: any TransactionProtocol
    ) async throws -> [GraphEdge] {
        var results: [GraphEdge] = []
        let orderingSubspace = subspaceForOrdering(ordering, base: indexSubspace)
        let (beginKey, endKey) = buildScanRange(ordering: ordering, subspace: orderingSubspace)

        let stream = transaction.getRange(
            beginSelector: .firstGreaterOrEqual(beginKey),
            endSelector: .firstGreaterOrEqual(endKey),
            snapshot: true
        )

        for try await (key, _) in stream {
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
        }
        return base.subspace(key)
    }

    private func buildScanRange(ordering: GraphIndexOrdering, subspace: Subspace) -> (begin: FDB.Bytes, end: FDB.Bytes) {
        var prefixElements: [any TupleElement] = []
        let elementOrder = ordering.elementOrder
        let patterns = [fromPattern, edgePattern, toPattern]

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
            // Build prefix subspace using proper Subspace API
            let prefixSubspace = Self.buildPrefixSubspace(from: subspace, elements: prefixElements)
            return prefixSubspace.range()
        }
    }

    /// Build a nested subspace from an array of tuple elements
    ///
    /// Uses the proper Subspace API pattern instead of manual byte concatenation.
    /// This is equivalent to chaining `subspace.subspace(elem1).subspace(elem2)...`
    ///
    /// - Parameters:
    ///   - base: The base subspace to extend
    ///   - elements: The tuple elements to nest
    /// - Returns: A new subspace with all elements nested
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

    private func parseKey(_ key: FDB.Bytes, ordering: GraphIndexOrdering, subspace: Subspace) throws -> GraphEdge? {
        let tuple = try subspace.unpack(key)

        guard tuple.count >= 3 else {
            return nil
        }

        let elementOrder = ordering.elementOrder
        var fromValue: String?
        var edgeValue: String?
        var toValue: String?

        for (tupleIdx, componentIdx) in elementOrder.enumerated() {
            guard tupleIdx < tuple.count, let element = tuple[tupleIdx] else {
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
public enum GraphQueryError: Error, CustomStringConvertible {
    /// Index not configured
    case indexNotConfigured

    /// Index not found
    case indexNotFound(String)

    /// executeItems() is not supported for graph indexes
    ///
    /// Graph indexes store edges (from, edge, to) without item IDs.
    /// To fetch items, query by the edge field values directly using
    /// `context.filter()` or `context.query()` instead.
    case executeItemsNotSupported

    public var description: String {
        switch self {
        case .indexNotConfigured:
            return "Graph index not configured. Use .index() to specify fields or .defaultIndex()."
        case .indexNotFound(let name):
            return "Graph index not found: \(name)"
        case .executeItemsNotSupported:
            return "executeItems() is not supported for graph indexes. Graph indexes store edges without item IDs. Use execute() to get edges, or query by field values to fetch items."
        }
    }
}
