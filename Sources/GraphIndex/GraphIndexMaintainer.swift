// GraphIndexMaintainer.swift
// GraphIndex - Unified index maintainer for graph/RDF triple indexes
//
// Maintains graph edge indexes using configurable storage strategies.
// Supports adjacency (2-index), tripleStore (3-index), and hexastore (6-index).

import Foundation
import Core
import Graph
import DatabaseEngine
import FoundationDB

/// Maintainer for unified graph indexes
///
/// **Functionality**:
/// - Supports multiple storage strategies (adjacency, tripleStore, hexastore)
/// - Handles edge label field (optional for adjacency strategy)
/// - Efficient key generation with cached subspaces
///
/// **Index Structures by Strategy**:
///
/// ```
/// adjacency (2-index):
///   [out]/[edge]/[from]/[to]     - outgoing edges
///   [in]/[edge]/[to]/[from]      - incoming edges
///
/// tripleStore (3-index):
///   [spo]/[from]/[edge]/[to]     - Subject-Predicate-Object
///   [pos]/[edge]/[to]/[from]     - Predicate-Object-Subject
///   [osp]/[to]/[from]/[edge]     - Object-Subject-Predicate
///
/// hexastore (6-index):
///   All 6 permutations of (from, edge, to)
/// ```
///
/// **Reference**: Weiss, C., Karras, P., & Bernstein, A. (2008).
/// "Hexastore: sextuple indexing for semantic web data management"
public struct GraphIndexMaintainer<Item: Persistable>: IndexMaintainer {
    // MARK: - Properties

    /// Index definition
    public let index: Index

    /// Subspace for index storage
    public let subspace: Subspace

    /// ID expression for extracting item's unique identifier
    public let idExpression: KeyExpression

    /// From/Subject/Source field name
    private let fromField: String

    /// Edge/Predicate/Label field name (empty = no edge field)
    private let edgeField: String

    /// To/Object/Target field name
    private let toField: String

    /// Graph/Named Graph field name (nil = no graph field)
    private let graphField: String?

    /// Storage strategy
    private let strategy: GraphIndexStrategy

    /// Strategy-specific cached subspaces
    private let strategySubspaces: StrategySubspaces

    // MARK: - Initialization

    /// Initialize graph index maintainer
    ///
    /// - Parameters:
    ///   - index: Index definition
    ///   - subspace: FDB subspace for this index
    ///   - idExpression: Expression for extracting item's unique identifier
    ///   - fromField: From node field name
    ///   - edgeField: Edge label field name (empty for no edge field)
    ///   - toField: To node field name
    ///   - graphField: Graph field name (nil for no graph field)
    ///   - strategy: Storage strategy
    public init(
        index: Index,
        subspace: Subspace,
        idExpression: KeyExpression,
        fromField: String,
        edgeField: String,
        toField: String,
        graphField: String? = nil,
        strategy: GraphIndexStrategy
    ) {
        self.index = index
        self.subspace = subspace
        self.idExpression = idExpression
        self.fromField = fromField
        self.edgeField = edgeField
        self.toField = toField
        self.graphField = graphField
        self.strategy = strategy
        self.strategySubspaces = StrategySubspaces(base: subspace, strategy: strategy)
    }

    // MARK: - IndexMaintainer Protocol

    /// Update index when edge changes
    ///
    /// - Parameters:
    ///   - oldItem: Previous edge (nil for insert)
    ///   - newItem: New edge (nil for delete)
    ///   - transaction: FDB transaction
    public func updateIndex(
        oldItem: Item?,
        newItem: Item?,
        transaction: any TransactionProtocol
    ) async throws {
        // Remove old index entries
        if let oldItem = oldItem {
            let keys = try buildIndexKeys(for: oldItem)
            for key in keys {
                transaction.clear(key: key)
            }
        }

        // Add new index entries
        if let newItem = newItem {
            let keys = try buildIndexKeys(for: newItem)
            let value = try CoveringValueBuilder.build(for: newItem, storedFieldNames: index.storedFieldNames)
            for key in keys {
                transaction.setValue(value, for: key)
            }
        }
    }

    /// Build index entries for an edge during batch indexing
    ///
    /// - Parameters:
    ///   - item: Edge to index
    ///   - id: The edge's unique identifier
    ///   - transaction: FDB transaction
    public func scanItem(
        _ item: Item,
        id: Tuple,
        transaction: any TransactionProtocol
    ) async throws {
        let keys = try buildIndexKeys(for: item)
        let value = try CoveringValueBuilder.build(for: item, storedFieldNames: index.storedFieldNames)
        for key in keys {
            transaction.setValue(value, for: key)
        }
    }

    /// Compute expected index keys for an edge (for scrubber verification)
    ///
    /// Returns the index keys that should exist for this edge.
    public func computeIndexKeys(
        for item: Item,
        id: Tuple
    ) async throws -> [FDB.Bytes] {
        return try buildIndexKeys(for: item)
    }

    // MARK: - Private Methods

    /// Build all index keys for an item based on the strategy
    ///
    /// **Sparse index behavior**:
    /// If any required field (from, to) is nil, returns an empty array (no index entries).
    private func buildIndexKeys(for item: Item) throws -> [FDB.Bytes] {
        // Sparse index: if any required field is nil, skip indexing
        let from: any TupleElement
        let edge: any TupleElement
        let to: any TupleElement
        let graph: (any TupleElement)?

        do {
            from = try extractField(from: item, fieldName: fromField)
            edge = try extractEdgeField(from: item)
            to = try extractField(from: item, fieldName: toField)
            graph = try extractGraphField(from: item)
        } catch DataAccessError.nilValueCannotBeIndexed {
            // Sparse index: nil field values are not indexed
            return []
        }

        switch strategy {
        case .adjacency:
            return try buildAdjacencyKeys(from: from, edge: edge, to: to, graph: graph)
        case .tripleStore:
            return try buildTripleStoreKeys(from: from, edge: edge, to: to, graph: graph)
        case .hexastore:
            return try buildHexastoreKeys(from: from, edge: edge, to: to, graph: graph)
        }
    }

    /// Build adjacency strategy keys (2 indexes)
    ///
    /// - out: [edge]/[from]/[to]{/[graph]} - for outgoing edge queries
    /// - in: [edge]/[to]/[from]{/[graph]} - for incoming edge queries
    private func buildAdjacencyKeys(
        from: any TupleElement,
        edge: any TupleElement,
        to: any TupleElement,
        graph: (any TupleElement)?
    ) throws -> [FDB.Bytes] {
        var keys: [FDB.Bytes] = []
        keys.reserveCapacity(2)

        // [out]/[edge]/[from]/[to]{/[graph]}
        var outElements: [any TupleElement] = [edge, from, to]
        if let graph { outElements.append(graph) }
        let outKey = strategySubspaces.out.pack(Tuple(outElements))
        try validateKeySize(outKey)
        keys.append(outKey)

        // [in]/[edge]/[to]/[from]{/[graph]}
        var inElements: [any TupleElement] = [edge, to, from]
        if let graph { inElements.append(graph) }
        let inKey = strategySubspaces.in.pack(Tuple(inElements))
        try validateKeySize(inKey)
        keys.append(inKey)

        return keys
    }

    /// Build tripleStore strategy keys (3 indexes: SPO/POS/OSP)
    ///
    /// Standard RDF triple store pattern covering most SPARQL query patterns.
    ///
    /// - spo: [from]/[edge]/[to]{/[graph]} - S??, SP?, SPO queries
    /// - pos: [edge]/[to]/[from]{/[graph]} - ?P?, ?PO queries
    /// - osp: [to]/[from]/[edge]{/[graph]} - ??O queries
    private func buildTripleStoreKeys(
        from: any TupleElement,
        edge: any TupleElement,
        to: any TupleElement,
        graph: (any TupleElement)?
    ) throws -> [FDB.Bytes] {
        var keys: [FDB.Bytes] = []
        keys.reserveCapacity(3)

        // [spo]/[from]/[edge]/[to]{/[graph]}
        var spoElements: [any TupleElement] = [from, edge, to]
        if let graph { spoElements.append(graph) }
        let spoKey = strategySubspaces.spo.pack(Tuple(spoElements))
        try validateKeySize(spoKey)
        keys.append(spoKey)

        // [pos]/[edge]/[to]/[from]{/[graph]}
        var posElements: [any TupleElement] = [edge, to, from]
        if let graph { posElements.append(graph) }
        let posKey = strategySubspaces.pos.pack(Tuple(posElements))
        try validateKeySize(posKey)
        keys.append(posKey)

        // [osp]/[to]/[from]/[edge]{/[graph]}
        var ospElements: [any TupleElement] = [to, from, edge]
        if let graph { ospElements.append(graph) }
        let ospKey = strategySubspaces.osp.pack(Tuple(ospElements))
        try validateKeySize(ospKey)
        keys.append(ospKey)

        return keys
    }

    /// Build hexastore strategy keys (6 indexes: all permutations)
    ///
    /// Maximum query performance with O(1) index selection for any pattern.
    private func buildHexastoreKeys(
        from: any TupleElement,
        edge: any TupleElement,
        to: any TupleElement,
        graph: (any TupleElement)?
    ) throws -> [FDB.Bytes] {
        var keys: [FDB.Bytes] = []
        keys.reserveCapacity(6)

        // SPO: [from]/[edge]/[to]{/[graph]}
        var spoElements: [any TupleElement] = [from, edge, to]
        if let graph { spoElements.append(graph) }
        let spoKey = strategySubspaces.spo.pack(Tuple(spoElements))
        try validateKeySize(spoKey)
        keys.append(spoKey)

        // SOP: [from]/[to]/[edge]{/[graph]}
        var sopElements: [any TupleElement] = [from, to, edge]
        if let graph { sopElements.append(graph) }
        let sopKey = strategySubspaces.sop.pack(Tuple(sopElements))
        try validateKeySize(sopKey)
        keys.append(sopKey)

        // PSO: [edge]/[from]/[to]{/[graph]}
        var psoElements: [any TupleElement] = [edge, from, to]
        if let graph { psoElements.append(graph) }
        let psoKey = strategySubspaces.pso.pack(Tuple(psoElements))
        try validateKeySize(psoKey)
        keys.append(psoKey)

        // POS: [edge]/[to]/[from]{/[graph]}
        var posElements: [any TupleElement] = [edge, to, from]
        if let graph { posElements.append(graph) }
        let posKey = strategySubspaces.pos.pack(Tuple(posElements))
        try validateKeySize(posKey)
        keys.append(posKey)

        // OSP: [to]/[from]/[edge]{/[graph]}
        var ospElements: [any TupleElement] = [to, from, edge]
        if let graph { ospElements.append(graph) }
        let ospKey = strategySubspaces.osp.pack(Tuple(ospElements))
        try validateKeySize(ospKey)
        keys.append(ospKey)

        // OPS: [to]/[edge]/[from]{/[graph]}
        var opsElements: [any TupleElement] = [to, edge, from]
        if let graph { opsElements.append(graph) }
        let opsKey = strategySubspaces.ops.pack(Tuple(opsElements))
        try validateKeySize(opsKey)
        keys.append(opsKey)

        return keys
    }

    /// Extract edge field value (or empty string if no edge field)
    private func extractEdgeField(from item: Item) throws -> any TupleElement {
        if edgeField.isEmpty {
            // No edge field - use empty string as default
            return ""
        }
        return try extractField(from: item, fieldName: edgeField)
    }

    /// Extract graph field value (nil if no graph field configured)
    private func extractGraphField(from item: Item) throws -> (any TupleElement)? {
        guard let graphField else { return nil }
        return try extractField(from: item, fieldName: graphField)
    }

    /// Extract a field value from an item
    ///
    /// **Sparse index behavior**:
    /// If the field value is nil (e.g., Optional FK field), throws
    /// `DataAccessError.nilValueCannotBeIndexed` which is caught by
    /// `buildIndexKeys` to skip indexing.
    private func extractField(from item: Item, fieldName: String) throws -> any TupleElement {
        guard let value = item[dynamicMember: fieldName] else {
            // Sparse index: nil value cannot be indexed
            throw DataAccessError.nilValueCannotBeIndexed
        }

        guard let tupleElement = value as? any TupleElement else {
            throw GraphIndexError.invalidFieldType(
                fieldName: fieldName,
                expectedType: "TupleElement",
                actualType: String(describing: type(of: value))
            )
        }

        return tupleElement
    }
}

// MARK: - StrategySubspaces

/// Cached subspaces for each index ordering
///
/// Pre-computed at initialization to avoid repeated string operations.
struct StrategySubspaces: Sendable {
    // Adjacency orderings
    let out: Subspace
    let `in`: Subspace

    // TripleStore orderings (SPO/POS/OSP)
    let spo: Subspace
    let pos: Subspace
    let osp: Subspace

    // Hexastore additional orderings (SOP/PSO/OPS)
    let sop: Subspace
    let pso: Subspace
    let ops: Subspace

    /// Initialize subspaces based on strategy
    ///
    /// Only creates subspaces needed for the given strategy to minimize memory.
    init(base: Subspace, strategy: GraphIndexStrategy) {
        // Use integer keys for storage efficiency
        // Keys: 0=out, 1=in, 2=spo, 3=pos, 4=osp, 5=sop, 6=pso, 7=ops
        self.out = base.subspace(Int64(0))
        self.in = base.subspace(Int64(1))
        self.spo = base.subspace(Int64(2))
        self.pos = base.subspace(Int64(3))
        self.osp = base.subspace(Int64(4))
        self.sop = base.subspace(Int64(5))
        self.pso = base.subspace(Int64(6))
        self.ops = base.subspace(Int64(7))
    }
}

// MARK: - Errors

/// Errors specific to graph index operations
public enum GraphIndexError: Error, CustomStringConvertible {
    case fieldNotFound(fieldName: String, itemType: String)
    case invalidFieldType(fieldName: String, expectedType: String, actualType: String)
    case unsupportedQueryPattern(pattern: String, strategy: GraphIndexStrategy)
    case unexpectedElementType(expected: String, actual: String)

    public var description: String {
        switch self {
        case .fieldNotFound(let fieldName, let itemType):
            return "Field '\(fieldName)' not found in '\(itemType)'"
        case .invalidFieldType(let fieldName, let expectedType, let actualType):
            return "Field '\(fieldName)' has invalid type: expected \(expectedType), got \(actualType)"
        case .unsupportedQueryPattern(let pattern, let strategy):
            return "Query pattern '\(pattern)' is not optimally supported by \(strategy) strategy"
        case .unexpectedElementType(let expected, let actual):
            return "Unexpected TupleElement type: expected \(expected), got \(actual)"
        }
    }
}
