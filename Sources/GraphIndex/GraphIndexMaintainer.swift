// GraphIndexMaintainer.swift
// GraphIndex - Property graph index maintainer
//
// Maintains graph edge indexes using configurable storage strategies.
// Supports adjacency (2-index), tripleStore (3-index), and hexastore (6-index).

import DatabaseKit
import DatabaseEngine
import DatabaseTypes
import StorageKit

/// Maintainer for property graph indexes.
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
///   [out]/[from]/[edge]/[to]     - outgoing edges
///   [in]/[to]/[edge]/[from]      - incoming edges
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
public struct GraphIndexMaintainer<Item: PersistedEntityValue>: IndexMaintainer {
    // MARK: - Properties

    /// Index definition
    public let index: Index

    /// Subspace for index storage
    public let subspace: Subspace

    /// ID expression for extracting item's unique identifier
    public let idExpression: KeyExpression

    /// Source node field.
    private let sourceField: String

    /// Edge label field. `nil` represents one implicit edge label.
    private let labelField: String?

    /// Target node field.
    private let targetField: String

    /// Optional property-graph namespace field.
    private let namespaceField: String?

    /// Storage strategy
    private let strategy: PropertyGraphIndexStrategy

    /// Strategy-specific cached subspaces
    private let strategySubspaces: StrategySubspaces

    // MARK: - Initialization

    /// Initialize graph index maintainer
    ///
    /// - Parameters:
    ///   - index: Index definition
    ///   - subspace: FDB subspace for this index
    ///   - idExpression: Expression for extracting item's unique identifier
    ///   - metadata: Validated property-graph declaration.
    public init(
        index: Index,
        subspace: Subspace,
        idExpression: KeyExpression,
        metadata: PropertyGraphIndexMetadata
    ) throws {
        self.index = index
        self.subspace = subspace
        self.idExpression = idExpression
        self.sourceField = metadata.sourceFieldName
        self.labelField = metadata.labelFieldName.isEmpty
            ? nil
            : metadata.labelFieldName
        self.targetField = metadata.targetFieldName
        self.namespaceField = metadata.namespaceFieldName
        self.strategy = metadata.declarativeStrategy
        self.strategySubspaces = StrategySubspaces(
            base: subspace,
            strategy: metadata.declarativeStrategy
        )
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
        transaction: any TransactionAccess
    ) async throws {
        // Remove old index entries
        if let oldItem = oldItem {
            let keys = try buildIndexKeys(for: oldItem)
            for key in keys {
                try transaction.clear(key: key)
            }
        }

        // Add new index entries
        if let newItem = newItem {
            let keys = try buildIndexKeys(for: newItem)
            let value = try CoveringValueBuilder.build(for: newItem, index: index)
            for key in keys {
                try transaction.setValue(value, for: key)
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
        transaction: any TransactionAccess
    ) async throws {
        let keys = try buildIndexKeys(for: item)
        let value = try CoveringValueBuilder.build(for: item, index: index)
        for key in keys {
            try transaction.setValue(value, for: key)
        }
    }

    /// Compute expected index keys for an edge (for scrubber verification)
    ///
    /// Returns the index keys that should exist for this edge.
    public func computeIndexKeys(
        for item: Item,
        id: Tuple
    ) async throws -> [ByteString] {
        return try buildIndexKeys(for: item)
    }

    // MARK: - Private Methods

    /// Build all index keys for an item based on the strategy
    ///
    /// **Sparse index behavior**:
    /// If any required field (from, to) is nil, returns an empty array (no index entries).
    private func buildIndexKeys(for item: Item) throws -> [ByteString] {
        // Sparse index: if any required field is nil, skip indexing
        let from: any TupleElement
        let edge: any TupleElement
        let to: any TupleElement
        let graph: (any TupleElement)?

        do {
            from = try extractField(from: item, field: sourceField)
            edge = try extractEdgeField(from: item)
            to = try extractField(from: item, field: targetField)
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
        case .namedGraphStore:
            return try buildNamedGraphStoreKeys(from: from, edge: edge, to: to, graph: graph)
        }
    }

    /// Build adjacency strategy keys (2 indexes)
    ///
    /// - out: [from]/[edge]/[to]{/[graph]} - for outgoing edge queries
    /// - in: [to]/[edge]/[from]{/[graph]} - for incoming edge queries
    private func buildAdjacencyKeys(
        from: any TupleElement,
        edge: any TupleElement,
        to: any TupleElement,
        graph: (any TupleElement)?
    ) throws -> [ByteString] {
        var keys: [ByteString] = []
        keys.reserveCapacity(2)

        // [out]/[from]/[edge]/[to]{/[graph]}
        var outElements: [any TupleElement] = [from, edge, to]
        if let graph { outElements.append(graph) }
        let outKey = strategySubspaces.out.pack(Tuple(outElements))
        try validateKeySize(outKey)
        keys.append(outKey)

        // [in]/[to]/[edge]/[from]{/[graph]}
        var inElements: [any TupleElement] = [to, edge, from]
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
    ) throws -> [ByteString] {
        var keys: [ByteString] = []
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
    ) throws -> [ByteString] {
        var keys: [ByteString] = []
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

    /// Build namedGraphStore strategy keys (3 graph-first indexes: GSPO/GPOS/GOSP)
    private func buildNamedGraphStoreKeys(
        from: any TupleElement,
        edge: any TupleElement,
        to: any TupleElement,
        graph: (any TupleElement)?
    ) throws -> [ByteString] {
        var keys: [ByteString] = []
        keys.reserveCapacity(3)

        // An empty byte value is disjoint from every property-graph String,
        // including the valid empty identifier.
        let graphElement: any TupleElement = graph ?? ByteString()

        let gspoKey = strategySubspaces.gspo.pack(Tuple([graphElement, from, edge, to]))
        try validateKeySize(gspoKey)
        keys.append(gspoKey)

        let gposKey = strategySubspaces.gpos.pack(Tuple([graphElement, edge, to, from]))
        try validateKeySize(gposKey)
        keys.append(gposKey)

        let gospKey = strategySubspaces.gosp.pack(Tuple([graphElement, to, from, edge]))
        try validateKeySize(gospKey)
        keys.append(gospKey)

        return keys
    }

    /// Extract edge field value (or empty string if no edge field)
    private func extractEdgeField(from item: Item) throws -> any TupleElement {
        guard let labelField else {
            // No edge field - use empty string as default
            return ""
        }
        return try extractField(from: item, field: labelField)
    }

    /// Extract graph field value (nil if no graph field configured)
    private func extractGraphField(from item: Item) throws -> (any TupleElement)? {
        guard let namespaceField else { return nil }

        // Graph field is special: nil means "default graph", not a missing field
        // Unlike other fields, nil graph values should be indexed
        guard let value = try item.persistedValue(
            forFieldNamed: namespaceField
        )
        else {
            throw GraphIndexError.fieldNotFound(
                fieldName: namespaceField,
                itemType: item.persistedEntityName
            )
        }
        guard value != .null else { return nil }
        return try tupleElement(value, fieldName: namespaceField)
    }

    /// Extract a field value from an item
    ///
    /// **Sparse index behavior**:
    /// If the field value is nil (e.g., Optional FK field), throws
    /// `DataAccessError.nilValueCannotBeIndexed` which is caught by
    /// `buildIndexKeys` to skip indexing.
    private func extractField(
        from item: Item,
        field: String
    ) throws -> any TupleElement {
        guard let value = try item.persistedValue(forFieldNamed: field) else {
            throw GraphIndexError.fieldNotFound(
                fieldName: field,
                itemType: item.persistedEntityName
            )
        }
        guard value != .null else {
            throw DataAccessError.nilValueCannotBeIndexed
        }
        return try tupleElement(value, fieldName: field)
    }

    private func tupleElement(
        _ value: FieldValue,
        fieldName: String
    ) throws -> any TupleElement {
        guard case .string(let string) = value else {
            throw GraphIndexError.invalidFieldType(
                fieldName: fieldName,
                expectedType: "String",
                actualType: GraphValueSemanticName.field(value)
            )
        }
        return string
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

    // Named graph orderings (GSPO/GPOS/GOSP)
    let gspo: Subspace
    let gpos: Subspace
    let gosp: Subspace

    /// Initialize subspaces based on strategy
    ///
    /// Only creates subspaces needed for the given strategy to minimize memory.
    init(base: Subspace, strategy: PropertyGraphIndexStrategy) {
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
        self.gspo = base.subspace(Int64(8))
        self.gpos = base.subspace(Int64(9))
        self.gosp = base.subspace(Int64(10))
    }
}

// MARK: - Errors

/// Errors specific to graph index operations
public enum GraphIndexError: Error, CustomStringConvertible, Sendable {
    case fieldNotFound(fieldName: String, itemType: String)
    case invalidFieldType(fieldName: String, expectedType: String, actualType: String)
    case unsupportedQueryPattern(pattern: String, strategy: GraphIndexStrategy)
    case unexpectedElementType(expected: String, actual: String)
    case malformedIndexKey(ordering: GraphIndexOrdering, elementCount: Int)
    case identityRepresentationMismatch(
        expected: GraphIdentity.Representation,
        actual: GraphIdentity.Representation
    )
    case invalidRDFPredicate
    case invalidRDFSubject
    case invalidRDFObject
    case invalidRDFGraphName
    case invalidRDFEncoding(RDFTermStorageError)
    case rdfPhysicalIndex(RDFQuadIndexPhysicalCodecError)
    case invalidScanState

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
        case .malformedIndexKey(let ordering, let elementCount):
            return "Malformed \(ordering) graph index key with \(elementCount) elements"
        case .identityRepresentationMismatch(let expected, let actual):
            return "Graph identity representation mismatch: expected \(expected), got \(actual)"
        case .invalidRDFPredicate:
            return "RDF graph index key contains a non-IRI predicate"
        case .invalidRDFSubject:
            return "RDF graph index key contains an invalid subject"
        case .invalidRDFObject:
            return "RDF graph index key contains an invalid object"
        case .invalidRDFGraphName:
            return "RDF graph index key contains an invalid graph name"
        case .invalidRDFEncoding(let reason):
            return "RDF graph index key contains invalid canonical bytes: \(reason)"
        case .rdfPhysicalIndex(let reason):
            return "RDF graph index key has an invalid physical layout: \(reason)"
        case .invalidScanState:
            return "Graph scan iterator lost its prepared range state"
        }
    }
}
