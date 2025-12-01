// AdjacencyIndexMaintainer.swift
// GraphIndexLayer - Index maintainer for graph adjacency indexes
//
// Maintains graph edge indexes for efficient traversal queries.

import Foundation
import Core
import Core
import DatabaseEngine
import FoundationDB
import Graph

/// Maintainer for graph adjacency indexes
///
/// **Functionality**:
/// - Outgoing edge index: source → target
/// - Incoming edge index: target → source (when bidirectional)
/// - Label-based filtering
///
/// **Index Structure**:
/// ```
/// // Outgoing edges
/// Key: [subspace]/adj/[label]/[source]/[target] = ''
///
/// // Incoming edges (when bidirectional)
/// Key: [subspace]/adj_in/[label]/[target]/[source] = ''
/// ```
///
/// **Query Patterns**:
/// - Find outgoing: scan [subspace]/adj/[label]/[source]/
/// - Find incoming: scan [subspace]/adj_in/[label]/[target]/
/// - Check edge exists: get [subspace]/adj/[label]/[source]/[target]
///
/// **Usage**:
/// ```swift
/// let maintainer = AdjacencyIndexMaintainer<Edge>(
///     index: adjacencyIndex,
///     subspace: indexSubspace,
///     idExpression: FieldKeyExpression(fieldName: "id"),
///     kind: AdjacencyIndexKind(...)
/// )
/// ```
public struct AdjacencyIndexMaintainer<Item: Persistable>: IndexMaintainer {
    // MARK: - Properties

    /// Index definition
    public let index: Index

    /// Subspace for index storage
    public let subspace: Subspace

    /// ID expression for extracting item's unique identifier
    public let idExpression: KeyExpression

    /// Adjacency index kind with configuration
    public let kind: AdjacencyIndexKind

    /// Cached subspace for outgoing edges (computed once at init)
    private let outgoingSubspace: Subspace

    /// Cached subspace for incoming edges (computed once at init)
    private let incomingSubspace: Subspace

    // MARK: - Initialization

    /// Initialize adjacency index maintainer
    ///
    /// - Parameters:
    ///   - index: Index definition
    ///   - subspace: FDB subspace for this index
    ///   - idExpression: Expression for extracting item's unique identifier
    ///   - kind: Adjacency index kind configuration
    public init(
        index: Index,
        subspace: Subspace,
        idExpression: KeyExpression,
        kind: AdjacencyIndexKind
    ) {
        self.index = index
        self.subspace = subspace
        self.idExpression = idExpression
        self.kind = kind
        // Cache subspaces at initialization
        self.outgoingSubspace = subspace.subspace("adj")
        self.incomingSubspace = subspace.subspace("adj_in")
    }

    // MARK: - IndexMaintainer

    /// Update index when edge changes
    ///
    /// **Process**:
    /// 1. Remove old index entries (if oldItem exists)
    /// 2. Add new index entries (if newItem exists)
    /// 3. If bidirectional, update both outgoing and incoming indexes
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
            let (outKey, inKey) = try buildIndexKeys(for: oldItem)
            transaction.clear(key: outKey)
            if kind.bidirectional, let inKey = inKey {
                transaction.clear(key: inKey)
            }
        }

        // Add new index entries
        if let newItem = newItem {
            let (outKey, inKey) = try buildIndexKeys(for: newItem)
            transaction.setValue([], for: outKey)
            if kind.bidirectional, let inKey = inKey {
                transaction.setValue([], for: inKey)
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
        let (outKey, inKey) = try buildIndexKeys(for: item)
        transaction.setValue([], for: outKey)
        if kind.bidirectional, let inKey = inKey {
            transaction.setValue([], for: inKey)
        }
    }

    /// Compute expected index keys for an edge (for scrubber verification)
    ///
    /// Returns the index keys that should exist for this edge.
    public func computeIndexKeys(
        for item: Item,
        id: Tuple
    ) async throws -> [FDB.Bytes] {
        let (outKey, inKey) = try buildIndexKeys(for: item)
        var keys: [FDB.Bytes] = [outKey]
        if kind.bidirectional, let inKey = inKey {
            keys.append(inKey)
        }
        return keys
    }

    // MARK: - Private Methods

    /// Build outgoing and incoming index keys for an edge
    ///
    /// **Outgoing key structure**: [subspace]/adj/[label]/[source]/[target]
    /// **Incoming key structure**: [subspace]/adj_in/[label]/[target]/[source]
    private func buildIndexKeys(for item: Item) throws -> (outgoing: FDB.Bytes, incoming: FDB.Bytes?) {
        // Extract field values using dynamicMember subscript
        let sourceValue = try extractField(from: item, fieldName: kind.sourceField)
        let targetValue = try extractField(from: item, fieldName: kind.targetField)

        // Extract label value if specified
        var labelValue: (any TupleElement)?
        if let labelField = kind.labelField {
            labelValue = try extractField(from: item, fieldName: labelField)
        }

        // Build outgoing key: [adj]/[label]/[source]/[target]
        var outElements: [any TupleElement] = []
        if let label = labelValue {
            outElements.append(label)
        }
        outElements.append(sourceValue)
        outElements.append(targetValue)
        let outKey = outgoingSubspace.pack(Tuple(outElements))
        try validateKeySize(outKey)

        // Build incoming key: [adj_in]/[label]/[target]/[source]
        var inKey: FDB.Bytes?
        if kind.bidirectional {
            var inElements: [any TupleElement] = []
            if let label = labelValue {
                inElements.append(label)
            }
            inElements.append(targetValue)
            inElements.append(sourceValue)
            inKey = incomingSubspace.pack(Tuple(inElements))
            if let key = inKey {
                try validateKeySize(key)
            }
        }

        return (outKey, inKey)
    }

    /// Extract a field value from an item
    private func extractField(from item: Item, fieldName: String) throws -> any TupleElement {
        guard let value = item[dynamicMember: fieldName] else {
            throw AdjacencyIndexError.fieldNotFound(fieldName: fieldName, itemType: Item.persistableType)
        }

        guard let tupleElement = value as? any TupleElement else {
            throw AdjacencyIndexError.invalidFieldType(
                fieldName: fieldName,
                expectedType: "TupleElement",
                actualType: String(describing: type(of: value))
            )
        }

        return tupleElement
    }
}

// MARK: - Errors

/// Errors specific to adjacency index operations
public enum AdjacencyIndexError: Error, CustomStringConvertible {
    case fieldNotFound(fieldName: String, itemType: String)
    case invalidFieldType(fieldName: String, expectedType: String, actualType: String)

    public var description: String {
        switch self {
        case .fieldNotFound(let fieldName, let itemType):
            return "Field '\(fieldName)' not found in '\(itemType)'"
        case .invalidFieldType(let fieldName, let expectedType, let actualType):
            return "Field '\(fieldName)' has invalid type: expected \(expectedType), got \(actualType)"
        }
    }
}
