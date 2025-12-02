// TripleIndexMaintainer.swift
// TripleIndex - Index maintainer for RDF triple indexes
//
// Maintains three index orderings (SPO/POS/OSP) for efficient triple queries.

import Foundation
import Core
import Triple
import DatabaseEngine
import FoundationDB

/// Maintainer for RDF triple indexes
///
/// **Index Structure** (3 orderings):
/// ```
/// [subspace]/spo/[subject]/[predicate]/[object]/[id] = ''
/// [subspace]/pos/[predicate]/[object]/[subject]/[id] = ''
/// [subspace]/osp/[object]/[subject]/[predicate]/[id] = ''
/// ```
///
/// **Query Patterns**:
/// - SPO: S??, SP?, SPO queries (subject-first)
/// - POS: ?P?, ?PO queries (predicate-first)
/// - OSP: ??O queries (object-first)
public struct TripleIndexMaintainer<Item: Persistable>: IndexMaintainer {
    public let index: Index
    public let subspace: Subspace
    public let idExpression: KeyExpression

    private let subjectField: String
    private let predicateField: String
    private let objectField: String

    // Cached subspaces (computed once at init)
    private let spoSubspace: Subspace
    private let posSubspace: Subspace
    private let ospSubspace: Subspace

    public init(
        index: Index,
        subspace: Subspace,
        idExpression: KeyExpression,
        subjectField: String,
        predicateField: String,
        objectField: String
    ) {
        self.index = index
        self.subspace = subspace
        self.idExpression = idExpression
        self.subjectField = subjectField
        self.predicateField = predicateField
        self.objectField = objectField
        // Cache subspaces at initialization
        self.spoSubspace = subspace.subspace("spo")
        self.posSubspace = subspace.subspace("pos")
        self.ospSubspace = subspace.subspace("osp")
    }

    // MARK: - IndexMaintainer

    public func updateIndex(
        oldItem: Item?,
        newItem: Item?,
        transaction: any TransactionProtocol
    ) async throws {
        if let oldItem = oldItem {
            let keys = try buildAllIndexKeys(for: oldItem)
            for key in keys {
                transaction.clear(key: key)
            }
        }

        if let newItem = newItem {
            let keys = try buildAllIndexKeys(for: newItem)
            for key in keys {
                transaction.setValue([], for: key)
            }
        }
    }

    public func scanItem(
        _ item: Item,
        id: Tuple,
        transaction: any TransactionProtocol
    ) async throws {
        let keys = try buildAllIndexKeys(for: item, id: id)
        for key in keys {
            transaction.setValue([], for: key)
        }
    }

    public func computeIndexKeys(
        for item: Item,
        id: Tuple
    ) async throws -> [FDB.Bytes] {
        return try buildAllIndexKeys(for: item, id: id)
    }

    // MARK: - Private

    private func buildAllIndexKeys(for item: Item, id: Tuple? = nil) throws -> [FDB.Bytes] {
        let subject = try extractField(from: item, fieldName: subjectField)
        let predicate = try extractField(from: item, fieldName: predicateField)
        let object = try extractField(from: item, fieldName: objectField)
        let itemId = try id ?? DataAccess.extractId(from: item, using: idExpression)

        // Pre-extract ID elements once (avoid repeated extraction in loop)
        var idElements: [any TupleElement] = []
        idElements.reserveCapacity(itemId.count)
        for i in 0..<itemId.count {
            if let element = itemId[i] {
                idElements.append(element)
            }
        }

        // Pre-allocate result array
        var keys: [FDB.Bytes] = []
        keys.reserveCapacity(3)

        // Build keys with minimal allocation
        // SPO: [subject]/[predicate]/[object]/[id]
        let spoKey = spoSubspace.pack(Tuple([subject, predicate, object] + idElements))
        try validateKeySize(spoKey)
        keys.append(spoKey)

        // POS: [predicate]/[object]/[subject]/[id]
        let posKey = posSubspace.pack(Tuple([predicate, object, subject] + idElements))
        try validateKeySize(posKey)
        keys.append(posKey)

        // OSP: [object]/[subject]/[predicate]/[id]
        let ospKey = ospSubspace.pack(Tuple([object, subject, predicate] + idElements))
        try validateKeySize(ospKey)
        keys.append(ospKey)

        return keys
    }

    private func extractField(from item: Item, fieldName: String) throws -> any TupleElement {
        guard let value = item[dynamicMember: fieldName] else {
            throw TripleIndexError.fieldNotFound(
                fieldName: fieldName,
                itemType: Item.persistableType
            )
        }

        guard let tupleElement = value as? any TupleElement else {
            throw TripleIndexError.invalidFieldType(
                fieldName: fieldName,
                expectedType: "TupleElement",
                actualType: String(describing: type(of: value))
            )
        }

        return tupleElement
    }
}

// MARK: - Errors

public enum TripleIndexError: Error, CustomStringConvertible {
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
