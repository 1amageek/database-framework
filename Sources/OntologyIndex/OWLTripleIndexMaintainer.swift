// OWLTripleIndexMaintainer.swift
// OntologyIndex - IndexMaintainer that materializes @OWLClass entity properties as SPO entries
//
// Uses the tripleStore strategy (3 indexes: SPO/POS/OSP) for SPARQL query compatibility.
// Triple entries are key-only (no Protobuf serialization) for minimal storage overhead.
//
// Reference: Weiss, C., Karras, P., & Bernstein, A. (2008).
// "Hexastore: sextuple indexing for semantic web data management"

import Foundation
import Core
import Graph
import DatabaseEngine
import StorageKit

/// Maintains SPO/POS/OSP triple entries derived from @OWLClass entity properties.
///
/// **How it works**:
/// 1. Receives oldItem/newItem from IndexMaintenanceService (standard diff pattern)
/// 2. Generates entity IRI: `{prefix}:{lowercased_type}/{id}`
/// 3. Materializes `rdf:type` + all `@OWLDataProperty` values as triple keys
/// 4. Writes 3 keys per triple (SPO, POS, OSP) for query flexibility
///
/// **Key format** (tripleStore strategy):
/// ```
/// [subspace]/spo/[subject]/[predicate]/[object]/[graph]
/// [subspace]/pos/[predicate]/[object]/[subject]/[graph]
/// [subspace]/osp/[object]/[subject]/[predicate]/[graph]
/// ```
///
/// **Performance**:
/// - Key-only entries (empty value) — no Protobuf overhead
/// - 3 keys per property + 3 keys for rdf:type
/// - Incremental: oldItem → clear, newItem → set (same transaction)
///
/// **Generic constraint note**:
/// The protocol `IndexKindMaintainable.makeIndexMaintainer` requires `Item: Persistable`,
/// not `Item: OWLClassEntity`. This maintainer accepts `Persistable` and checks
/// `OWLClassEntity` conformance at runtime. The check is guaranteed to pass because
/// `OWLTripleIndexKind<Root: OWLClassEntity>` constrains the Root type at compile time.
public struct OWLTripleIndexMaintainer<Item: Persistable>: IndexMaintainer {

    private let subspace: Subspace
    private let graph: String
    private let prefix: String

    /// Pre-computed subspaces for SPO/POS/OSP orderings.
    /// Uses integer keys matching GraphIndexMaintainer's StrategySubspaces convention.
    private let spoSubspace: Subspace  // key = 2
    private let posSubspace: Subspace  // key = 3
    private let ospSubspace: Subspace  // key = 4

    // MARK: - Initialization

    public init(subspace: Subspace, graph: String, prefix: String) {
        self.subspace = subspace
        self.graph = graph
        self.prefix = prefix
        self.spoSubspace = subspace.subspace(Int64(2))
        self.posSubspace = subspace.subspace(Int64(3))
        self.ospSubspace = subspace.subspace(Int64(4))
    }

    // MARK: - IndexMaintainer Protocol

    public func updateIndex(
        oldItem: Item?,
        newItem: Item?,
        transaction: any Transaction
    ) async throws {
        if let old = oldItem {
            let keys = buildAllKeys(for: old)
            for key in keys { transaction.clear(key: key) }
        }
        if let new = newItem {
            let keys = buildAllKeys(for: new)
            for key in keys { transaction.setValue([], for: key) }
        }
    }

    public func scanItem(_ item: Item, id: Tuple, transaction: any Transaction) async throws {
        let keys = buildAllKeys(for: item)
        for key in keys { transaction.setValue([], for: key) }
    }

    public func computeIndexKeys(for item: Item, id: Tuple) async throws -> [Bytes] {
        buildAllKeys(for: item)
    }

    // MARK: - Key Building

    private func buildAllKeys(for item: Item) -> [Bytes] {
        // Item must be OWLClassEntity — guaranteed by OWLTripleIndexKind<Root: OWLClassEntity>.
        // The guard in makeIndexMaintainer prevents non-OWL types from reaching here.
        guard let owlType = Item.self as? any OWLClassEntity.Type else {
            assertionFailure(
                "OWLTripleIndexMaintainer used with non-OWLClassEntity type '\(Item.persistableType)'. "
                + "This is unreachable — OWLTripleIndexKind constrains Root: OWLClassEntity."
            )
            return []
        }

        let typeName = Item.persistableType.lowercased()
        let entityIRI = "\(prefix):\(typeName)/\(item.id)"

        var keys: [Bytes] = []
        let classIRI = owlType.ontologyClassIRI
        let descriptors = owlType.ontologyPropertyDescriptors

        keys.reserveCapacity((1 + descriptors.count) * 3)

        // rdf:type triple
        keys.append(contentsOf: buildTripleKeys(subject: entityIRI, predicate: "rdf:type", object: classIRI))

        // @OWLDataProperty triples
        for descriptor in descriptors where !descriptor.isObjectProperty {
            guard let value = item[dynamicMember: descriptor.fieldName] else { continue }
            guard let str = stringValue(from: value), !str.isEmpty else { continue }
            keys.append(contentsOf: buildTripleKeys(subject: entityIRI, predicate: descriptor.iri, object: str))
        }

        return keys
    }

    /// Build SPO/POS/OSP keys for a single triple.
    ///
    /// Returns exactly 3 keys (one per ordering).
    private func buildTripleKeys(subject: String, predicate: String, object: String) -> [Bytes] {
        let s: any TupleElement = subject
        let p: any TupleElement = predicate
        let o: any TupleElement = object
        let g: any TupleElement = graph

        return [
            spoSubspace.pack(Tuple([s, p, o, g])),
            posSubspace.pack(Tuple([p, o, s, g])),
            ospSubspace.pack(Tuple([o, s, p, g])),
        ]
    }

    /// Extract string value from a Sendable value for RDF object literal.
    ///
    /// Returns nil for unsupported types (the property is skipped, not silently converted).
    private func stringValue(from value: any Sendable) -> String? {
        if let s = value as? String { return s }
        if let i = value as? Int { return String(i) }
        if let i = value as? Int64 { return String(i) }
        if let d = value as? Double { return String(d) }
        if let f = value as? Float { return String(f) }
        if let b = value as? Bool { return b ? "true" : "false" }
        if let u = value as? UUID { return u.uuidString }
        if let date = value as? Date {
            return date.formatted(.iso8601.year().month().day().time(includingFractionalSeconds: true))
        }
        return nil
    }
}
