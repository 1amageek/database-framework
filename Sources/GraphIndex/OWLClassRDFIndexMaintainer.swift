import Core
import DatabaseEngine
import Graph
import StorageKit

/// Maintains the canonical RDF projection of an OWL-bound entity.
public struct OWLClassRDFIndexMaintainer<Item: Persistable>: IndexMaintainer {
    private let subspace: Subspace
    private let physicalCodec: RDFQuadIndexPhysicalCodec

    public init(subspace: Subspace) {
        self.subspace = subspace
        self.physicalCodec = RDFQuadIndexPhysicalCodec(baseSubspace: subspace)
    }

    public func updateIndex(
        oldItem: Item?,
        newItem: Item?,
        transaction: any TransactionAccess
    ) async throws {
        if let oldItem {
            for key in try buildAllKeys(for: oldItem) {
                try transaction.clear(key: key)
            }
        }
        if let newItem {
            for key in try buildAllKeys(for: newItem) {
                try transaction.setValue([], for: key)
            }
        }
    }

    public func scanItem(
        _ item: Item,
        id: Tuple,
        transaction: any TransactionAccess
    ) async throws {
        for key in try buildAllKeys(for: item) {
            try transaction.setValue([], for: key)
        }
    }

    public func computeIndexKeys(
        for item: Item,
        id: Tuple
    ) async throws -> [Bytes] {
        try buildAllKeys(for: item)
    }

    private func buildAllKeys(for item: Item) throws -> [Bytes] {
        let quads = try buildQuads(for: item)
        var keys: [Bytes] = []
        keys.reserveCapacity(quads.count * 6)

        for quad in quads {
            let writePlan = try RDFQuadIndexWritePlan(quad: quad)
            try writePlan.forEachEntry { entry in
                let key = try physicalCodec.encode(entry)
                try validateKeySize(key)
                keys.append(key)
            }
        }
        return keys
    }

    private func buildQuads(for item: Item) throws -> [RDFQuad] {
        guard let owlItem = item as? any OWLClassEntity else {
            throw OWLClassRDFIndexError.rootDoesNotConform(
                typeName: Item.persistableType
            )
        }
        let quads = try owlItem.ontologyQuads()
        for quad in quads {
            try quad.validate()
        }
        return quads
    }
}
