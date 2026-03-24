// OWLTripleIndexKind+Maintainable.swift
// OntologyIndex - Bridges OWLTripleIndexKind (metadata) with OWLTripleIndexMaintainer (runtime)

import Foundation
import Core
import Graph
import DatabaseEngine
import StorageKit

// MARK: - IndexKindMaintainable Extension

extension OWLTripleIndexKind: IndexKindMaintainable {

    /// Create an OWLTripleIndexMaintainer for this index kind.
    ///
    /// Since `OWLTripleIndexKind<Root>` constrains `Root: OWLClassEntity`,
    /// and `makeIndexMaintainer` is called with `Item` matching `Root`,
    /// the runtime type check is a defensive guard against misuse.
    public func makeIndexMaintainer<Item: Persistable>(
        index: Index,
        subspace: Subspace,
        idExpression: KeyExpression,
        configurations: [any IndexConfiguration]
    ) throws -> any IndexMaintainer<Item> {
        guard Item.self is any OWLClassEntity.Type else {
            throw OntologyIndexError.notOWLClassEntity(typeName: Item.persistableType)
        }
        return OWLTripleIndexMaintainer<Item>(
            subspace: subspace,
            graph: graph,
            prefix: prefix
        )
    }
}
