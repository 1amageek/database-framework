// TripleIndexKind+Maintainable.swift
// TripleIndex - IndexKindMaintainable conformance for TripleIndexKind

import Foundation
import Core
import Triple
import DatabaseEngine
import FoundationDB

extension TripleIndexKind: IndexKindMaintainable {
    public func makeIndexMaintainer<Item: Persistable>(
        index: Index,
        subspace: Subspace,
        idExpression: KeyExpression,
        configurations: [any IndexConfiguration]
    ) -> any IndexMaintainer<Item> {
        return TripleIndexMaintainer<Item>(
            index: index,
            subspace: subspace.subspace(index.name),
            idExpression: idExpression,
            kind: self
        )
    }
}
