// CountUpdatesIndexKind+Maintainable.swift
// AggregationIndexLayer - IndexKindMaintainable extension for CountUpdatesIndexKind
//
// Tracks the number of times each record has been updated.
// Reference: FDB Record Layer COUNT_UPDATES index type

import Foundation
import Core
import DatabaseEngine
import FoundationDB

// MARK: - IndexKindMaintainable Extension

/// Extends CountUpdatesIndexKind with IndexKindMaintainable conformance
extension CountUpdatesIndexKind: IndexKindMaintainable {
    /// Create a CountUpdatesIndexMaintainer for this index kind
    public func makeIndexMaintainer<Item: Persistable>(
        index: Index,
        subspace: Subspace,
        idExpression: KeyExpression,
        configurations: [any IndexConfiguration]
    ) -> any IndexMaintainer<Item> {
        return CountUpdatesIndexMaintainer<Item>(
            index: index,
            subspace: subspace.subspace(index.name),
            idExpression: idExpression
        )
    }
}
