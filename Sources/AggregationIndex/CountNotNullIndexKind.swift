// CountNotNullIndexKind+Maintainable.swift
// AggregationIndexLayer - IndexKindMaintainable extension for CountNotNullIndexKind
//
// Tracks counts of non-null values grouped by other fields.
// Reference: FDB Record Layer COUNT_NOT_NULL index type

import Foundation
import Core
import DatabaseEngine
import FoundationDB

// MARK: - IndexKindMaintainable Extension

/// Extends CountNotNullIndexKind with IndexKindMaintainable conformance
extension CountNotNullIndexKind: IndexKindMaintainable {
    /// Create a CountNotNullIndexMaintainer for this index kind
    public func makeIndexMaintainer<Item: Persistable>(
        index: Index,
        subspace: Subspace,
        idExpression: KeyExpression,
        configurations: [any IndexConfiguration]
    ) -> any IndexMaintainer<Item> {
        return CountNotNullIndexMaintainer<Item>(
            index: index,
            subspace: subspace.subspace(index.name),
            idExpression: idExpression,
            valueFieldName: valueFieldName
        )
    }
}
