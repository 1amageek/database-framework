// AverageIndexKind+Maintainable.swift
// AggregationIndexLayer - IndexKindMaintainable extension for AverageIndexKind
//
// This file provides the bridge between AverageIndexKind (defined in FDBModel)
// and AverageIndexMaintainer (defined in this package).

import Foundation
import Core
import Core
import DatabaseEngine
import FoundationDB

// MARK: - IndexKindMaintainable Extension

/// Extends AverageIndexKind (from FDBModel) with IndexKindMaintainable conformance
extension AverageIndexKind: IndexKindMaintainable {
    /// Create an AverageIndexMaintainer for this index kind
    ///
    /// This bridges `AverageIndexKind` (metadata) with `AverageIndexMaintainer` (runtime).
    /// Called by the system when building or maintaining indexes.
    ///
    /// - Parameters:
    ///   - index: Index definition
    ///   - subspace: FDB subspace for this index
    ///   - idExpression: Expression for extracting item's unique identifier
    /// - Returns: AverageIndexMaintainer instance
    public func makeIndexMaintainer<Item: Persistable>(
        index: Index,
        subspace: Subspace,
        idExpression: KeyExpression,
        configurations: [any IndexConfiguration]
    ) -> any IndexMaintainer<Item> {
        return AverageIndexMaintainer<Item>(
            index: index,
            subspace: subspace.subspace(index.name),
            idExpression: idExpression
        )
    }
}
