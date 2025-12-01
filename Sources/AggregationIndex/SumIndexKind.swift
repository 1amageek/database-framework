// SumIndexKind+Maintainable.swift
// AggregationIndexLayer - IndexKindMaintainable extension for SumIndexKind
//
// This file provides the bridge between SumIndexKind (defined in FDBModel)
// and SumIndexMaintainer (defined in this package).

import Foundation
import Core
import Core
import DatabaseEngine
import FoundationDB

// MARK: - IndexKindMaintainable Extension

/// Extends SumIndexKind (from FDBModel) with IndexKindMaintainable conformance
extension SumIndexKind: IndexKindMaintainable {
    /// Create a SumIndexMaintainer for this index kind
    ///
    /// This bridges `SumIndexKind` (metadata) with `SumIndexMaintainer` (runtime).
    /// Called by the system when building or maintaining indexes.
    ///
    /// - Parameters:
    ///   - index: Index definition
    ///   - subspace: FDB subspace for this index
    ///   - idExpression: Expression for extracting item's unique identifier
    /// - Returns: SumIndexMaintainer instance
    public func makeIndexMaintainer<Item: Persistable>(
        index: Index,
        subspace: Subspace,
        idExpression: KeyExpression,
        configurations: [any IndexConfiguration]
    ) -> any IndexMaintainer<Item> {
        return SumIndexMaintainer<Item>(
            index: index,
            subspace: subspace.subspace(index.name),
            idExpression: idExpression
        )
    }
}
