// CountIndexKind+Maintainable.swift
// AggregationIndexLayer - IndexKindMaintainable extension for CountIndexKind
//
// This file provides the bridge between CountIndexKind (defined in FDBModel)
// and CountIndexMaintainer (defined in this package).

import Foundation
import Core
import Core
import DatabaseEngine
import FoundationDB

// MARK: - IndexKindMaintainable Extension

/// Extends CountIndexKind (from FDBModel) with IndexKindMaintainable conformance
///
/// **Design**:
/// - CountIndexKind struct is defined in FDBModel (FDB-independent)
/// - CountIndexMaintainer is defined in AggregationIndexLayer (FDB-dependent)
/// - This extension bridges them together
extension CountIndexKind: IndexKindMaintainable {
    /// Create a CountIndexMaintainer for this index kind
    ///
    /// This bridges `CountIndexKind` (metadata) with `CountIndexMaintainer` (runtime).
    /// Called by the system when building or maintaining indexes.
    ///
    /// - Parameters:
    ///   - index: Index definition
    ///   - subspace: FDB subspace for this index
    ///   - idExpression: Expression for extracting item's unique identifier
    /// - Returns: CountIndexMaintainer instance
    public func makeIndexMaintainer<Item: Persistable>(
        index: Index,
        subspace: Subspace,
        idExpression: KeyExpression,
        configurations: [any IndexConfiguration]
    ) -> any IndexMaintainer<Item> {
        return CountIndexMaintainer<Item>(
            index: index,
            subspace: subspace.subspace(index.name),
            idExpression: idExpression
        )
    }
}
