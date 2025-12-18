// ScalarIndexKind+Maintainable.swift
// ScalarIndexLayer - IndexKindMaintainable extension for ScalarIndexKind
//
// This file provides the bridge between ScalarIndexKind (defined in FDBModel)
// and ScalarIndexMaintainer (defined in this package).

import Foundation
import Core
import Core
import DatabaseEngine
import FoundationDB

// MARK: - IndexKindMaintainable Extension

/// Extends ScalarIndexKind (from FDBModel) with IndexKindMaintainable conformance
///
/// **Design**:
/// - ScalarIndexKind struct is defined in FDBModel (FDB-independent)
/// - ScalarIndexMaintainer is defined in ScalarIndexLayer (FDB-dependent)
/// - This extension bridges them together
///
/// **Usage**:
/// ```swift
/// let kind = ScalarIndexKind()  // From FDBModel
/// let maintainer = kind.makeIndexMaintainer(...)  // Returns ScalarIndexMaintainer
/// ```
extension ScalarIndexKind: IndexKindMaintainable {
    /// Create a ScalarIndexMaintainer for this index kind
    ///
    /// This bridges `ScalarIndexKind` (metadata) with `ScalarIndexMaintainer` (runtime).
    /// Called by the system when building or maintaining indexes.
    ///
    /// - Parameters:
    ///   - index: Index definition
    ///   - subspace: FDB subspace for this index
    ///   - idExpression: Expression for extracting item's unique identifier
    ///   - configurations: Index configurations (not used for scalar indexes)
    /// - Returns: ScalarIndexMaintainer instance
    public func makeIndexMaintainer<Item: Persistable>(
        index: Index,
        subspace: Subspace,
        idExpression: KeyExpression,
        configurations: [any IndexConfiguration]
    ) -> any IndexMaintainer<Item> {
        // ScalarIndexKind doesn't use configurations (no heavy runtime parameters)
        // Note: subspace is already index-specific (caller passes indexSubspace.subspace(indexName))
        return ScalarIndexMaintainer<Item>(
            index: index,
            subspace: subspace,
            idExpression: idExpression
        )
    }
}
