// AdjacencyIndexKind+Maintainable.swift
// GraphIndexLayer - IndexKindMaintainable extension for AdjacencyIndexKind
//
// This file provides the bridge between AdjacencyIndexKind (defined in GraphIndexModel)
// and AdjacencyIndexMaintainer (defined in this package).

import Foundation
import Core
import Core
import DatabaseEngine
import FoundationDB
import Graph

// MARK: - IndexKindMaintainable Extension

/// Extends AdjacencyIndexKind (from GraphIndexModel) with IndexKindMaintainable conformance
///
/// **Design**:
/// - AdjacencyIndexKind struct is defined in GraphIndexModel (FDB-independent)
/// - AdjacencyIndexMaintainer is defined in GraphIndexLayer (FDB-dependent)
/// - This extension bridges them together
///
/// **Usage**:
/// ```swift
/// let kind = AdjacencyIndexKind(...)  // From GraphIndexModel
/// let maintainer = kind.makeIndexMaintainer(...)  // Returns AdjacencyIndexMaintainer
/// ```
extension AdjacencyIndexKind: IndexKindMaintainable {
    /// Create an AdjacencyIndexMaintainer for this index kind
    ///
    /// This bridges `AdjacencyIndexKind` (metadata) with `AdjacencyIndexMaintainer` (runtime).
    /// Called by the system when building or maintaining indexes.
    ///
    /// - Parameters:
    ///   - index: Index definition
    ///   - subspace: FDB subspace for this index
    ///   - idExpression: Expression for extracting item's unique identifier
    ///   - configurations: Index configurations (not used for adjacency indexes)
    /// - Returns: AdjacencyIndexMaintainer instance
    public func makeIndexMaintainer<Item: Persistable>(
        index: Index,
        subspace: Subspace,
        idExpression: KeyExpression,
        configurations: [any IndexConfiguration]
    ) -> any IndexMaintainer<Item> {
        return AdjacencyIndexMaintainer<Item>(
            index: index,
            subspace: subspace.subspace(index.name),
            idExpression: idExpression,
            sourceField: sourceField,
            targetField: targetField,
            labelField: labelField,
            bidirectional: bidirectional
        )
    }
}
