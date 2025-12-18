// GraphIndexKind+Maintainable.swift
// GraphIndex - IndexKindMaintainable extension for GraphIndexKind
//
// Bridges GraphIndexKind (database-kit) with GraphIndexMaintainer (database-framework).

import Foundation
import Core
import Graph
import DatabaseEngine
import FoundationDB

// MARK: - IndexKindMaintainable Extension

/// Extends GraphIndexKind with IndexKindMaintainable conformance
///
/// **Design**:
/// - GraphIndexKind is defined in database-kit (FDB-independent)
/// - GraphIndexMaintainer is defined in database-framework (FDB-dependent)
/// - This extension bridges them together
///
/// **Usage**:
/// ```swift
/// let kind = GraphIndexKind.rdf(subject: \.s, predicate: \.p, object: \.o)
/// let maintainer = kind.makeIndexMaintainer(...)  // Returns GraphIndexMaintainer
/// ```
extension GraphIndexKind: IndexKindMaintainable {
    /// Create a GraphIndexMaintainer for this index kind
    ///
    /// This bridges `GraphIndexKind` (metadata) with `GraphIndexMaintainer` (runtime).
    /// Called by the system when building or maintaining indexes.
    ///
    /// - Parameters:
    ///   - index: Index definition
    ///   - subspace: FDB subspace for this index
    ///   - idExpression: Expression for extracting item's unique identifier
    ///   - configurations: Index configurations (not used for graph indexes)
    /// - Returns: GraphIndexMaintainer instance
    public func makeIndexMaintainer<Item: Persistable>(
        index: Index,
        subspace: Subspace,
        idExpression: KeyExpression,
        configurations: [any IndexConfiguration]
    ) -> any IndexMaintainer<Item> {
        return GraphIndexMaintainer<Item>(
            index: index,
            subspace: subspace,  // Already index-specific from caller
            idExpression: idExpression,
            fromField: fromField,
            edgeField: edgeField,
            toField: toField,
            strategy: strategy
        )
    }
}
