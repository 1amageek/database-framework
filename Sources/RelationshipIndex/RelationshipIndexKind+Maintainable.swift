// RelationshipIndexKind+Maintainable.swift
// RelationshipIndex - IndexKindMaintainable extension for RelationshipIndexKind
//
// This file provides the bridge between RelationshipIndexKind (defined in Relationship package)
// and RelationshipIndexMaintainer (defined in this package).

import Foundation
import Core
import Relationship
import DatabaseEngine
import FoundationDB

// MARK: - IndexKindMaintainable Extension

/// Extends RelationshipIndexKind (from Relationship) with IndexKindMaintainable conformance
///
/// **Design**:
/// - RelationshipIndexKind struct is defined in Relationship (FDB-independent)
/// - RelationshipIndexMaintainer is defined in RelationshipIndex (FDB-dependent)
/// - This extension bridges them together
///
/// **Configuration**:
/// Relationship indexes require `RelationshipIndexConfiguration` to provide the
/// `relatedItemLoader` callback. Without this configuration, relationship indexes
/// will be created but will not build index entries (they'll be skipped).
///
/// **Usage**:
/// ```swift
/// let kind = RelationshipIndexKind<Order, Customer>(
///     foreignKey: \.customerID,
///     relatedFields: [\.name],
///     localFields: [\.total]
/// )
/// let maintainer = kind.makeIndexMaintainer(...)  // Returns RelationshipIndexMaintainer
/// ```
extension RelationshipIndexKind: IndexKindMaintainable {
    /// Create a RelationshipIndexMaintainer for this index kind
    ///
    /// This bridges `RelationshipIndexKind` (metadata) with `RelationshipIndexMaintainer` (runtime).
    /// Called by the system when building or maintaining indexes.
    ///
    /// - Parameters:
    ///   - index: Index definition
    ///   - subspace: FDB subspace for this index
    ///   - idExpression: Expression for extracting item's unique identifier
    ///   - configurations: Index configurations (may contain RelationshipIndexConfiguration)
    /// - Returns: RelationshipIndexMaintainer instance
    public func makeIndexMaintainer<Item: Persistable>(
        index: Index,
        subspace: Subspace,
        idExpression: KeyExpression,
        configurations: [any IndexConfiguration]
    ) -> any IndexMaintainer<Item> {
        // Look for RelationshipIndexConfiguration matching this index
        let relationshipConfig = configurations
            .compactMap { $0 as? RelationshipIndexConfiguration }
            .first { $0.indexName == index.name }

        // Extract the related item loader from configuration (if available)
        let relatedItemLoader = relationshipConfig?.relatedItemLoader

        // Pass the metadata strings to the maintainer
        return RelationshipIndexMaintainer<Item>(
            relationshipPropertyName: self.relationshipPropertyName,
            foreignKeyFieldName: self.foreignKeyFieldName,
            relatedTypeName: self.relatedTypeName,
            relatedFieldNames: self.relatedFieldNames,
            localFieldNames: self.localFieldNames,
            index: index,
            subspace: subspace.subspace(index.name),
            idExpression: idExpression,
            relatedItemLoader: relatedItemLoader
        )
    }
}
