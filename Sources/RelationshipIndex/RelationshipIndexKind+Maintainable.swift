// RelationshipIndexKind+Maintainable.swift
// RelationshipIndex - IndexKindMaintainable extension for RelationshipIndexKind
//
// Bridges RelationshipIndexKind (database-kit) with RelationshipIndexMaintainer (database-framework).

import Foundation
import Core
import Relationship
import DatabaseEngine
import FoundationDB

// MARK: - IndexKindMaintainable Extension

extension RelationshipIndexKind: IndexKindMaintainable {
    /// Create a RelationshipIndexMaintainer for this index kind
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

        return RelationshipIndexMaintainer<Item>(
            foreignKeyFieldName: self.foreignKeyFieldName,
            relatedTypeName: self.relatedTypeName,
            relatedFieldNames: self.relatedFieldNames,
            isToMany: self.isToMany,
            index: index,
            subspace: subspace,  // Already index-specific from caller
            idExpression: idExpression,
            relatedItemLoader: relatedItemLoader
        )
    }
}
