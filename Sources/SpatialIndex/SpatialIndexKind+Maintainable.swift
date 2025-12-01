// SpatialIndexKind+Maintainable.swift
// SpatialIndexLayer - Geospatial indexing with S2/Morton encoding (FDB-dependent)
//
// Provides IndexKindMaintainable conformance for SpatialIndexKind.
// SpatialIndexKind and SpatialEncoding are defined in SpatialIndexModel (FDB-independent).

import Foundation
import Core
import Core
import DatabaseEngine
import FoundationDB
import Spatial

// Re-export SpatialIndexModel types for convenience
@_exported import Spatial

// MARK: - IndexKindMaintainable Conformance

extension SpatialIndexKind: IndexKindMaintainable {
    /// Create index maintainer for spatial indexes
    public func makeIndexMaintainer<Item: Persistable>(
        index: Index,
        subspace: Subspace,
        idExpression: KeyExpression,
        configurations: [any IndexConfiguration]
    ) -> any IndexMaintainer<Item> {
        return SpatialIndexMaintainer<Item>(
            index: index,
            kind: self,
            subspace: subspace.subspace(index.name),
            idExpression: idExpression
        )
    }
}
