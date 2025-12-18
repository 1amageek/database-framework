// RankIndexKind+Maintainable.swift
// RankIndexLayer - RANK indexes for leaderboards and percentile queries (FDB-dependent)
//
// Provides IndexKindMaintainable conformance for RankIndexKind.
// RankIndexKind is defined in RankIndexModel (FDB-independent).

import Foundation
import Core
import DatabaseEngine
import FoundationDB
import Rank

// Re-export RankIndexModel types for convenience
@_exported import Rank

// MARK: - IndexKindMaintainable Conformance

extension RankIndexKind: IndexKindMaintainable {
    /// Create index maintainer for rank indexes
    ///
    /// This bridges `RankIndexKind<Root, Score>` (metadata) with `RankIndexMaintainer<Item, Score>` (runtime).
    /// The `Score` type parameter is preserved at compile time for type-safe queries.
    public func makeIndexMaintainer<Item: Persistable>(
        index: Index,
        subspace: Subspace,
        idExpression: KeyExpression,
        configurations: [any IndexConfiguration]
    ) -> any IndexMaintainer<Item> {
        // Score type is preserved from RankIndexKind<Root, Score>
        return RankIndexMaintainer<Item, Score>(
            index: index,
            bucketSize: bucketSize,
            subspace: subspace,  // Already index-specific from caller
            idExpression: idExpression
        )
    }
}
