// RankIndexKind+Maintainable.swift
// RankIndexLayer - RANK indexes for leaderboards and percentile queries (FDB-dependent)
//
// Provides IndexKindMaintainable conformance for RankIndexKind.
// RankIndexKind is defined in RankIndexModel (FDB-independent).

import Foundation
import Core
import Core
import DatabaseEngine
import FoundationDB
import Rank

// Re-export RankIndexModel types for convenience
@_exported import Rank

// MARK: - IndexKindMaintainable Conformance

extension RankIndexKind: IndexKindMaintainable {
    /// Create index maintainer for rank indexes
    public func makeIndexMaintainer<Item: Persistable>(
        index: Index,
        subspace: Subspace,
        idExpression: KeyExpression,
        configurations: [any IndexConfiguration]
    ) -> any IndexMaintainer<Item> {
        return RankIndexMaintainer<Item>(
            index: index,
            bucketSize: bucketSize,
            subspace: subspace.subspace(index.name),
            idExpression: idExpression
        )
    }
}
