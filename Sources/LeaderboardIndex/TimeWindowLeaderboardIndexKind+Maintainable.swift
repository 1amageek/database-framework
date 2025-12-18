// TimeWindowLeaderboardIndexKind+Maintainable.swift
// LeaderboardIndexLayer - IndexKindMaintainable extension for TimeWindowLeaderboardIndexKind
//
// Time-windowed ranking with automatic window rotation.
// Reference: FDB Record Layer TIME_WINDOW_LEADERBOARD index type

import Foundation
import Core
import DatabaseEngine
import FoundationDB

// MARK: - IndexKindMaintainable Extension

/// Extends TimeWindowLeaderboardIndexKind with IndexKindMaintainable conformance
extension TimeWindowLeaderboardIndexKind: IndexKindMaintainable {
    /// Create a TimeWindowLeaderboardIndexMaintainer for this index kind
    ///
    /// This bridges `TimeWindowLeaderboardIndexKind<Root, Score>` (metadata) with
    /// `TimeWindowLeaderboardIndexMaintainer<Item, Score>` (runtime).
    /// The `Score` type parameter is preserved at compile time for type-safe queries.
    public func makeIndexMaintainer<Item: Persistable>(
        index: Index,
        subspace: Subspace,
        idExpression: KeyExpression,
        configurations: [any IndexConfiguration]
    ) -> any IndexMaintainer<Item> {
        // Score type is preserved from TimeWindowLeaderboardIndexKind<Root, Score>
        return TimeWindowLeaderboardIndexMaintainer<Item, Score>(
            index: index,
            subspace: subspace,  // Already index-specific from caller
            idExpression: idExpression,
            window: window,
            windowCount: windowCount
        )
    }
}
