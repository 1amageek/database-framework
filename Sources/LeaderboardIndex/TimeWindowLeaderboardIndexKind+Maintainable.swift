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
    public func makeIndexMaintainer<Item: Persistable>(
        index: Index,
        subspace: Subspace,
        idExpression: KeyExpression,
        configurations: [any IndexConfiguration]
    ) -> any IndexMaintainer<Item> {
        return TimeWindowLeaderboardIndexMaintainer<Item>(
            index: index,
            subspace: subspace.subspace(index.name),
            idExpression: idExpression,
            window: window,
            windowCount: windowCount
        )
    }
}
