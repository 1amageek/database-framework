import Core
import DatabaseEngine
import StorageKit

/// Canonical runtime provider for time-window leaderboard indexes.
public struct TimeWindowLeaderboardIndexMaintainerProvider: IndexMaintainerProvider {
    public let kindIdentifier = "time_window_leaderboard"

    public init() {}

    public func makeIndexMaintainer<Item: Persistable>(
        index: Index,
        subspace: Subspace,
        idExpression: KeyExpression,
        configurations: [any IndexConfiguration]
    ) throws -> any IndexMaintainer<Item> {
        let kind = try TimeWindowLeaderboardIndexKind<Item>(canonical: index.kind)
        return TimeWindowLeaderboardIndexMaintainer<Item>(
            index: index,
            subspace: subspace,
            idExpression: idExpression,
            window: kind.window,
            windowCount: kind.windowCount
        )
    }
}
