import DatabaseKit
import DatabaseEngine
import StorageKit

/// Canonical runtime provider for time-window leaderboard indexes.
public struct TimeWindowLeaderboardIndexMaintainerProvider: IndexMaintainerProvider {
    public let kindIdentifier = "time_window_leaderboard"

    public init() {}

    public func makeIndexMaintainer<Item: PersistedEntityValue>(
        index: Index,
        subspace: Subspace,
        idExpression: KeyExpression,
        configurations: [any IndexRuntimeConfiguration],
        wallClock: any WallClock
    ) throws -> any IndexMaintainer<Item> {
        guard index.kind.identifier == kindIdentifier else {
            throw IndexMaintainerProviderError.kindMismatch(
                registered: kindIdentifier,
                actual: index.kind.identifier
            )
        }
        let configuration = try TimeWindowLeaderboardConfiguration(
            metadata: index.kind
        )
        return TimeWindowLeaderboardIndexMaintainer<Item>(
            index: index,
            subspace: subspace,
            idExpression: idExpression,
            window: configuration.window,
            windowCount: configuration.windowCount,
            wallClock: wallClock
        )
    }
}
