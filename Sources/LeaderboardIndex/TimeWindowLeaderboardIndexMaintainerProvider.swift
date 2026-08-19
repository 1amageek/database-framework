import DatabaseEngine
import DatabaseKit
import StorageKit

/// Canonical runtime provider for time-window leaderboard indexes.
public struct TimeWindowLeaderboardIndexMaintainerProvider: IndexMaintainerProvider {
    public let indexType: IndexType = .leaderboard

    public init() {}

    public func makeIndexMaintainer<Item: PersistedEntityValue>(
        index: ResolvedIndex,
        subspace: Subspace,
        idExpression: KeyExpression,
        configurations: [any IndexRuntimeConfiguration],
        wallClock: any WallClock
    ) throws -> any IndexMaintainer<Item> {
        guard
            case .leaderboard(
                _,
                _,
                let window,
                let windowCount
            ) = index.definition
        else {
            throw IndexMaintainerProviderError.typeMismatch(
                registered: indexType,
                actual: index.type
            )
        }
        return TimeWindowLeaderboardIndexMaintainer<Item>(
            index: index,
            subspace: subspace,
            idExpression: idExpression,
            window: window,
            windowCount: windowCount,
            wallClock: wallClock
        )
    }
}
