import DatabaseEngine
import DatabaseKit
import StorageKit

/// Canonical runtime provider for spatial indexes.
public struct SpatialIndexMaintainerProvider: IndexMaintainerProvider {
    public let indexType: IndexType = .spatial

    public init() {}

    public func makeIndexMaintainer<Item: PersistedEntityValue>(
        index: ResolvedIndex,
        subspace: Subspace,
        idExpression: KeyExpression,
        configurations: [any IndexRuntimeConfiguration],
        wallClock: any WallClock
    ) throws -> any IndexMaintainer<Item> {
        guard case .spatial(_, let encoding, let level) = index.definition else {
            throw IndexMaintainerProviderError.typeMismatch(
                registered: indexType,
                actual: index.type
            )
        }
        return SpatialIndexMaintainer<Item>(
            index: index,
            encoding: encoding,
            level: level,
            subspace: subspace,
            idExpression: idExpression
        )
    }
}
