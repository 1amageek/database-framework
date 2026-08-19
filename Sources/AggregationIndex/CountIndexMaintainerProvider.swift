import DatabaseEngine
import DatabaseKit
import StorageKit

/// Canonical runtime provider for count indexes.
public struct CountIndexMaintainerProvider: IndexMaintainerProvider {
    public let indexType: IndexType = .aggregate(.count)

    public init() {}

    public func makeIndexMaintainer<Item: PersistedEntityValue>(
        index: ResolvedIndex,
        subspace: Subspace,
        idExpression: KeyExpression,
        configurations: [any IndexRuntimeConfiguration],
        wallClock: any WallClock
    ) throws -> any IndexMaintainer<Item> {
        _ = try index.aggregateDefinition(.count)
        return CountIndexMaintainer<Item>(
            index: index,
            subspace: subspace,
            idExpression: idExpression
        )
    }
}
