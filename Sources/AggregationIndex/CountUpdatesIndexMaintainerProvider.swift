import DatabaseEngine
import DatabaseKit
import StorageKit

/// Canonical runtime provider for count-updates indexes.
public struct CountUpdatesIndexMaintainerProvider: IndexMaintainerProvider {
    public let indexType: IndexType = .updateCount

    public init() {}

    public func makeIndexMaintainer<Item: PersistedEntityValue>(
        index: ResolvedIndex,
        subspace: Subspace,
        idExpression: KeyExpression,
        configurations: [any IndexRuntimeConfiguration],
        wallClock: any WallClock
    ) throws -> any IndexMaintainer<Item> {
        guard case .updateCount = index.definition else {
            throw IndexMaintainerProviderError.typeMismatch(
                registered: indexType,
                actual: index.type
            )
        }
        return CountUpdatesIndexMaintainer<Item>(
            index: index,
            subspace: subspace,
            idExpression: idExpression
        )
    }
}
