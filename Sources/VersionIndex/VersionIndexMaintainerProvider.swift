import DatabaseEngine
import DatabaseKit
import StorageKit

/// Canonical runtime provider for version-history indexes.
public struct VersionIndexMaintainerProvider: IndexMaintainerProvider {
    public let indexType: IndexType = .history
    public let runtimeRequirements: IndexRuntimeRequirements = .versionHistory

    public init() {}

    public func makeIndexMaintainer<Item: PersistedEntityValue>(
        index: ResolvedIndex,
        subspace: Subspace,
        idExpression: KeyExpression,
        configurations: [any IndexRuntimeConfiguration],
        wallClock: any WallClock
    ) throws -> any IndexMaintainer<Item> {
        guard case .history(_, let strategy) = index.definition else {
            throw IndexMaintainerProviderError.typeMismatch(
                registered: indexType,
                actual: index.type
            )
        }

        return VersionIndexMaintainer<Item>(
            index: index,
            strategy: strategy,
            subspace: subspace,
            idExpression: idExpression,
            wallClock: wallClock
        )
    }
}
