import DatabaseEngine
import DatabaseKit
import StorageKit

/// Canonical runtime provider for bitmap indexes.
public struct BitmapIndexMaintainerProvider: IndexMaintainerProvider {
    public let indexType: IndexType = .bitmap
    public let runtimeRequirements: IndexRuntimeRequirements = .entityAndPolymorphicReads

    public init() {}

    public func makeIndexMaintainer<Item: PersistedEntityValue>(
        index: ResolvedIndex,
        subspace: Subspace,
        idExpression: KeyExpression,
        configurations: [any IndexRuntimeConfiguration],
        wallClock: any WallClock
    ) throws -> any IndexMaintainer<Item> {
        guard case .bitmap = index.definition else {
            throw IndexMaintainerProviderError.typeMismatch(
                registered: indexType,
                actual: index.type
            )
        }
        return BitmapIndexMaintainer<Item>(
            index: index,
            subspace: subspace,
            idExpression: idExpression
        )
    }
}
