import DatabaseKit
import DatabaseEngine
import StorageKit

/// Canonical runtime provider for bitmap indexes.
public struct BitmapIndexMaintainerProvider: IndexMaintainerProvider {
    public let kindIdentifier = BitmapIndexSpecification.identifier
    public let runtimeRequirements: IndexRuntimeRequirements = .entityAndPolymorphicReads

    public init() {}

    public func makeIndexMaintainer<Item: Persistable>(
        index: Index,
        subspace: Subspace,
        idExpression: KeyExpression,
        configurations: [any IndexRuntimeConfiguration],
        wallClock: any WallClock
    ) throws -> any IndexMaintainer<Item> {
        _ = try BitmapIndexSpecification(index.kind)
        return BitmapIndexMaintainer<Item>(
            index: index,
            subspace: subspace,
            idExpression: idExpression
        )
    }
}
