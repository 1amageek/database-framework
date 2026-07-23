import Core
import DatabaseEngine
import StorageKit

/// Canonical runtime provider for bitmap indexes.
public struct BitmapIndexMaintainerProvider: IndexMaintainerProvider {
    public let kindIdentifier = "bitmap"
    public let runtimeRequirements: IndexRuntimeRequirements = .entityAndPolymorphicReads

    public init() {}

    public func makeIndexMaintainer<Item: Persistable>(
        index: Index,
        subspace: Subspace,
        idExpression: KeyExpression,
        configurations: [any IndexConfiguration]
    ) throws -> any IndexMaintainer<Item> {
        try index.kind.validateIdentity(
            identifier: kindIdentifier,
            subspaceStructure: .hierarchical
        )
        try index.kind.validateMetadataKeys()
        try index.kind.validateFieldCount(minimum: 1)
        return BitmapIndexMaintainer<Item>(
            index: index,
            subspace: subspace,
            idExpression: idExpression
        )
    }
}
