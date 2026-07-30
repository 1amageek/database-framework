import DatabaseKit
import DatabaseEngine
import StorageKit

/// Canonical runtime provider for count-updates indexes.
public struct CountUpdatesIndexMaintainerProvider: IndexMaintainerProvider {
    public let kindIdentifier = "count_updates"

    public init() {}

    public func makeIndexMaintainer<Item: Persistable>(
        index: Index,
        subspace: Subspace,
        idExpression: KeyExpression,
        configurations: [any IndexRuntimeConfiguration],
        wallClock: any WallClock
    ) throws -> any IndexMaintainer<Item> {
        try index.kind.validateIdentity(
            identifier: kindIdentifier,
            subspaceStructure: .flat
        )
        try index.kind.validateMetadataKeys()
        try index.kind.validateFieldCount(1)
        return CountUpdatesIndexMaintainer<Item>(
            index: index,
            subspace: subspace,
            idExpression: idExpression
        )
    }
}
