import DatabaseKit
import DatabaseEngine
import StorageKit

/// Canonical runtime provider for count indexes.
public struct CountIndexMaintainerProvider: IndexMaintainerProvider {
    public let kindIdentifier = "count"

    public init() {}

    public func makeIndexMaintainer<Item: Persistable>(
        index: Index,
        subspace: Subspace,
        idExpression: KeyExpression,
        configurations: [any IndexRuntimeConfiguration]
    ) throws -> any IndexMaintainer<Item> {
        try index.kind.validateIdentity(
            identifier: kindIdentifier,
            subspaceStructure: .aggregation
        )
        try index.kind.validateMetadataKeys()
        try index.kind.validateFieldNames()
        return CountIndexMaintainer<Item>(
            index: index,
            subspace: subspace,
            idExpression: idExpression
        )
    }
}
