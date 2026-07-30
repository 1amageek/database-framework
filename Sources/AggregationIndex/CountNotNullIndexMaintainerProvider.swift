import DatabaseKit
import DatabaseEngine
import StorageKit

/// Canonical runtime provider for count-not-null indexes.
public struct CountNotNullIndexMaintainerProvider: IndexMaintainerProvider {
    public let kindIdentifier = "count_not_null"

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
            subspaceStructure: .aggregation
        )
        try index.kind.validateMetadataKeys()
        try index.kind.validateFieldCount(minimum: 1)
        guard let valueFieldName = index.kind.fieldNames.last else {
            throw IndexMaintainerProviderError.invalidMetadata(
                kindIdentifier: kindIdentifier,
                key: "fieldNames"
            )
        }
        return CountNotNullIndexMaintainer<Item>(
            index: index,
            subspace: subspace,
            idExpression: idExpression,
            groupByFieldNames: Array(index.kind.fieldNames.dropLast()),
            valueFieldName: valueFieldName
        )
    }
}
