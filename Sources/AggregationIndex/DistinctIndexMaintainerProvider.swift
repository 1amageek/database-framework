import DatabaseKit
import DatabaseEngine
import StorageKit

/// Canonical runtime provider for distinct-count indexes.
public struct DistinctIndexMaintainerProvider: IndexMaintainerProvider {
    public let kindIdentifier = "distinct"

    public init() {}

    public func makeIndexMaintainer<Item: PersistedEntityValue>(
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
        try index.kind.validateMetadataKeys(required: ["precision"])
        try index.kind.validateFieldCount(minimum: 1)
        let precision = try index.kind.requireInt("precision")
        guard DistinctIndexMaintainer<Item>.supportedPersistedPrecision.contains(
            precision
        ) else {
            throw IndexMaintainerProviderError.invalidMetadata(
                kindIdentifier: kindIdentifier,
                key: "precision"
            )
        }
        return DistinctIndexMaintainer<Item>(
            index: index,
            subspace: subspace,
            idExpression: idExpression,
            precision: precision
        )
    }
}
