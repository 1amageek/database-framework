import Core
import DatabaseEngine
import StorageKit

/// Canonical runtime provider for percentile indexes.
public struct PercentileIndexMaintainerProvider: IndexMaintainerProvider {
    public let kindIdentifier = "percentile"

    public init() {}

    public func makeIndexMaintainer<Item: Persistable>(
        index: Index,
        subspace: Subspace,
        idExpression: KeyExpression,
        configurations: [any IndexConfiguration]
    ) throws -> any IndexMaintainer<Item> {
        try index.kind.validateIdentity(
            identifier: kindIdentifier,
            subspaceStructure: .aggregation
        )
        try index.kind.validateMetadataKeys(required: ["compression"])
        try index.kind.validateFieldCount(minimum: 1)
        let compression = try index.kind.requireDouble("compression")
        guard compression.isFinite,
              TDigest.supportedCompression.contains(compression) else {
            throw IndexMaintainerProviderError.invalidMetadata(
                kindIdentifier: kindIdentifier,
                key: "compression"
            )
        }
        return PercentileIndexMaintainer<Item>(
            index: index,
            subspace: subspace,
            idExpression: idExpression,
            compression: compression
        )
    }
}
