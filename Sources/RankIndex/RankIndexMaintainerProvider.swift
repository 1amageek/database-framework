import DatabaseKit
import DatabaseEngine
import StorageKit

/// Canonical runtime provider for rank indexes.
public struct RankIndexMaintainerProvider: IndexMaintainerProvider {
    public let kindIdentifier = "rank"

    public var runtimeRequirements: IndexRuntimeRequirements {
        .entityAndPolymorphicReads
    }

    public init() {}

    public func makeIndexMaintainer<Item: Persistable>(
        index: Index,
        subspace: Subspace,
        idExpression: KeyExpression,
        configurations: [any IndexRuntimeConfiguration]
    ) throws -> any IndexMaintainer<Item> {
        try index.kind.validateIdentity(identifier: kindIdentifier, subspaceStructure: .hierarchical)
        try index.kind.validateMetadataKeys(required: ["scoreType", "bucketSize"])
        try index.kind.validateFieldCount(1)

        let bucketSize = try index.kind.requireInt("bucketSize")
        guard bucketSize > 0 else {
            throw IndexMaintainerProviderError.invalidMetadata(
                kindIdentifier: kindIdentifier,
                key: "bucketSize"
            )
        }

        switch try index.kind.requireScalarType("scoreType") {
        case .int8:
            return make(Int8.self, index: index, bucketSize: bucketSize, subspace: subspace, idExpression: idExpression)
        case .int16:
            return make(Int16.self, index: index, bucketSize: bucketSize, subspace: subspace, idExpression: idExpression)
        case .int32:
            return make(Int32.self, index: index, bucketSize: bucketSize, subspace: subspace, idExpression: idExpression)
        case .int64:
            return make(Int64.self, index: index, bucketSize: bucketSize, subspace: subspace, idExpression: idExpression)
        case .uint8:
            return make(UInt8.self, index: index, bucketSize: bucketSize, subspace: subspace, idExpression: idExpression)
        case .uint16:
            return make(UInt16.self, index: index, bucketSize: bucketSize, subspace: subspace, idExpression: idExpression)
        case .uint32:
            return make(UInt32.self, index: index, bucketSize: bucketSize, subspace: subspace, idExpression: idExpression)
        case .uint64:
            return make(UInt64.self, index: index, bucketSize: bucketSize, subspace: subspace, idExpression: idExpression)
        case .float32:
            return make(Float.self, index: index, bucketSize: bucketSize, subspace: subspace, idExpression: idExpression)
        case .float64:
            return make(Double.self, index: index, bucketSize: bucketSize, subspace: subspace, idExpression: idExpression)
        case .string, .date, .timestamp:
            throw IndexMaintainerProviderError.invalidMetadata(
                kindIdentifier: kindIdentifier,
                key: "scoreType"
            )
        }
    }

    private func make<Item: Persistable, Score: IndexNumericValue>(
        _ scoreType: Score.Type,
        index: Index,
        bucketSize: Int,
        subspace: Subspace,
        idExpression: KeyExpression
    ) -> RankIndexMaintainer<Item, Score> {
        RankIndexMaintainer<Item, Score>(
            index: index,
            bucketSize: bucketSize,
            subspace: subspace,
            idExpression: idExpression
        )
    }
}
