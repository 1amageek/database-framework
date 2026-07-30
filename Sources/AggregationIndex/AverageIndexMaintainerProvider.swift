import DatabaseKit
import DatabaseEngine
import StorageKit

/// Canonical runtime provider for average indexes.
public struct AverageIndexMaintainerProvider: IndexMaintainerProvider {
    public let kindIdentifier = "average"

    public init() {}

    public func makeIndexMaintainer<Item: Persistable>(
        index: Index,
        subspace: Subspace,
        idExpression: KeyExpression,
        configurations: [any IndexRuntimeConfiguration],
        wallClock: any WallClock
    ) throws -> any IndexMaintainer<Item> {
        let valueType = try validate(kind: index.kind)
        switch valueType {

        case .int8:
            return AverageIndexMaintainer<Item, Int8>(index: index, subspace: subspace, idExpression: idExpression)
        case .int16:
            return AverageIndexMaintainer<Item, Int16>(index: index, subspace: subspace, idExpression: idExpression)
        case .int32:
            return AverageIndexMaintainer<Item, Int32>(index: index, subspace: subspace, idExpression: idExpression)
        case .int64:
            return AverageIndexMaintainer<Item, Int64>(index: index, subspace: subspace, idExpression: idExpression)

        case .uint8:
            return AverageIndexMaintainer<Item, UInt8>(index: index, subspace: subspace, idExpression: idExpression)
        case .uint16:
            return AverageIndexMaintainer<Item, UInt16>(index: index, subspace: subspace, idExpression: idExpression)
        case .uint32:
            return AverageIndexMaintainer<Item, UInt32>(index: index, subspace: subspace, idExpression: idExpression)
        case .uint64:
            return AverageIndexMaintainer<Item, UInt64>(index: index, subspace: subspace, idExpression: idExpression)
        case .float32:
            return AverageIndexMaintainer<Item, Float>(index: index, subspace: subspace, idExpression: idExpression)
        case .float64:
            return AverageIndexMaintainer<Item, Double>(index: index, subspace: subspace, idExpression: idExpression)
        case .string, .date, .timestamp:
            throw IndexMaintainerProviderError.invalidMetadata(
                kindIdentifier: kindIdentifier,
                key: "valueType"
            )
        }
    }

    private func validate(kind: IndexKindMetadata) throws -> IndexScalarType {
        try kind.validateIdentity(identifier: kindIdentifier, subspaceStructure: .aggregation)
        try kind.validateMetadataKeys(required: ["valueType"])
        try kind.validateFieldCount(minimum: 1)
        return try kind.requireScalarType("valueType")
    }
}
