import Core
import DatabaseEngine
import StorageKit

/// Canonical runtime provider for sum indexes.
public struct SumIndexMaintainerProvider: IndexMaintainerProvider {
    public let kindIdentifier = "sum"

    public init() {}

    public func makeIndexMaintainer<Item: Persistable>(
        index: Index,
        subspace: Subspace,
        idExpression: KeyExpression,
        configurations: [any IndexConfiguration]
    ) throws -> any IndexMaintainer<Item> {
        let valueType = try validate(kind: index.kind)
        switch valueType {
        case .int:
            return SumIndexMaintainer<Item, Int>(index: index, subspace: subspace, idExpression: idExpression)
        case .int8:
            return SumIndexMaintainer<Item, Int8>(index: index, subspace: subspace, idExpression: idExpression)
        case .int16:
            return SumIndexMaintainer<Item, Int16>(index: index, subspace: subspace, idExpression: idExpression)
        case .int32:
            return SumIndexMaintainer<Item, Int32>(index: index, subspace: subspace, idExpression: idExpression)
        case .int64:
            return SumIndexMaintainer<Item, Int64>(index: index, subspace: subspace, idExpression: idExpression)
        case .uint:
            return SumIndexMaintainer<Item, UInt>(index: index, subspace: subspace, idExpression: idExpression)
        case .uint8:
            return SumIndexMaintainer<Item, UInt8>(index: index, subspace: subspace, idExpression: idExpression)
        case .uint16:
            return SumIndexMaintainer<Item, UInt16>(index: index, subspace: subspace, idExpression: idExpression)
        case .uint32:
            return SumIndexMaintainer<Item, UInt32>(index: index, subspace: subspace, idExpression: idExpression)
        case .uint64:
            return SumIndexMaintainer<Item, UInt64>(index: index, subspace: subspace, idExpression: idExpression)
        case .float:
            return SumIndexMaintainer<Item, Float>(index: index, subspace: subspace, idExpression: idExpression)
        case .double:
            return SumIndexMaintainer<Item, Double>(index: index, subspace: subspace, idExpression: idExpression)
        case .string, .date:
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
