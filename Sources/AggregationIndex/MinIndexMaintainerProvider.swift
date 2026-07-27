import DatabaseKit
import DatabaseEngine
import DatabaseTypes
import StorageKit

/// Canonical runtime provider for minimum indexes.
public struct MinIndexMaintainerProvider: IndexMaintainerProvider {
    public let kindIdentifier = "min"

    public init() {}

    public func makeIndexMaintainer<Item: Persistable>(
        index: Index,
        subspace: Subspace,
        idExpression: KeyExpression,
        configurations: [any IndexRuntimeConfiguration]
    ) throws -> any IndexMaintainer<Item> {
        let valueType = try validate(kind: index.kind)
        switch valueType {

        case .int8:
            return MinIndexMaintainer<Item, Int8>(index: index, subspace: subspace, idExpression: idExpression)
        case .int16:
            return MinIndexMaintainer<Item, Int16>(index: index, subspace: subspace, idExpression: idExpression)
        case .int32:
            return MinIndexMaintainer<Item, Int32>(index: index, subspace: subspace, idExpression: idExpression)
        case .int64:
            return MinIndexMaintainer<Item, Int64>(index: index, subspace: subspace, idExpression: idExpression)

        case .uint8:
            return MinIndexMaintainer<Item, UInt8>(index: index, subspace: subspace, idExpression: idExpression)
        case .uint16:
            return MinIndexMaintainer<Item, UInt16>(index: index, subspace: subspace, idExpression: idExpression)
        case .uint32:
            return MinIndexMaintainer<Item, UInt32>(index: index, subspace: subspace, idExpression: idExpression)
        case .uint64:
            return MinIndexMaintainer<Item, UInt64>(index: index, subspace: subspace, idExpression: idExpression)
        case .float32:
            return MinIndexMaintainer<Item, Float>(index: index, subspace: subspace, idExpression: idExpression)
        case .float64:
            return MinIndexMaintainer<Item, Double>(index: index, subspace: subspace, idExpression: idExpression)
        case .string:
            return MinIndexMaintainer<Item, String>(index: index, subspace: subspace, idExpression: idExpression)
        case .date:
            return MinIndexMaintainer<Item, CivilDate>(index: index, subspace: subspace, idExpression: idExpression)
        case .timestamp:
            return MinIndexMaintainer<Item, Timestamp>(index: index, subspace: subspace, idExpression: idExpression)
        }
    }

    private func validate(kind: IndexKindMetadata) throws -> IndexScalarType {
        try kind.validateIdentity(identifier: kindIdentifier, subspaceStructure: .flat)
        try kind.validateMetadataKeys(required: ["valueType"])
        try kind.validateFieldCount(minimum: 1)
        return try kind.requireScalarType("valueType")
    }
}
