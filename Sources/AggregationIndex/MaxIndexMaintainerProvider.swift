import DatabaseEngine
import DatabaseKit
import DatabaseTypes
import StorageKit

/// Canonical runtime provider for maximum indexes.
public struct MaxIndexMaintainerProvider: IndexMaintainerProvider {
    public let indexType: IndexType = .aggregate(.maximum)

    public init() {}

    public func makeIndexMaintainer<Item: PersistedEntityValue>(
        index: ResolvedIndex,
        subspace: Subspace,
        idExpression: KeyExpression,
        configurations: [any IndexRuntimeConfiguration],
        wallClock: any WallClock
    ) throws -> any IndexMaintainer<Item> {
        let valueType = try index.aggregateValueType(.maximum)
        switch valueType {

        case .int8:
            return MaxIndexMaintainer<Item, Int8>(index: index, subspace: subspace, idExpression: idExpression)
        case .int16:
            return MaxIndexMaintainer<Item, Int16>(index: index, subspace: subspace, idExpression: idExpression)
        case .int32:
            return MaxIndexMaintainer<Item, Int32>(index: index, subspace: subspace, idExpression: idExpression)
        case .int64:
            return MaxIndexMaintainer<Item, Int64>(index: index, subspace: subspace, idExpression: idExpression)

        case .uint8:
            return MaxIndexMaintainer<Item, UInt8>(index: index, subspace: subspace, idExpression: idExpression)
        case .uint16:
            return MaxIndexMaintainer<Item, UInt16>(index: index, subspace: subspace, idExpression: idExpression)
        case .uint32:
            return MaxIndexMaintainer<Item, UInt32>(index: index, subspace: subspace, idExpression: idExpression)
        case .uint64:
            return MaxIndexMaintainer<Item, UInt64>(index: index, subspace: subspace, idExpression: idExpression)
        case .float32:
            return MaxIndexMaintainer<Item, Float>(index: index, subspace: subspace, idExpression: idExpression)
        case .float64:
            return MaxIndexMaintainer<Item, Double>(index: index, subspace: subspace, idExpression: idExpression)
        case .string:
            return MaxIndexMaintainer<Item, String>(index: index, subspace: subspace, idExpression: idExpression)
        case .date:
            return MaxIndexMaintainer<Item, CivilDate>(index: index, subspace: subspace, idExpression: idExpression)
        case .timestamp:
            return MaxIndexMaintainer<Item, Timestamp>(index: index, subspace: subspace, idExpression: idExpression)
        }
    }

}
