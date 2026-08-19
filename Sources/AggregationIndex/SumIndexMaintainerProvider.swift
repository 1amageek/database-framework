import DatabaseEngine
import DatabaseKit
import StorageKit

/// Canonical runtime provider for sum indexes.
public struct SumIndexMaintainerProvider: IndexMaintainerProvider {
    public let indexType: IndexType = .aggregate(.sum)

    public init() {}

    public func makeIndexMaintainer<Item: PersistedEntityValue>(
        index: ResolvedIndex,
        subspace: Subspace,
        idExpression: KeyExpression,
        configurations: [any IndexRuntimeConfiguration],
        wallClock: any WallClock
    ) throws -> any IndexMaintainer<Item> {
        let valueType = try index.aggregateValueType(.sum)
        switch valueType {

        case .int8:
            return SumIndexMaintainer<Item, Int8>(index: index, subspace: subspace, idExpression: idExpression)
        case .int16:
            return SumIndexMaintainer<Item, Int16>(index: index, subspace: subspace, idExpression: idExpression)
        case .int32:
            return SumIndexMaintainer<Item, Int32>(index: index, subspace: subspace, idExpression: idExpression)
        case .int64:
            return SumIndexMaintainer<Item, Int64>(index: index, subspace: subspace, idExpression: idExpression)

        case .uint8:
            return SumIndexMaintainer<Item, UInt8>(index: index, subspace: subspace, idExpression: idExpression)
        case .uint16:
            return SumIndexMaintainer<Item, UInt16>(index: index, subspace: subspace, idExpression: idExpression)
        case .uint32:
            return SumIndexMaintainer<Item, UInt32>(index: index, subspace: subspace, idExpression: idExpression)
        case .uint64:
            return SumIndexMaintainer<Item, UInt64>(index: index, subspace: subspace, idExpression: idExpression)
        case .float32:
            return SumIndexMaintainer<Item, Float>(index: index, subspace: subspace, idExpression: idExpression)
        case .float64:
            return SumIndexMaintainer<Item, Double>(index: index, subspace: subspace, idExpression: idExpression)
        case .string, .date, .timestamp:
            throw IndexMaintainerProviderError.invalidDefinition(
                indexType: indexType,
                reason: "Sum requires a numeric value field"
            )
        }
    }
}
