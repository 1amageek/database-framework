import DatabaseEngine
import DatabaseKit
import StorageKit

/// Canonical runtime provider for distinct-count indexes.
public struct DistinctIndexMaintainerProvider: IndexMaintainerProvider {
    public let indexType: IndexType = .aggregate(.approximateDistinct)

    public init() {}

    public func makeIndexMaintainer<Item: PersistedEntityValue>(
        index: ResolvedIndex,
        subspace: Subspace,
        idExpression: KeyExpression,
        configurations: [any IndexRuntimeConfiguration],
        wallClock: any WallClock
    ) throws -> any IndexMaintainer<Item> {
        let definition = try index.aggregateDefinition(.approximateDistinct)
        guard case .approximateDistinct(let precision) = definition.function else {
            throw IndexMaintainerProviderError.typeMismatch(
                registered: indexType,
                actual: index.type
            )
        }
        guard
            DistinctIndexMaintainer<Item>.supportedPersistedPrecision.contains(
            precision
        ) else {
            throw IndexMaintainerProviderError.invalidDefinition(
                indexType: indexType,
                reason: "Distinct precision is unsupported"
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
