import DatabaseEngine
import DatabaseKit
import StorageKit

/// Canonical runtime provider for percentile indexes.
public struct PercentileIndexMaintainerProvider: IndexMaintainerProvider {
    public let indexType: IndexType = .aggregate(.percentile)

    public init() {}

    public func makeIndexMaintainer<Item: PersistedEntityValue>(
        index: ResolvedIndex,
        subspace: Subspace,
        idExpression: KeyExpression,
        configurations: [any IndexRuntimeConfiguration],
        wallClock: any WallClock
    ) throws -> any IndexMaintainer<Item> {
        let definition = try index.aggregateDefinition(.percentile)
        guard case .percentile(let compression) = definition.function else {
            throw IndexMaintainerProviderError.typeMismatch(
                registered: indexType,
                actual: index.type
            )
        }
        guard compression.isFinite,
              TDigest.supportedCompression.contains(compression) else {
            throw IndexMaintainerProviderError.invalidDefinition(
                indexType: indexType,
                reason: "Percentile compression is unsupported"
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
