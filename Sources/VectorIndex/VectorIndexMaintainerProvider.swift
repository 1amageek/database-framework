import DatabaseKit
import DatabaseEngine
import StorageKit

/// Container-scoped vector maintenance provider.
///
/// The provider owns the immutable HNSW snapshot cache used by maintainers it
/// creates. Separate provider instances never share cached database state.
public struct VectorIndexMaintainerProvider: IndexMaintainerProvider {
    public let kindIdentifier = "vector"

    public var runtimeRequirements: IndexRuntimeRequirements {
        .entityAndPolymorphicReads
    }

    private let graphCache: HNSWGraphCache

    public init(maximumGraphCacheCost: Int = 64 * 1024 * 1024) {
        self.graphCache = HNSWGraphCache(
            maximumCost: maximumGraphCacheCost
        )
    }

    public func makeIndexMaintainer<Item: Persistable>(
        index: Index,
        subspace: Subspace,
        idExpression: KeyExpression,
        configurations: [any IndexRuntimeConfiguration]
    ) throws -> any IndexMaintainer<Item> {
        let specification = try VectorIndexSpecification(index.kind)
        return try specification.makeIndexMaintainer(
            index: index,
            subspace: subspace,
            idExpression: idExpression,
            configurations: configurations,
            graphCache: graphCache
        )
    }
}
