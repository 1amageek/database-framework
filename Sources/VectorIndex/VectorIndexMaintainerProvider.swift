import DatabaseEngine
import DatabaseKit
import StorageKit

/// Container-scoped vector maintenance provider.
///
/// The provider owns the immutable HNSW snapshot cache used by maintainers it
/// creates. Separate provider instances never share cached database state.
public struct VectorIndexMaintainerProvider: IndexMaintainerProvider {
    public let indexType: IndexType = .vector

    public var runtimeRequirements: IndexRuntimeRequirements {
        .entityAndPolymorphicReads
    }

    private let graphCache: HNSWGraphCache
    private let graphResourceLimits: HNSWGraphResourceLimits
    private let trainingResourceLimits: VectorTrainingResourceLimits

    public init(
        maximumGraphCacheCost: Int = 24 * 1_024 * 1_024,
        graphResourceLimits: HNSWGraphResourceLimits = .default,
        trainingResourceLimits: VectorTrainingResourceLimits = .default
    ) {
        self.graphCache = HNSWGraphCache(
            maximumCost: maximumGraphCacheCost
        )
        self.graphResourceLimits = graphResourceLimits
        self.trainingResourceLimits = trainingResourceLimits
    }

    public func physicalLayout(
        for index: ResolvedIndex,
        configurations: [any IndexRuntimeConfiguration]
    ) throws -> IndexPhysicalLayout {
        let matchingConfigurations = configurations.filter {
            $0.indexType == .vector && $0.indexName == index.name
        }
        return try VectorRuntimePolicy.resolve(
            in: matchingConfigurations
        )?.physicalLayout
            ?? IndexPhysicalLayout(
                name: "vector.flat",
                revision: 1
            )
    }

    public func makeIndexMaintainer<Item: PersistedEntityValue>(
        index: ResolvedIndex,
        subspace: Subspace,
        idExpression: KeyExpression,
        configurations: [any IndexRuntimeConfiguration],
        wallClock: any WallClock
    ) throws -> any IndexMaintainer<Item> {
        let specification = try VectorIndexSpecification(index.definition)
        return try specification.makeIndexMaintainer(
            index: index,
            subspace: subspace,
            idExpression: idExpression,
            configurations: configurations,
            graphCache: graphCache,
            graphResourceLimits: graphResourceLimits,
            trainingResourceLimits: trainingResourceLimits
        )
    }
}
