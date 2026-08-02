#if DATABASE_RUNTIME_AGGREGATION_INDEXES
import AggregationIndex
#endif
#if DATABASE_RUNTIME_BITMAP_INDEXES
import BitmapIndex
#endif
import DatabaseEngine
import DatabaseKit
#if DATABASE_RUNTIME_FULL_TEXT_INDEXES
import FullTextIndex
#endif
#if DATABASE_RUNTIME_GRAPH_INDEXES
import GraphIndex
#endif
#if DATABASE_RUNTIME_LEADERBOARD_INDEXES
import LeaderboardIndex
#endif
#if DATABASE_RUNTIME_PERMUTED_INDEXES
import PermutedIndex
#endif
#if DATABASE_RUNTIME_RANK_INDEXES
import RankIndex
#endif
#if DATABASE_RUNTIME_RELATIONSHIPS
import RelationshipIndex
#endif
#if DATABASE_RUNTIME_SCALAR_INDEXES
import ScalarIndex
#endif
#if DATABASE_RUNTIME_SPATIAL_INDEXES
import SpatialIndex
#endif
#if DATABASE_RUNTIME_VECTOR_INDEXES
import VectorIndex
#endif
#if DATABASE_RUNTIME_VERSION_INDEXES
import VersionIndex
#endif

/// Runtime composition assembled from the capabilities selected by package traits.
public enum DatabaseFrameworkRuntime {
    public static func configuration(
        entityRuntimes: [EntityRuntimeRegistration],
        authorizationPolicies: [AuthorizationPolicyHandler] = []
    ) throws(DatabaseRuntimeConfigurationError) -> DatabaseRuntimeConfiguration {
        #if DATABASE_RUNTIME_GRAPH_INDEXES
        try configuration(
            entityRuntimes: entityRuntimes,
            sparqlFunctionRegistry: .empty,
            authorizationPolicies: authorizationPolicies
        )
        #else
        try makeConfiguration(
            entityRuntimes: entityRuntimes,
            authorizationPolicies: authorizationPolicies,
            graphTableSourceExecutor: nil,
            sparqlSourceExecutor: nil
        )
        #endif
    }

    #if DATABASE_RUNTIME_GRAPH_INDEXES
    public static func configuration(
        entityRuntimes: [EntityRuntimeRegistration],
        sparqlFunctionRegistry: SPARQLFunctionRegistry,
        authorizationPolicies: [AuthorizationPolicyHandler] = []
    ) throws(DatabaseRuntimeConfigurationError) -> DatabaseRuntimeConfiguration {
        try makeConfiguration(
            entityRuntimes: entityRuntimes,
            authorizationPolicies: authorizationPolicies,
            graphTableSourceExecutor: GraphTableReadExecutors.sourceExecutor,
            sparqlSourceExecutor: SPARQLReadExecutors.sourceExecutor(
                functionRegistry: sparqlFunctionRegistry
            )
        )
    }
    #endif

    public static func entity<Model: Persistable>(
        _ model: Model.Type,
        including additionalIndexes: [IndexDescriptor] = []
    ) throws -> EntityRuntimeRegistration {
        try definition(
            model,
            including: additionalIndexes
        ).registration()
    }

    #if DATABASE_RUNTIME_GRAPH_INDEXES
    public static func entity<Model: OWLClassEntity>(
        _ model: Model.Type,
        including additionalIndexes: [IndexDescriptor] = []
    ) throws -> EntityRuntimeRegistration {
        var definition = try definition(
            model,
            including: additionalIndexes
        )
        try definition.register(
            OWLClassRDFIndexMaintainerProvider<Model>()
        )
        return definition.registration()
    }
    #endif

    private static func makeConfiguration(
        entityRuntimes: [EntityRuntimeRegistration],
        authorizationPolicies: [AuthorizationPolicyHandler],
        graphTableSourceExecutor: (any GraphTableSourceExecutor)?,
        sparqlSourceExecutor: (any SPARQLSourceExecutor)?
    ) throws(DatabaseRuntimeConfigurationError) -> DatabaseRuntimeConfiguration {
        try DatabaseRuntimeConfiguration(
            indexMaintainerProviderDescriptors: maintainerProviderDescriptors(),
            polymorphicIndexReadExecutors: polymorphicIndexReadExecutors(),
            graphTableSourceExecutor: graphTableSourceExecutor,
            sparqlSourceExecutor: sparqlSourceExecutor,
            persistableMutationMaintainers: persistableMutationMaintainers(),
            entityRuntimes: entityRuntimes,
            authorizationPolicies: authorizationPolicies
        )
    }

    private static func definition<Model: Persistable>(
        _ model: Model.Type,
        including additionalIndexes: [IndexDescriptor]
    ) throws -> EntityRuntimeDefinition<Model> {
        var definition = try EntityRuntimeDefinition(
            model,
            including: additionalIndexes
        )

        #if DATABASE_RUNTIME_VECTOR_INDEXES
        try VectorReadExecutors.register(with: &definition)
        try definition.register(VectorIndexMaintainerProvider())
        #endif
        #if DATABASE_RUNTIME_FULL_TEXT_INDEXES
        try FullTextReadExecutors.register(with: &definition)
        try definition.register(FullTextIndexMaintainerProvider())
        try definition.register(AutocompleteIndexMaintainerProvider())
        #endif
        #if DATABASE_RUNTIME_RANK_INDEXES
        try RankReadExecutors.register(with: &definition)
        try definition.register(RankIndexMaintainerProvider())
        #endif
        #if DATABASE_RUNTIME_BITMAP_INDEXES
        try BitmapReadExecutors.register(with: &definition)
        try definition.register(BitmapIndexMaintainerProvider())
        #endif
        #if DATABASE_RUNTIME_VERSION_INDEXES
        try VersionReadExecutors.register(with: &definition)
        try definition.register(VersionIndexMaintainerProvider())
        #endif
        #if DATABASE_RUNTIME_PERMUTED_INDEXES
        try PermutedReadExecutors.register(with: &definition)
        try definition.register(PermutedIndexMaintainerProvider())
        #endif
        #if DATABASE_RUNTIME_SCALAR_INDEXES
        try definition.register(ScalarIndexMaintainerProvider())
        #endif
        #if DATABASE_RUNTIME_AGGREGATION_INDEXES
        try definition.register(CountIndexMaintainerProvider())
        try definition.register(SumIndexMaintainerProvider())
        try definition.register(MinIndexMaintainerProvider())
        try definition.register(MaxIndexMaintainerProvider())
        try definition.register(AverageIndexMaintainerProvider())
        try definition.register(CountUpdatesIndexMaintainerProvider())
        try definition.register(CountNotNullIndexMaintainerProvider())
        try definition.register(DistinctIndexMaintainerProvider())
        try definition.register(PercentileIndexMaintainerProvider())
        #endif
        #if DATABASE_RUNTIME_LEADERBOARD_INDEXES
        try definition.register(TimeWindowLeaderboardIndexMaintainerProvider())
        #endif
        #if DATABASE_RUNTIME_SPATIAL_INDEXES
        try definition.register(SpatialIndexMaintainerProvider())
        #endif
        #if DATABASE_RUNTIME_GRAPH_INDEXES
        try definition.register(GraphIndexMaintainerProvider())
        try definition.register(RDFQuadIndexMaintainerProvider())
        #endif

        return definition
    }

    private static func maintainerProviderDescriptors() -> [
        IndexMaintainerProviderDescriptor
    ] {
        var descriptors: [IndexMaintainerProviderDescriptor] = []

        #if DATABASE_RUNTIME_SCALAR_INDEXES
        descriptors.append(.init(describing: ScalarIndexMaintainerProvider()))
        #endif
        #if DATABASE_RUNTIME_AGGREGATION_INDEXES
        descriptors.append(.init(describing: CountIndexMaintainerProvider()))
        descriptors.append(.init(describing: SumIndexMaintainerProvider()))
        descriptors.append(.init(describing: MinIndexMaintainerProvider()))
        descriptors.append(.init(describing: MaxIndexMaintainerProvider()))
        descriptors.append(.init(describing: AverageIndexMaintainerProvider()))
        descriptors.append(.init(describing: CountUpdatesIndexMaintainerProvider()))
        descriptors.append(.init(describing: CountNotNullIndexMaintainerProvider()))
        descriptors.append(.init(describing: DistinctIndexMaintainerProvider()))
        descriptors.append(.init(describing: PercentileIndexMaintainerProvider()))
        #endif
        #if DATABASE_RUNTIME_VERSION_INDEXES
        descriptors.append(.init(describing: VersionIndexMaintainerProvider()))
        #endif
        #if DATABASE_RUNTIME_BITMAP_INDEXES
        descriptors.append(.init(describing: BitmapIndexMaintainerProvider()))
        #endif
        #if DATABASE_RUNTIME_LEADERBOARD_INDEXES
        descriptors.append(
            .init(describing: TimeWindowLeaderboardIndexMaintainerProvider())
        )
        #endif
        #if DATABASE_RUNTIME_VECTOR_INDEXES
        descriptors.append(.init(describing: VectorIndexMaintainerProvider()))
        #endif
        #if DATABASE_RUNTIME_FULL_TEXT_INDEXES
        descriptors.append(.init(describing: FullTextIndexMaintainerProvider()))
        descriptors.append(.init(describing: AutocompleteIndexMaintainerProvider()))
        #endif
        #if DATABASE_RUNTIME_SPATIAL_INDEXES
        descriptors.append(.init(describing: SpatialIndexMaintainerProvider()))
        #endif
        #if DATABASE_RUNTIME_RANK_INDEXES
        descriptors.append(.init(describing: RankIndexMaintainerProvider()))
        #endif
        #if DATABASE_RUNTIME_PERMUTED_INDEXES
        descriptors.append(.init(describing: PermutedIndexMaintainerProvider()))
        #endif
        #if DATABASE_RUNTIME_GRAPH_INDEXES
        descriptors.append(.init(describing: GraphIndexMaintainerProvider()))
        descriptors.append(.init(describing: RDFQuadIndexMaintainerProvider()))
        #endif

        return descriptors
    }

    private static func polymorphicIndexReadExecutors() -> [
        any PolymorphicIndexReadExecutor
    ] {
        var executors: [any PolymorphicIndexReadExecutor] = []

        #if DATABASE_RUNTIME_VECTOR_INDEXES
        executors.append(
            VectorReadExecutors.polymorphicIndexExecutor()
        )
        #endif
        #if DATABASE_RUNTIME_FULL_TEXT_INDEXES
        executors.append(FullTextReadExecutors.polymorphicIndexExecutor)
        #endif
        #if DATABASE_RUNTIME_RANK_INDEXES
        executors.append(RankReadExecutors.polymorphicIndexExecutor)
        #endif
        #if DATABASE_RUNTIME_BITMAP_INDEXES
        executors.append(BitmapReadExecutors.polymorphicIndexExecutor)
        #endif
        #if DATABASE_RUNTIME_VERSION_INDEXES
        executors.append(VersionReadExecutors.polymorphicIndexExecutor)
        #endif
        #if DATABASE_RUNTIME_PERMUTED_INDEXES
        executors.append(PermutedReadExecutors.polymorphicIndexExecutor)
        #endif

        return executors
    }

    private static func persistableMutationMaintainers() -> [
        any PersistableMutationMaintainer
    ] {
        #if DATABASE_RUNTIME_RELATIONSHIPS
        [RelationshipReferenceMaintainer()]
        #else
        []
        #endif
    }
}
