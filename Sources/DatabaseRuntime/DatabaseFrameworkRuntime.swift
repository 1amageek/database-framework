import AggregationIndex
import BitmapIndex
import DatabaseEngine
import DatabaseKit
import FullTextIndex
import GraphIndex
import LeaderboardIndex
import PermutedIndex
import RankIndex
import RelationshipIndex
import ScalarIndex
import SpatialIndex
import VectorIndex
import VersionIndex

/// Runtime composition that exposes the complete database-framework feature set.
public enum DatabaseFrameworkRuntime {
    public static func configuration(
        entityRuntimes: [EntityRuntimeRegistration],
        authorizationPolicies: [AuthorizationPolicyHandler] = []
    ) throws(DatabaseRuntimeConfigurationError) -> DatabaseRuntimeConfiguration {
        try configuration(
            entityRuntimes: entityRuntimes,
            sparqlFunctionRegistry: .empty,
            authorizationPolicies: authorizationPolicies
        )
    }

    public static func configuration(
        entityRuntimes: [EntityRuntimeRegistration],
        sparqlFunctionRegistry: SPARQLFunctionRegistry,
        authorizationPolicies: [AuthorizationPolicyHandler] = []
    ) throws(DatabaseRuntimeConfigurationError) -> DatabaseRuntimeConfiguration {
        try DatabaseRuntimeConfiguration(
            indexMaintainerProviderDescriptors: maintainerProviderDescriptors(),
            polymorphicIndexReadExecutors: [
                VectorReadExecutors.polymorphicIndexExecutor(),
                FullTextReadExecutors.polymorphicIndexExecutor,
                RankReadExecutors.polymorphicIndexExecutor,
                BitmapReadExecutors.polymorphicIndexExecutor,
                VersionReadExecutors.polymorphicIndexExecutor,
                PermutedReadExecutors.polymorphicIndexExecutor,
            ],
            graphTableSourceExecutor: GraphTableReadExecutors.sourceExecutor,
            sparqlSourceExecutor: SPARQLReadExecutors.sourceExecutor(
                functionRegistry: sparqlFunctionRegistry
            ),
            persistableMutationMaintainers: [RelationshipReferenceMaintainer()],
            entityRuntimes: entityRuntimes,
            authorizationPolicies: authorizationPolicies
        )
    }

    public static func entity<Model: Persistable>(
        _ model: Model.Type
    ) throws -> EntityRuntimeRegistration {
        try definition(model).registration()
    }

    public static func entity<Model: OWLClassEntity>(
        _ model: Model.Type
    ) throws -> EntityRuntimeRegistration {
        var definition = try definition(model)
        try definition.register(
            OWLClassRDFIndexMaintainerProvider<Model>()
        )
        return definition.registration()
    }

    private static func definition<Model: Persistable>(
        _ model: Model.Type
    ) throws -> EntityRuntimeDefinition<Model> {
        var definition = try EntityRuntimeDefinition(model)
        try VectorReadExecutors.register(with: &definition)
        try FullTextReadExecutors.register(with: &definition)
        try RankReadExecutors.register(with: &definition)
        try BitmapReadExecutors.register(with: &definition)
        try VersionReadExecutors.register(with: &definition)
        try PermutedReadExecutors.register(with: &definition)
        try definition.register(ScalarIndexMaintainerProvider())
        try definition.register(CountIndexMaintainerProvider())
        try definition.register(SumIndexMaintainerProvider())
        try definition.register(MinIndexMaintainerProvider())
        try definition.register(MaxIndexMaintainerProvider())
        try definition.register(AverageIndexMaintainerProvider())
        try definition.register(VersionIndexMaintainerProvider())
        try definition.register(CountUpdatesIndexMaintainerProvider())
        try definition.register(CountNotNullIndexMaintainerProvider())
        try definition.register(BitmapIndexMaintainerProvider())
        try definition.register(TimeWindowLeaderboardIndexMaintainerProvider())
        try definition.register(DistinctIndexMaintainerProvider())
        try definition.register(PercentileIndexMaintainerProvider())
        try definition.register(VectorIndexMaintainerProvider())
        try definition.register(FullTextIndexMaintainerProvider())
        try definition.register(AutocompleteIndexMaintainerProvider())
        try definition.register(SpatialIndexMaintainerProvider())
        try definition.register(RankIndexMaintainerProvider())
        try definition.register(PermutedIndexMaintainerProvider())
        try definition.register(GraphIndexMaintainerProvider())
        try definition.register(RDFQuadIndexMaintainerProvider())
        return definition
    }

    private static func maintainerProviderDescriptors() -> [
        IndexMaintainerProviderDescriptor
    ] {
        [
            .init(describing: ScalarIndexMaintainerProvider()),
            .init(describing: CountIndexMaintainerProvider()),
            .init(describing: SumIndexMaintainerProvider()),
            .init(describing: MinIndexMaintainerProvider()),
            .init(describing: MaxIndexMaintainerProvider()),
            .init(describing: AverageIndexMaintainerProvider()),
            .init(describing: VersionIndexMaintainerProvider()),
            .init(describing: CountUpdatesIndexMaintainerProvider()),
            .init(describing: CountNotNullIndexMaintainerProvider()),
            .init(describing: BitmapIndexMaintainerProvider()),
            .init(describing: TimeWindowLeaderboardIndexMaintainerProvider()),
            .init(describing: DistinctIndexMaintainerProvider()),
            .init(describing: PercentileIndexMaintainerProvider()),
            .init(describing: VectorIndexMaintainerProvider()),
            .init(describing: FullTextIndexMaintainerProvider()),
            .init(describing: AutocompleteIndexMaintainerProvider()),
            .init(describing: SpatialIndexMaintainerProvider()),
            .init(describing: RankIndexMaintainerProvider()),
            .init(describing: PermutedIndexMaintainerProvider()),
            .init(describing: GraphIndexMaintainerProvider()),
            .init(describing: RDFQuadIndexMaintainerProvider()),
        ]
    }
}
