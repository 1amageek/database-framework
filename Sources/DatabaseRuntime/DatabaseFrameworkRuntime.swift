@_exported import DatabaseEngine
@_exported import DatabaseKit

#if DATABASE_RUNTIME_AGGREGATION_INDEXES
@_exported import AggregationIndex
#endif
#if DATABASE_RUNTIME_BITMAP_INDEXES
@_exported import BitmapIndex
#endif
#if DATABASE_RUNTIME_FULL_TEXT_INDEXES
@_exported import FullTextIndex
#endif
#if DATABASE_RUNTIME_GRAPH_INDEXES
@_exported import GraphIndex
@_exported import OntologyIndex
#endif
#if DATABASE_RUNTIME_LEADERBOARD_INDEXES
@_exported import LeaderboardIndex
#endif
#if DATABASE_RUNTIME_RANK_INDEXES
@_exported import RankIndex
#endif
#if DATABASE_RUNTIME_RELATIONSHIPS
@_exported import RelationshipIndex
#endif
#if DATABASE_RUNTIME_SCALAR_INDEXES
@_exported import ScalarIndex
#endif
#if DATABASE_RUNTIME_SPATIAL_INDEXES
@_exported import SpatialIndex
#endif
#if DATABASE_RUNTIME_VECTOR_INDEXES
@_exported import VectorIndex
#endif
#if DATABASE_RUNTIME_VERSION_INDEXES
@_exported import VersionIndex
#endif

/// Runtime composition assembled from the capabilities selected by package traits.
public enum DatabaseFrameworkRuntime {
    public static func configuration(
        executionIdentity: DatabaseExecutionRuntimeIdentity,
        entityRuntimes: [EntityRuntimeRegistration],
        authorizationPolicies: [AuthorizationPolicyHandler] = [],
        indexConfigurations: [any IndexRuntimeConfiguration] = []
    ) throws(DatabaseRuntimeConfigurationError) -> DatabaseRuntimeConfiguration {
        #if DATABASE_RUNTIME_GRAPH_INDEXES
        try configuration(
            executionIdentity: executionIdentity,
            entityRuntimes: entityRuntimes,
            sparqlFunctionRegistry: .empty,
            authorizationPolicies: authorizationPolicies,
            indexConfigurations: indexConfigurations
        )
        #else
        try makeConfiguration(
            executionIdentity: executionIdentity,
            entityRuntimes: entityRuntimes,
            authorizationPolicies: authorizationPolicies,
            indexConfigurations: indexConfigurations,
            graphTableSourceExecutor: nil,
            sparqlSourceExecutor: nil
        )
        #endif
    }

    public static func configuration(
        executionIdentity: DatabaseExecutionRuntimeIdentity,
        schema: Schema,
        authorizationPolicies: [AuthorizationPolicyHandler] = [],
        indexConfigurations: [any IndexRuntimeConfiguration] = []
    ) throws -> DatabaseRuntimeConfiguration {
        #if DATABASE_RUNTIME_GRAPH_INDEXES
        try configuration(
            executionIdentity: executionIdentity,
            schema: schema,
            sparqlFunctionRegistry: .empty,
            authorizationPolicies: authorizationPolicies,
            indexConfigurations: indexConfigurations
        )
        #else
        try makeConfiguration(
            executionIdentity: executionIdentity,
            entityRuntimes: try schema.entities.map(schemaDrivenEntity),
            authorizationPolicies: authorizationPolicies,
            indexConfigurations: indexConfigurations,
            graphTableSourceExecutor: nil,
            sparqlSourceExecutor: nil
        )
        #endif
    }

    #if DATABASE_RUNTIME_GRAPH_INDEXES
    public static func configuration(
        executionIdentity: DatabaseExecutionRuntimeIdentity,
        entityRuntimes: [EntityRuntimeRegistration],
        sparqlFunctionRegistry: SPARQLFunctionRegistry,
        authorizationPolicies: [AuthorizationPolicyHandler] = [],
        indexConfigurations: [any IndexRuntimeConfiguration] = []
    ) throws(DatabaseRuntimeConfigurationError) -> DatabaseRuntimeConfiguration {
        try makeConfiguration(
            executionIdentity: executionIdentity,
            entityRuntimes: entityRuntimes,
            authorizationPolicies: authorizationPolicies,
            indexConfigurations: indexConfigurations,
            graphTableSourceExecutor: GraphTableReadExecutors.sourceExecutor,
            sparqlSourceExecutor: SPARQLReadExecutors.sourceExecutor(
                functionRegistry: sparqlFunctionRegistry
            )
        )
    }

    public static func configuration(
        executionIdentity: DatabaseExecutionRuntimeIdentity,
        schema: Schema,
        sparqlFunctionRegistry: SPARQLFunctionRegistry,
        authorizationPolicies: [AuthorizationPolicyHandler] = [],
        indexConfigurations: [any IndexRuntimeConfiguration] = []
    ) throws -> DatabaseRuntimeConfiguration {
        try makeConfiguration(
            executionIdentity: executionIdentity,
            entityRuntimes: try schema.entities.map(schemaDrivenEntity),
            authorizationPolicies: authorizationPolicies,
            indexConfigurations: indexConfigurations,
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
            OWLClassRDFIndexMaintainerProvider(entity: definition.entity)
        )
        return definition.registration()
    }
    #endif

    private static func makeConfiguration(
        executionIdentity: DatabaseExecutionRuntimeIdentity,
        entityRuntimes: [EntityRuntimeRegistration],
        authorizationPolicies: [AuthorizationPolicyHandler],
        indexConfigurations: [any IndexRuntimeConfiguration],
        graphTableSourceExecutor: (any GraphTableSourceExecutor)?,
        sparqlSourceExecutor: (any SPARQLSourceExecutor)?
    ) throws(DatabaseRuntimeConfigurationError) -> DatabaseRuntimeConfiguration {
        try DatabaseRuntimeConfiguration(
            executionIdentity: executionIdentity,
            indexMaintainerProviderDescriptors: maintainerProviderDescriptors(),
            polymorphicIndexReadExecutors: polymorphicIndexReadExecutors(),
            graphTableSourceExecutor: graphTableSourceExecutor,
            sparqlSourceExecutor: sparqlSourceExecutor,
            persistableMutationMaintainers: persistableMutationMaintainers(),
            entityRuntimes: entityRuntimes,
            authorizationPolicies: authorizationPolicies,
            indexConfigurations: indexConfigurations
        )
    }

    private static func definition<Model: Persistable>(
        _ model: Model.Type,
        including additionalIndexes: [IndexDescriptor]
    ) throws -> EntityRuntimeDefinition {
        var definition = try EntityRuntimeDefinition(
            model,
            including: additionalIndexes
        )

        #if DATABASE_RUNTIME_VECTOR_INDEXES
        try VectorReadExecutors.register(with: &definition)
        #endif
        #if DATABASE_RUNTIME_FULL_TEXT_INDEXES
        try FullTextReadExecutors.register(with: &definition)
        #endif
        #if DATABASE_RUNTIME_RANK_INDEXES
        try RankReadExecutors.register(with: &definition)
        #endif
        #if DATABASE_RUNTIME_BITMAP_INDEXES
        try BitmapReadExecutors.register(with: &definition)
        #endif
        #if DATABASE_RUNTIME_VERSION_INDEXES
        try VersionReadExecutors.register(with: &definition)
        #endif
        try registerMaintainers(with: &definition)

        return definition
    }

    private static func schemaDrivenEntity(
        _ entity: Schema.Entity
    ) throws -> EntityRuntimeRegistration {
        var definition = EntityRuntimeDefinition(schemaDriven: entity)
        #if DATABASE_RUNTIME_VECTOR_INDEXES
        try VectorReadExecutors.register(with: &definition)
        #endif
        #if DATABASE_RUNTIME_FULL_TEXT_INDEXES
        try FullTextReadExecutors.register(with: &definition)
        #endif
        #if DATABASE_RUNTIME_RANK_INDEXES
        try RankReadExecutors.register(with: &definition)
        #endif
        #if DATABASE_RUNTIME_BITMAP_INDEXES
        try BitmapReadExecutors.register(with: &definition)
        #endif
        #if DATABASE_RUNTIME_VERSION_INDEXES
        try VersionReadExecutors.register(with: &definition)
        #endif
        try registerMaintainers(with: &definition)
        #if DATABASE_RUNTIME_GRAPH_INDEXES
        if case .owlClass = entity.ontology,
           entity.indexes.contains(where: {
                $0.type == .graph(.ontologyProjection)
            }) {
            try definition.register(
                OWLClassRDFIndexMaintainerProvider(entity: entity)
            )
        }
        #endif
        return definition.registration()
    }

    private static func registerMaintainers(
        with definition: inout EntityRuntimeDefinition
    ) throws {
        #if DATABASE_RUNTIME_VECTOR_INDEXES
        try definition.register(VectorIndexMaintainerProvider())
        #endif
        #if DATABASE_RUNTIME_FULL_TEXT_INDEXES
        try definition.register(FullTextIndexMaintainerProvider())
        try definition.register(AutocompleteIndexMaintainerProvider())
        #endif
        #if DATABASE_RUNTIME_RANK_INDEXES
        try definition.register(RankIndexMaintainerProvider())
        #endif
        #if DATABASE_RUNTIME_BITMAP_INDEXES
        try definition.register(BitmapIndexMaintainerProvider())
        #endif
        #if DATABASE_RUNTIME_VERSION_INDEXES
        try definition.register(VersionIndexMaintainerProvider())
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
