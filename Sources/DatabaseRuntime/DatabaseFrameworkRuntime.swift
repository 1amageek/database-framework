import AggregationIndex
import BitmapIndex
import DatabaseEngine
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
    public static func configuration()
        throws(DatabaseRuntimeConfigurationError) -> DatabaseRuntimeConfiguration {
        try configuration(sparqlFunctionRegistry: .empty)
    }

    public static func configuration(
        sparqlFunctionRegistry: SPARQLFunctionRegistry
    ) throws(DatabaseRuntimeConfigurationError) -> DatabaseRuntimeConfiguration {
        try DatabaseRuntimeConfiguration(
            indexMaintainerProviders: maintainerProviders(),
            indexReadExecutors: [
                VectorReadExecutors.indexExecutor,
                FullTextReadExecutors.indexExecutor,
                RankReadExecutors.indexExecutor,
                BitmapReadExecutors.indexExecutor,
                VersionReadExecutors.indexExecutor,
                PermutedReadExecutors.indexExecutor,
            ],
            polymorphicIndexReadExecutors: [
                VectorReadExecutors.polymorphicIndexExecutor,
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
            recordMutationMaintainers: [RelationshipReferenceMaintainer()]
        )
    }

    private static func maintainerProviders() -> [any IndexMaintainerProvider] {
        [
            ScalarIndexMaintainerProvider(),
            CountIndexMaintainerProvider(),
            SumIndexMaintainerProvider(),
            MinIndexMaintainerProvider(),
            MaxIndexMaintainerProvider(),
            AverageIndexMaintainerProvider(),
            VersionIndexMaintainerProvider(),
            CountUpdatesIndexMaintainerProvider(),
            CountNotNullIndexMaintainerProvider(),
            BitmapIndexMaintainerProvider(),
            TimeWindowLeaderboardIndexMaintainerProvider(),
            DistinctIndexMaintainerProvider(),
            PercentileIndexMaintainerProvider(),
            VectorIndexMaintainerProvider(),
            FullTextIndexMaintainerProvider(),
            SpatialIndexMaintainerProvider(),
            RankIndexMaintainerProvider(),
            PermutedIndexMaintainerProvider(),
            GraphIndexMaintainerProvider(),
            RDFQuadIndexMaintainerProvider(),
            OWLClassRDFIndexMaintainerProvider(),
        ]
    }
}
