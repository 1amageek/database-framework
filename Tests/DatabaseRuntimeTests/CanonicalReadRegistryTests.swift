import DatabaseKit
import DatabaseRuntime
import DatabaseTypes
import RelationshipIndex
import ScalarIndex
import Testing

@testable import DatabaseEngine

@Suite("Canonical Read Registry")
struct CanonicalReadRegistryTests {
    private struct EmptyPolymorphicReadExecutor: PolymorphicIndexReadExecutor {
        let indexType: IndexType = .custom("test.polymorphic.runtime")

        func executeRows(
            context: DatabaseContext,
            selectQuery: SelectQuery,
            index: IndexDeclaration<String>,
            indexScan: IndexScanSource,
            group: PolymorphicGroup,
            options: DatabaseEngine.ReadExecutionContext,
            partitions: FieldObject
        ) async throws -> IndexReadResult {
            .empty
        }
    }

    @Test("Unknown polymorphic kind identifier is not resolved")
    func unknownKindIdentifierReturnsNil() throws {
        let configuration = try DatabaseRuntimeConfiguration(
            executionIdentity: DatabaseExecutionRuntimeIdentity(
                identifier: "database-tests",
                revision: 1
            ),
        )
        #expect(
            configuration.readExecutors.polymorphicIndexExecutor(
                for: .custom("com.example.unknown")
            ) == nil
        )
    }

    @Test("Polymorphic executors register independently from typed executors")
    func polymorphicExecutorsRegisterSeparately() throws {
        let executor = EmptyPolymorphicReadExecutor()
        let configuration = try DatabaseRuntimeConfiguration(
            executionIdentity: DatabaseExecutionRuntimeIdentity(
                identifier: "database-tests",
                revision: 1
            ),
            polymorphicIndexReadExecutors: [executor]
        )

        #expect(
            configuration.readExecutors.polymorphicIndexExecutor(
                for: executor.indexType
            ) != nil
        )
    }

    @Test("Builtin runtime matches the selected feature traits exactly")
    func builtinRuntimeMatchesSelectedFeatureTraits() throws {
        let entityRuntime = try DatabaseFrameworkRuntime.entity(
            RuntimeConfigurationScalarEntity.self
        )
        let configuration = try DatabaseFrameworkRuntime.configuration(
            executionIdentity: DatabaseExecutionRuntimeIdentity(
                identifier: "database-tests",
                revision: 1
            ),
            entityRuntimes: [entityRuntime]
        )
        let registry = configuration.readExecutors
        let maintainers = configuration.indexMaintainerProviders

        let maintainerExpectations: [(IndexType, Bool)] = [
            (.ordered, RuntimeFeatureExpectations.scalarIndexes),
            (.vector, RuntimeFeatureExpectations.vectorIndexes),
            (.text(.fullText), RuntimeFeatureExpectations.fullTextIndexes),
            (.text(.autocomplete), RuntimeFeatureExpectations.fullTextIndexes),
            (.spatial, RuntimeFeatureExpectations.spatialIndexes),
            (.rank, RuntimeFeatureExpectations.rankIndexes),
            (.bitmap, RuntimeFeatureExpectations.bitmapIndexes),
            (.history, RuntimeFeatureExpectations.versionIndexes),
            (.graph(.property), RuntimeFeatureExpectations.graphIndexes),
            (.graph(.rdf), RuntimeFeatureExpectations.graphIndexes),
            (.aggregate(.count), RuntimeFeatureExpectations.aggregationIndexes),
            (.aggregate(.sum), RuntimeFeatureExpectations.aggregationIndexes),
            (.aggregate(.minimum), RuntimeFeatureExpectations.aggregationIndexes),
            (.aggregate(.maximum), RuntimeFeatureExpectations.aggregationIndexes),
            (.aggregate(.average), RuntimeFeatureExpectations.aggregationIndexes),
            (.updateCount, RuntimeFeatureExpectations.aggregationIndexes),
            (.aggregate(.nonNullCount), RuntimeFeatureExpectations.aggregationIndexes),
            (
                .aggregate(.approximateDistinct),
                RuntimeFeatureExpectations.aggregationIndexes
            ),
            (.aggregate(.percentile), RuntimeFeatureExpectations.aggregationIndexes),
            (.leaderboard, RuntimeFeatureExpectations.leaderboardIndexes),
        ]
        for (indexType, isExpected) in maintainerExpectations {
            #expect(maintainers.contains(indexType: indexType) == isExpected)
        }

        let typedReadExpectations: [(IndexType, Bool)] = [
            (.vector, RuntimeFeatureExpectations.vectorIndexes),
            (.text(.fullText), RuntimeFeatureExpectations.fullTextIndexes),
            (.rank, RuntimeFeatureExpectations.rankIndexes),
            (.bitmap, RuntimeFeatureExpectations.bitmapIndexes),
            (.history, RuntimeFeatureExpectations.versionIndexes),
        ]
        for (indexType, isExpected) in typedReadExpectations {
            #expect(entityRuntime.hasIndexReader(for: indexType) == isExpected)
            #expect(
                (registry.polymorphicIndexExecutor(for: indexType) != nil)
                    == isExpected
            )
        }

        let fusionReadExpectations: [(IndexType, Bool)] = [
            (.vector, RuntimeFeatureExpectations.vectorIndexes),
            (.text(.fullText), RuntimeFeatureExpectations.fullTextIndexes),
            (.spatial, RuntimeFeatureExpectations.spatialIndexes),
            (.bitmap, RuntimeFeatureExpectations.bitmapIndexes),
            (.leaderboard, RuntimeFeatureExpectations.leaderboardIndexes),
        ]
        for (indexType, isExpected) in fusionReadExpectations {
            #expect(
                (configuration.fusionReadExecutors.indexExecutor(
                    for: indexType
                ) != nil) == isExpected
            )
        }

        #expect(
            (configuration.fusionReadExecutors.connectedExecutor(
                for: .graph(.property)
            ) != nil) == RuntimeFeatureExpectations.graphIndexes
        )
        #expect(
            (configuration.logicalSourceExecutors.graphTableExecutor != nil)
                == RuntimeFeatureExpectations.graphIndexes
        )
        #expect(
            (configuration.logicalSourceExecutors.sparqlExecutor != nil)
                == RuntimeFeatureExpectations.graphIndexes
        )
        #expect(
            configuration.persistableMutationMaintainers.contains(where: {
                $0.identifier == RelationshipReferenceMaintainer().identifier
            }) == RuntimeFeatureExpectations.relationships
        )
    }

    @Test("Duplicate maintainer providers fail configuration")
    func duplicateMaintainerProvidersFailConfiguration() {
        #expect(throws: DatabaseRuntimeConfigurationError.self) {
            try DatabaseRuntimeConfiguration(
                executionIdentity: DatabaseExecutionRuntimeIdentity(
                    identifier: "database-tests",
                    revision: 1
                ),
                indexMaintainerProviderDescriptors: [
                    .init(describing: ScalarIndexMaintainerProvider()),
                    .init(describing: ScalarIndexMaintainerProvider()),
                ]
            )
        }
    }
}
