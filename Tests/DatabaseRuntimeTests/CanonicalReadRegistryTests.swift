import Testing
import DatabaseKit
import DatabaseTypes
@testable import DatabaseEngine
import DatabaseRuntime
import RelationshipIndex
import ScalarIndex

@Suite("Canonical Read Registry")
struct CanonicalReadRegistryTests {
    private struct EmptyPolymorphicReadExecutor: PolymorphicIndexReadExecutor {
        let kindIdentifier = "test.polymorphic.runtime"

        func executeRows(
            context: DatabaseContext,
            selectQuery: SelectQuery,
            index: PolymorphicIndexMetadata,
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
        let configuration = try DatabaseRuntimeConfiguration()
        #expect(
            configuration.readExecutors.polymorphicIndexExecutor(
                for: "com.example.unknown"
            ) == nil
        )
    }

    @Test("Polymorphic executors register independently from typed executors")
    func polymorphicExecutorsRegisterSeparately() throws {
        let executor = EmptyPolymorphicReadExecutor()
        let configuration = try DatabaseRuntimeConfiguration(
            polymorphicIndexReadExecutors: [executor]
        )

        #expect(
            configuration.readExecutors.polymorphicIndexExecutor(
                for: executor.kindIdentifier
            ) != nil
        )
    }

    @Test("Builtin runtime composes canonical providers and read executors")
    func builtinRuntimeComposesProvidersAndExecutors() throws {
        let entityRuntime = try DatabaseFrameworkRuntime.entity(
            RuntimeConfigurationScalarEntity.self
        )
        let configuration = try DatabaseFrameworkRuntime.configuration(
            entityRuntimes: [entityRuntime]
        )
        let registry = configuration.readExecutors
        let maintainers = configuration.indexMaintainerProviders

        #expect(maintainers.contains(kindIdentifier: "scalar"))
        #expect(maintainers.contains(kindIdentifier: "vector"))
        #expect(maintainers.contains(kindIdentifier: "graph"))
        #expect(maintainers.contains(kindIdentifier: "rdf_quad"))
        #expect(configuration.persistableMutationMaintainers.contains(where: {
            $0.identifier == RelationshipReferenceMaintainer().identifier
        }))

        #expect(entityRuntime.hasIndexReader(for: "vector"))
        #expect(entityRuntime.hasIndexReader(for: "fulltext"))
        #expect(entityRuntime.hasIndexReader(for: "rank"))
        #expect(entityRuntime.hasIndexReader(for: "bitmap"))
        #expect(entityRuntime.hasIndexReader(for: "version"))
        #expect(entityRuntime.hasIndexReader(for: "permuted"))
        #expect(registry.polymorphicIndexExecutor(for: "vector") != nil)
        #expect(registry.polymorphicIndexExecutor(for: "fulltext") != nil)
        #expect(registry.polymorphicIndexExecutor(for: "rank") != nil)
        #expect(registry.polymorphicIndexExecutor(for: "bitmap") != nil)
        #expect(registry.polymorphicIndexExecutor(for: "permuted") != nil)
        #expect(registry.polymorphicIndexExecutor(for: "version") != nil)
        #expect(configuration.logicalSourceExecutors.graphTableExecutor != nil)
        #expect(configuration.logicalSourceExecutors.sparqlExecutor != nil)
    }

    @Test("Duplicate maintainer providers fail configuration")
    func duplicateMaintainerProvidersFailConfiguration() {
        #expect(throws: DatabaseRuntimeConfigurationError.self) {
            try DatabaseRuntimeConfiguration(
                indexMaintainerProviderDescriptors: [
                    .init(describing: ScalarIndexMaintainerProvider()),
                    .init(describing: ScalarIndexMaintainerProvider()),
                ]
            )
        }
    }
}
