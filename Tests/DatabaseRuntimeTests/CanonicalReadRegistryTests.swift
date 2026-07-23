import Testing
import Core
import DatabaseValue
import QueryIR
import DatabaseEngine
import DatabaseRuntime
import RelationshipIndex
import ScalarIndex

@Suite("Canonical Read Registry")
struct CanonicalReadRegistryTests {
    private struct EmptyPolymorphicReadExecutor: PolymorphicIndexReadExecutor {
        let kindIdentifier = "test.polymorphic.runtime"

        func executeRows(
            context: FDBContext,
            selectQuery: SelectQuery,
            indexScan: IndexScanSource,
            group: PolymorphicGroup,
            options: DatabaseEngine.ReadExecutionContext,
            partitions: [DatabaseObjectField]
        ) async throws -> IndexReadResult {
            .empty
        }
    }

    @Test("Unknown kindIdentifier is not resolved")
    func unknownKindIdentifierReturnsNil() throws {
        let configuration = try DatabaseRuntimeConfiguration()
        #expect(
            configuration.readExecutors.indexExecutor(
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
            configuration.readExecutors.indexExecutor(
                for: executor.kindIdentifier
            ) == nil
        )
        #expect(
            configuration.readExecutors.polymorphicIndexExecutor(
                for: executor.kindIdentifier
            ) != nil
        )
    }

    @Test("Builtin runtime composes canonical providers and read executors")
    func builtinRuntimeComposesProvidersAndExecutors() throws {
        let configuration = try DatabaseFrameworkRuntime.configuration()
        let registry = configuration.readExecutors
        let maintainers = configuration.indexMaintainerProviders

        #expect(maintainers.contains(kindIdentifier: "scalar"))
        #expect(maintainers.contains(kindIdentifier: "vector"))
        #expect(maintainers.contains(kindIdentifier: "graph"))
        #expect(maintainers.contains(kindIdentifier: "rdf_quad"))
        #expect(configuration.persistableMutationMaintainers.contains(where: {
            $0.identifier == RelationshipReferenceMaintainer().identifier
        }))

        #expect(registry.indexExecutor(for: "vector") != nil)
        #expect(registry.indexExecutor(for: "fulltext") != nil)
        #expect(registry.indexExecutor(for: "rank") != nil)
        #expect(registry.indexExecutor(for: "bitmap") != nil)
        #expect(registry.indexExecutor(for: "version") != nil)
        #expect(registry.indexExecutor(for: "permuted") != nil)
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
                indexMaintainerProviders: [
                    ScalarIndexMaintainerProvider(),
                    ScalarIndexMaintainerProvider(),
                ]
            )
        }
    }
}
