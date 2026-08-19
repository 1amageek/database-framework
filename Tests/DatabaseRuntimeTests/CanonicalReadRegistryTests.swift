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

    @Test("Builtin runtime composes canonical providers and read executors")
    func builtinRuntimeComposesProvidersAndExecutors() throws {
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

        #expect(maintainers.contains(indexType: .ordered))
        #expect(maintainers.contains(indexType: .vector))
        #expect(maintainers.contains(indexType: .graph(.property)))
        #expect(maintainers.contains(indexType: .graph(.rdf)))
        #expect(configuration.persistableMutationMaintainers.contains(where: {
            $0.identifier == RelationshipReferenceMaintainer().identifier
        }))

        #expect(entityRuntime.hasIndexReader(for: .vector))
        #expect(entityRuntime.hasIndexReader(for: .text(.fullText)))
        #expect(entityRuntime.hasIndexReader(for: .rank))
        #expect(entityRuntime.hasIndexReader(for: .bitmap))
        #expect(entityRuntime.hasIndexReader(for: .history))
        #expect(registry.polymorphicIndexExecutor(for: .vector) != nil)
        #expect(registry.polymorphicIndexExecutor(for: .text(.fullText)) != nil)
        #expect(registry.polymorphicIndexExecutor(for: .rank) != nil)
        #expect(registry.polymorphicIndexExecutor(for: .bitmap) != nil)
        #expect(registry.polymorphicIndexExecutor(for: .history) != nil)
        #expect(configuration.logicalSourceExecutors.graphTableExecutor != nil)
        #expect(configuration.logicalSourceExecutors.sparqlExecutor != nil)
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
