import DatabaseEngine
import DatabaseKit
import DatabaseRuntime
import DatabaseTypes
import StorageKit
import TestSupport
import Testing

@Suite("Index State Initialization Tests", .serialized)
struct IndexStateInitializationTests {
    @Test("Missing state initializes readable only for an empty source range")
    func initializesEmptyStore() async throws {
        let indexContext = try await makeIndexInitializationContext()

        try await indexContext.container.withTestBaseOperation {
            try await indexContext.indexStateManager.ensureReadable(
                [indexContext.indexName],
                entityRange: indexContext.entityRange
            )

            #expect(
                try await indexContext.indexStateManager.state(
                    of: indexContext.indexName
                ) == .readable
            )
        }
    }

    @Test("Missing state fails when source entities already exist")
    func rejectsMissingStateForNonEmptyStore() async throws {
        let indexContext = try await makeIndexInitializationContext()
        try await indexContext.container.withTestBaseTransaction { transaction in
            try transaction.setValue([1], for: indexContext.entitySubspace.pack(Tuple("entity")))
        }

        try await indexContext.container.withTestBaseOperation {
            await #expect(throws: IndexStateError.self) {
                try await indexContext.indexStateManager.ensureReadable(
                    [indexContext.indexName],
                    entityRange: indexContext.entityRange
                )
            }
            let state = try await indexContext.indexStateManager.state(
                of: indexContext.indexName
            )
            #expect(state == .disabled)
        }
    }

    @Test("Write-only state remains incomplete")
    func rejectsWriteOnlyState() async throws {
        let indexContext = try await makeIndexInitializationContext()
        try await indexContext.container.withTestBaseOperation {
            try await indexContext.indexStateManager.enable(indexContext.indexName)

            await #expect(throws: IndexStateError.self) {
                try await indexContext.indexStateManager.ensureReadable(
                    [indexContext.indexName],
                    entityRange: indexContext.entityRange
                )
            }
            let state = try await indexContext.indexStateManager.state(
                of: indexContext.indexName
            )
            #expect(state == .writeOnly)
        }
    }

    @Test("Disabled state remains incomplete")
    func rejectsDisabledState() async throws {
        let indexContext = try await makeIndexInitializationContext()
        try await indexContext.container.withTestBaseOperation {
            try await indexContext.indexStateManager.disable(indexContext.indexName)

            await #expect(throws: IndexStateError.self) {
                try await indexContext.indexStateManager.ensureReadable(
                    [indexContext.indexName],
                    entityRange: indexContext.entityRange
                )
            }
            let state = try await indexContext.indexStateManager.state(
                of: indexContext.indexName
            )
            #expect(state == .disabled)
        }
    }

    private func makeIndexInitializationContext()
        async throws -> IndexInitializationContext
    {
        let engine = InMemoryEngine()
        let schema = try Schema(
            entities: [BootstrapIndexedEntity.schemaEntity],
            version: Schema.Version(1, 0, 0)
        )
        let container = try await DBContainer.open(
            for: schema,
            configuration: .testing(storageEngine: engine),
            runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(
                executionIdentity: DatabaseExecutionRuntimeIdentity(
                    identifier: "database-tests",
                    revision: 1
                ),
                entityRuntimes: [
                    try DatabaseFrameworkRuntime.entity(
                        BootstrapIndexedEntity.self
                    )
                ]
            ),
            security: .testingDisabled
        )
        let root = try await container.testBaseDataRoot()
            .subspace("index-state-initialization")
        let entitySubspace = root.subspace("entities")
        return IndexInitializationContext(
            container: container,
            indexStateManager: IndexLifecycleStore(
                container: container,
                subspace: root
            ),
            entitySubspace: entitySubspace,
            indexName: "bootstrap_value"
        )
    }

    private struct IndexInitializationContext {
        let container: DBContainer
        let indexStateManager: IndexLifecycleStore
        let entitySubspace: Subspace
        let indexName: String

        var entityRange: (begin: ByteString, end: ByteString) {
            entitySubspace.range()
        }
    }
}
