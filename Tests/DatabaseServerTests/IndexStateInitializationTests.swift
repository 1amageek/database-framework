import DatabaseTypes
import DatabaseKit
import DatabaseEngine
import DatabaseRuntime
import StorageKit
import Testing

@Suite("Index State Initialization Tests", .serialized)
struct IndexStateInitializationTests {
    @Test("Missing state initializes readable only for an empty source range")
    func initializesEmptyStore() async throws {
        let indexContext = try await makeIndexInitializationContext()

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

    @Test("Missing state fails when source entities already exist")
    func rejectsMissingStateForNonEmptyStore() async throws {
        let indexContext = try await makeIndexInitializationContext()
        try await indexContext.engine.withTransaction { transaction in
            try transaction.setValue([1], for: indexContext.entitySubspace.pack(Tuple("entity")))
        }

        await #expect(throws: IndexStateError.self) {
            try await indexContext.indexStateManager.ensureReadable(
                [indexContext.indexName],
                entityRange: indexContext.entityRange
            )
        }
        #expect(
            try await indexContext.indexStateManager.state(
                of: indexContext.indexName
            ) == .disabled
        )
    }

    @Test("Write-only state remains incomplete")
    func rejectsWriteOnlyState() async throws {
        let indexContext = try await makeIndexInitializationContext()
        try await indexContext.indexStateManager.enable(indexContext.indexName)

        await #expect(throws: IndexStateError.self) {
            try await indexContext.indexStateManager.ensureReadable(
                [indexContext.indexName],
                entityRange: indexContext.entityRange
            )
        }
        #expect(
            try await indexContext.indexStateManager.state(
                of: indexContext.indexName
            ) == .writeOnly
        )
    }

    @Test("Disabled state remains incomplete")
    func rejectsDisabledState() async throws {
        let indexContext = try await makeIndexInitializationContext()
        try await indexContext.indexStateManager.disable(indexContext.indexName)

        await #expect(throws: IndexStateError.self) {
            try await indexContext.indexStateManager.ensureReadable(
                [indexContext.indexName],
                entityRange: indexContext.entityRange
            )
        }
        #expect(
            try await indexContext.indexStateManager.state(
                of: indexContext.indexName
            ) == .disabled
        )
    }

    private func makeIndexInitializationContext() async throws -> IndexInitializationContext {
        let engine = InMemoryEngine()
        let schema = try Schema(
            entities: [DatabaseEndpointEntity.schemaEntity],
            version: Schema.Version(1, 0, 0)
        )
        let container = try await DBContainer.open(
            for: schema,
            configuration: .init(backend: .custom(engine)),
            runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(
                persistableTypes: [DatabaseEndpointEntity.self]
            ),
            security: .disabled
        )
        let root = Subspace(prefix: Tuple("index-state-initialization").pack())
        let entitySubspace = root.subspace("entities")
        return IndexInitializationContext(
            engine: engine,
            indexStateManager: IndexLifecycleStore(container: container, subspace: root),
            entitySubspace: entitySubspace,
            indexName: "initialization_index"
        )
    }

    private struct IndexInitializationContext {
        let engine: any StorageEngine
        let indexStateManager: IndexLifecycleStore
        let entitySubspace: Subspace
        let indexName: String

        var entityRange: (begin: ByteString, end: ByteString) {
            entitySubspace.range()
        }
    }
}
