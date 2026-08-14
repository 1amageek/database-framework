#if !MultipleBases
import DatabaseKit
import DatabaseRuntime
import DatabaseTypes
import StorageKit
import TestSupport
import Testing
@_spi(DatabaseExecution) @_spi(Testing) @testable import DatabaseEngine

@Persistable
private struct StandardDatabaseRootEntity {
    #Directory<StandardDatabaseRootEntity>("standard-root", "entities")

    var id: String = ""
    var value: String = ""
}

@Suite("Standard database root", .serialized)
struct StandardDatabaseRootTests {
    private static let authorization = AuthorizationContext.authenticated(
        Principal(identifier: "standard-root")
    )

    @Test("The engine root is authoritative by default and reopens")
    func engineRootReopens() async throws {
        let sharedEngine = InMemoryEngine()
        let firstContainer = try await makeContainer(
            engine: RetainedInMemoryEngine(sharedEngine)
        )
        let firstContext = firstContainer.newContext(
            authorization: Self.authorization
        )
        var entity = StandardDatabaseRootEntity()
        entity.id = "entity-1"
        entity.value = "persisted"

        try firstContext.insert(entity)
        try await firstContext.save()

        let executionStorage = try firstContainer.executionStorage()
        #expect(executionStorage.root == Subspace())
        let resolvedDirectory = try await firstContainer
            .resolveDirectoryForTesting(for: StandardDatabaseRootEntity.self)
        #expect(
            resolvedDirectory
                == Subspace()
                    .subspace("data")
                    .subspace("standard-root")
                    .subspace("entities")
        )

        await firstContainer.shutdown()

        let reopenedContainer = try await DBContainer.openRestoringSchema(
            configuration: DBConfiguration(
                storageEngine: RetainedInMemoryEngine(sharedEngine),
                monotonicClock: TestProcessMonotonicClock(),
                wallClock: FixedTestWallClock()
            ),
            security: .testingDisabled
        ) { _ in
            try Self.runtimeConfiguration()
        }
        let reopened = try await reopenedContainer.newContext(
            authorization: Self.authorization
        ).model(for: entity.id, as: StandardDatabaseRootEntity.self)
        #expect(reopened?.value == "persisted")

        await reopenedContainer.shutdown()
        sharedEngine.requestShutdown()
        await sharedEngine.waitUntilShutdown()
    }

    @Test("A host-selected root is retained without namespace resolution")
    func hostSelectedRootIsRetained() async throws {
        let sharedEngine = InMemoryEngine()
        let root = Subspace("host-selected-root")
        let container = try await makeContainer(
            engine: RetainedInMemoryEngine(sharedEngine),
            root: root
        )

        #expect(try container.executionStorage().root == root)
        let rootDescriptor = root
            .subspace("_database-framework")
            .pack(Tuple("format"))
        let engineRootDescriptor = Subspace()
            .subspace("_database-framework")
            .pack(Tuple("format"))
        let stored = try await sharedEngine.withTransaction { transaction in
            (
                try await transaction.getValue(
                    for: rootDescriptor,
                    snapshot: true
                ),
                try await transaction.getValue(
                    for: engineRootDescriptor,
                    snapshot: true
                )
            )
        }
        #expect(stored.0 != nil)
        #expect(stored.1 == nil)

        await container.shutdown()
        sharedEngine.requestShutdown()
        await sharedEngine.waitUntilShutdown()
    }

    @Test("A populated selected root without a descriptor is not mutated")
    func populatedRootWithoutDescriptorIsRejected() async throws {
        let sharedEngine = InMemoryEngine()
        let root = Subspace("selected-root")
        let sentinelKey = root.subspace("data").pack(Tuple("existing-data"))
        let sentinelValue: ByteString = [0x01, 0x02, 0x03]
        try await sharedEngine.withTransaction { transaction in
            try transaction.setValue(sentinelValue, for: sentinelKey)
        }

        await #expect(
            throws: DatabaseFormatCatalogError
                .descriptorMissingInNonEmptyDatabase
        ) {
            _ = try await self.makeContainer(
                engine: RetainedInMemoryEngine(sharedEngine),
                root: root
            )
        }

        let descriptorKey = root
            .subspace("_database-framework")
            .pack(Tuple("format"))
        let stored = try await sharedEngine.withTransaction { transaction in
            (
                try await transaction.getValue(for: sentinelKey, snapshot: true),
                try await transaction.getValue(for: descriptorKey, snapshot: true)
            )
        }
        #expect(stored.0 == sentinelValue)
        #expect(stored.1 == nil)

        sharedEngine.requestShutdown()
        await sharedEngine.waitUntilShutdown()
    }

    private func makeContainer(
        engine: any StorageEngine,
        root: Subspace = Subspace()
    ) async throws -> DBContainer {
        try await DBContainer.open(
            for: Self.schema(),
            configuration: DBConfiguration(
                storageEngine: engine,
                databaseRoot: root,
                monotonicClock: TestProcessMonotonicClock(),
                wallClock: FixedTestWallClock()
            ),
            runtimeConfiguration: Self.runtimeConfiguration(),
            security: .testingDisabled
        )
    }

    private static func schema() throws -> Schema {
        try Schema(
            entities: [try StandardDatabaseRootEntity.schemaEntity],
            version: Schema.Version(1, 0, 0)
        )
    }

    private static func runtimeConfiguration() throws
        -> DatabaseRuntimeConfiguration {
        try DatabaseFrameworkRuntime.configuration(
            entityRuntimes: [
                try DatabaseFrameworkRuntime.entity(
                    StandardDatabaseRootEntity.self
                ),
            ]
        )
    }
}

private final class RetainedInMemoryEngine: StorageEngine, Sendable {
    struct Configuration: Sendable {}
    typealias TransactionType = InMemoryTransaction

    private let shared: InMemoryEngine

    init(_ shared: InMemoryEngine) {
        self.shared = shared
    }

    init(configuration: Configuration) async throws {
        self.shared = InMemoryEngine()
    }

    var namespaceResolver: any NamespaceResolver {
        shared.namespaceResolver
    }

    var namespaceCatalog: (any NamespaceCatalog)? {
        shared.namespaceCatalog
    }

    func createTransaction() throws -> InMemoryTransaction {
        try shared.createTransaction()
    }

    func requestShutdown() {}

    func waitUntilShutdown() async {}
}
#endif
