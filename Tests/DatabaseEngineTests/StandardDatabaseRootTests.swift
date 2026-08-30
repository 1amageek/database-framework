#if !MultiBase
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

    @Test("An empty root path places the database root at the store root")
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

        // Section 13 places the Default Partition directly below the database
        // root, so an empty root path makes it a child of the store root.
        let rootChildren = try await Self.storeRootChildren(of: sharedEngine)
        #expect(
            rootChildren.contains(
                DirectoryEntry(name: "default", layer: .partition)
            )
        )

        // Application data resolves into its own Directory below the Partition's
        // `data` Directory, never into the framework metadata root.
        let executionStorage = try firstContainer.executionStorage()
        let resolvedDirectory = try await firstContainer
            .resolveDirectoryForTesting(for: StandardDatabaseRootEntity.self)
        #expect(resolvedDirectory != executionStorage.systemRoot)
        #expect(resolvedDirectory != executionStorage.dataRoot)

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

    @Test("A host-selected root path contains the whole fixed layout")
    func hostSelectedRootIsRetained() async throws {
        let sharedEngine = InMemoryEngine()
        let container = try await makeContainer(
            engine: RetainedInMemoryEngine(sharedEngine),
            rootPath: ["host-selected-root"]
        )

        // Nothing of the fixed layout escapes the selected database root: the
        // store root holds that Directory alone, and the Default Partition is
        // created below it rather than beside it.
        let rootChildren = try await Self.storeRootChildren(of: sharedEngine)
        #expect(
            rootChildren == [
                DirectoryEntry(name: "host-selected-root", layer: .default)
            ]
        )

        let access = sharedEngine.directoryAccess
        let selectedChildren = try await sharedEngine.withTransaction {
            transaction in
            guard let storeRoot = try await access.openRoot(
                transaction: transaction
            ),
            let selected = try await access.openDirectory(
                "host-selected-root",
                in: storeRoot,
                transaction: transaction
            ) else {
                return [DirectoryEntry]()
            }
            return try await access.listChildren(
                in: selected,
                after: nil,
                limit: 16,
                transaction: transaction
            )
        }
        #expect(
            selectedChildren.contains(
                DirectoryEntry(name: "default", layer: .partition)
            )
        )

        // The physical format descriptor is Framework metadata, so it lives
        // below the Default Partition's `system/database-framework` Directory.
        let descriptorKey = try container.executionStorage()
            .systemRoot
            .pack(Tuple("format"))
        let storedDescriptor = try await sharedEngine.withTransaction {
            transaction in
            try await transaction.getValue(for: descriptorKey, snapshot: true)
        }
        #expect(storedDescriptor != nil)

        await container.shutdown()
        sharedEngine.requestShutdown()
        await sharedEngine.waitUntilShutdown()
    }

    @Test("A populated store no Directory catalog wrote is rejected and unchanged")
    func populatedRootWithoutDescriptorIsRejected() async throws {
        let sharedEngine = InMemoryEngine()
        let sentinelKey = Subspace("existing").pack(Tuple("data"))
        let sentinelValue: ByteString = [0x01, 0x02, 0x03]
        try await sharedEngine.withTransaction { transaction in
            try transaction.setValue(sentinelValue, for: sentinelKey)
        }

        // StorageKit's root state machine rejects a non-empty keyspace that
        // holds no Directory catalog state, so opening the database root fails
        // before the format catalog is ever consulted.
        let failure = await #expect(throws: StorageError.self) {
            _ = try await self.makeContainer(
                engine: RetainedInMemoryEngine(sharedEngine),
                rootPath: ["selected-root"]
            )
        }
        #expect(failure?.code == .incompatibleStorageLayout)

        // A rejected open writes nothing at all.
        let remaining = try await sharedEngine.withTransaction { transaction in
            try await transaction.collectRange(
                begin: ByteString(),
                end: ByteString([0xFF]),
                snapshot: true
            )
        }
        #expect(remaining.count == 1)
        #expect(remaining.first?.0 == sentinelKey)
        #expect(remaining.first?.1 == sentinelValue)

        sharedEngine.requestShutdown()
        await sharedEngine.waitUntilShutdown()
    }

    private func makeContainer(
        engine: any StorageEngine,
        rootPath: [String] = []
    ) async throws -> DBContainer {
        try await DBContainer.open(
            for: Self.schema(),
            configuration: DBConfiguration(
                storageEngine: engine,
                databaseRootPath: rootPath,
                monotonicClock: TestProcessMonotonicClock(),
                wallClock: FixedTestWallClock()
            ),
            runtimeConfiguration: Self.runtimeConfiguration(),
            security: .testingDisabled
        )
    }

    /// Children of the store root Directory, which Section 13 populates with
    /// the database root a configuration selects.
    private static func storeRootChildren(
        of engine: InMemoryEngine
    ) async throws -> [DirectoryEntry] {
        let access = engine.directoryAccess
        return try await engine.withTransaction { transaction in
            guard let storeRoot = try await access.openRoot(
                transaction: transaction
            ) else {
                return [DirectoryEntry]()
            }
            return try await access.listChildren(
                in: storeRoot,
                after: nil,
                limit: 16,
                transaction: transaction
            )
        }
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
            executionIdentity: DatabaseExecutionRuntimeIdentity(
                identifier: "database-tests",
                revision: 1
            ),
            entityRuntimes: [
                try DatabaseFrameworkRuntime.entity(
                    StandardDatabaseRootEntity.self
                )
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

    var transactionDomain: StorageTransactionDomain {
        shared.transactionDomain
    }

    var directoryAccess: any DirectoryAccess {
        shared.directoryAccess
    }

    func createTransaction() throws -> InMemoryTransaction {
        try shared.createTransaction()
    }

    func requestShutdown() {}

    func waitUntilShutdown() async {}
}
#endif
