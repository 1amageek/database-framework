#if !MultiBase
import DatabaseKit
import DatabaseRuntime
import DatabaseTypes
import StorageKit
import TestSupport
import Testing
@_spi(DatabaseExecution) @_spi(Testing) @testable import DatabaseEngine

@Persistable
private struct DirectoryReadProbeEntity {
    #Directory<DirectoryReadProbeEntity>("read-probe", "entities")

    var id: String = ""
    var value: Int32 = 0
}

/// Section 12.1 makes the two directions asymmetric: a read opens the nodes
/// that exist and creates nothing, while a write creates what is missing and
/// commits that metadata with the mutation it serves.
///
/// The evidence here is the store's own Directory tree, walked through the
/// engine's `DirectoryAccess` rather than through the container, so a caller
/// that quietly resolved a Directory in a transaction of its own would leave a
/// node behind that these tests observe.
@Suite("Directory reads create nothing", .serialized)
struct DirectoryReadCreatesNothingTests {
    private static let authorization = AuthorizationContext.authenticated(
        Principal(identifier: "directory-read-probe")
    )

    private static let directoryEntry = DirectoryEntry(
        name: "read-probe",
        layer: .default
    )

    @Test("Opening a container creates no Directory for an entity no write reached")
    func openingContainerCreatesNoApplicationDirectory() async throws {
        let engine = InMemoryEngine()
        let container = try await Self.makeContainer(
            engine: RetainedProbeEngine(engine)
        )

        // The probe entity declares no index, so it owns no lifecycle state to
        // initialize. Bootstrapping therefore opens its Directory instead of
        // creating one, and the application subtree stays empty until a write
        // reaches it.
        #expect(try await Self.dataChildren(of: engine).isEmpty)

        await container.shutdown()
        engine.requestShutdown()
        await engine.waitUntilShutdown()
    }

    @Test("Reading a model that was never written creates no Directory")
    func readingAbsentModelCreatesNoDirectory() async throws {
        let engine = InMemoryEngine()
        let container = try await Self.makeContainer(
            engine: RetainedProbeEngine(engine)
        )
        let context = container.newContext(authorization: Self.authorization)

        let missing = try await context.model(
            for: "never-written",
            as: DirectoryReadProbeEntity.self
        )
        #expect(missing == nil)
        #expect(try await Self.dataChildren(of: engine).isEmpty)

        let queried = try await context.fetch(DirectoryReadProbeEntity.self)
            .where(DirectoryReadProbeEntity.fields.id == "never-written")
            .execute()
        #expect(queried.isEmpty)
        #expect(try await Self.dataChildren(of: engine).isEmpty)

        await container.shutdown()
        engine.requestShutdown()
        await engine.waitUntilShutdown()
    }

    @Test("Administrative reads of an absent collection report an empty keyspace")
    func administrativeReadsReportAbsentKeyspace() async throws {
        let engine = InMemoryEngine()
        let container = try await Self.makeContainer(
            engine: RetainedProbeEngine(engine)
        )
        let admin = container.admin(authorization: Self.authorization)

        let statistics = try await admin.collectionStatistics(
            DirectoryReadProbeEntity.self
        )
        #expect(statistics.entityName == DirectoryReadProbeEntity.persistableType)
        #expect(statistics.documentCount == 0)
        #expect(statistics.storageByteCount == 0)
        #expect(statistics.averageDocumentByteCount == 0)
        #expect(statistics.lastModified == nil)
        #expect(statistics.keyRangeStart == ByteString())
        #expect(statistics.keyRangeEnd == ByteString())

        let size = try await admin.estimatedStorageSize(
            for: DirectoryReadProbeEntity.self
        )
        #expect(size == 0)

        // Statistics maintenance skips a collection whose Directory no write
        // created rather than publishing one to measure.
        try await admin.updateStatistics()

        #expect(try await Self.dataChildren(of: engine).isEmpty)

        await container.shutdown()
        engine.requestShutdown()
        await engine.waitUntilShutdown()
    }

    @Test("A write creates the Directory that admits it")
    func writeCreatesTheDirectoryItNeeds() async throws {
        let engine = InMemoryEngine()
        let container = try await Self.makeContainer(
            engine: RetainedProbeEngine(engine)
        )
        let context = container.newContext(authorization: Self.authorization)

        var entity = DirectoryReadProbeEntity()
        entity.id = "written"
        entity.value = 7
        try context.insert(entity)
        try await context.save()

        #expect(try await Self.dataChildren(of: engine) == [Self.directoryEntry])

        let stored = try await context.model(
            for: entity.id,
            as: DirectoryReadProbeEntity.self
        )
        #expect(stored?.value == 7)

        // A read that misses inside a Directory a write created adds nothing
        // beside it.
        let missing = try await context.model(
            for: "still-never-written",
            as: DirectoryReadProbeEntity.self
        )
        #expect(missing == nil)
        #expect(try await Self.dataChildren(of: engine) == [Self.directoryEntry])

        await container.shutdown()
        engine.requestShutdown()
        await engine.waitUntilShutdown()
    }

    // MARK: - Helpers

    /// Children of the Default Partition's `data` Directory, read straight from
    /// the engine so the observation does not depend on the container paths
    /// under test.
    private static func dataChildren(
        of engine: InMemoryEngine
    ) async throws -> [DirectoryEntry] {
        let access = engine.directoryAccess
        return try await engine.withTransaction { transaction in
            guard let storeRoot = try await access.openRoot(
                transaction: transaction
            ),
            let partition = try await access.openPartition(
                DatabaseDirectoryLayout.defaultPartitionName,
                in: storeRoot,
                transaction: transaction
            ),
            let data = try await access.openDirectory(
                DatabaseDirectoryLayout.dataDirectoryName,
                in: partition.root,
                transaction: transaction
            ) else {
                return [DirectoryEntry]()
            }
            return try await access.listChildren(
                in: data,
                after: nil,
                limit: 16,
                transaction: transaction
            )
        }
    }

    private static func makeContainer(
        engine: any StorageEngine
    ) async throws -> DBContainer {
        try await DBContainer.open(
            for: try schema(),
            configuration: DBConfiguration(
                storageEngine: engine,
                monotonicClock: TestProcessMonotonicClock(),
                wallClock: FixedTestWallClock()
            ),
            runtimeConfiguration: try runtimeConfiguration(),
            security: .testingDisabled
        )
    }

    private static func schema() throws -> Schema {
        try Schema(
            entities: [try DirectoryReadProbeEntity.schemaEntity],
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
                    DirectoryReadProbeEntity.self
                )
            ]
        )
    }
}

/// Keeps one `InMemoryEngine` alive across a container's lifetime so the test
/// can observe the store after the container it opened has shut down.
private final class RetainedProbeEngine: StorageEngine, Sendable {
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
