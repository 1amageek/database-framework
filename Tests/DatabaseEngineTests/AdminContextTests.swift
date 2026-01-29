import Testing
import Foundation
import FoundationDB
import Core
@testable import DatabaseEngine

/// Tests for AdminContext directory resolution
///
/// **Coverage**:
/// - Verifies AdminContext methods respect #Directory macro definitions
/// - Tests indexStatistics, rebuildIndex, collectionStatistics, updateStatistics
/// - Ensures correct directory paths are used (not entity.name)
@Suite("AdminContext Tests", .serialized)
struct AdminContextTests {

    // MARK: - Helper Types

    @Persistable
    struct AdminTestEntity {
        #Directory<AdminTestEntity>("test", "admin", "custom", "path")

        var id: String = ULID().ulidString
        var value: String = ""

        #Index(ScalarIndexKind<AdminTestEntity>(fields: [\.value]))
    }

    @Persistable
    struct AdminTestEntityNoIndex {
        #Directory<AdminTestEntityNoIndex>("test", "admin", "no", "index")

        var id: String = ULID().ulidString
        var name: String = ""
    }

    // MARK: - Helper Methods

    private func setupContainer() async throws -> FDBContainer {
        try await FDBTestEnvironment.shared.ensureInitialized()
        let database = try FDBClient.openDatabase()

        let schema = Schema([
            AdminTestEntity.self,
            AdminTestEntityNoIndex.self
        ], version: Schema.Version(1, 0, 0))

        // Register types in IndexBuilderRegistry
        IndexBuilderRegistry.shared.register(AdminTestEntity.self)
        IndexBuilderRegistry.shared.register(AdminTestEntityNoIndex.self)

        return FDBContainer(
            database: database,
            schema: schema,
            security: .disabled
        )
    }

    private func cleanup(container: FDBContainer) async throws {
        let directoryLayer = DirectoryLayer(database: container.database)
        try? await directoryLayer.remove(path: ["test", "admin"])
    }

    /// Get the first index name for AdminTestEntity from schema
    private func getTestIndexName(from container: FDBContainer) -> String? {
        guard let entity = container.schema.entity(for: AdminTestEntity.self),
              let firstIndex = entity.indexDescriptors.first else {
            return nil
        }
        return firstIndex.name
    }

    // MARK: - Directory Resolution Tests

    @Test("indexStatistics uses correct directory from #Directory macro")
    func indexStatisticsUsesCorrectDirectory() async throws {
        let container = try await setupContainer()
        try await cleanup(container: container)

        // Get index name from schema
        guard let indexName = getTestIndexName(from: container) else {
            throw TestError("No index found for AdminTestEntity")
        }

        // Get index statistics via AdminContext
        // If wrong directory path was used, this would throw because index wouldn't be found
        let admin = container.newAdminContext()
        let stats = try await admin.indexStatistics(indexName)

        // Verify we got valid statistics (index was found at correct path)
        #expect(stats.indexName == indexName)
    }

    @Test("rebuildIndex uses correct directory from #Directory macro")
    func rebuildIndexUsesCorrectDirectory() async throws {
        let container = try await setupContainer()
        try await cleanup(container: container)

        let context = container.newContext()

        // Insert test data
        for i in 0..<10 {
            let entity = AdminTestEntity(value: "value-\(i)")
            context.insert(entity)
        }
        try await context.save()

        // Get index name from schema
        guard let indexName = getTestIndexName(from: container) else {
            throw TestError("No index found for AdminTestEntity")
        }

        // Rebuild index via AdminContext
        // If wrong directory path was used, this would fail because:
        // 1. The index wouldn't be found
        // 2. The data wouldn't be found for rebuilding
        let admin = container.newAdminContext()
        try await admin.rebuildIndex(indexName, progress: nil)

        // Verify rebuild completed (index state is readable)
        let stats = try await admin.indexStatistics(indexName)
        #expect(stats.state == .ready)
    }

    @Test("collectionStatistics uses correct directory from #Directory macro")
    func collectionStatisticsUsesCorrectDirectory() async throws {
        let container = try await setupContainer()
        try await cleanup(container: container)

        let context = container.newContext()

        // Insert test data
        for i in 0..<5 {
            let entity = AdminTestEntity(value: "value-\(i)")
            context.insert(entity)
        }
        try await context.save()

        // Get collection statistics via AdminContext
        let admin = container.newAdminContext()
        let stats = try await admin.collectionStatistics(AdminTestEntity.self)

        // If correct path is used, documentCount should be 5
        #expect(stats.documentCount == 5)
    }

    @Test("updateStatistics uses correct directory from #Directory macro")
    func updateStatisticsUsesCorrectDirectory() async throws {
        let container = try await setupContainer()
        try await cleanup(container: container)

        let context = container.newContext()

        // Insert test data
        for i in 0..<3 {
            let entity = AdminTestEntity(value: "value-\(i)")
            context.insert(entity)
        }
        try await context.save()

        // Update statistics via AdminContext
        let admin = container.newAdminContext()

        // If this completes without error, correct path is being used
        try await admin.updateStatistics()
    }

    @Test("allIndexStatistics uses correct directory from #Directory macro")
    func allIndexStatisticsUsesCorrectDirectory() async throws {
        let container = try await setupContainer()
        try await cleanup(container: container)

        // Get index name from schema
        guard let indexName = getTestIndexName(from: container) else {
            throw TestError("No index found for AdminTestEntity")
        }

        // Get all index statistics via AdminContext
        // If wrong directory path was used, the index wouldn't be found
        let admin = container.newAdminContext()
        let allStats = try await admin.allIndexStatistics()

        // Should include our test index (found at correct path)
        let testIndexStats = allStats.first { $0.indexName == indexName }
        #expect(testIndexStats != nil)
        #expect(testIndexStats?.indexName == indexName)
    }

    // MARK: - Consistency Tests

    @Test("AdminContext and FDBContainer resolve to same directory")
    func adminContextAndContainerResolveToSameDirectory() async throws {
        let container = try await setupContainer()
        try await cleanup(container: container)

        // Resolve directory via FDBContainer
        let containerSubspace = try await container.resolveDirectory(for: AdminTestEntity.self)

        // Insert data and verify it's accessible
        let context = container.newContext()
        let entity = AdminTestEntity(value: "consistency-test")
        context.insert(entity)
        try await context.save()

        // AdminContext operations should work on the same data
        let admin = container.newAdminContext()
        let stats = try await admin.collectionStatistics(AdminTestEntity.self)

        // If paths match, documentCount should be 1
        #expect(stats.documentCount == 1)

        // Verify the key range starts with the correct subspace prefix
        if let keyRangeStart = stats.keyRangeStart {
            #expect(keyRangeStart.starts(with: containerSubspace.prefix))
        }
    }
}

// MARK: - Test Error

private struct TestError: Error, CustomStringConvertible {
    let message: String

    init(_ message: String) {
        self.message = message
    }

    var description: String { message }
}
