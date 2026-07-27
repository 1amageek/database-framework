#if !os(WASI)
#if FOUNDATION_DB
import Testing
import Foundation
import StorageKit
import FDBStorage
import DatabaseKit
import TestSupport
@testable import DatabaseEngine
import DatabaseRuntime

/// Tests for AdminContext directory resolution
///
/// **Coverage**:
/// - Verifies AdminContext methods respect #Directory macro definitions
/// - Tests indexStatistics, rebuildIndex, collectionStatistics, updateStatistics
/// - Ensures correct directory paths are used (not entity.name)
@Suite("AdminContext Tests", .foundationDBScenario, .serialized, .heartbeat)
struct AdminContextTests {

    // MARK: - Helper Types

    @Persistable
    struct AdminIndexedEntity {
        #Directory<AdminIndexedEntity>("test", "admin", "custom", "path")

        var id: String = UUID().uuidString
        var value: String = ""

        #Index(.scalar, fields: [\AdminIndexedEntity.value])
    }

    @Persistable
    struct AdminUnindexedEntity {
        #Directory<AdminUnindexedEntity>("test", "admin", "no", "index")

        var id: String = UUID().uuidString
        var name: String = ""
    }

    // MARK: - Helper Methods

    private func setupContainer() async throws -> DBContainer {
        try await FoundationDBScenarioEnvironment.shared.ensureInitialized()
        let database = try await FoundationDBScenarioCoordinator.shared.makeEngine()

        let schema = try Schema(
            entities: [
                try AdminIndexedEntity.schemaEntity,
                try AdminUnindexedEntity.schemaEntity,
            ],
            version: Schema.Version(1, 0, 0)
        )

        return try await DBContainer.open(
            for: schema,
            configuration: .init(backend: .custom(database)),
            runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(persistableTypes: [AdminIndexedEntity.self, AdminUnindexedEntity.self]),
            security: .disabled
            )
    }

    private func cleanup(container: DBContainer) async throws {
        try? await container.engine.removeDirectory(path: ["test", "admin"])
    }

    /// Get the first index name for AdminIndexedEntity from schema
    private func getTestIndexName(from container: DBContainer) -> String? {
        guard let entity = container.schema.entity(for: AdminIndexedEntity.self),
              let firstIndex = entity.indexDescriptors.first else {
            return nil
        }
        return firstIndex.name
    }

    // MARK: - Directory Resolution Tests

    @Test("indexStatistics uses correct directory from #Directory macro")
    func indexStatisticsUsesCorrectDirectory() async throws {
        try await FoundationDBScenarioCoordinator.shared.withSerializedAccess {
            let container = try await setupContainer()
            try await cleanup(container: container)

            // Get index name from schema
            guard let indexName = getTestIndexName(from: container) else {
                throw AdminIndexLookupFailure("No index found for AdminIndexedEntity")
            }

            // Get index statistics via AdminContext
            // If wrong directory path was used, this would throw because index wouldn't be found
            let admin = container.newAdminContext()
            let stats = try await admin.indexStatistics(indexName)

            // Verify we got valid statistics (index was found at correct path)
            #expect(stats.indexName == indexName)
        }
    }

    @Test("rebuildIndex uses correct directory from #Directory macro")
    func rebuildIndexUsesCorrectDirectory() async throws {
        try await FoundationDBScenarioCoordinator.shared.withSerializedAccess {
            let container = try await setupContainer()
            try await cleanup(container: container)

            let context = container.newContext()

            // Insert test data
            for i in 0..<10 {
                let entity = AdminIndexedEntity(value: "value-\(i)")
                try context.insert(entity)
            }
            try await context.save()

            // Get index name from schema
            guard let indexName = getTestIndexName(from: container) else {
                throw AdminIndexLookupFailure("No index found for AdminIndexedEntity")
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
    }

    @Test("collectionStatistics uses correct directory from #Directory macro")
    func collectionStatisticsUsesCorrectDirectory() async throws {
        try await FoundationDBScenarioCoordinator.shared.withSerializedAccess {
            let container = try await setupContainer()
            try await cleanup(container: container)

            let context = container.newContext()

            // Insert test data
            for i in 0..<5 {
                let entity = AdminIndexedEntity(value: "value-\(i)")
                try context.insert(entity)
            }
            try await context.save()

            // Get collection statistics via AdminContext
            let admin = container.newAdminContext()
            let stats = try await admin.collectionStatistics(AdminIndexedEntity.self)

            // If correct path is used, documentCount should be 5
            #expect(stats.documentCount == 5)
        }
    }

    @Test("updateStatistics uses correct directory from #Directory macro")
    func updateStatisticsUsesCorrectDirectory() async throws {
        try await FoundationDBScenarioCoordinator.shared.withSerializedAccess {
            let container = try await setupContainer()
            try await cleanup(container: container)

            let context = container.newContext()

            // Insert test data
            for i in 0..<3 {
                let entity = AdminIndexedEntity(value: "value-\(i)")
                try context.insert(entity)
            }
            try await context.save()

            // Update statistics via AdminContext
            let admin = container.newAdminContext()

            // If this completes without error, correct path is being used
            try await admin.updateStatistics()
        }
    }

    @Test("allIndexStatistics uses correct directory from #Directory macro")
    func allIndexStatisticsUsesCorrectDirectory() async throws {
        try await FoundationDBScenarioCoordinator.shared.withSerializedAccess {
            let container = try await setupContainer()
            try await cleanup(container: container)

            // Get index name from schema
            guard let indexName = getTestIndexName(from: container) else {
                throw AdminIndexLookupFailure("No index found for AdminIndexedEntity")
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
    }

    // MARK: - Consistency Tests

    @Test("AdminContext and DBContainer resolve to same directory")
    func adminContextAndContainerResolveToSameDirectory() async throws {
        try await FoundationDBScenarioCoordinator.shared.withSerializedAccess {
            let container = try await setupContainer()
            try await cleanup(container: container)

            // Resolve directory via DBContainer
            let containerSubspace = try await container.resolveDirectory(for: AdminIndexedEntity.self)

            // Insert data and verify it's accessible
            let context = container.newContext()
            let entity = AdminIndexedEntity(value: "consistency-test")
            try context.insert(entity)
            try await context.save()

            // AdminContext operations should work on the same data
            let admin = container.newAdminContext()
            let stats = try await admin.collectionStatistics(AdminIndexedEntity.self)

            // If paths match, documentCount should be 1
            #expect(stats.documentCount == 1)

            // Verify the key range starts with the correct subspace prefix
            #expect(
                stats.keyRangeStart.starts(with: containerSubspace.prefix)
            )
        }
    }
}

// MARK: - Test Error

private struct AdminIndexLookupFailure: Error, CustomStringConvertible {
    let message: String

    init(_ message: String) {
        self.message = message
    }

    var description: String { message }
}
#endif

#endif
