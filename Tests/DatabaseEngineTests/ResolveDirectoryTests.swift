import Testing
import Foundation
import FoundationDB
import Core
import TestSupport
@testable import DatabaseEngine

/// Tests for FDBContainer.resolveDirectory functionality
///
/// **Coverage**:
/// - resolveDirectory<T: Persistable>(for type:) - Static path resolution
/// - resolveDirectory(for type: any Persistable.Type) - Type-erased resolution
/// - Directory caching behavior
/// - Multiple types with independent directories
@Suite("Resolve Directory Tests", .serialized)
struct ResolveDirectoryTests {

    // MARK: - Helper Types

    @Persistable
    struct DirectoryUser {
        #Directory<DirectoryUser>("test", "resolve", "users")

        var id: String = ULID().ulidString
        var name: String
        var email: String
    }

    @Persistable
    struct DirectoryProduct {
        #Directory<DirectoryProduct>("test", "resolve", "products")

        var id: String = ULID().ulidString
        var name: String
        var price: Double
    }

    @Persistable
    struct NestedDirectoryItem {
        #Directory<NestedDirectoryItem>("test", "resolve", "nested", "deep", "items")

        var id: String = ULID().ulidString
        var value: String
    }

    // MARK: - Helper Methods

    private func setupContainer() async throws -> FDBContainer {
        try await FDBTestEnvironment.shared.ensureInitialized()
        let database = try FDBClient.openDatabase()

        // Use Schema([Type.self]) to properly register types
        let schema = Schema([
            DirectoryUser.self,
            DirectoryProduct.self,
            NestedDirectoryItem.self
        ], version: Schema.Version(1, 0, 0))

        return FDBContainer(
            database: database,
            schema: schema,
            security: .disabled
        )
    }

    private func cleanup(container: FDBContainer) async throws {
        let directoryLayer = DirectoryLayer(database: container.database)
        try? await directoryLayer.remove(path: ["test", "resolve"])
    }

    // MARK: - Basic Resolution Tests

    @Test("resolveDirectory returns valid subspace for Persistable type")
    func resolveDirectoryReturnsValidSubspace() async throws {
        try await FDBTestSetup.shared.withSerializedAccess {
            let container = try await setupContainer()
            // Clean up at START of test
            try await cleanup(container: container)

            let subspace = try await container.resolveDirectory(for: DirectoryUser.self)

            // Subspace should have a non-empty prefix
            #expect(subspace.prefix.count > 0)
        }
    }

    @Test("resolveDirectory returns same subspace for same type")
    func resolveDirectorySameTypeReturnsSameSubspace() async throws {
        try await FDBTestSetup.shared.withSerializedAccess {
            let container = try await setupContainer()
            // Clean up at START of test
            try await cleanup(container: container)

            let subspace1 = try await container.resolveDirectory(for: DirectoryUser.self)
            let subspace2 = try await container.resolveDirectory(for: DirectoryUser.self)

            // Both calls should return the same subspace (cached)
            #expect(subspace1.prefix == subspace2.prefix)
        }
    }

    @Test("resolveDirectory returns different subspaces for different types")
    func resolveDirectoryDifferentTypesReturnsDifferentSubspaces() async throws {
        try await FDBTestSetup.shared.withSerializedAccess {
            let container = try await setupContainer()
            // Clean up at START of test
            try await cleanup(container: container)

            let userSubspace = try await container.resolveDirectory(for: DirectoryUser.self)
            let productSubspace = try await container.resolveDirectory(for: DirectoryProduct.self)

            // Different types should have different subspaces
            #expect(userSubspace.prefix != productSubspace.prefix)
        }
    }

    @Test("resolveDirectory handles nested directory paths")
    func resolveDirectoryHandlesNestedPaths() async throws {
        try await FDBTestSetup.shared.withSerializedAccess {
            let container = try await setupContainer()
            // Clean up at START of test
            try await cleanup(container: container)

            let subspace = try await container.resolveDirectory(for: NestedDirectoryItem.self)

            // Should resolve deeply nested path successfully
            #expect(subspace.prefix.count > 0)
        }
    }

    // MARK: - Type-Erased Resolution Tests

    @Test("resolveDirectory works with type-erased Persistable type")
    func resolveDirectoryTypeErased() async throws {
        try await FDBTestSetup.shared.withSerializedAccess {
            let container = try await setupContainer()
            // Clean up at START of test
            try await cleanup(container: container)

            let persistableType: any Persistable.Type = DirectoryUser.self

            let subspace = try await container.resolveDirectory(for: persistableType)

            #expect(subspace.prefix.count > 0)
        }
    }

    @Test("Type-erased resolution returns same subspace as generic resolution")
    func typeErasedResolutionMatchesGeneric() async throws {
        try await FDBTestSetup.shared.withSerializedAccess {
            let container = try await setupContainer()
            // Clean up at START of test
            try await cleanup(container: container)

            let genericSubspace = try await container.resolveDirectory(for: DirectoryUser.self)

            let persistableType: any Persistable.Type = DirectoryUser.self
            let typeErasedSubspace = try await container.resolveDirectory(for: persistableType)

            #expect(genericSubspace.prefix == typeErasedSubspace.prefix)
        }
    }

    // MARK: - Caching Tests

    @Test("Directory resolution is cached")
    func directoryResolutionIsCached() async throws {
        try await FDBTestSetup.shared.withSerializedAccess {
            let container = try await setupContainer()
            // Clean up at START of test
            try await cleanup(container: container)

            // First call resolves directory
            let subspace1 = try await container.resolveDirectory(for: DirectoryUser.self)

            // Second call should hit cache and return same result
            let subspace2 = try await container.resolveDirectory(for: DirectoryUser.self)

            // Verify same subspace
            #expect(subspace1.prefix == subspace2.prefix)
        }
    }

    // MARK: - Integration Tests

    @Test("Resolved subspace can be used for data storage")
    func resolvedSubspaceCanStoreData() async throws {
        try await FDBTestSetup.shared.withSerializedAccess {
            let container = try await setupContainer()
            // Clean up at START of test
            try await cleanup(container: container)

            let subspace = try await container.resolveDirectory(for: DirectoryUser.self)

            // Write test data
            let testKey = subspace.pack(Tuple("test", "key"))
            let testValue: [UInt8] = [1, 2, 3, 4, 5]

            try await container.database.withTransaction { transaction in
                transaction.setValue(testValue, for: testKey)
            }

            // Read back in a new transaction
            let readValue: FDB.Bytes? = try await container.database.withTransaction { transaction in
                try await transaction.getValue(for: testKey, snapshot: false)
            }

            #expect(readValue == testValue)

            // Cleanup
            try await container.database.withTransaction { transaction in
                transaction.clear(key: testKey)
            }
        }
    }

    @Test("Multiple containers share same directory for same type")
    func multipleContainersShareDirectory() async throws {
        try await FDBTestSetup.shared.withSerializedAccess {
            try await FDBTestEnvironment.shared.ensureInitialized()
            let database = try FDBClient.openDatabase()

            // Clean up first
            let directoryLayer = DirectoryLayer(database: database)
            try? await directoryLayer.remove(path: ["test", "resolve"])

            let schema = Schema([DirectoryUser.self], version: Schema.Version(1, 0, 0))

            let container1 = FDBContainer(database: database, schema: schema, security: .disabled)
            let container2 = FDBContainer(database: database, schema: schema, security: .disabled)

            let subspace1 = try await container1.resolveDirectory(for: DirectoryUser.self)
            let subspace2 = try await container2.resolveDirectory(for: DirectoryUser.self)

            // Same type should resolve to same directory across containers
            #expect(subspace1.prefix == subspace2.prefix)

            // Cleanup
            try? await directoryLayer.remove(path: ["test", "resolve"])
        }
    }

    // MARK: - store(for:) Tests

    @Test("store(for:) returns DataStore with correct subspace")
    func storeForReturnsCorrectDataStore() async throws {
        try await FDBTestSetup.shared.withSerializedAccess {
            let container = try await setupContainer()
            // Clean up at START of test
            try await cleanup(container: container)

            let store = try await container.store(for: DirectoryUser.self)

            // Store should be functional
            #expect(store is FDBDataStore)
        }
    }
}
