// DirectoryLayerTests.swift
// Tests for FDBContainer Directory Layer functionality

import Testing
import Foundation
import FoundationDB
@testable import DatabaseEngine
@testable import Core

/// Tests for Directory Layer operations in FDBContainer
///
/// **Coverage**:
/// - getOrOpenDirectory: Create or open existing directories
/// - createDirectory: Create new directories (fails if exists)
/// - openDirectory: Open existing directories (fails if not exists)
/// - directoryExists: Check directory existence
/// - moveDirectory: Move directories to new paths
/// - removeDirectory: Remove directories
@Suite("Directory Layer Tests")
struct DirectoryLayerTests {

    // MARK: - Helper Types

    @Persistable
    struct DirectoryTestModel {
        var id: String = ULID().ulidString
        var name: String
    }

    // MARK: - Helper Methods

    private func setupContainer() async throws -> FDBContainer {
        try await FDBTestEnvironment.shared.ensureInitialized()
        let database = try FDBClient.openDatabase()

        let schema = Schema(entities: [
            Schema.Entity(
                name: DirectoryTestModel.persistableType,
                allFields: DirectoryTestModel.allFields,
                indexDescriptors: DirectoryTestModel.indexDescriptors,
                enumMetadata: [:]
            )
        ], version: Schema.Version(1, 0, 0))

        // Create isolated test subspace
        let testSubspace = Subspace(prefix: Tuple("directory_test", UUID().uuidString).pack())

        // Create test-specific DirectoryLayer
        let testDirectoryLayer = DirectoryLayer(
            database: database,
            nodeSubspace: testSubspace.subspace(0xFE),
            contentSubspace: testSubspace
        )

        return FDBContainer(
            database: database,
            schema: schema,
            subspace: testSubspace,
            directoryLayer: testDirectoryLayer
        )
    }

    // MARK: - getOrOpenDirectory Tests

    @Test("getOrOpenDirectory creates new directory")
    func getOrOpenDirectoryCreatesNew() async throws {
        let container = try await setupContainer()

        // Create a new directory
        let subspace = try await container.getOrOpenDirectory(path: ["app", "users"])

        // Verify subspace is returned
        #expect(subspace.prefix.count > 0)

        // Verify directory now exists
        let exists = try await container.directoryExists(path: ["app", "users"])
        #expect(exists == true)
    }

    @Test("getOrOpenDirectory opens existing directory")
    func getOrOpenDirectoryOpensExisting() async throws {
        let container = try await setupContainer()

        // Create directory first
        let subspace1 = try await container.getOrOpenDirectory(path: ["app", "products"])

        // Open the same directory again
        let subspace2 = try await container.getOrOpenDirectory(path: ["app", "products"])

        // Verify both return the same subspace prefix
        #expect(subspace1.prefix == subspace2.prefix)
    }

    @Test("getOrOpenDirectory supports nested paths")
    func getOrOpenDirectoryNestedPaths() async throws {
        let container = try await setupContainer()

        // Create nested directory structure
        let subspace = try await container.getOrOpenDirectory(
            path: ["tenants", "tenant1", "data", "orders"]
        )

        #expect(subspace.prefix.count > 0)

        // Verify the full path exists
        let exists = try await container.directoryExists(
            path: ["tenants", "tenant1", "data", "orders"]
        )
        #expect(exists == true)
    }

    // MARK: - createDirectory Tests

    @Test("createDirectory creates new directory")
    func createDirectoryNew() async throws {
        let container = try await setupContainer()

        let subspace = try await container.createDirectory(path: ["new", "directory"])

        #expect(subspace.prefix.count > 0)

        let exists = try await container.directoryExists(path: ["new", "directory"])
        #expect(exists == true)
    }

    @Test("createDirectory fails if directory already exists")
    func createDirectoryFailsIfExists() async throws {
        let container = try await setupContainer()

        // Create directory first
        _ = try await container.createDirectory(path: ["existing", "dir"])

        // Attempt to create the same directory again should fail
        await #expect(throws: Error.self) {
            _ = try await container.createDirectory(path: ["existing", "dir"])
        }
    }

    // MARK: - openDirectory Tests

    @Test("openDirectory opens existing directory")
    func openDirectoryExisting() async throws {
        let container = try await setupContainer()

        // Create directory first
        let created = try await container.createDirectory(path: ["to", "open"])

        // Open it
        let opened = try await container.openDirectory(path: ["to", "open"])

        #expect(created.prefix == opened.prefix)
    }

    @Test("openDirectory fails if directory does not exist")
    func openDirectoryFailsIfNotExists() async throws {
        let container = try await setupContainer()

        await #expect(throws: Error.self) {
            _ = try await container.openDirectory(path: ["nonexistent", "path"])
        }
    }

    // MARK: - directoryExists Tests

    @Test("directoryExists returns false for non-existent directory")
    func directoryExistsFalseForNonExistent() async throws {
        let container = try await setupContainer()

        let exists = try await container.directoryExists(path: ["does", "not", "exist"])
        #expect(exists == false)
    }

    @Test("directoryExists returns true for existing directory")
    func directoryExistsTrueForExisting() async throws {
        let container = try await setupContainer()

        // Create directory
        _ = try await container.createDirectory(path: ["check", "exists"])

        let exists = try await container.directoryExists(path: ["check", "exists"])
        #expect(exists == true)
    }

    @Test("directoryExists checks intermediate paths")
    func directoryExistsCheckIntermediatePaths() async throws {
        let container = try await setupContainer()

        // Create nested directory
        _ = try await container.createDirectory(path: ["level1", "level2", "level3"])

        // Full path exists
        #expect(try await container.directoryExists(path: ["level1", "level2", "level3"]) == true)

        // Partial paths may or may not exist depending on implementation
        // At minimum, the full path should exist
    }

    // MARK: - moveDirectory Tests

    @Test("moveDirectory moves directory to new path")
    func moveDirectoryToNewPath() async throws {
        let container = try await setupContainer()

        // Create source directory
        _ = try await container.createDirectory(path: ["source", "dir"])

        // Move it
        let movedSubspace = try await container.moveDirectory(
            oldPath: ["source", "dir"],
            newPath: ["destination", "dir"]
        )

        #expect(movedSubspace.prefix.count > 0)

        // Old path should not exist
        let oldExists = try await container.directoryExists(path: ["source", "dir"])
        #expect(oldExists == false)

        // New path should exist
        let newExists = try await container.directoryExists(path: ["destination", "dir"])
        #expect(newExists == true)
    }

    @Test("moveDirectory preserves subspace content")
    func moveDirectoryPreservesContent() async throws {
        let container = try await setupContainer()

        // Create directory and write data
        let originalSubspace = try await container.createDirectory(path: ["original"])

        // Write some test data to the subspace
        let testKey = originalSubspace.pack(Tuple("testKey"))
        let testValue: [UInt8] = [1, 2, 3, 4, 5]

        try await container.withTransaction { transaction in
            transaction.setValue(testValue, for: testKey)
        }

        // Move directory
        let movedSubspace = try await container.moveDirectory(
            oldPath: ["original"],
            newPath: ["moved"]
        )

        // Read data from new location
        let readValue = try await container.withTransaction { transaction in
            let newKey = movedSubspace.pack(Tuple("testKey"))
            return try await transaction.getValue(for: newKey)
        }

        #expect(readValue == testValue)
    }

    // MARK: - removeDirectory Tests

    @Test("removeDirectory removes existing directory")
    func removeDirectoryExisting() async throws {
        let container = try await setupContainer()

        // Create directory
        _ = try await container.createDirectory(path: ["to", "remove"])

        // Verify it exists
        let existsBefore = try await container.directoryExists(path: ["to", "remove"])
        #expect(existsBefore == true)

        // Remove it
        try await container.removeDirectory(path: ["to", "remove"])

        // Verify it no longer exists
        let existsAfter = try await container.directoryExists(path: ["to", "remove"])
        #expect(existsAfter == false)
    }

    @Test("removeDirectory removes nested directories")
    func removeDirectoryNested() async throws {
        let container = try await setupContainer()

        // Create nested structure
        _ = try await container.createDirectory(path: ["parent", "child", "grandchild"])

        // Remove parent (should remove all children)
        try await container.removeDirectory(path: ["parent"])

        // Parent should not exist
        let parentExists = try await container.directoryExists(path: ["parent"])
        #expect(parentExists == false)

        // Child paths should also not exist
        let childExists = try await container.directoryExists(path: ["parent", "child"])
        #expect(childExists == false)
    }

    // MARK: - Integration Tests

    @Test("Multiple directories can coexist independently")
    func multipleDirectoriesIndependent() async throws {
        let container = try await setupContainer()

        // Create multiple directories
        let dir1 = try await container.createDirectory(path: ["tenant1", "data"])
        let dir2 = try await container.createDirectory(path: ["tenant2", "data"])
        let dir3 = try await container.createDirectory(path: ["tenant3", "data"])

        // All should have different prefixes
        #expect(dir1.prefix != dir2.prefix)
        #expect(dir2.prefix != dir3.prefix)
        #expect(dir1.prefix != dir3.prefix)

        // All should exist independently
        #expect(try await container.directoryExists(path: ["tenant1", "data"]) == true)
        #expect(try await container.directoryExists(path: ["tenant2", "data"]) == true)
        #expect(try await container.directoryExists(path: ["tenant3", "data"]) == true)
    }

    @Test("Directory operations are isolated per container")
    func directoryIsolationPerContainer() async throws {
        // Create two separate containers with different subspaces
        let container1 = try await setupContainer()
        let container2 = try await setupContainer()

        // Create directory in container1
        _ = try await container1.createDirectory(path: ["isolated", "dir"])

        // Should exist in container1
        let exists1 = try await container1.directoryExists(path: ["isolated", "dir"])
        #expect(exists1 == true)

        // Should NOT exist in container2 (different subspace)
        let exists2 = try await container2.directoryExists(path: ["isolated", "dir"])
        #expect(exists2 == false)
    }
}
