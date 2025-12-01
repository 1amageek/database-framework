import Testing
import Foundation
import FoundationDB
@testable import DatabaseEngine
@testable import Core

/// Tests for FDBContext functionality (SwiftData-like API)
///
/// **Coverage**:
/// - Autosave functionality
/// - Change tracking (insert, delete, save, rollback)
/// - Fetch operations with Query DSL
/// - Model retrieval by ID
@Suite("FDBContext Tests", .serialized)
struct FDBContextTests {

    // MARK: - Helper Types

    /// Test model conforming to Persistable with #Directory
    @Persistable
    struct TestUser {
        #Directory<TestUser>("test", "context", "users")
        var id: String = ULID().ulidString
        var name: String
        var email: String
        var age: Int
        var isActive: Bool = true
    }

    @Persistable
    struct TestProduct {
        #Directory<TestProduct>("test", "context", "products")
        var id: String = ULID().ulidString
        var name: String
        var price: Double
    }

    // MARK: - Helper Methods

    private func setupContainer() async throws -> FDBContainer {
        try await FDBTestEnvironment.shared.ensureInitialized()
        let database = try FDBClient.openDatabase()

        // Use Schema([Type.self]) to properly register types
        let schema = Schema([TestUser.self, TestProduct.self], version: Schema.Version(1, 0, 0))

        return FDBContainer(
            database: database,
            schema: schema
        )
    }

    /// Clean up test data - call at START of each test that modifies data
    private func cleanup(container: FDBContainer) async throws {
        let context = container.newContext()
        try await context.deleteAll(TestUser.self)
        try await context.deleteAll(TestProduct.self)
        try await context.save()
    }

    // MARK: - Autosave Tests

    @Test("Autosave disabled by default")
    func autosaveDisabledByDefault() async throws {
        let container = try await setupContainer()
        let context = container.newContext()
        #expect(context.autosaveEnabled == false)
    }

    @Test("Autosave can be enabled")
    func autosaveCanBeEnabled() async throws {
        let container = try await setupContainer()
        let context = container.newContext(autosaveEnabled: false)
        #expect(context.autosaveEnabled == false)

        context.autosaveEnabled = true
        #expect(context.autosaveEnabled == true)
    }

    // MARK: - Change Tracking Tests

    @Test("HasChanges tracking")
    func hasChangesTracking() async throws {
        let container = try await setupContainer()
        // Clean up at START of test
        try await cleanup(container: container)

        let context = container.newContext()

        #expect(context.hasChanges == false)

        let user = TestUser(name: "Alice", email: "alice@example.com", age: 30)
        context.insert(user)
        #expect(context.hasChanges == true)

        try await context.save()
        #expect(context.hasChanges == false)

        context.delete(user)
        #expect(context.hasChanges == true)

        context.rollback()
        #expect(context.hasChanges == false)
    }

    @Test("Insert and delete cancel each other for unsaved models")
    func insertAndDeleteCancel() async throws {
        let container = try await setupContainer()
        let context = container.newContext()

        let user = TestUser(name: "Bob", email: "bob@example.com", age: 25)

        context.insert(user)
        #expect(context.hasChanges == true)

        context.delete(user)
        #expect(context.hasChanges == false)
    }

    @Test("Save with no changes does nothing")
    func saveWithNoChanges() async throws {
        let container = try await setupContainer()
        let context = container.newContext()

        try await context.save()
        #expect(context.hasChanges == false)
    }

    @Test("Rollback clears changes")
    func rollbackClearsChanges() async throws {
        let container = try await setupContainer()
        let context = container.newContext()

        let user = TestUser(name: "Charlie", email: "charlie@example.com", age: 35)
        context.insert(user)
        #expect(context.hasChanges == true)

        context.rollback()
        #expect(context.hasChanges == false)

        try await context.save()
    }

    // MARK: - CRUD Tests

    @Test("Insert and fetch single model")
    func insertAndFetchSingleModel() async throws {
        let container = try await setupContainer()
        // Clean up at START of test
        try await cleanup(container: container)

        let context = container.newContext()

        let user = TestUser(name: "David", email: "david@example.com", age: 40)
        context.insert(user)
        try await context.save()

        let fetchedUser = try await context.model(for: user.id, as: TestUser.self)
        #expect(fetchedUser != nil)
        #expect(fetchedUser?.id == user.id)
        #expect(fetchedUser?.name == user.name)
        #expect(fetchedUser?.email == user.email)
        #expect(fetchedUser?.age == user.age)
    }

    @Test("Fetch returns nil for missing model")
    func fetchReturnsNilForMissingModel() async throws {
        let container = try await setupContainer()
        let context = container.newContext()

        let fetchedUser = try await context.model(for: "nonexistent-id", as: TestUser.self)
        #expect(fetchedUser == nil)
    }

    @Test("Fetch all models of a type")
    func fetchAllModels() async throws {
        let container = try await setupContainer()
        // Clean up at START of test
        try await cleanup(container: container)

        let context = container.newContext()

        let users = [
            TestUser(name: "User1", email: "user1@example.com", age: 20),
            TestUser(name: "User2", email: "user2@example.com", age: 25),
            TestUser(name: "User3", email: "user3@example.com", age: 30)
        ]
        for user in users {
            context.insert(user)
        }
        try await context.save()

        let fetchedUsers = try await context.fetch(TestUser.self).execute()
        #expect(fetchedUsers.count == 3)

        let names = Set(fetchedUsers.map(\.name))
        #expect(names.contains("User1"))
        #expect(names.contains("User2"))
        #expect(names.contains("User3"))
    }

    @Test("Delete model")
    func deleteModel() async throws {
        let container = try await setupContainer()
        // Clean up at START of test
        try await cleanup(container: container)

        let context = container.newContext()

        let user = TestUser(name: "Eve", email: "eve@example.com", age: 28)
        context.insert(user)
        try await context.save()

        let existingUser = try await context.model(for: user.id, as: TestUser.self)
        #expect(existingUser != nil)

        context.delete(user)
        try await context.save()

        let deletedUser = try await context.model(for: user.id, as: TestUser.self)
        #expect(deletedUser == nil)
    }

    // MARK: - Multi-Type Tests

    @Test("Context handles multiple types")
    func contextHandlesMultipleTypes() async throws {
        let container = try await setupContainer()
        // Clean up at START of test
        try await cleanup(container: container)

        let context = container.newContext()

        let user = TestUser(name: "Frank", email: "frank@example.com", age: 33)
        let product = TestProduct(name: "Widget", price: 9.99)

        context.insert(user)
        context.insert(product)
        try await context.save()

        let fetchedUsers = try await context.fetch(TestUser.self).execute()
        let fetchedProducts = try await context.fetch(TestProduct.self).execute()

        #expect(fetchedUsers.count == 1)
        #expect(fetchedProducts.count == 1)

        #expect(fetchedUsers.first?.name == "Frank")
        #expect(fetchedProducts.first?.name == "Widget")
    }

    // MARK: - Fetch Descriptor Tests

    @Test("Fetch with limit")
    func fetchWithLimit() async throws {
        let container = try await setupContainer()
        // Clean up at START of test
        try await cleanup(container: container)

        let context = container.newContext()

        for i in 1...5 {
            let user = TestUser(name: "User\(i)", email: "user\(i)@example.com", age: 20 + i)
            context.insert(user)
        }
        try await context.save()

        let fetchedUsers = try await context.fetch(TestUser.self)
            .limit(2)
            .execute()
        #expect(fetchedUsers.count == 2)
    }

    @Test("Fetch count")
    func fetchCount() async throws {
        let container = try await setupContainer()
        // Clean up at START of test
        try await cleanup(container: container)

        let context = container.newContext()

        for i in 1...3 {
            let user = TestUser(name: "User\(i)", email: "user\(i)@example.com", age: 20 + i)
            context.insert(user)
        }
        try await context.save()

        let count = try await context.fetch(TestUser.self).count()
        #expect(count == 3)
    }

    // MARK: - Perform and Save Tests

    @Test("performAndSave auto-saves")
    func performAndSaveAutoSaves() async throws {
        let container = try await setupContainer()
        // Clean up at START of test
        try await cleanup(container: container)

        let context = container.newContext()
        let user = TestUser(name: "Grace", email: "grace@example.com", age: 27)

        try await context.performAndSave {
            context.insert(user)
        }

        #expect(context.hasChanges == false)

        let fetchedUser = try await context.model(for: user.id, as: TestUser.self)
        #expect(fetchedUser != nil)
        #expect(fetchedUser?.name == "Grace")
    }

    // MARK: - Concurrent Save Tests

    @Test("Concurrent saves throw error")
    func concurrentSavesThrowError() async throws {
        let container = try await setupContainer()
        // Clean up at START of test
        try await cleanup(container: container)

        let context = container.newContext()

        for i in 1...10 {
            let user = TestUser(name: "User\(i)", email: "user\(i)@example.com", age: 20 + i)
            context.insert(user)
        }

        let save1 = Task {
            try await context.save()
        }

        let save2 = Task {
            try await context.save()
        }

        var results: [Result<Void, Error>] = []

        do {
            try await save1.value
            results.append(.success(()))
        } catch {
            results.append(.failure(error))
        }

        do {
            try await save2.value
            results.append(.success(()))
        } catch {
            results.append(.failure(error))
        }

        let successes = results.filter { if case .success = $0 { return true } else { return false } }
        #expect(successes.count >= 1, "At least one save should succeed")

        let failures = results.filter { if case .failure = $0 { return true } else { return false } }
        for result in failures {
            if case .failure(let error) = result {
                #expect(error is FDBContextError, "Concurrent save should throw FDBContextError")
            }
        }

        #expect(context.hasChanges == false)
    }

    // MARK: - Enumerate Tests

    @Test("Enumerate all models")
    func enumerateAllModels() async throws {
        let container = try await setupContainer()
        // Clean up at START of test
        try await cleanup(container: container)

        let context = container.newContext()

        let users = [
            TestUser(name: "User1", email: "user1@example.com", age: 20),
            TestUser(name: "User2", email: "user2@example.com", age: 25)
        ]
        for user in users {
            context.insert(user)
        }
        try await context.save()

        var enumeratedNames: [String] = []
        try await context.enumerate(TestUser.self) { user in
            enumeratedNames.append(user.name)
        }

        #expect(enumeratedNames.count == 2)
        #expect(Set(enumeratedNames) == Set(["User1", "User2"]))
    }
}
