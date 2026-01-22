// CLIContextTests.swift
// DatabaseCLI - Tests for CLIContext type-erased database operations

import Testing
import Foundation
@testable import DatabaseCLI
@testable import DatabaseEngine
@testable import TestSupport
import Core

// MARK: - Entity Access Tests

@Suite("CLIContext - Entity Access", .serialized)
struct CLIContextEntityTests {

    init() async throws {
        try await FDBTestSetup.shared.initialize()
    }

    @Test func entityNames() async throws {
        let schema = Schema([CLITestUser.self, CLITestOrder.self])
        let context = try CLIContext(schema: schema)

        let names = context.entityNames
        #expect(names.contains("CLITestUser"))
        #expect(names.contains("CLITestOrder"))
        #expect(names.count == 2)
    }

    @Test func entityByName() async throws {
        let schema = Schema([CLITestUser.self])
        let context = try CLIContext(schema: schema)

        let entity = context.entity(named: "CLITestUser")
        #expect(entity != nil)
        #expect(entity?.name == "CLITestUser")
    }

    @Test func entityNotFound() async throws {
        let schema = Schema([CLITestUser.self])
        let context = try CLIContext(schema: schema)

        let entity = context.entity(named: "NonExistent")
        #expect(entity == nil)
    }

    @Test func entityCaseSensitive() async throws {
        let schema = Schema([CLITestUser.self])
        let context = try CLIContext(schema: schema)

        // Exact match should work
        let exactMatch = context.entity(named: "CLITestUser")
        #expect(exactMatch != nil)

        // Wrong case should not match
        let wrongCase = context.entity(named: "clitestuser")
        #expect(wrongCase == nil)
    }

    @Test func indexDescriptorsForType() async throws {
        let schema = Schema([CLITestUser.self])
        let context = try CLIContext(schema: schema)

        let indexes = context.indexDescriptors(for: "CLITestUser")
        #expect(!indexes.isEmpty)
        #expect(indexes.contains { $0.name == "CLITestUser_email" })
    }

    @Test func indexDescriptorsForUnknownType() async throws {
        let schema = Schema([CLITestUser.self])
        let context = try CLIContext(schema: schema)

        let indexes = context.indexDescriptors(for: "Unknown")
        #expect(indexes.isEmpty)
    }

    @Test func allIndexDescriptors() async throws {
        let schema = Schema([CLITestUser.self, CLITestOrder.self])
        let context = try CLIContext(schema: schema)

        let indexes = context.allIndexDescriptors
        #expect(!indexes.isEmpty)
        // Should include indexes from both types
        #expect(indexes.contains { $0.name == "CLITestUser_email" })
        #expect(indexes.contains { $0.name == "CLITestOrder_userId" })
    }

    @Test func schemaAccess() async throws {
        let schema = Schema([CLITestUser.self, CLITestOrder.self])
        let context = try CLIContext(schema: schema)

        #expect(context.schema.entities.count == 2)
    }
}

// MARK: - Data Operation Tests

@Suite("CLIContext - Data Operations", .serialized)
struct CLIContextDataTests {

    init() async throws {
        try await FDBTestSetup.shared.initialize()
    }

    /// Generate unique test ID to avoid conflicts with parallel tests
    private func uniqueID(_ prefix: String) -> String {
        "\(prefix)-\(UUID().uuidString.prefix(8))"
    }

    @Test func fetchItemNotFound() async throws {
        let schema = Schema([CLITestUser.self])
        let context = try CLIContext(schema: schema)

        let uniqueId = uniqueID("nonexistent")
        let result = try await context.fetchItem(typeName: "CLITestUser", id: uniqueId)
        #expect(result == nil)
    }

    @Test func fetchItemUnknownType() async throws {
        let schema = Schema([CLITestUser.self])
        let context = try CLIContext(schema: schema)

        do {
            _ = try await context.fetchItem(typeName: "UnknownType", id: "123")
            Issue.record("Should throw CLIError.unknownType")
        } catch let error as CLIError {
            if case .unknownType(let name) = error {
                #expect(name == "UnknownType")
            } else {
                Issue.record("Expected unknownType error, got: \(error)")
            }
        }
    }

    @Test func fetchItemsUnknownType() async throws {
        let schema = Schema([CLITestUser.self])
        let context = try CLIContext(schema: schema)

        do {
            _ = try await context.fetchItems(typeName: "UnknownType")
            Issue.record("Should throw CLIError.unknownType")
        } catch let error as CLIError {
            if case .unknownType = error { } else {
                Issue.record("Expected unknownType error, got: \(error)")
            }
        }
    }

    @Test func countItemsUnknownType() async throws {
        let schema = Schema([CLITestUser.self])
        let context = try CLIContext(schema: schema)

        do {
            _ = try await context.countItems(typeName: "UnknownType")
            Issue.record("Should throw CLIError.unknownType")
        } catch let error as CLIError {
            if case .unknownType = error { } else {
                Issue.record("Expected unknownType error, got: \(error)")
            }
        }
    }

    @Test func deleteItemNotFound() async throws {
        let schema = Schema([CLITestUser.self])
        let context = try CLIContext(schema: schema)

        let uniqueId = uniqueID("nonexistent")
        let deleted = try await context.deleteItem(typeName: "CLITestUser", id: uniqueId)
        #expect(deleted == false)
    }

    @Test func deleteItemUnknownType() async throws {
        let schema = Schema([CLITestUser.self])
        let context = try CLIContext(schema: schema)

        do {
            _ = try await context.deleteItem(typeName: "UnknownType", id: "123")
            Issue.record("Should throw CLIError.unknownType")
        } catch let error as CLIError {
            if case .unknownType = error { } else {
                Issue.record("Expected unknownType error, got: \(error)")
            }
        }
    }

    @Test func fetchItemsWithLimit() async throws {
        let schema = Schema([CLITestUser.self])
        let context = try CLIContext(schema: schema)

        // Should not throw even with limit
        let results = try await context.fetchItems(typeName: "CLITestUser", limit: 5)
        #expect(results.count <= 5)
    }
}

// MARK: - Container Initialization Tests

@Suite("CLIContext - Container Initialization", .serialized)
struct CLIContextContainerTests {

    init() async throws {
        try await FDBTestSetup.shared.initialize()
    }

    @Test func initWithSchema() async throws {
        let schema = Schema([CLITestUser.self])
        let context = try CLIContext(schema: schema)

        #expect(context.entityNames.contains("CLITestUser"))
    }

    @Test func initWithContainer() async throws {
        let schema = Schema([CLITestUser.self])
        let container = try FDBContainer(for: schema)
        let context = CLIContext(container: container)

        #expect(context.entityNames.contains("CLITestUser"))
    }
}
