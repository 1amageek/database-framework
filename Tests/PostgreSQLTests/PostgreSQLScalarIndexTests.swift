#if POSTGRESQL
// PostgreSQLScalarIndexTests.swift
// Scalar index tests against PostgreSQL backend

import Testing
import DatabaseRuntime
import Foundation
import StorageKit
import PostgreSQLStorage
@testable import DatabaseEngine
@testable import DatabaseKit
@testable import ScalarIndex
import TestSupport

// MARK: - Test Models

@Persistable
struct PGUser: Equatable {
    #Directory<PGUser>("test", "pg", "users")
    #Index(
        .scalar,
        fields: [\PGUser.email],
        unique: true,
        name: "PGUser_email"
    )
    #Index(.scalar, fields: [\PGUser.age], name: "PGUser_age")

    var id: String = UUID().uuidString
    var email: String = ""
    var name: String = ""
    var age: Int64 = 0
}

@Persistable
struct PGProduct: Equatable {
    #Directory<PGProduct>("test", "pg", "products")
    #Index(
        .scalar,
        fields: [\PGProduct.category, \PGProduct.price],
        name: "PGProduct_category_price"
    )

    var id: String = UUID().uuidString
    var category: String = ""
    var price: Double = 0.0
    var name: String = ""
}

@Suite("PostgreSQL Scalar Index Tests", .serialized, .heartbeat, .enabled(if: PostgreSQLScenarioCoordinator.isConfigured))
struct PostgreSQLScalarIndexTests {

    private func uniqueID(_ prefix: String) -> String {
        "\(prefix)-\(UUID().uuidString.prefix(8))"
    }

    private func setupUserContainer() async throws -> DBContainer {
        let schema = try Schema(entities: [try PGUser.schemaEntity], version: Schema.Version(1, 0, 0))
        return try await PostgreSQLScenarioCoordinator.shared.makeContainer(schema: schema, entityRuntimes: [try DatabaseFrameworkRuntime.entity(PGUser.self)])
    }

    private func setupProductContainer() async throws -> DBContainer {
        let schema = try Schema(entities: [try PGProduct.schemaEntity], version: Schema.Version(1, 0, 0))
        return try await PostgreSQLScenarioCoordinator.shared.makeContainer(schema: schema, entityRuntimes: [try DatabaseFrameworkRuntime.entity(PGProduct.self)])
    }

    // MARK: - Basic Index CRUD

    @Test("Insert and fetch by indexed field")
    func insertAndFetchByIndex() async throws {
        try await PostgreSQLScenarioCoordinator.shared.withIsolatedScenario {
            let container = try await setupUserContainer()
            let context = container.testBaseContext()

            let email = "user-\(UUID().uuidString.prefix(8))@test.com"

            var user = PGUser()
            user.email = email
            user.name = "Alice"
            user.age = 30

            try context.insert(user)
            try await context.save()

            // Fetch by email
            let fetched = try await context.fetch(PGUser.self)
                .where(PGUser.fields.email == email)
                .first()

            #expect(fetched != nil)
            #expect(fetched?.name == "Alice")
            #expect(fetched?.age == 30)
        }
    }

    @Test("Update indexed field")
    func updateIndexedField() async throws {
        try await PostgreSQLScenarioCoordinator.shared.withIsolatedScenario {
            let container = try await setupUserContainer()
            let context = container.testBaseContext()

            let originalEmail = "orig-\(UUID().uuidString.prefix(8))@test.com"
            let updatedEmail = "upd-\(UUID().uuidString.prefix(8))@test.com"

            var user = PGUser()
            user.email = originalEmail
            user.name = "Bob"
            user.age = 25
            let userId = user.id

            try context.insert(user)
            try await context.save()

            // Update email
            user.email = updatedEmail
            try context.update(user)

            try await context.save()

            // Old email should not find the user
            let oldResult = try await context.fetch(PGUser.self)
                .where(PGUser.fields.email == originalEmail)
                .first()
            #expect(oldResult == nil)

            // New email should find the user
            let newResult = try await context.fetch(PGUser.self)
                .where(PGUser.fields.email == updatedEmail)
                .first()
            #expect(newResult != nil)
            #expect(newResult?.id == userId)
        }
    }

    @Test("Delete removes index entry")
    func deleteRemovesIndex() async throws {
        try await PostgreSQLScenarioCoordinator.shared.withIsolatedScenario {
            let container = try await setupUserContainer()
            let context = container.testBaseContext()

            let email = "del-\(UUID().uuidString.prefix(8))@test.com"

            var user = PGUser()
            user.email = email
            user.name = "Charlie"
            user.age = 35

            try context.insert(user)
            try await context.save()

            // Verify exists
            let before = try await context.fetch(PGUser.self)
                .where(PGUser.fields.email == email)
                .first()
            #expect(before != nil)

            // Delete
            try context.delete(user)
            try await context.save()

            // Should not be found
            let after = try await context.fetch(PGUser.self)
                .where(PGUser.fields.email == email)
                .first()
            #expect(after == nil)
        }
    }

    // MARK: - Uniqueness Constraint

    @Test("Unique index prevents duplicate values")
    func uniqueIndexPreventsDuplicates() async throws {
        try await PostgreSQLScenarioCoordinator.shared.withIsolatedScenario {
            let container = try await setupUserContainer()
            let context = container.testBaseContext()

            let email = "unique-\(UUID().uuidString.prefix(8))@test.com"

            // First insert succeeds
            var user1 = PGUser()
            user1.email = email
            user1.name = "First"
            user1.age = 20
            try context.insert(user1)
            try await context.save()

            // Second insert with same email should fail
            var user2 = PGUser()
            user2.email = email
            user2.name = "Second"
            user2.age = 25
            try context.insert(user2)

            await #expect(throws: Error.self) {
                try await context.save()
            }
        }
    }

    // MARK: - Range Query

    @Test("Fetch by range on indexed field")
    func fetchByRange() async throws {
        try await PostgreSQLScenarioCoordinator.shared.withIsolatedScenario {
            let container = try await setupUserContainer()
            let context = container.testBaseContext()

            let prefix = UUID().uuidString.prefix(6)

            // Insert users with different ages
            for age: Int64 in [20, 25, 30, 35, 40] {
                var user = PGUser()
                user.email = "\(prefix)-age\(age)@test.com"
                user.name = "User\(age)"
                user.age = age
                try context.insert(user)
            }
            try await context.save()

            // Fetch users age >= 30
            let results = try await context.fetch(PGUser.self)
                .where(PGUser.fields.age >= 30)
                .execute()

            let matching = results.filter { $0.email.hasPrefix(String(prefix)) }
            #expect(matching.count >= 3) // age 30, 35, 40
            #expect(matching.allSatisfy { $0.age >= 30 })
        }
    }

    // MARK: - Composite Index

    @Test("Composite index: category + price")
    func compositeIndex() async throws {
        try await PostgreSQLScenarioCoordinator.shared.withIsolatedScenario {
            let container = try await setupProductContainer()
            let context = container.testBaseContext()

            let category = "electronics-\(UUID().uuidString.prefix(6))"

            var p1 = PGProduct(); p1.category = category; p1.price = 99.99; p1.name = "Widget"
            var p2 = PGProduct(); p2.category = category; p2.price = 199.99; p2.name = "Gadget"
            var p3 = PGProduct(); p3.category = "books"; p3.price = 29.99; p3.name = "Novel"

            try context.insert(p1)
            try context.insert(p2)
            try context.insert(p3)
            try await context.save()

            // Verify data exists by reading back individual items
            let ctx2 = container.testBaseContext()
            let fetchedP1 = try await ctx2.fetch(PGProduct.self)
                .where(PGProduct.fields.id == p1.id)
                .first()
            #expect(fetchedP1 != nil, "Product p1 should exist after save")

            // Fetch all products
            let all = try await ctx2.fetch(PGProduct.self).execute()
            let matching = all.filter { $0.category == category }

            #expect(matching.count >= 2, "Expected at least 2 products with category '\(category)', got \(matching.count) out of \(all.count) total")
        }
    }

    // MARK: - Multiple items with same non-unique index value

    @Test("Non-unique index allows multiple entries")
    func nonUniqueIndexMultipleEntries() async throws {
        try await PostgreSQLScenarioCoordinator.shared.withIsolatedScenario {
            let container = try await setupUserContainer()
            let context = container.testBaseContext()

            let prefix = UUID().uuidString.prefix(6)
            let age: Int64 = 42

            // Insert multiple users with same age
            for i in 0..<3 {
                var user = PGUser()
                user.email = "\(prefix)-multi\(i)@test.com"
                user.name = "User \(i)"
                user.age = age
                try context.insert(user)
            }
            try await context.save()

            let results = try await context.fetch(PGUser.self)
                .where(PGUser.fields.age == age)
                .execute()

            let matching = results.filter { $0.email.hasPrefix(String(prefix)) }
            #expect(matching.count == 3)
        }
    }
}
#endif
