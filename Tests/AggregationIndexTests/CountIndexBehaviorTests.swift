// CountIndexBehaviorTests.swift
// Integration tests for CountIndex behavior with FDB

import Testing
import Foundation
import FoundationDB
import Core
import TestSupport
@testable import DatabaseEngine
@testable import AggregationIndex

// MARK: - Test Model

struct CountTestUser: Persistable {
    typealias ID = String

    var id: String
    var city: String
    var department: String
    var active: Bool

    init(id: String = UUID().uuidString, city: String, department: String, active: Bool = true) {
        self.id = id
        self.city = city
        self.department = department
        self.active = active
    }

    static var persistableType: String { "CountTestUser" }
    static var allFields: [String] { ["id", "city", "department", "active"] }
    static var indexDescriptors: [IndexDescriptor] { [] }

    static func fieldNumber(for fieldName: String) -> Int? { nil }
    static func enumMetadata(for fieldName: String) -> EnumMetadata? { nil }

    subscript(dynamicMember member: String) -> (any Sendable)? {
        switch member {
        case "id": return id
        case "city": return city
        case "department": return department
        case "active": return active
        default: return nil
        }
    }

    static func fieldName<Value>(for keyPath: KeyPath<CountTestUser, Value>) -> String {
        switch keyPath {
        case \CountTestUser.id: return "id"
        case \CountTestUser.city: return "city"
        case \CountTestUser.department: return "department"
        case \CountTestUser.active: return "active"
        default: return "\(keyPath)"
        }
    }

    static func fieldName(for keyPath: PartialKeyPath<CountTestUser>) -> String {
        switch keyPath {
        case \CountTestUser.id: return "id"
        case \CountTestUser.city: return "city"
        case \CountTestUser.department: return "department"
        case \CountTestUser.active: return "active"
        default: return "\(keyPath)"
        }
    }

    static func fieldName(for keyPath: AnyKeyPath) -> String {
        if let partial = keyPath as? PartialKeyPath<CountTestUser> {
            return fieldName(for: partial)
        }
        return "\(keyPath)"
    }
}

// MARK: - Test Helper

private struct TestContext {
    nonisolated(unsafe) let database: any DatabaseProtocol
    let subspace: Subspace
    let indexSubspace: Subspace
    let maintainer: CountIndexMaintainer<CountTestUser>

    init(indexName: String = "CountTestUser_city") throws {
        self.database = try FDBClient.openDatabase()
        let testId = UUID().uuidString.prefix(8)
        self.subspace = Subspace(prefix: Tuple("test", "count", String(testId)).pack())
        self.indexSubspace = subspace.subspace("I").subspace(indexName)

        let index = Index(
            name: indexName,
            kind: CountIndexKind<CountTestUser>(groupBy: [\.city]),
            rootExpression: FieldKeyExpression(fieldName: "city"),
            subspaceKey: indexName,
            itemTypes: Set(["CountTestUser"])
        )

        self.maintainer = CountIndexMaintainer<CountTestUser>(
            index: index,
            subspace: indexSubspace,
            idExpression: FieldKeyExpression(fieldName: "id")
        )
    }

    func cleanup() async throws {
        try await database.withTransaction { transaction in
            let (begin, end) = subspace.range()
            transaction.clearRange(beginKey: begin, endKey: end)
        }
    }

    func getCount(for city: String) async throws -> Int64 {
        try await database.withTransaction { transaction in
            try await maintainer.getCount(
                groupingValues: [city],
                transaction: transaction
            )
        }
    }

    func getAllCounts() async throws -> [(grouping: [any TupleElement], count: Int64)] {
        try await database.withTransaction { transaction in
            try await maintainer.getAllCounts(transaction: transaction)
        }
    }
}

// MARK: - Behavior Tests

@Suite("CountIndex Behavior Tests", .tags(.fdb))
struct CountIndexBehaviorTests {

    // MARK: - Insert Tests

    @Test("Insert increments count")
    func testInsertIncrementsCount() async throws {
        try await FDBTestSetup.shared.initialize()
        let ctx = try TestContext()

        let user = CountTestUser(id: "user1", city: "Tokyo", department: "Engineering")

        try await ctx.database.withTransaction { transaction in
            try await ctx.maintainer.updateIndex(
                oldItem: nil,
                newItem: user,
                transaction: transaction
            )
        }

        let count = try await ctx.getCount(for: "Tokyo")
        #expect(count == 1, "Count should be 1 after first insert")

        try await ctx.cleanup()
    }

    @Test("Multiple inserts to same group increment count")
    func testMultipleInsertsIncrement() async throws {
        try await FDBTestSetup.shared.initialize()
        let ctx = try TestContext()

        let users = [
            CountTestUser(id: "user1", city: "Tokyo", department: "Engineering"),
            CountTestUser(id: "user2", city: "Tokyo", department: "Sales"),
            CountTestUser(id: "user3", city: "Tokyo", department: "Marketing")
        ]

        try await ctx.database.withTransaction { transaction in
            for user in users {
                try await ctx.maintainer.updateIndex(
                    oldItem: nil,
                    newItem: user,
                    transaction: transaction
                )
            }
        }

        let count = try await ctx.getCount(for: "Tokyo")
        #expect(count == 3, "Count should be 3 after 3 inserts to same city")

        try await ctx.cleanup()
    }

    @Test("Inserts to different groups are independent")
    func testDifferentGroupsIndependent() async throws {
        try await FDBTestSetup.shared.initialize()
        let ctx = try TestContext()

        let users = [
            CountTestUser(id: "user1", city: "Tokyo", department: "Engineering"),
            CountTestUser(id: "user2", city: "Tokyo", department: "Sales"),
            CountTestUser(id: "user3", city: "Osaka", department: "Marketing"),
            CountTestUser(id: "user4", city: "Kyoto", department: "Engineering")
        ]

        try await ctx.database.withTransaction { transaction in
            for user in users {
                try await ctx.maintainer.updateIndex(
                    oldItem: nil,
                    newItem: user,
                    transaction: transaction
                )
            }
        }

        let tokyoCount = try await ctx.getCount(for: "Tokyo")
        let osakaCount = try await ctx.getCount(for: "Osaka")
        let kyotoCount = try await ctx.getCount(for: "Kyoto")

        #expect(tokyoCount == 2, "Tokyo count should be 2")
        #expect(osakaCount == 1, "Osaka count should be 1")
        #expect(kyotoCount == 1, "Kyoto count should be 1")

        try await ctx.cleanup()
    }

    // MARK: - Delete Tests

    @Test("Delete decrements count")
    func testDeleteDecrementsCount() async throws {
        try await FDBTestSetup.shared.initialize()
        let ctx = try TestContext()

        let user = CountTestUser(id: "user1", city: "Tokyo", department: "Engineering")

        // Insert
        try await ctx.database.withTransaction { transaction in
            try await ctx.maintainer.updateIndex(
                oldItem: nil,
                newItem: user,
                transaction: transaction
            )
        }

        let countBefore = try await ctx.getCount(for: "Tokyo")
        #expect(countBefore == 1)

        // Delete
        try await ctx.database.withTransaction { transaction in
            try await ctx.maintainer.updateIndex(
                oldItem: user,
                newItem: nil,
                transaction: transaction
            )
        }

        let countAfter = try await ctx.getCount(for: "Tokyo")
        #expect(countAfter == 0, "Count should be 0 after delete")

        try await ctx.cleanup()
    }

    @Test("Delete one from multiple decrements correctly")
    func testDeleteOneFromMultiple() async throws {
        try await FDBTestSetup.shared.initialize()
        let ctx = try TestContext()

        let user1 = CountTestUser(id: "user1", city: "Tokyo", department: "Engineering")
        let user2 = CountTestUser(id: "user2", city: "Tokyo", department: "Sales")
        let user3 = CountTestUser(id: "user3", city: "Tokyo", department: "Marketing")

        // Insert all
        try await ctx.database.withTransaction { transaction in
            try await ctx.maintainer.updateIndex(oldItem: nil, newItem: user1, transaction: transaction)
            try await ctx.maintainer.updateIndex(oldItem: nil, newItem: user2, transaction: transaction)
            try await ctx.maintainer.updateIndex(oldItem: nil, newItem: user3, transaction: transaction)
        }

        let countBefore = try await ctx.getCount(for: "Tokyo")
        #expect(countBefore == 3)

        // Delete one
        try await ctx.database.withTransaction { transaction in
            try await ctx.maintainer.updateIndex(
                oldItem: user2,
                newItem: nil,
                transaction: transaction
            )
        }

        let countAfter = try await ctx.getCount(for: "Tokyo")
        #expect(countAfter == 2, "Count should be 2 after deleting one of three")

        try await ctx.cleanup()
    }

    // MARK: - Update Tests

    @Test("Update same group does not change count")
    func testUpdateSameGroupNoChange() async throws {
        try await FDBTestSetup.shared.initialize()
        let ctx = try TestContext()

        let user = CountTestUser(id: "user1", city: "Tokyo", department: "Engineering")

        // Insert
        try await ctx.database.withTransaction { transaction in
            try await ctx.maintainer.updateIndex(
                oldItem: nil,
                newItem: user,
                transaction: transaction
            )
        }

        // Update department (same city)
        let updatedUser = CountTestUser(id: "user1", city: "Tokyo", department: "Sales")
        try await ctx.database.withTransaction { transaction in
            try await ctx.maintainer.updateIndex(
                oldItem: user,
                newItem: updatedUser,
                transaction: transaction
            )
        }

        let count = try await ctx.getCount(for: "Tokyo")
        #expect(count == 1, "Count should remain 1 when updating within same group")

        try await ctx.cleanup()
    }

    @Test("Update different group moves count")
    func testUpdateDifferentGroupMovesCount() async throws {
        try await FDBTestSetup.shared.initialize()
        let ctx = try TestContext()

        let user = CountTestUser(id: "user1", city: "Tokyo", department: "Engineering")

        // Insert
        try await ctx.database.withTransaction { transaction in
            try await ctx.maintainer.updateIndex(
                oldItem: nil,
                newItem: user,
                transaction: transaction
            )
        }

        let tokyoCountBefore = try await ctx.getCount(for: "Tokyo")
        let osakaCountBefore = try await ctx.getCount(for: "Osaka")
        #expect(tokyoCountBefore == 1)
        #expect(osakaCountBefore == 0)

        // Update city from Tokyo to Osaka
        let updatedUser = CountTestUser(id: "user1", city: "Osaka", department: "Engineering")
        try await ctx.database.withTransaction { transaction in
            try await ctx.maintainer.updateIndex(
                oldItem: user,
                newItem: updatedUser,
                transaction: transaction
            )
        }

        let tokyoCountAfter = try await ctx.getCount(for: "Tokyo")
        let osakaCountAfter = try await ctx.getCount(for: "Osaka")
        #expect(tokyoCountAfter == 0, "Tokyo count should be 0 after moving user")
        #expect(osakaCountAfter == 1, "Osaka count should be 1 after moving user")

        try await ctx.cleanup()
    }

    // MARK: - Scan Tests

    @Test("ScanItem increments count")
    func testScanItemIncrementsCount() async throws {
        try await FDBTestSetup.shared.initialize()
        let ctx = try TestContext()

        let users = [
            CountTestUser(id: "user1", city: "Tokyo", department: "Engineering"),
            CountTestUser(id: "user2", city: "Tokyo", department: "Sales"),
            CountTestUser(id: "user3", city: "Osaka", department: "Marketing")
        ]

        try await ctx.database.withTransaction { transaction in
            for user in users {
                try await ctx.maintainer.scanItem(
                    user,
                    id: Tuple(user.id),
                    transaction: transaction
                )
            }
        }

        let tokyoCount = try await ctx.getCount(for: "Tokyo")
        let osakaCount = try await ctx.getCount(for: "Osaka")

        #expect(tokyoCount == 2, "Tokyo count should be 2")
        #expect(osakaCount == 1, "Osaka count should be 1")

        try await ctx.cleanup()
    }

    // MARK: - Query Tests

    @Test("GetAllCounts returns all groups")
    func testGetAllCountsReturnsAllGroups() async throws {
        try await FDBTestSetup.shared.initialize()
        let ctx = try TestContext()

        let users = [
            CountTestUser(id: "user1", city: "Tokyo", department: "Engineering"),
            CountTestUser(id: "user2", city: "Tokyo", department: "Sales"),
            CountTestUser(id: "user3", city: "Osaka", department: "Marketing"),
            CountTestUser(id: "user4", city: "Kyoto", department: "Engineering")
        ]

        try await ctx.database.withTransaction { transaction in
            for user in users {
                try await ctx.maintainer.updateIndex(
                    oldItem: nil,
                    newItem: user,
                    transaction: transaction
                )
            }
        }

        let allCounts = try await ctx.getAllCounts()
        #expect(allCounts.count == 3, "Should have 3 groups")

        // Verify total
        let total = allCounts.reduce(0) { $0 + $1.count }
        #expect(total == 4, "Total count should be 4")

        try await ctx.cleanup()
    }

    @Test("GetCount for non-existent group returns zero")
    func testGetCountNonExistentGroupReturnsZero() async throws {
        try await FDBTestSetup.shared.initialize()
        let ctx = try TestContext()

        let count = try await ctx.getCount(for: "NonExistentCity")
        #expect(count == 0, "Count for non-existent group should be 0")

        try await ctx.cleanup()
    }

    // MARK: - Composite Grouping Tests

    @Test("Composite grouping with multiple fields")
    func testCompositeGrouping() async throws {
        try await FDBTestSetup.shared.initialize()
        let database = try FDBClient.openDatabase()
        let testId = UUID().uuidString.prefix(8)
        let subspace = Subspace(prefix: Tuple("test", "count", "composite", String(testId)).pack())
        let indexSubspace = subspace.subspace("I").subspace("CountTestUser_city_department")

        let index = Index(
            name: "CountTestUser_city_department",
            kind: CountIndexKind<CountTestUser>(groupBy: [\.city, \.department]),
            rootExpression: ConcatenateKeyExpression(children: [
                FieldKeyExpression(fieldName: "city"),
                FieldKeyExpression(fieldName: "department")
            ]),
            subspaceKey: "CountTestUser_city_department",
            itemTypes: Set(["CountTestUser"])
        )

        let maintainer = CountIndexMaintainer<CountTestUser>(
            index: index,
            subspace: indexSubspace,
            idExpression: FieldKeyExpression(fieldName: "id")
        )

        let users = [
            CountTestUser(id: "user1", city: "Tokyo", department: "Engineering"),
            CountTestUser(id: "user2", city: "Tokyo", department: "Engineering"),
            CountTestUser(id: "user3", city: "Tokyo", department: "Sales"),
            CountTestUser(id: "user4", city: "Osaka", department: "Engineering")
        ]

        try await database.withTransaction { transaction in
            for user in users {
                try await maintainer.updateIndex(
                    oldItem: nil,
                    newItem: user,
                    transaction: transaction
                )
            }
        }

        // Query Tokyo+Engineering
        let tokyoEngineering = try await database.withTransaction { transaction in
            try await maintainer.getCount(
                groupingValues: ["Tokyo", "Engineering"],
                transaction: transaction
            )
        }

        // Query Tokyo+Sales
        let tokyoSales = try await database.withTransaction { transaction in
            try await maintainer.getCount(
                groupingValues: ["Tokyo", "Sales"],
                transaction: transaction
            )
        }

        // Query Osaka+Engineering
        let osakaEngineering = try await database.withTransaction { transaction in
            try await maintainer.getCount(
                groupingValues: ["Osaka", "Engineering"],
                transaction: transaction
            )
        }

        #expect(tokyoEngineering == 2, "Tokyo+Engineering should have 2")
        #expect(tokyoSales == 1, "Tokyo+Sales should have 1")
        #expect(osakaEngineering == 1, "Osaka+Engineering should have 1")

        // Cleanup
        try await database.withTransaction { transaction in
            let (begin, end) = subspace.range()
            transaction.clearRange(beginKey: begin, endKey: end)
        }
    }
}
