// DistinctIndexBehaviorTests.swift
// Integration tests for DistinctIndex behavior with FDB

import Testing
import Foundation
import FoundationDB
import Core
import TestSupport
@testable import DatabaseEngine
@testable import AggregationIndex

// MARK: - Test Model

struct DistinctTestPageView: Persistable {
    typealias ID = String

    var id: String
    var pageId: String
    var userId: String
    var timestamp: Date

    init(id: String = UUID().uuidString, pageId: String, userId: String, timestamp: Date = Date()) {
        self.id = id
        self.pageId = pageId
        self.userId = userId
        self.timestamp = timestamp
    }

    static var persistableType: String { "DistinctTestPageView" }
    static var allFields: [String] { ["id", "pageId", "userId", "timestamp"] }
    static var indexDescriptors: [IndexDescriptor] { [] }

    static func fieldNumber(for fieldName: String) -> Int? { nil }
    static func enumMetadata(for fieldName: String) -> EnumMetadata? { nil }

    subscript(dynamicMember member: String) -> (any Sendable)? {
        switch member {
        case "id": return id
        case "pageId": return pageId
        case "userId": return userId
        case "timestamp": return timestamp
        default: return nil
        }
    }

    static func fieldName<Value>(for keyPath: KeyPath<DistinctTestPageView, Value>) -> String {
        switch keyPath {
        case \DistinctTestPageView.id: return "id"
        case \DistinctTestPageView.pageId: return "pageId"
        case \DistinctTestPageView.userId: return "userId"
        case \DistinctTestPageView.timestamp: return "timestamp"
        default: return "\(keyPath)"
        }
    }

    static func fieldName(for keyPath: PartialKeyPath<DistinctTestPageView>) -> String {
        switch keyPath {
        case \DistinctTestPageView.id: return "id"
        case \DistinctTestPageView.pageId: return "pageId"
        case \DistinctTestPageView.userId: return "userId"
        case \DistinctTestPageView.timestamp: return "timestamp"
        default: return "\(keyPath)"
        }
    }

    static func fieldName(for keyPath: AnyKeyPath) -> String {
        if let partial = keyPath as? PartialKeyPath<DistinctTestPageView> {
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
    let maintainer: DistinctIndexMaintainer<DistinctTestPageView>

    init(indexName: String = "DistinctTestPageView_pageId_userId") throws {
        self.database = try FDBClient.openDatabase()
        let testId = UUID().uuidString.prefix(8)
        self.subspace = Subspace(prefix: Tuple("test", "distinct", String(testId)).pack())
        self.indexSubspace = subspace.subspace("I").subspace(indexName)

        // Expression: pageId + userId (grouping + distinct value)
        let index = Index(
            name: indexName,
            kind: DistinctIndexKind<DistinctTestPageView>(
                groupBy: [\.pageId],
                value: \.userId
            ),
            rootExpression: ConcatenateKeyExpression(children: [
                FieldKeyExpression(fieldName: "pageId"),
                FieldKeyExpression(fieldName: "userId")
            ]),
            subspaceKey: indexName,
            itemTypes: Set(["DistinctTestPageView"])
        )

        self.maintainer = DistinctIndexMaintainer<DistinctTestPageView>(
            index: index,
            subspace: indexSubspace,
            idExpression: FieldKeyExpression(fieldName: "id"),
            precision: 14
        )
    }

    func cleanup() async throws {
        try await database.withTransaction { transaction in
            let (begin, end) = subspace.range()
            transaction.clearRange(beginKey: begin, endKey: end)
        }
    }

    func getDistinctCount(for pageId: String) async throws -> (estimated: Int64, errorRate: Double) {
        try await database.withTransaction { transaction in
            try await maintainer.getDistinctCount(
                groupingValues: [pageId],
                transaction: transaction
            )
        }
    }

    func getAllDistinctCounts() async throws -> [(grouping: [any TupleElement], estimated: Int64, errorRate: Double)] {
        try await database.withTransaction { transaction in
            try await maintainer.getAllDistinctCounts(transaction: transaction)
        }
    }
}

// MARK: - Behavior Tests

@Suite("DistinctIndex Behavior Tests", .tags(.fdb), .serialized)
struct DistinctIndexBehaviorTests {

    // MARK: - Insert Tests

    @Test("Insert adds value to HyperLogLog")
    func testInsertAddsValue() async throws {
        try await FDBTestSetup.shared.initialize()
        let ctx = try TestContext()

        let pageView = DistinctTestPageView(pageId: "page1", userId: "user1")

        try await ctx.database.withTransaction { transaction in
            try await ctx.maintainer.updateIndex(
                oldItem: nil as DistinctTestPageView?,
                newItem: pageView,
                transaction: transaction
            )
        }

        let (estimated, errorRate) = try await ctx.getDistinctCount(for: "page1")
        #expect(estimated == 1, "Distinct count should be 1 after single insert")
        #expect(errorRate > 0, "Error rate should be positive")

        try await ctx.cleanup()
    }

    @Test("Multiple unique users increment distinct count")
    func testMultipleUniqueUsers() async throws {
        try await FDBTestSetup.shared.initialize()
        let ctx = try TestContext()

        let pageViews = (1...10).map { i in
            DistinctTestPageView(pageId: "page1", userId: "user\(i)")
        }

        try await ctx.database.withTransaction { transaction in
            for pageView in pageViews {
                try await ctx.maintainer.updateIndex(
                    oldItem: nil as DistinctTestPageView?,
                    newItem: pageView,
                    transaction: transaction
                )
            }
        }

        let (estimated, _) = try await ctx.getDistinctCount(for: "page1")
        // HyperLogLog is approximate, allow some tolerance
        #expect(estimated >= 8 && estimated <= 12, "Distinct count should be approximately 10 (actual: \(estimated))")

        try await ctx.cleanup()
    }

    @Test("Duplicate users do not increment distinct count")
    func testDuplicateUsersNotCounted() async throws {
        try await FDBTestSetup.shared.initialize()
        let ctx = try TestContext()

        // Same user visits same page 5 times
        let pageViews = (1...5).map { i in
            DistinctTestPageView(id: "view\(i)", pageId: "page1", userId: "user1")
        }

        try await ctx.database.withTransaction { transaction in
            for pageView in pageViews {
                try await ctx.maintainer.updateIndex(
                    oldItem: nil as DistinctTestPageView?,
                    newItem: pageView,
                    transaction: transaction
                )
            }
        }

        let (estimated, _) = try await ctx.getDistinctCount(for: "page1")
        #expect(estimated == 1, "Distinct count should be 1 for duplicate user visits")

        try await ctx.cleanup()
    }

    @Test("Different groups have independent counts")
    func testDifferentGroupsIndependent() async throws {
        try await FDBTestSetup.shared.initialize()
        let ctx = try TestContext()

        let pageViews = [
            DistinctTestPageView(pageId: "page1", userId: "user1"),
            DistinctTestPageView(pageId: "page1", userId: "user2"),
            DistinctTestPageView(pageId: "page1", userId: "user3"),
            DistinctTestPageView(pageId: "page2", userId: "user1"),
            DistinctTestPageView(pageId: "page2", userId: "user4")
        ]

        try await ctx.database.withTransaction { transaction in
            for pageView in pageViews {
                try await ctx.maintainer.updateIndex(
                    oldItem: nil as DistinctTestPageView?,
                    newItem: pageView,
                    transaction: transaction
                )
            }
        }

        let (page1Count, _) = try await ctx.getDistinctCount(for: "page1")
        let (page2Count, _) = try await ctx.getDistinctCount(for: "page2")

        #expect(page1Count == 3, "page1 should have 3 unique users")
        #expect(page2Count == 2, "page2 should have 2 unique users")

        try await ctx.cleanup()
    }

    // MARK: - Add-Only Behavior Tests

    @Test("Delete does NOT decrease distinct count (add-only)")
    func testDeleteDoesNotDecreaseCount() async throws {
        try await FDBTestSetup.shared.initialize()
        let ctx = try TestContext()

        let pageView = DistinctTestPageView(pageId: "page1", userId: "user1")

        // Insert
        try await ctx.database.withTransaction { transaction in
            try await ctx.maintainer.updateIndex(
                oldItem: nil as DistinctTestPageView?,
                newItem: pageView,
                transaction: transaction
            )
        }

        let (countBefore, _) = try await ctx.getDistinctCount(for: "page1")
        #expect(countBefore == 1)

        // Delete - HLL is add-only, count should NOT decrease
        try await ctx.database.withTransaction { transaction in
            try await ctx.maintainer.updateIndex(
                oldItem: pageView,
                newItem: nil as DistinctTestPageView?,
                transaction: transaction
            )
        }

        let (countAfter, _) = try await ctx.getDistinctCount(for: "page1")
        // HLL is add-only: delete does NOT remove value from HLL
        #expect(countAfter == 1, "Distinct count should remain 1 after delete (add-only)")

        try await ctx.cleanup()
    }

    @Test("Update adds new value (old value remains in HLL)")
    func testUpdateAddsNewValue() async throws {
        try await FDBTestSetup.shared.initialize()
        let ctx = try TestContext()

        let pageView = DistinctTestPageView(id: "view1", pageId: "page1", userId: "user1")

        // Insert
        try await ctx.database.withTransaction { transaction in
            try await ctx.maintainer.updateIndex(
                oldItem: nil as DistinctTestPageView?,
                newItem: pageView,
                transaction: transaction
            )
        }

        // Update userId
        let updatedPageView = DistinctTestPageView(id: "view1", pageId: "page1", userId: "user2")
        try await ctx.database.withTransaction { transaction in
            try await ctx.maintainer.updateIndex(
                oldItem: pageView,
                newItem: updatedPageView,
                transaction: transaction
            )
        }

        let (count, _) = try await ctx.getDistinctCount(for: "page1")
        // Both user1 (old) and user2 (new) should be in HLL (add-only)
        #expect(count == 2, "Distinct count should be 2 after update (both old and new values)")

        try await ctx.cleanup()
    }

    // MARK: - Query Tests

    @Test("GetAllDistinctCounts returns all groups")
    func testGetAllDistinctCounts() async throws {
        try await FDBTestSetup.shared.initialize()
        let ctx = try TestContext()

        let pageViews = [
            DistinctTestPageView(pageId: "page1", userId: "user1"),
            DistinctTestPageView(pageId: "page1", userId: "user2"),
            DistinctTestPageView(pageId: "page2", userId: "user3"),
            DistinctTestPageView(pageId: "page3", userId: "user4"),
            DistinctTestPageView(pageId: "page3", userId: "user5"),
            DistinctTestPageView(pageId: "page3", userId: "user6")
        ]

        try await ctx.database.withTransaction { transaction in
            for pageView in pageViews {
                try await ctx.maintainer.updateIndex(
                    oldItem: nil as DistinctTestPageView?,
                    newItem: pageView,
                    transaction: transaction
                )
            }
        }

        let allCounts = try await ctx.getAllDistinctCounts()
        #expect(allCounts.count == 3, "Should have 3 groups")

        try await ctx.cleanup()
    }

    @Test("GetDistinctCount for non-existent group returns zero")
    func testGetDistinctCountNonExistentReturnsZero() async throws {
        try await FDBTestSetup.shared.initialize()
        let ctx = try TestContext()

        let (count, _) = try await ctx.getDistinctCount(for: "nonexistent")
        #expect(count == 0, "Distinct count for non-existent group should be 0")

        try await ctx.cleanup()
    }

    // MARK: - Scan Tests

    @Test("ScanItem adds to HyperLogLog")
    func testScanItemAddsToHLL() async throws {
        try await FDBTestSetup.shared.initialize()
        let ctx = try TestContext()

        let pageViews = [
            DistinctTestPageView(pageId: "page1", userId: "user1"),
            DistinctTestPageView(pageId: "page1", userId: "user2"),
            DistinctTestPageView(pageId: "page1", userId: "user3")
        ]

        try await ctx.database.withTransaction { transaction in
            for pageView in pageViews {
                try await ctx.maintainer.scanItem(
                    pageView,
                    id: Tuple(pageView.id),
                    transaction: transaction
                )
            }
        }

        let (count, _) = try await ctx.getDistinctCount(for: "page1")
        #expect(count == 3, "Distinct count should be 3 after scanItem")

        try await ctx.cleanup()
    }

    // MARK: - Large Scale Tests

    @Test("HyperLogLog accuracy with large cardinality")
    func testLargeCardinality() async throws {
        try await FDBTestSetup.shared.initialize()
        let ctx = try TestContext()

        let uniqueUserCount = 1000

        try await ctx.database.withTransaction { transaction in
            for i in 1...uniqueUserCount {
                let pageView = DistinctTestPageView(pageId: "popular-page", userId: "user\(i)")
                try await ctx.maintainer.updateIndex(
                    oldItem: nil as DistinctTestPageView?,
                    newItem: pageView,
                    transaction: transaction
                )
            }
        }

        let (estimated, errorRate) = try await ctx.getDistinctCount(for: "popular-page")

        // HyperLogLog++ with precision 14: ~0.81% standard error
        // For 1000 values, expected error ~8
        let expectedMin = Int64(Double(uniqueUserCount) * 0.95)  // Allow 5% under
        let expectedMax = Int64(Double(uniqueUserCount) * 1.05)  // Allow 5% over

        #expect(
            estimated >= expectedMin && estimated <= expectedMax,
            "Estimated \(estimated) should be within 5% of \(uniqueUserCount)"
        )
        #expect(errorRate < 0.02, "Error rate should be less than 2%")

        try await ctx.cleanup()
    }
}
