#if FOUNDATION_DB
// DistinctIndexBehaviorTests.swift
// Integration tests for DistinctIndex behavior with FDB

import Testing
import Foundation
import StorageKit
import FDBStorage
import Core
import DatabaseValue
import TestSupport
@testable import DatabaseEngine
@testable import AggregationIndex

// MARK: - Test Model

struct DistinctIndexedPageView: Persistable {
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

    static var persistableType: String { "DistinctIndexedPageView" }
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

    static func fieldName<Value>(for keyPath: KeyPath<DistinctIndexedPageView, Value>) -> String {
        switch keyPath {
        case \DistinctIndexedPageView.id: return "id"
        case \DistinctIndexedPageView.pageId: return "pageId"
        case \DistinctIndexedPageView.userId: return "userId"
        case \DistinctIndexedPageView.timestamp: return "timestamp"
        default: return "\(keyPath)"
        }
    }

    static func fieldName(for keyPath: PartialKeyPath<DistinctIndexedPageView>) -> String {
        switch keyPath {
        case \DistinctIndexedPageView.id: return "id"
        case \DistinctIndexedPageView.pageId: return "pageId"
        case \DistinctIndexedPageView.userId: return "userId"
        case \DistinctIndexedPageView.timestamp: return "timestamp"
        default: return "\(keyPath)"
        }
    }

    static func fieldName(for keyPath: AnyKeyPath) -> String {
        if let partial = keyPath as? PartialKeyPath<DistinctIndexedPageView> {
            return fieldName(for: partial)
        }
        return "\(keyPath)"
    }
}

// MARK: - Distinct Index Context

private struct DistinctIndexContext {
    let database: any StorageEngine
    let subspace: Subspace
    let indexSubspace: Subspace
    let maintainer: DistinctIndexMaintainer<DistinctIndexedPageView>

    init(indexName: String = "DistinctIndexedPageView_pageId_userId") async throws {
        self.database = try await FoundationDBScenarioCoordinator.shared.makeEngine()
        let testId = UUID().uuidString.prefix(8)
        self.subspace = Subspace(prefix: Tuple("test", "distinct", String(testId)).pack())
        self.indexSubspace = subspace.subspace("I").subspace(indexName)

        // Expression: pageId + userId (grouping + distinct value)
        let index = Index(
            name: indexName,
            kind: DistinctIndexKind<DistinctIndexedPageView>(
                groupBy: [\.pageId],
                value: \.userId
            ),
            rootExpression: ConcatenateKeyExpression(children: [
                FieldKeyExpression(fieldName: "pageId"),
                FieldKeyExpression(fieldName: "userId")
            ]),
            subspaceKey: indexName,
            itemTypes: Set(["DistinctIndexedPageView"])
        )

        self.maintainer = DistinctIndexMaintainer<DistinctIndexedPageView>(
            index: index,
            subspace: indexSubspace,
            idExpression: FieldKeyExpression(fieldName: "id"),
            precision: 14
        )
    }

    func cleanup() async throws {
        try await database.withTransaction { transaction in
            let (begin, end) = subspace.range()
            try transaction.clearRange(beginKey: begin, endKey: end)
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

@Suite("DistinctIndex Behavior Tests", .tags(.fdb), .serialized, .heartbeat)
struct DistinctIndexBehaviorTests {

    // MARK: - Insert Tests

    @Test("Insert adds value to HyperLogLog")
    func testInsertAddsValue() async throws {
        try await FoundationDBScenarioCoordinator.shared.initialize()
        let ctx = try await DistinctIndexContext()

        let pageView = DistinctIndexedPageView(pageId: "page1", userId: "user1")

        try await ctx.database.withTransaction { transaction in
            try await ctx.maintainer.updateIndex(
                oldItem: nil as DistinctIndexedPageView?,
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
        try await FoundationDBScenarioCoordinator.shared.initialize()
        let ctx = try await DistinctIndexContext()

        let pageViews = (1...10).map { i in
            DistinctIndexedPageView(pageId: "page1", userId: "user\(i)")
        }

        try await ctx.database.withTransaction { transaction in
            for pageView in pageViews {
                try await ctx.maintainer.updateIndex(
                    oldItem: nil as DistinctIndexedPageView?,
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
        try await FoundationDBScenarioCoordinator.shared.initialize()
        let ctx = try await DistinctIndexContext()

        // Same user visits same page 5 times
        let pageViews = (1...5).map { i in
            DistinctIndexedPageView(id: "view\(i)", pageId: "page1", userId: "user1")
        }

        try await ctx.database.withTransaction { transaction in
            for pageView in pageViews {
                try await ctx.maintainer.updateIndex(
                    oldItem: nil as DistinctIndexedPageView?,
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
        try await FoundationDBScenarioCoordinator.shared.initialize()
        let ctx = try await DistinctIndexContext()

        let pageViews = [
            DistinctIndexedPageView(pageId: "page1", userId: "user1"),
            DistinctIndexedPageView(pageId: "page1", userId: "user2"),
            DistinctIndexedPageView(pageId: "page1", userId: "user3"),
            DistinctIndexedPageView(pageId: "page2", userId: "user1"),
            DistinctIndexedPageView(pageId: "page2", userId: "user4")
        ]

        try await ctx.database.withTransaction { transaction in
            for pageView in pageViews {
                try await ctx.maintainer.updateIndex(
                    oldItem: nil as DistinctIndexedPageView?,
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

    // MARK: - Delete and Update Tests

    @Test("Delete removes the final distinct value reference")
    func testDeleteRemovesFinalReference() async throws {
        try await FoundationDBScenarioCoordinator.shared.initialize()
        let ctx = try await DistinctIndexContext()

        let pageView = DistinctIndexedPageView(pageId: "page1", userId: "user1")

        // Insert
        try await ctx.database.withTransaction { transaction in
            try await ctx.maintainer.updateIndex(
                oldItem: nil as DistinctIndexedPageView?,
                newItem: pageView,
                transaction: transaction
            )
        }

        let (countBefore, _) = try await ctx.getDistinctCount(for: "page1")
        #expect(countBefore == 1)

        // Delete the only reference.
        try await ctx.database.withTransaction { transaction in
            try await ctx.maintainer.updateIndex(
                oldItem: pageView,
                newItem: nil as DistinctIndexedPageView?,
                transaction: transaction
            )
        }

        let (countAfter, _) = try await ctx.getDistinctCount(for: "page1")
        #expect(countAfter == 0, "Distinct count should be zero after final delete")

        try await ctx.cleanup()
    }

    @Test("Update replaces the old distinct value")
    func testUpdateReplacesOldValue() async throws {
        try await FoundationDBScenarioCoordinator.shared.initialize()
        let ctx = try await DistinctIndexContext()

        let pageView = DistinctIndexedPageView(id: "view1", pageId: "page1", userId: "user1")

        // Insert
        try await ctx.database.withTransaction { transaction in
            try await ctx.maintainer.updateIndex(
                oldItem: nil as DistinctIndexedPageView?,
                newItem: pageView,
                transaction: transaction
            )
        }

        // Update userId
        let updatedPageView = DistinctIndexedPageView(id: "view1", pageId: "page1", userId: "user2")
        try await ctx.database.withTransaction { transaction in
            try await ctx.maintainer.updateIndex(
                oldItem: pageView,
                newItem: updatedPageView,
                transaction: transaction
            )
        }

        let (count, _) = try await ctx.getDistinctCount(for: "page1")
        #expect(count == 1, "Distinct count should contain only the replacement value")

        try await ctx.cleanup()
    }

    // MARK: - Query Tests

    @Test("GetAllDistinctCounts returns all groups")
    func testGetAllDistinctCounts() async throws {
        try await FoundationDBScenarioCoordinator.shared.initialize()
        let ctx = try await DistinctIndexContext()

        let pageViews = [
            DistinctIndexedPageView(pageId: "page1", userId: "user1"),
            DistinctIndexedPageView(pageId: "page1", userId: "user2"),
            DistinctIndexedPageView(pageId: "page2", userId: "user3"),
            DistinctIndexedPageView(pageId: "page3", userId: "user4"),
            DistinctIndexedPageView(pageId: "page3", userId: "user5"),
            DistinctIndexedPageView(pageId: "page3", userId: "user6")
        ]

        try await ctx.database.withTransaction { transaction in
            for pageView in pageViews {
                try await ctx.maintainer.updateIndex(
                    oldItem: nil as DistinctIndexedPageView?,
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
        try await FoundationDBScenarioCoordinator.shared.initialize()
        let ctx = try await DistinctIndexContext()

        let (count, _) = try await ctx.getDistinctCount(for: "nonexistent")
        #expect(count == 0, "Distinct count for non-existent group should be 0")

        try await ctx.cleanup()
    }

    // MARK: - Scan Tests

    @Test("ScanItem adds to HyperLogLog")
    func testScanItemAddsToHLL() async throws {
        try await FoundationDBScenarioCoordinator.shared.initialize()
        let ctx = try await DistinctIndexContext()

        let pageViews = [
            DistinctIndexedPageView(pageId: "page1", userId: "user1"),
            DistinctIndexedPageView(pageId: "page1", userId: "user2"),
            DistinctIndexedPageView(pageId: "page1", userId: "user3")
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
        try await FoundationDBScenarioCoordinator.shared.initialize()
        let ctx = try await DistinctIndexContext()

        let uniqueUserCount = 1000

        try await ctx.database.withTransaction { transaction in
            for i in 1...uniqueUserCount {
                let pageView = DistinctIndexedPageView(pageId: "popular-page", userId: "user\(i)")
                try await ctx.maintainer.updateIndex(
                    oldItem: nil as DistinctIndexedPageView?,
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
#endif
