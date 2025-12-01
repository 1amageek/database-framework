// MaxIndexBehaviorTests.swift
// Integration tests for MaxIndex behavior with FDB

import Testing
import Foundation
import FoundationDB
import Core
import TestSupport
@testable import DatabaseEngine
@testable import AggregationIndex

// MARK: - Test Model

struct MaxTestScore: Persistable {
    typealias ID = String

    var id: String
    var subject: String
    var studentName: String
    var score: Int64

    init(id: String = UUID().uuidString, subject: String, studentName: String, score: Int64) {
        self.id = id
        self.subject = subject
        self.studentName = studentName
        self.score = score
    }

    static var persistableType: String { "MaxTestScore" }
    static var allFields: [String] { ["id", "subject", "studentName", "score"] }
    static var indexDescriptors: [IndexDescriptor] { [] }

    static func fieldNumber(for fieldName: String) -> Int? { nil }
    static func enumMetadata(for fieldName: String) -> EnumMetadata? { nil }

    subscript(dynamicMember member: String) -> (any Sendable)? {
        switch member {
        case "id": return id
        case "subject": return subject
        case "studentName": return studentName
        case "score": return score
        default: return nil
        }
    }

    static func fieldName<Value>(for keyPath: KeyPath<MaxTestScore, Value>) -> String {
        switch keyPath {
        case \MaxTestScore.id: return "id"
        case \MaxTestScore.subject: return "subject"
        case \MaxTestScore.studentName: return "studentName"
        case \MaxTestScore.score: return "score"
        default: return "\(keyPath)"
        }
    }

    static func fieldName(for keyPath: PartialKeyPath<MaxTestScore>) -> String {
        switch keyPath {
        case \MaxTestScore.id: return "id"
        case \MaxTestScore.subject: return "subject"
        case \MaxTestScore.studentName: return "studentName"
        case \MaxTestScore.score: return "score"
        default: return "\(keyPath)"
        }
    }

    static func fieldName(for keyPath: AnyKeyPath) -> String {
        if let partial = keyPath as? PartialKeyPath<MaxTestScore> {
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
    let maintainer: MaxIndexMaintainer<MaxTestScore>

    init(indexName: String = "MaxTestScore_subject_score") throws {
        self.database = try FDBClient.openDatabase()
        let testId = UUID().uuidString.prefix(8)
        self.subspace = Subspace(prefix: Tuple("test", "max", String(testId)).pack())
        self.indexSubspace = subspace.subspace("I").subspace(indexName)

        // Expression: subject + score (grouping + max value)
        let index = Index(
            name: indexName,
            kind: MaxIndexKind(),
            rootExpression: ConcatenateKeyExpression(children: [
                FieldKeyExpression(fieldName: "subject"),
                FieldKeyExpression(fieldName: "score")
            ]),
            subspaceKey: indexName,
            itemTypes: Set(["MaxTestScore"])
        )

        self.maintainer = MaxIndexMaintainer<MaxTestScore>(
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

    func countIndexEntries() async throws -> Int {
        try await database.withTransaction { transaction -> Int in
            let (begin, end) = indexSubspace.range()
            var count = 0
            for try await _ in transaction.getRange(begin: begin, end: end, snapshot: true) {
                count += 1
            }
            return count
        }
    }

    func getMax(for subject: String) async throws -> Int64 {
        try await database.withTransaction { transaction in
            try await maintainer.getMax(
                groupingValues: [subject],
                transaction: transaction
            )
        }
    }
}

// MARK: - Behavior Tests

@Suite("MaxIndex Behavior Tests", .tags(.fdb))
struct MaxIndexBehaviorTests {

    // MARK: - Insert Tests

    @Test("Insert adds to sorted set")
    func testInsertAddsToSortedSet() async throws {
        try await FDBTestSetup.shared.initialize()
        let ctx = try TestContext()

        let score = MaxTestScore(id: "s1", subject: "Math", studentName: "Alice", score: 95)

        try await ctx.database.withTransaction { transaction in
            try await ctx.maintainer.updateIndex(
                oldItem: nil,
                newItem: score,
                transaction: transaction
            )
        }

        let count = try await ctx.countIndexEntries()
        #expect(count == 1, "Should have 1 index entry after insert")

        try await ctx.cleanup()
    }

    @Test("Multiple inserts create multiple entries")
    func testMultipleInserts() async throws {
        try await FDBTestSetup.shared.initialize()
        let ctx = try TestContext()

        let scores = [
            MaxTestScore(id: "s1", subject: "Math", studentName: "Alice", score: 95),
            MaxTestScore(id: "s2", subject: "Math", studentName: "Bob", score: 88),
            MaxTestScore(id: "s3", subject: "Math", studentName: "Charlie", score: 72)
        ]

        try await ctx.database.withTransaction { transaction in
            for score in scores {
                try await ctx.maintainer.updateIndex(
                    oldItem: nil,
                    newItem: score,
                    transaction: transaction
                )
            }
        }

        let count = try await ctx.countIndexEntries()
        #expect(count == 3, "Should have 3 index entries")

        try await ctx.cleanup()
    }

    // MARK: - Delete Tests

    @Test("Delete removes from sorted set")
    func testDeleteRemovesFromSortedSet() async throws {
        try await FDBTestSetup.shared.initialize()
        let ctx = try TestContext()

        let score = MaxTestScore(id: "s1", subject: "Math", studentName: "Alice", score: 95)

        // Insert
        try await ctx.database.withTransaction { transaction in
            try await ctx.maintainer.updateIndex(
                oldItem: nil,
                newItem: score,
                transaction: transaction
            )
        }

        let countBefore = try await ctx.countIndexEntries()
        #expect(countBefore == 1)

        // Delete
        try await ctx.database.withTransaction { transaction in
            try await ctx.maintainer.updateIndex(
                oldItem: score,
                newItem: nil,
                transaction: transaction
            )
        }

        let countAfter = try await ctx.countIndexEntries()
        #expect(countAfter == 0, "Should have 0 entries after delete")

        try await ctx.cleanup()
    }

    // MARK: - Update Tests

    @Test("Update changes position in sorted set")
    func testUpdateChangesPosition() async throws {
        try await FDBTestSetup.shared.initialize()
        let ctx = try TestContext()

        let score = MaxTestScore(id: "s1", subject: "Math", studentName: "Alice", score: 85)

        // Insert
        try await ctx.database.withTransaction { transaction in
            try await ctx.maintainer.updateIndex(
                oldItem: nil,
                newItem: score,
                transaction: transaction
            )
        }

        // Update score
        let updatedScore = MaxTestScore(id: "s1", subject: "Math", studentName: "Alice", score: 98)
        try await ctx.database.withTransaction { transaction in
            try await ctx.maintainer.updateIndex(
                oldItem: score,
                newItem: updatedScore,
                transaction: transaction
            )
        }

        let count = try await ctx.countIndexEntries()
        #expect(count == 1, "Should still have 1 entry after update")

        let max = try await ctx.getMax(for: "Math")
        #expect(max == 98, "Max should be updated to 98")

        try await ctx.cleanup()
    }

    // MARK: - Query Tests

    @Test("getMax returns maximum value")
    func testGetMaxReturnsMaximum() async throws {
        try await FDBTestSetup.shared.initialize()
        let ctx = try TestContext()

        let scores = [
            MaxTestScore(id: "s1", subject: "Math", studentName: "Alice", score: 95),
            MaxTestScore(id: "s2", subject: "Math", studentName: "Bob", score: 88),
            MaxTestScore(id: "s3", subject: "Math", studentName: "Charlie", score: 72)
        ]

        try await ctx.database.withTransaction { transaction in
            for score in scores {
                try await ctx.maintainer.updateIndex(
                    oldItem: nil,
                    newItem: score,
                    transaction: transaction
                )
            }
        }

        let max = try await ctx.getMax(for: "Math")
        #expect(max == 95, "Max should be 95 (highest score)")

        try await ctx.cleanup()
    }

    @Test("Multiple groups are independent")
    func testMultipleGroupsIndependent() async throws {
        try await FDBTestSetup.shared.initialize()
        let ctx = try TestContext()

        let scores = [
            MaxTestScore(id: "s1", subject: "Math", studentName: "Alice", score: 95),
            MaxTestScore(id: "s2", subject: "Math", studentName: "Bob", score: 88),
            MaxTestScore(id: "s3", subject: "Science", studentName: "Alice", score: 92),
            MaxTestScore(id: "s4", subject: "Science", studentName: "Charlie", score: 99)
        ]

        try await ctx.database.withTransaction { transaction in
            for score in scores {
                try await ctx.maintainer.updateIndex(
                    oldItem: nil,
                    newItem: score,
                    transaction: transaction
                )
            }
        }

        let mathMax = try await ctx.getMax(for: "Math")
        let scienceMax = try await ctx.getMax(for: "Science")

        #expect(mathMax == 95, "Math max should be 95")
        #expect(scienceMax == 99, "Science max should be 99")

        try await ctx.cleanup()
    }

    @Test("getMax for non-existent group throws error")
    func testGetMaxNonExistentGroupThrowsError() async throws {
        try await FDBTestSetup.shared.initialize()
        let ctx = try TestContext()

        await #expect(throws: IndexError.self) {
            _ = try await ctx.getMax(for: "NonExistent")
        }

        try await ctx.cleanup()
    }

    // MARK: - Scan Tests

    @Test("ScanItem adds to sorted set")
    func testScanItemAddsToSortedSet() async throws {
        try await FDBTestSetup.shared.initialize()
        let ctx = try TestContext()

        let scores = [
            MaxTestScore(id: "s1", subject: "Math", studentName: "Alice", score: 95),
            MaxTestScore(id: "s2", subject: "Math", studentName: "Bob", score: 88)
        ]

        try await ctx.database.withTransaction { transaction in
            for score in scores {
                try await ctx.maintainer.scanItem(
                    score,
                    id: Tuple(score.id),
                    transaction: transaction
                )
            }
        }

        let count = try await ctx.countIndexEntries()
        #expect(count == 2, "Should have 2 entries after scanItem")

        let max = try await ctx.getMax(for: "Math")
        #expect(max == 95, "Max should be 95")

        try await ctx.cleanup()
    }

    // MARK: - Edge Cases

    @Test("Max updates correctly when maximum item is deleted")
    func testMaxUpdatesOnMaximumDelete() async throws {
        try await FDBTestSetup.shared.initialize()
        let ctx = try TestContext()

        let scores = [
            MaxTestScore(id: "s1", subject: "Math", studentName: "Low", score: 60),
            MaxTestScore(id: "s2", subject: "Math", studentName: "High", score: 100)
        ]

        // Insert both
        try await ctx.database.withTransaction { transaction in
            for score in scores {
                try await ctx.maintainer.updateIndex(
                    oldItem: nil,
                    newItem: score,
                    transaction: transaction
                )
            }
        }

        let maxBefore = try await ctx.getMax(for: "Math")
        #expect(maxBefore == 100, "Max should be 100")

        // Delete the maximum item
        try await ctx.database.withTransaction { transaction in
            try await ctx.maintainer.updateIndex(
                oldItem: scores[1],
                newItem: nil,
                transaction: transaction
            )
        }

        let maxAfter = try await ctx.getMax(for: "Math")
        #expect(maxAfter == 60, "Max should now be 60 after deleting 100")

        try await ctx.cleanup()
    }
}
