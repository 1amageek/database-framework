#if FOUNDATION_DB
// MaxIndexBehaviorTests.swift
// Integration tests for MaxIndex behavior with FDB

import Testing
import Foundation
import StorageKit
import FDBStorage
import DatabaseKit
import DatabaseTypes
import TestSupport
@testable import DatabaseEngine
@testable import AggregationIndex

// MARK: - Test Model

@Persistable
struct SubjectScore {
    var id: String = ""
    var subject: String
    var studentName: String
    var score: Int64
}

// MARK: - Maximum Index Context

private struct MaximumIndexContext {
    let database: any StorageEngine
    let subspace: Subspace
    let indexSubspace: Subspace
    let maintainer: MaxIndexMaintainer<SubjectScore, Int64>

    init(indexName: String = "SubjectScore_subject_score") async throws {
        self.database = try await FoundationDBScenarioCoordinator.shared.makeEngine()
        let testId = UUID().uuidString.prefix(8)
        self.subspace = Subspace(prefix: Tuple("test", "max", String(testId)).pack())
        self.indexSubspace = subspace.subspace("I").subspace(indexName)

        // Expression: subject + score (grouping + max value)
        let index = try ResolvedIndex(
            for: SubjectScore.self,
            name: indexName,
            definition: numericAggregationIndexDefinition(
                .maximum,
                groupingFields: [
                    FieldIdentity(name: "subject", number: 2)
                ],
                valueField: FieldIdentity(name: "score", number: 4),
            ),
            rootExpression: ConcatenateKeyExpression(children: [
                FieldKeyExpression(fieldName: "subject"),
                FieldKeyExpression(fieldName: "score"),
            ]),
            itemTypes: Set(["SubjectScore"])
        )

        self.maintainer = MaxIndexMaintainer<SubjectScore, Int64>(
            index: index,
            subspace: indexSubspace,
            idExpression: FieldKeyExpression(fieldName: "id")
        )
    }

    func cleanup() async throws {
        try await database.withTransaction { transaction in
            let (begin, end) = subspace.range()
            try transaction.clearRange(beginKey: begin, endKey: end)
        }
    }

    func countIndexEntries() async throws -> Int {
        try await database.withTransaction { transaction -> Int in
            let (begin, end) = indexSubspace.range()
            return try await transaction.collectRange(
                begin: begin,
                end: end,
                snapshot: true
            ).count
        }
    }

    func getMax(for subject: String) async throws -> Int64 {
        try await database.withTransaction { transaction in
            try await maintainer.getMax(
                groupingValues: [.string(subject)],
                transaction: transaction
            )
        }
    }
}

// MARK: - Behavior Tests

@Suite("MaxIndex Behavior Tests", .tags(.fdb), .foundationDBScenario, .serialized, .heartbeat)
struct MaxIndexBehaviorTests {

    // MARK: - Insert Tests

    @Test("Insert adds to sorted set")
    func testInsertAddsToSortedSet() async throws {
        try await FoundationDBScenarioCoordinator.shared.initialize()
        let ctx = try await MaximumIndexContext()

        let score = SubjectScore(id: "s1", subject: "Math", studentName: "Alice", score: 95)

        try await ctx.database.withTransaction { transaction in
            try await ctx.maintainer.updateIndex(
                oldItem: nil as SubjectScore?,
                newItem: score,
                transaction: transaction
            )
        }

        let count = try await ctx.countIndexEntries()
        // 2-layer architecture: Layer 1 (individual) + Layer 2 (aggregate) = 2 entries
        #expect(count == 2, "Should have 2 index entries after insert (Layer 1 + Layer 2)")

        try await ctx.cleanup()
    }

    @Test("Multiple inserts create multiple entries")
    func testMultipleInserts() async throws {
        try await FoundationDBScenarioCoordinator.shared.initialize()
        let ctx = try await MaximumIndexContext()

        let scores = [
            SubjectScore(id: "s1", subject: "Math", studentName: "Alice", score: 95),
            SubjectScore(id: "s2", subject: "Math", studentName: "Bob", score: 88),
            SubjectScore(id: "s3", subject: "Math", studentName: "Charlie", score: 72),
        ]

        try await ctx.database.withTransaction { transaction in
            for score in scores {
                try await ctx.maintainer.updateIndex(
                    oldItem: nil as SubjectScore?,
                    newItem: score,
                    transaction: transaction
                )
            }
        }

        let count = try await ctx.countIndexEntries()
        // 2-layer: Layer 1 (3 items) + Layer 2 (1 group aggregate) = 4 entries
        #expect(count == 4, "Should have 4 index entries")

        try await ctx.cleanup()
    }

    // MARK: - Delete Tests

    @Test("Delete removes from sorted set")
    func testDeleteRemovesFromSortedSet() async throws {
        try await FoundationDBScenarioCoordinator.shared.initialize()
        let ctx = try await MaximumIndexContext()

        let score = SubjectScore(id: "s1", subject: "Math", studentName: "Alice", score: 95)

        // Insert
        try await ctx.database.withTransaction { transaction in
            try await ctx.maintainer.updateIndex(
                oldItem: nil as SubjectScore?,
                newItem: score,
                transaction: transaction
            )
        }

        let countBefore = try await ctx.countIndexEntries()
        // 2-layer: Layer 1 + Layer 2 = 2 entries
        #expect(countBefore == 2)

        // Delete
        try await ctx.database.withTransaction { transaction in
            try await ctx.maintainer.updateIndex(
                oldItem: score,
                newItem: nil as SubjectScore?,
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
        try await FoundationDBScenarioCoordinator.shared.initialize()
        let ctx = try await MaximumIndexContext()

        let score = SubjectScore(id: "s1", subject: "Math", studentName: "Alice", score: 85)

        // Insert
        try await ctx.database.withTransaction { transaction in
            try await ctx.maintainer.updateIndex(
                oldItem: nil as SubjectScore?,
                newItem: score,
                transaction: transaction
            )
        }

        // Update score
        let updatedScore = SubjectScore(id: "s1", subject: "Math", studentName: "Alice", score: 98)
        try await ctx.database.withTransaction { transaction in
            try await ctx.maintainer.updateIndex(
                oldItem: score,
                newItem: updatedScore,
                transaction: transaction
            )
        }

        let count = try await ctx.countIndexEntries()
        // 2-layer: Layer 1 + Layer 2 = 2 entries
        #expect(count == 2, "Should still have 2 entries after update")

        let max = try await ctx.getMax(for: "Math")
        #expect(max == 98, "Max should be updated to 98")

        try await ctx.cleanup()
    }

    // MARK: - Query Tests

    @Test("getMax returns maximum value")
    func testGetMaxReturnsMaximum() async throws {
        try await FoundationDBScenarioCoordinator.shared.initialize()
        let ctx = try await MaximumIndexContext()

        let scores = [
            SubjectScore(id: "s1", subject: "Math", studentName: "Alice", score: 95),
            SubjectScore(id: "s2", subject: "Math", studentName: "Bob", score: 88),
            SubjectScore(id: "s3", subject: "Math", studentName: "Charlie", score: 72),
        ]

        try await ctx.database.withTransaction { transaction in
            for score in scores {
                try await ctx.maintainer.updateIndex(
                    oldItem: nil as SubjectScore?,
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
        try await FoundationDBScenarioCoordinator.shared.initialize()
        let ctx = try await MaximumIndexContext()

        let scores = [
            SubjectScore(id: "s1", subject: "Math", studentName: "Alice", score: 95),
            SubjectScore(id: "s2", subject: "Math", studentName: "Bob", score: 88),
            SubjectScore(id: "s3", subject: "Science", studentName: "Alice", score: 92),
            SubjectScore(id: "s4", subject: "Science", studentName: "Charlie", score: 99),
        ]

        try await ctx.database.withTransaction { transaction in
            for score in scores {
                try await ctx.maintainer.updateIndex(
                    oldItem: nil as SubjectScore?,
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
        try await FoundationDBScenarioCoordinator.shared.initialize()
        let ctx = try await MaximumIndexContext()

        await #expect(throws: AggregationIndexError.self) {
            _ = try await ctx.getMax(for: "NonExistent")
        }

        try await ctx.cleanup()
    }

    // MARK: - Scan Tests

    @Test("ScanItem adds to sorted set")
    func testScanItemAddsToSortedSet() async throws {
        try await FoundationDBScenarioCoordinator.shared.initialize()
        let ctx = try await MaximumIndexContext()

        let scores = [
            SubjectScore(id: "s1", subject: "Math", studentName: "Alice", score: 95),
            SubjectScore(id: "s2", subject: "Math", studentName: "Bob", score: 88),
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
        // 2-layer: Layer 1 (2 items) + Layer 2 (1 group aggregate) = 3 entries
        #expect(count == 3, "Should have 3 entries after scanItem")

        let max = try await ctx.getMax(for: "Math")
        #expect(max == 95, "Max should be 95")

        try await ctx.cleanup()
    }

    // MARK: - Edge Cases

    @Test("Max updates correctly when maximum item is deleted")
    func testMaxUpdatesOnMaximumDelete() async throws {
        try await FoundationDBScenarioCoordinator.shared.initialize()
        let ctx = try await MaximumIndexContext()

        let scores = [
            SubjectScore(id: "s1", subject: "Math", studentName: "Low", score: 60),
            SubjectScore(id: "s2", subject: "Math", studentName: "High", score: 100),
        ]

        // Insert both
        try await ctx.database.withTransaction { transaction in
            for score in scores {
                try await ctx.maintainer.updateIndex(
                    oldItem: nil as SubjectScore?,
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
                newItem: nil as SubjectScore?,
                transaction: transaction
            )
        }

        let maxAfter = try await ctx.getMax(for: "Math")
        #expect(maxAfter == 60, "Max should now be 60 after deleting 100")

        try await ctx.cleanup()
    }
}
#endif
