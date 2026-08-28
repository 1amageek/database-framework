#if FOUNDATION_DB
// BM25ScoringTests.swift
// Tests for BM25 scoring functionality in FullTextIndex

import Testing
import Foundation
import StorageKit
import FDBStorage
import DatabaseKit
import DatabaseTypes
import TestSupport
@testable import DatabaseEngine
@testable import FullTextIndex

// MARK: - Test Model

@Persistable
struct BM25Article {
    var id: String
    var title: String
    var content: String
}

// MARK: - BM25 Scoring Context

private struct BM25ScoringContext {
    let database: any StorageEngine
    let subspace: Subspace
    let indexSubspace: Subspace
    let maintainer: FullTextIndexMaintainer<BM25Article>

    init(indexName: String = "BM25Article_content") async throws {
        self.database = try await FoundationDBScenarioCoordinator.shared.makeEngine()
        let testId = UUID().uuidString.prefix(8)
        self.subspace = Subspace(prefix: Tuple("test", "bm25", String(testId)).pack())
        self.indexSubspace = subspace.subspace("I").subspace(indexName)

        let index = try ResolvedIndex(
            for: BM25Article.self,
            name: indexName,
            definition: fullTextIndexDefinition(
                fieldName: "content",
                fieldNumber: 3,
                tokenizer: .simple,
                storePositions: false
            ),
            rootExpression: FieldKeyExpression(fieldName: "content"),
            itemTypes: Set(["BM25Article"])
        )

        self.maintainer = FullTextIndexMaintainer<BM25Article>(
            index: index,
            tokenizer: .simple,
            storePositions: false,
            ngramSize: 3,
            minTermLength: 2,
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

    func indexArticle(_ article: BM25Article) async throws {
        try await database.withTransaction { transaction in
            try await maintainer.updateIndex(
                oldItem: nil,
                newItem: article,
                transaction: transaction
            )
        }
    }

    func indexArticles(_ articles: [BM25Article]) async throws {
        try await database.withTransaction { transaction in
            for article in articles {
                try await maintainer.updateIndex(
                    oldItem: nil,
                    newItem: article,
                    transaction: transaction
                )
            }
        }
    }

    func readPersistedBM25Statistics() async throws -> BM25Statistics {
        try await database.withTransaction { transaction in
            let documentCount = try await transaction.getValue(
                for: FullTextStorageLayout.documentCountKey(in: indexSubspace),
                snapshot: true
            )
            let totalLength = try await transaction.getValue(
                for: FullTextStorageLayout.totalDocumentLengthKey(in: indexSubspace),
                snapshot: true
            )
            return BM25Statistics(
                totalDocuments: try documentCount.map(ByteConversion.bytesToInt64) ?? 0,
                totalLength: try totalLength.map(ByteConversion.bytesToInt64) ?? 0
            )
        }
    }
}

// MARK: - BM25 Scorer Unit Tests

@Suite("BM25 Scorer Unit Tests", .heartbeat)
struct BM25ScorerUnitTests {

    @Test("IDF calculation - standard formula")
    func testIDFCalculation() {
        let scorer = BM25Scorer(params: .default, totalDocuments: 100, averageDocumentLength: 50.0)

        // Term in 10 documents out of 100
        let idf10 = scorer.idf(documentFrequency: 10)
        #expect(idf10 > 0, "IDF should be positive for rare terms")

        // Term in 50 documents out of 100 (half the corpus)
        let idf50 = scorer.idf(documentFrequency: 50)
        #expect(idf50 < idf10, "IDF for common terms should be lower")

        // Term in 90 documents out of 100 (very common)
        let idf90 = scorer.idf(documentFrequency: 90)
        #expect(idf90 < 0, "IDF should be negative for terms in majority of docs (standard BM25)")
    }

    @Test("BM25 score calculation")
    func testBM25ScoreCalculation() {
        let scorer = BM25Scorer(params: .default, totalDocuments: 100, averageDocumentLength: 50.0)

        // Simple case: single term, appears once
        let score1 = scorer.score(
            termFrequencies: ["swift": 1],
            documentFrequencies: ["swift": 10],
            docLength: 50
        )
        #expect(score1 > 0, "Score should be positive for matching term")

        // Higher TF should increase score (but with saturation)
        let score2 = scorer.score(
            termFrequencies: ["swift": 5],
            documentFrequencies: ["swift": 10],
            docLength: 50
        )
        #expect(score2 > score1, "Higher TF should increase score")
        #expect(score2 < score1 * 5, "TF saturation should limit score increase")

        // Longer document should have lower score (length normalization)
        let scoreLong = scorer.score(
            termFrequencies: ["swift": 1],
            documentFrequencies: ["swift": 10],
            docLength: 100  // Twice the average
        )
        #expect(scoreLong < score1, "Longer documents should score lower")
    }

    @Test("BM25 parameters affect scoring")
    func testBM25ParametersAffectScoring() {
        let defaultParams = BM25Parameters.default
        let noLengthNorm = BM25Parameters.noLengthNorm

        let scorerDefault = BM25Scorer(params: defaultParams, totalDocuments: 100, averageDocumentLength: 50.0)
        let scorerNoNorm = BM25Scorer(params: noLengthNorm, totalDocuments: 100, averageDocumentLength: 50.0)

        // Long document with b=0.75 (default) vs b=0 (no normalization)
        let scoreDefault = scorerDefault.score(
            termFrequencies: ["swift": 1],
            documentFrequencies: ["swift": 10],
            docLength: 200  // 4x average
        )

        let scoreNoNorm = scorerNoNorm.score(
            termFrequencies: ["swift": 1],
            documentFrequencies: ["swift": 10],
            docLength: 200
        )

        #expect(scoreNoNorm > scoreDefault, "b=0 should not penalize long documents")
    }

    @Test("Multiple query terms")
    func testMultipleQueryTerms() {
        let scorer = BM25Scorer(params: .default, totalDocuments: 100, averageDocumentLength: 50.0)

        let singleTermScore = scorer.score(
            termFrequencies: ["swift": 1],
            documentFrequencies: ["swift": 10],
            docLength: 50
        )

        let twoTermScore = scorer.score(
            termFrequencies: ["swift": 1, "concurrency": 1],
            documentFrequencies: ["swift": 10, "concurrency": 5],
            docLength: 50
        )

        #expect(twoTermScore > singleTermScore, "More matching terms should increase score")
    }
}

// MARK: - BM25 Integration Tests

@Suite("BM25 Integration Tests", .tags(.fdb), .foundationDBScenario, .heartbeat)
struct BM25IntegrationTests {

    @Test("BM25 statistics are maintained")
    func testBM25StatisticsAreMaintained() async throws {
        try await FoundationDBScenarioCoordinator.shared.initialize()
        let ctx = try await BM25ScoringContext()

        // Index some articles
        let articles = [
            BM25Article(id: "a1", title: "Swift", content: "Swift programming language is modern"),
            BM25Article(id: "a2", title: "Python", content: "Python is also a programming language"),
            BM25Article(id: "a3", title: "Rust", content: "Rust programming is safe"),
        ]

        try await ctx.indexArticles(articles)

        // Check statistics
        let stats = try await ctx.readPersistedBM25Statistics()

        #expect(stats.totalDocuments == 3, "Should have 3 documents")
        #expect(stats.averageDocumentLength > 0, "Average doc length should be positive")

        try await ctx.cleanup()
    }

    @Test("BM25 statistics update on delete")
    func testBM25StatisticsUpdateOnDelete() async throws {
        try await FoundationDBScenarioCoordinator.shared.initialize()
        let ctx = try await BM25ScoringContext()

        let article = BM25Article(id: "a1", title: "Test", content: "Swift programming language")
        try await ctx.indexArticle(article)

        let statsBefore = try await ctx.readPersistedBM25Statistics()
        #expect(statsBefore.totalDocuments == 1)

        // Delete the article
        try await ctx.database.withTransaction { transaction in
            try await ctx.maintainer.updateIndex(
                oldItem: article,
                newItem: nil,
                transaction: transaction
            )
        }

        let statsAfter = try await ctx.readPersistedBM25Statistics()
        #expect(statsAfter.totalDocuments == 0, "Should have 0 documents after delete")

        try await ctx.cleanup()
    }

}
#endif
