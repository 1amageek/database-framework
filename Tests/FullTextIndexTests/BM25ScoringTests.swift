// BM25ScoringTests.swift
// Tests for BM25 scoring functionality in FullTextIndex

import Testing
import Foundation
import FoundationDB
import Core
import FullText
import TestSupport
@testable import DatabaseEngine
@testable import FullTextIndex

// MARK: - Test Model

struct BM25TestArticle: Persistable {
    typealias ID = String

    var id: String
    var title: String
    var content: String

    init(id: String = UUID().uuidString, title: String, content: String) {
        self.id = id
        self.title = title
        self.content = content
    }

    static var persistableType: String { "BM25TestArticle" }
    static var allFields: [String] { ["id", "title", "content"] }
    static var indexDescriptors: [IndexDescriptor] { [] }

    static func fieldNumber(for fieldName: String) -> Int? { nil }
    static func enumMetadata(for fieldName: String) -> EnumMetadata? { nil }

    subscript(dynamicMember member: String) -> (any Sendable)? {
        switch member {
        case "id": return id
        case "title": return title
        case "content": return content
        default: return nil
        }
    }

    static func fieldName<Value>(for keyPath: KeyPath<BM25TestArticle, Value>) -> String {
        switch keyPath {
        case \BM25TestArticle.id: return "id"
        case \BM25TestArticle.title: return "title"
        case \BM25TestArticle.content: return "content"
        default: return "\(keyPath)"
        }
    }

    static func fieldName(for keyPath: PartialKeyPath<BM25TestArticle>) -> String {
        switch keyPath {
        case \BM25TestArticle.id: return "id"
        case \BM25TestArticle.title: return "title"
        case \BM25TestArticle.content: return "content"
        default: return "\(keyPath)"
        }
    }

    static func fieldName(for keyPath: AnyKeyPath) -> String {
        if let partial = keyPath as? PartialKeyPath<BM25TestArticle> {
            return fieldName(for: partial)
        }
        return "\(keyPath)"
    }
}

// MARK: - Test Helper

private struct BM25TestContext {
    nonisolated(unsafe) let database: any DatabaseProtocol
    let subspace: Subspace
    let indexSubspace: Subspace
    let maintainer: FullTextIndexMaintainer<BM25TestArticle>
    let kind: FullTextIndexKind<BM25TestArticle>

    init(indexName: String = "BM25TestArticle_content") throws {
        self.database = try FDBClient.openDatabase()
        let testId = UUID().uuidString.prefix(8)
        self.subspace = Subspace(prefix: Tuple("test", "bm25", String(testId)).pack())
        self.indexSubspace = subspace.subspace("I").subspace(indexName)

        self.kind = FullTextIndexKind<BM25TestArticle>(
            fields: [\.content],
            tokenizer: .simple,
            storePositions: false
        )

        let index = Index(
            name: indexName,
            kind: kind,
            rootExpression: FieldKeyExpression(fieldName: "content"),
            subspaceKey: indexName,
            itemTypes: Set(["BM25TestArticle"])
        )

        self.maintainer = FullTextIndexMaintainer<BM25TestArticle>(
            index: index,
            tokenizer: .simple,
            storePositions: false,
            ngramSize: kind.ngramSize,
            minTermLength: kind.minTermLength,
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

    func indexArticle(_ article: BM25TestArticle) async throws {
        try await database.withTransaction { transaction in
            try await maintainer.updateIndex(
                oldItem: nil,
                newItem: article,
                transaction: transaction
            )
        }
    }

    func indexArticles(_ articles: [BM25TestArticle]) async throws {
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

    func getBM25Statistics() async throws -> BM25Statistics {
        try await database.withTransaction { transaction in
            try await maintainer.getBM25Statistics(transaction: transaction)
        }
    }

    func searchWithScores(terms: [String], params: BM25Parameters = .default) async throws -> [(id: Tuple, score: Double)] {
        try await database.withTransaction { transaction in
            try await maintainer.searchWithScores(
                terms: terms,
                bm25Params: params,
                transaction: transaction
            )
        }
    }
}

// MARK: - BM25 Scorer Unit Tests

@Suite("BM25 Scorer Unit Tests")
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

@Suite("BM25 Integration Tests", .tags(.fdb), .serialized)
struct BM25IntegrationTests {

    @Test("BM25 statistics are maintained")
    func testBM25StatisticsAreMaintained() async throws {
        try await FDBTestSetup.shared.initialize()
        let ctx = try BM25TestContext()

        // Index some articles
        let articles = [
            BM25TestArticle(id: "a1", title: "Swift", content: "Swift programming language is modern"),
            BM25TestArticle(id: "a2", title: "Python", content: "Python is also a programming language"),
            BM25TestArticle(id: "a3", title: "Rust", content: "Rust programming is safe")
        ]

        try await ctx.indexArticles(articles)

        // Check statistics
        let stats = try await ctx.getBM25Statistics()

        #expect(stats.totalDocuments == 3, "Should have 3 documents")
        #expect(stats.averageDocumentLength > 0, "Average doc length should be positive")

        try await ctx.cleanup()
    }

    @Test("BM25 statistics update on delete")
    func testBM25StatisticsUpdateOnDelete() async throws {
        try await FDBTestSetup.shared.initialize()
        let ctx = try BM25TestContext()

        let article = BM25TestArticle(id: "a1", title: "Test", content: "Swift programming language")
        try await ctx.indexArticle(article)

        let statsBefore = try await ctx.getBM25Statistics()
        #expect(statsBefore.totalDocuments == 1)

        // Delete the article
        try await ctx.database.withTransaction { transaction in
            try await ctx.maintainer.updateIndex(
                oldItem: article,
                newItem: nil,
                transaction: transaction
            )
        }

        let statsAfter = try await ctx.getBM25Statistics()
        #expect(statsAfter.totalDocuments == 0, "Should have 0 documents after delete")

        try await ctx.cleanup()
    }

    @Test("BM25 scored search returns ranked results")
    func testBM25ScoredSearchReturnsRankedResults() async throws {
        try await FDBTestSetup.shared.initialize()
        let ctx = try BM25TestContext()

        // Create articles with varying relevance to "swift programming"
        // Note: BM25 IDF is positive only when df < N/2
        // So we need enough non-matching documents to make query terms "rare"
        let articles = [
            // Most relevant: has both terms multiple times
            BM25TestArticle(id: "a1", title: "Swift Guide", content: "Swift programming Swift programming Swift"),
            // Somewhat relevant: has both terms once
            BM25TestArticle(id: "a2", title: "Languages", content: "Swift programming and other languages"),
            // Less relevant: only has one term
            BM25TestArticle(id: "a3", title: "Python", content: "Python development is fun"),
            // Non-matching documents to make query terms rarer (positive IDF)
            BM25TestArticle(id: "a4", title: "Other", content: "Something completely different"),
            BM25TestArticle(id: "a5", title: "Database", content: "Database systems and storage engines"),
            BM25TestArticle(id: "a6", title: "Networks", content: "Network protocols and communication"),
            BM25TestArticle(id: "a7", title: "Security", content: "Encryption and authentication methods"),
            BM25TestArticle(id: "a8", title: "Cloud", content: "Cloud computing and infrastructure"),
        ]

        try await ctx.indexArticles(articles)

        // Search for "swift programming"
        let results = try await ctx.searchWithScores(terms: ["swift", "programming"])

        // Should only match a1, a2 (both terms)
        #expect(results.count == 2, "Should match documents with both terms")

        // Results should be sorted by score (descending)
        if results.count >= 2 {
            for i in 0..<(results.count - 1) {
                #expect(results[i].score >= results[i + 1].score, "Results should be sorted by score descending")
            }
        }

        // a1 should be ranked higher than a2 (more term occurrences)
        if results.count >= 2 {
            let firstId = results[0].id[0] as? String
            #expect(firstId == "a1", "Document with most term occurrences should rank first")
        }

        try await ctx.cleanup()
    }

    @Test("BM25 length normalization affects ranking")
    func testBM25LengthNormalizationAffectsRanking() async throws {
        try await FDBTestSetup.shared.initialize()
        let ctx = try BM25TestContext()

        // Two documents with same TF but different lengths
        // Plus non-matching documents to ensure positive IDF for "swift"
        // (BM25 IDF is negative when term appears in majority of docs)
        let articles = [
            // Short document with "swift"
            BM25TestArticle(id: "short", title: "Short", content: "Swift is fast"),
            // Long document with "swift" (same TF=1, but much longer)
            BM25TestArticle(
                id: "long",
                title: "Long",
                content: "Swift is a wonderful language that was created by Apple and is used for iOS development and macOS development and many other things in the software industry"
            ),
            // Non-matching documents to make "swift" rare (positive IDF)
            BM25TestArticle(id: "d1", title: "Python", content: "Python is great for data science"),
            BM25TestArticle(id: "d2", title: "Java", content: "Java runs on billions of devices"),
            BM25TestArticle(id: "d3", title: "Rust", content: "Rust provides memory safety guarantees"),
            BM25TestArticle(id: "d4", title: "Go", content: "Go excels at concurrent programming"),
        ]

        try await ctx.indexArticles(articles)

        let results = try await ctx.searchWithScores(terms: ["swift"])

        #expect(results.count == 2, "Both swift documents should match")

        // Short document should rank higher due to length normalization
        if results.count == 2 {
            let firstId = results[0].id[0] as? String
            #expect(firstId == "short", "Shorter document should rank higher with same TF")
        }

        try await ctx.cleanup()
    }

    @Test("BM25 custom parameters work")
    func testBM25CustomParametersWork() async throws {
        try await FDBTestSetup.shared.initialize()
        let ctx = try BM25TestContext()

        let articles = [
            BM25TestArticle(id: "short", title: "Short", content: "Swift is great"),
            BM25TestArticle(
                id: "long",
                title: "Long",
                content: "Swift is a wonderful language with many features and capabilities for modern development"
            )
        ]

        try await ctx.indexArticles(articles)

        // With no length normalization (b=0), long document shouldn't be penalized
        let resultsNoNorm = try await ctx.searchWithScores(
            terms: ["swift"],
            params: BM25Parameters.noLengthNorm
        )

        // With strong normalization (b=1), short document should win more decisively
        let resultsStrongNorm = try await ctx.searchWithScores(
            terms: ["swift"],
            params: BM25Parameters.strongLengthNorm
        )

        #expect(resultsNoNorm.count == 2)
        #expect(resultsStrongNorm.count == 2)

        // The score difference should be larger with strong normalization
        if resultsStrongNorm.count == 2 && resultsNoNorm.count == 2 {
            let diffStrong = resultsStrongNorm[0].score - resultsStrongNorm[1].score
            let diffNoNorm = resultsNoNorm[0].score - resultsNoNorm[1].score

            // With b=0, scores should be more similar
            // With b=1, short doc should have much higher relative score
            #expect(diffStrong.magnitude >= diffNoNorm.magnitude,
                    "Strong normalization should create larger score difference")
        }

        try await ctx.cleanup()
    }

    @Test("BM25 rare terms score higher than common terms")
    func testBM25RareTermsScoreHigher() async throws {
        try await FDBTestSetup.shared.initialize()
        let ctx = try BM25TestContext()

        // Create corpus where "programming" is common but "swift" is rare
        let articles = [
            BM25TestArticle(id: "a1", title: "Swift", content: "Swift programming"),
            BM25TestArticle(id: "a2", title: "Python", content: "Python programming"),
            BM25TestArticle(id: "a3", title: "Java", content: "Java programming"),
            BM25TestArticle(id: "a4", title: "Rust", content: "Rust programming"),
            BM25TestArticle(id: "a5", title: "Go", content: "Go programming")
        ]

        try await ctx.indexArticles(articles)

        // Search for "swift" (rare - in 1/5 docs)
        let swiftResults = try await ctx.searchWithScores(terms: ["swift"])

        // Search for "programming" (common - in 5/5 docs)
        let programmingResults = try await ctx.searchWithScores(terms: ["programming"])

        // Swift search should give higher score to matching doc
        // because it's a rarer term (higher IDF)
        #expect(swiftResults.count == 1)
        #expect(programmingResults.count == 5)

        if !swiftResults.isEmpty && !programmingResults.isEmpty {
            // The rare term "swift" should have higher score than common term "programming"
            #expect(swiftResults[0].score > programmingResults[0].score,
                    "Rare term should produce higher score")
        }

        try await ctx.cleanup()
    }
}
