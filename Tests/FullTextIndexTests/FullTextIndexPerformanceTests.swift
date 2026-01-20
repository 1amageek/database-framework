// FullTextIndexPerformanceTests.swift
// Performance benchmarks for FullTextIndex

import Testing
import Foundation
import Core
import FoundationDB
import FullText
import TestSupport
@testable import DatabaseEngine
@testable import FullTextIndex

// MARK: - Test Model

struct BenchmarkArticle: Persistable {
    typealias ID = String

    var id: String
    var title: String
    var content: String

    init(id: String = UUID().uuidString, title: String, content: String) {
        self.id = id
        self.title = title
        self.content = content
    }

    static var persistableType: String { "BenchmarkArticle" }
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

    static func fieldName<Value>(for keyPath: KeyPath<BenchmarkArticle, Value>) -> String {
        switch keyPath {
        case \BenchmarkArticle.id: return "id"
        case \BenchmarkArticle.title: return "title"
        case \BenchmarkArticle.content: return "content"
        default: return "\(keyPath)"
        }
    }

    static func fieldName(for keyPath: PartialKeyPath<BenchmarkArticle>) -> String {
        switch keyPath {
        case \BenchmarkArticle.id: return "id"
        case \BenchmarkArticle.title: return "title"
        case \BenchmarkArticle.content: return "content"
        default: return "\(keyPath)"
        }
    }

    static func fieldName(for keyPath: AnyKeyPath) -> String {
        if let partial = keyPath as? PartialKeyPath<BenchmarkArticle> {
            return fieldName(for: partial)
        }
        return "\(keyPath)"
    }
}

// MARK: - Test Helper

private struct BenchmarkContext {
    nonisolated(unsafe) let database: any DatabaseProtocol
    let subspace: Subspace
    let indexSubspace: Subspace
    let maintainer: FullTextIndexMaintainer<BenchmarkArticle>
    let kind: FullTextIndexKind<BenchmarkArticle>

    init(tokenizer: TokenizationStrategy = .simple, storePositions: Bool = false, indexName: String = "BenchmarkArticle_content") throws {
        self.database = try FDBClient.openDatabase()
        let testId = UUID().uuidString.prefix(8)
        self.subspace = Subspace(prefix: Tuple("benchmark", "fulltext", String(testId)).pack())
        self.indexSubspace = subspace.subspace("I").subspace(indexName)

        self.kind = FullTextIndexKind<BenchmarkArticle>(
            fields: [\.content],
            tokenizer: tokenizer,
            storePositions: storePositions
        )

        let index = Index(
            name: indexName,
            kind: kind,
            rootExpression: FieldKeyExpression(fieldName: "content"),
            subspaceKey: indexName,
            itemTypes: Set(["BenchmarkArticle"])
        )

        self.maintainer = FullTextIndexMaintainer<BenchmarkArticle>(
            index: index,
            tokenizer: tokenizer,
            storePositions: storePositions,
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

    func searchTerm(_ term: String) async throws -> [[any TupleElement]] {
        try await database.withTransaction { transaction in
            try await maintainer.searchTerm(term, transaction: transaction)
        }
    }

    func searchTermsAND(_ terms: [String]) async throws -> [[any TupleElement]] {
        try await database.withTransaction { transaction in
            try await maintainer.searchTermsAND(terms, transaction: transaction)
        }
    }

    func searchTermsOR(_ terms: [String]) async throws -> [[any TupleElement]] {
        try await database.withTransaction { transaction in
            try await maintainer.searchTermsOR(terms, transaction: transaction)
        }
    }
}

// MARK: - Content Generation

private let sampleWords = [
    "swift", "programming", "language", "database", "performance",
    "index", "search", "query", "optimize", "algorithm",
    "data", "structure", "memory", "cache", "network",
    "server", "client", "api", "request", "response",
    "machine", "learning", "neural", "network", "training",
    "model", "inference", "tensor", "gradient", "loss"
]

/// Generate random content with specified word count
private func generateContent(wordCount: Int) -> String {
    (0..<wordCount).map { _ in
        sampleWords.randomElement()!
    }.joined(separator: " ")
}

/// Generate content containing specific terms
private func generateContentWithTerms(_ terms: [String], totalWords: Int) -> String {
    var words = terms
    while words.count < totalWords {
        words.append(sampleWords.randomElement()!)
    }
    words.shuffle()
    return words.joined(separator: " ")
}

// MARK: - Performance Tests

@Suite("FullTextIndex Performance Tests", .serialized)
struct FullTextIndexPerformanceTests {

    // MARK: - Setup

    private func uniqueID(_ prefix: String) -> String {
        "\(prefix)-\(UUID().uuidString.prefix(8))"
    }

    // MARK: - Index Performance

    @Test("Bulk insert performance - 100 documents")
    func testBulkInsert100Documents() async throws {
        try await FDBTestSetup.shared.initialize()
        let ctx = try BenchmarkContext()

        let documentCount = 100
        let wordsPerDoc = 50
        let articles = (0..<documentCount).map { i in
            BenchmarkArticle(
                id: "\(uniqueID("art"))-\(i)",
                title: "Article \(i)",
                content: generateContent(wordCount: wordsPerDoc)
            )
        }

        let startTime = DispatchTime.now()

        try await ctx.database.withTransaction { transaction in
            for article in articles {
                try await ctx.maintainer.updateIndex(
                    oldItem: nil,
                    newItem: article,
                    transaction: transaction
                )
            }
        }

        let endTime = DispatchTime.now()
        let totalNs = endTime.uptimeNanoseconds - startTime.uptimeNanoseconds
        let totalMs = Double(totalNs) / 1_000_000

        print("FullTextIndex Bulk Insert (100 docs, 50 words each):")
        print("  - Total time: \(String(format: "%.2f", totalMs))ms")
        print("  - Throughput: \(String(format: "%.0f", Double(documentCount) / (Double(totalNs) / 1_000_000_000)))/s")

        // Performance assertion
        #expect(totalMs < 10000, "Bulk insert of \(documentCount) documents should complete in under 10s")

        try await ctx.cleanup()
    }

    @Test("Bulk insert performance - varying document size")
    func testBulkInsertVaryingSize() async throws {
        try await FDBTestSetup.shared.initialize()

        for wordsPerDoc in [10, 50, 100, 200] {
            let ctx = try BenchmarkContext()
            let documentCount = 50

            let articles = (0..<documentCount).map { i in
                BenchmarkArticle(
                    id: "\(uniqueID("art"))-\(i)",
                    title: "Article \(i)",
                    content: generateContent(wordCount: wordsPerDoc)
                )
            }

            let startTime = DispatchTime.now()

            try await ctx.database.withTransaction { transaction in
                for article in articles {
                    try await ctx.maintainer.updateIndex(
                        oldItem: nil,
                        newItem: article,
                        transaction: transaction
                    )
                }
            }

            let endTime = DispatchTime.now()
            let totalNs = endTime.uptimeNanoseconds - startTime.uptimeNanoseconds
            let avgMs = Double(totalNs) / Double(documentCount) / 1_000_000

            print("FullTextIndex Insert (\(wordsPerDoc) words/doc): \(String(format: "%.2f", avgMs))ms/doc")

            try await ctx.cleanup()
        }
    }

    // MARK: - Single Term Search Performance

    @Test("Single term search performance")
    func testSingleTermSearchPerformance() async throws {
        try await FDBTestSetup.shared.initialize()
        let ctx = try BenchmarkContext()

        // Setup: Insert documents
        let documentCount = 200
        let articles = (0..<documentCount).map { i in
            BenchmarkArticle(
                id: "\(uniqueID("art"))-\(i)",
                title: "Article \(i)",
                content: generateContent(wordCount: 50)
            )
        }

        try await ctx.database.withTransaction { transaction in
            for article in articles {
                try await ctx.maintainer.updateIndex(
                    oldItem: nil,
                    newItem: article,
                    transaction: transaction
                )
            }
        }

        // Benchmark: Single term searches
        let searchTerms = ["swift", "database", "performance", "algorithm", "network"]
        let searchCount = searchTerms.count * 5
        let startTime = DispatchTime.now()

        for _ in 0..<5 {
            for term in searchTerms {
                let results = try await ctx.searchTerm(term)
                _ = results.count  // Process results
            }
        }

        let endTime = DispatchTime.now()
        let totalNs = endTime.uptimeNanoseconds - startTime.uptimeNanoseconds
        let avgMs = Double(totalNs) / Double(searchCount) / 1_000_000

        print("FullTextIndex Single Term Search:")
        print("  - Total searches: \(searchCount)")
        print("  - Average latency: \(String(format: "%.2f", avgMs))ms")
        print("  - Throughput: \(String(format: "%.0f", Double(searchCount) / (Double(totalNs) / 1_000_000_000)))/s")

        // Performance assertion
        #expect(avgMs < 50, "Single term search should be under 50ms average")

        try await ctx.cleanup()
    }

    // MARK: - Boolean AND Query Performance

    @Test("Boolean AND query performance")
    func testBooleanANDPerformance() async throws {
        try await FDBTestSetup.shared.initialize()
        let ctx = try BenchmarkContext()

        // Setup: Insert documents with known overlapping terms
        let documentCount = 200
        let articles = (0..<documentCount).map { i in
            BenchmarkArticle(
                id: "\(uniqueID("art"))-\(i)",
                title: "Article \(i)",
                content: generateContent(wordCount: 50)
            )
        }

        try await ctx.database.withTransaction { transaction in
            for article in articles {
                try await ctx.maintainer.updateIndex(
                    oldItem: nil,
                    newItem: article,
                    transaction: transaction
                )
            }
        }

        // Benchmark: AND queries
        let queries = [
            ["swift", "programming"],
            ["database", "performance"],
            ["machine", "learning", "model"],
            ["cache", "memory", "optimize"]
        ]

        let searchCount = queries.count * 5
        let startTime = DispatchTime.now()

        for _ in 0..<5 {
            for query in queries {
                let results = try await ctx.searchTermsAND(query)
                _ = results.count
            }
        }

        let endTime = DispatchTime.now()
        let totalNs = endTime.uptimeNanoseconds - startTime.uptimeNanoseconds
        let avgMs = Double(totalNs) / Double(searchCount) / 1_000_000

        print("FullTextIndex Boolean AND Query:")
        print("  - Total searches: \(searchCount)")
        print("  - Average latency: \(String(format: "%.2f", avgMs))ms")

        // Performance assertion
        #expect(avgMs < 100, "AND query should be under 100ms average")

        try await ctx.cleanup()
    }

    // MARK: - Boolean OR Query Performance

    @Test("Boolean OR query performance")
    func testBooleanORPerformance() async throws {
        try await FDBTestSetup.shared.initialize()
        let ctx = try BenchmarkContext()

        // Setup: Insert documents
        let documentCount = 200
        let articles = (0..<documentCount).map { i in
            BenchmarkArticle(
                id: "\(uniqueID("art"))-\(i)",
                title: "Article \(i)",
                content: generateContent(wordCount: 50)
            )
        }

        try await ctx.database.withTransaction { transaction in
            for article in articles {
                try await ctx.maintainer.updateIndex(
                    oldItem: nil,
                    newItem: article,
                    transaction: transaction
                )
            }
        }

        // Benchmark: OR queries
        let queries = [
            ["swift", "python"],
            ["database", "cache", "memory"],
            ["machine", "neural", "tensor"]
        ]

        let searchCount = queries.count * 5
        let startTime = DispatchTime.now()

        for _ in 0..<5 {
            for query in queries {
                let results = try await ctx.searchTermsOR(query)
                _ = results.count
            }
        }

        let endTime = DispatchTime.now()
        let totalNs = endTime.uptimeNanoseconds - startTime.uptimeNanoseconds
        let avgMs = Double(totalNs) / Double(searchCount) / 1_000_000

        print("FullTextIndex Boolean OR Query:")
        print("  - Total searches: \(searchCount)")
        print("  - Average latency: \(String(format: "%.2f", avgMs))ms")

        // Performance assertion
        #expect(avgMs < 150, "OR query should be under 150ms average")

        try await ctx.cleanup()
    }

    // MARK: - Phrase Search Performance

    @Test("Phrase search performance")
    func testPhraseSearchPerformance() async throws {
        try await FDBTestSetup.shared.initialize()
        let ctx = try BenchmarkContext(storePositions: true)

        // Setup: Insert documents with known phrases
        let documentCount = 100
        var articles: [BenchmarkArticle] = []

        for i in 0..<documentCount {
            let content: String
            if i % 5 == 0 {
                // Some documents contain the target phrase
                content = "This article discusses machine learning and neural network training."
            } else {
                content = generateContent(wordCount: 50)
            }

            articles.append(BenchmarkArticle(
                id: "\(uniqueID("art"))-\(i)",
                title: "Article \(i)",
                content: content
            ))
        }

        try await ctx.database.withTransaction { transaction in
            for article in articles {
                try await ctx.maintainer.updateIndex(
                    oldItem: nil,
                    newItem: article,
                    transaction: transaction
                )
            }
        }

        // Benchmark: Phrase searches
        let searchCount = 10
        let startTime = DispatchTime.now()

        for _ in 0..<searchCount {
            let results = try await ctx.database.withTransaction { transaction in
                try await ctx.maintainer.searchPhrase("machine learning", transaction: transaction)
            }
            _ = results.count
        }

        let endTime = DispatchTime.now()
        let totalNs = endTime.uptimeNanoseconds - startTime.uptimeNanoseconds
        let avgMs = Double(totalNs) / Double(searchCount) / 1_000_000

        print("FullTextIndex Phrase Search:")
        print("  - Total searches: \(searchCount)")
        print("  - Average latency: \(String(format: "%.2f", avgMs))ms")

        // Performance assertion
        #expect(avgMs < 200, "Phrase search should be under 200ms average")

        try await ctx.cleanup()
    }

    // MARK: - BM25 Scoring Performance

    @Test("BM25 scoring performance")
    func testBM25ScoringPerformance() async throws {
        try await FDBTestSetup.shared.initialize()
        let ctx = try BenchmarkContext()

        // Setup: Insert documents
        let documentCount = 100
        let articles = (0..<documentCount).map { i in
            BenchmarkArticle(
                id: "\(uniqueID("art"))-\(i)",
                title: "Article \(i)",
                content: generateContent(wordCount: 50)
            )
        }

        try await ctx.database.withTransaction { transaction in
            for article in articles {
                try await ctx.maintainer.updateIndex(
                    oldItem: nil,
                    newItem: article,
                    transaction: transaction
                )
            }
        }

        // Benchmark: Scored searches
        let searchCount = 10
        let startTime = DispatchTime.now()

        for _ in 0..<searchCount {
            let results = try await ctx.database.withTransaction { transaction in
                try await ctx.maintainer.searchWithScores(
                    terms: ["machine", "learning", "model"],
                    matchMode: .all,
                    transaction: transaction
                )
            }
            _ = results.count
        }

        let endTime = DispatchTime.now()
        let totalNs = endTime.uptimeNanoseconds - startTime.uptimeNanoseconds
        let avgMs = Double(totalNs) / Double(searchCount) / 1_000_000

        print("FullTextIndex BM25 Scoring:")
        print("  - Total searches: \(searchCount)")
        print("  - Average latency: \(String(format: "%.2f", avgMs))ms")

        // Performance assertion
        #expect(avgMs < 500, "BM25 scored search should be under 500ms average")

        try await ctx.cleanup()
    }

    // MARK: - Tokenizer Comparison

    @Test("Tokenizer comparison")
    func testTokenizerComparison() async throws {
        try await FDBTestSetup.shared.initialize()

        let documentCount = 50
        let content = generateContent(wordCount: 50)

        for tokenizer in [TokenizationStrategy.simple, TokenizationStrategy.stem] {
            let ctx = try BenchmarkContext(tokenizer: tokenizer)

            let articles = (0..<documentCount).map { i in
                BenchmarkArticle(
                    id: "\(uniqueID("art"))-\(i)",
                    title: "Article \(i)",
                    content: content
                )
            }

            let startTime = DispatchTime.now()

            try await ctx.database.withTransaction { transaction in
                for article in articles {
                    try await ctx.maintainer.updateIndex(
                        oldItem: nil,
                        newItem: article,
                        transaction: transaction
                    )
                }
            }

            let endTime = DispatchTime.now()
            let totalNs = endTime.uptimeNanoseconds - startTime.uptimeNanoseconds
            let totalMs = Double(totalNs) / 1_000_000

            print("FullTextIndex Tokenizer (\(tokenizer)): \(String(format: "%.2f", totalMs))ms for \(documentCount) docs")

            try await ctx.cleanup()
        }
    }

    // MARK: - Update Performance

    @Test("Update performance")
    func testUpdatePerformance() async throws {
        try await FDBTestSetup.shared.initialize()
        let ctx = try BenchmarkContext()

        // Setup: Insert initial documents
        let documentCount = 50
        var articles = (0..<documentCount).map { i in
            BenchmarkArticle(
                id: "\(uniqueID("art"))-\(i)",
                title: "Article \(i)",
                content: generateContent(wordCount: 50)
            )
        }

        try await ctx.database.withTransaction { transaction in
            for article in articles {
                try await ctx.maintainer.updateIndex(
                    oldItem: nil,
                    newItem: article,
                    transaction: transaction
                )
            }
        }

        // Benchmark: Updates
        let updateCount = 30
        let startTime = DispatchTime.now()

        for i in 0..<updateCount {
            let oldArticle = articles[i]
            let newArticle = BenchmarkArticle(
                id: oldArticle.id,
                title: "Updated \(i)",
                content: generateContent(wordCount: 50)
            )

            try await ctx.database.withTransaction { transaction in
                try await ctx.maintainer.updateIndex(
                    oldItem: oldArticle,
                    newItem: newArticle,
                    transaction: transaction
                )
            }

            articles[i] = newArticle
        }

        let endTime = DispatchTime.now()
        let totalNs = endTime.uptimeNanoseconds - startTime.uptimeNanoseconds
        let avgMs = Double(totalNs) / Double(updateCount) / 1_000_000

        print("FullTextIndex Update Performance:")
        print("  - Total updates: \(updateCount)")
        print("  - Average latency: \(String(format: "%.2f", avgMs))ms")

        // Performance assertion
        #expect(avgMs < 200, "Update should be under 200ms average")

        try await ctx.cleanup()
    }

    // MARK: - Delete Performance

    @Test("Delete performance")
    func testDeletePerformance() async throws {
        try await FDBTestSetup.shared.initialize()
        let ctx = try BenchmarkContext()

        // Setup: Insert documents
        let documentCount = 50
        let articles = (0..<documentCount).map { i in
            BenchmarkArticle(
                id: "\(uniqueID("art"))-\(i)",
                title: "Article \(i)",
                content: generateContent(wordCount: 50)
            )
        }

        try await ctx.database.withTransaction { transaction in
            for article in articles {
                try await ctx.maintainer.updateIndex(
                    oldItem: nil,
                    newItem: article,
                    transaction: transaction
                )
            }
        }

        // Benchmark: Deletes
        let deleteCount = 30
        let startTime = DispatchTime.now()

        for i in 0..<deleteCount {
            try await ctx.database.withTransaction { transaction in
                try await ctx.maintainer.updateIndex(
                    oldItem: articles[i],
                    newItem: nil,
                    transaction: transaction
                )
            }
        }

        let endTime = DispatchTime.now()
        let totalNs = endTime.uptimeNanoseconds - startTime.uptimeNanoseconds
        let avgMs = Double(totalNs) / Double(deleteCount) / 1_000_000

        print("FullTextIndex Delete Performance:")
        print("  - Total deletes: \(deleteCount)")
        print("  - Average latency: \(String(format: "%.2f", avgMs))ms")

        // Performance assertion
        #expect(avgMs < 100, "Delete should be under 100ms average")

        try await ctx.cleanup()
    }

    // MARK: - Scalability Test

    @Test("Search scalability - increasing document count")
    func testSearchScalability() async throws {
        try await FDBTestSetup.shared.initialize()

        for documentCount in [50, 100, 200] {
            let ctx = try BenchmarkContext()

            let articles = (0..<documentCount).map { i in
                BenchmarkArticle(
                    id: "\(uniqueID("art"))-\(i)",
                    title: "Article \(i)",
                    content: generateContent(wordCount: 50)
                )
            }

            try await ctx.database.withTransaction { transaction in
                for article in articles {
                    try await ctx.maintainer.updateIndex(
                        oldItem: nil,
                        newItem: article,
                        transaction: transaction
                    )
                }
            }

            let searchCount = 10
            let startTime = DispatchTime.now()

            for _ in 0..<searchCount {
                let results = try await ctx.searchTerm("database")
                _ = results.count
            }

            let endTime = DispatchTime.now()
            let totalNs = endTime.uptimeNanoseconds - startTime.uptimeNanoseconds
            let avgMs = Double(totalNs) / Double(searchCount) / 1_000_000

            print("FullTextIndex Search (\(documentCount) docs): \(String(format: "%.2f", avgMs))ms avg")

            try await ctx.cleanup()
        }
    }
}
