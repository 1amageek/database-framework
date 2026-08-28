#if FOUNDATION_DB
// FullTextIndexPerformanceBenchmarks.swift
// Performance benchmarks for FullTextIndex

import Testing
import Foundation
import DatabaseKit
import DatabaseTypes
import StorageKit
import FDBStorage
@testable import DatabaseEngine
@testable import FullTextIndex

// MARK: - Test Model

@Persistable
struct BenchmarkArticle {
    var id: String
    var title: String
    var content: String
}

// MARK: - Full-Text Benchmark Context

private struct BenchmarkContext {
    let database: any StorageEngine
    let subspace: Subspace
    let indexSubspace: Subspace
    let maintainer: FullTextIndexMaintainer<BenchmarkArticle>

    init(tokenizer: TokenizationStrategy = .simple, storePositions: Bool = false, indexName: String = "BenchmarkArticle_content") async throws {
        self.database = try await FoundationDBBenchmarkEnvironment.shared.makeEngine()
        let testId = UUID().uuidString.prefix(8)
        self.subspace = Subspace(prefix: Tuple("benchmark", "fulltext", String(testId)).pack())
        self.indexSubspace = subspace.subspace("I").subspace(indexName)

        let index = try ResolvedIndex(
            for: BenchmarkArticle.self,
            name: indexName,
            definition: benchmarkFullTextIndexDefinition(
                fieldName: "content",
                fieldNumber: 3,
                tokenizer: tokenizer,
                storePositions: storePositions
            ),
            rootExpression: FieldKeyExpression(fieldName: "content"),
            itemTypes: Set(["BenchmarkArticle"])
        )

        self.maintainer = FullTextIndexMaintainer<BenchmarkArticle>(
            index: index,
            tokenizer: tokenizer,
            storePositions: storePositions,
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

}

// MARK: - Content Generation

private let sampleWords = [
    "swift", "programming", "language", "database", "performance",
    "index", "search", "query", "optimize", "algorithm",
    "data", "structure", "memory", "cache", "network",
    "server", "client", "api", "request", "response",
    "machine", "learning", "neural", "network", "training",
    "model", "inference", "tensor", "gradient", "loss",
]

/// Generate random content with specified word count
private func generateContent(wordCount: Int) -> String {
    (0..<wordCount).map { _ in
        sampleWords.randomElement()!
    }.joined(separator: " ")
}

// MARK: - Performance Tests

@Suite("FullTextIndex Performance Tests", .serialized, .heartbeat)
struct FullTextIndexPerformanceBenchmarks {

    // MARK: - Setup

    private func uniqueID(_ prefix: String) -> String {
        "\(prefix)-\(UUID().uuidString.prefix(8))"
    }

    // MARK: - Index Performance

    @Test("Bulk insert performance - 100 documents")
    func testBulkInsert100Documents() async throws {
        try await FoundationDBBenchmarkEnvironment.shared.initialize()
        let ctx = try await BenchmarkContext()

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

        let initialArticles = articles
        try await ctx.database.withTransaction { transaction in
            for article in initialArticles {
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
        try await FoundationDBBenchmarkEnvironment.shared.initialize()

        for wordsPerDoc in [10, 50, 100, 200] {
            let ctx = try await BenchmarkContext()
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

    // MARK: - Tokenizer Comparison

    @Test("Tokenizer comparison")
    func testTokenizerComparison() async throws {
        try await FoundationDBBenchmarkEnvironment.shared.initialize()

        let documentCount = 50
        let content = generateContent(wordCount: 50)

        for tokenizer in [TokenizationStrategy.simple, TokenizationStrategy.stem] {
            let ctx = try await BenchmarkContext(tokenizer: tokenizer)

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
        try await FoundationDBBenchmarkEnvironment.shared.initialize()
        let ctx = try await BenchmarkContext()

        // Setup: Insert initial documents
        let documentCount = 50
        var articles = (0..<documentCount).map { i in
            BenchmarkArticle(
                id: "\(uniqueID("art"))-\(i)",
                title: "Article \(i)",
                content: generateContent(wordCount: 50)
            )
        }

        let initialArticles = articles
        try await ctx.database.withTransaction { transaction in
            for article in initialArticles {
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
        try await FoundationDBBenchmarkEnvironment.shared.initialize()
        let ctx = try await BenchmarkContext()

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

}
#endif
