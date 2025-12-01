// FullTextIndexBehaviorTests.swift
// Integration tests for FullTextIndex behavior with FDB

import Testing
import Foundation
import FoundationDB
import Core
import FullText
import TestSupport
@testable import DatabaseEngine
@testable import FullTextIndex

// MARK: - Test Model

struct TestArticle: Persistable {
    typealias ID = String

    var id: String
    var title: String
    var content: String

    init(id: String = UUID().uuidString, title: String, content: String) {
        self.id = id
        self.title = title
        self.content = content
    }

    static var persistableType: String { "TestArticle" }
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

    static func fieldName<Value>(for keyPath: KeyPath<TestArticle, Value>) -> String {
        switch keyPath {
        case \TestArticle.id: return "id"
        case \TestArticle.title: return "title"
        case \TestArticle.content: return "content"
        default: return "\(keyPath)"
        }
    }

    static func fieldName(for keyPath: PartialKeyPath<TestArticle>) -> String {
        switch keyPath {
        case \TestArticle.id: return "id"
        case \TestArticle.title: return "title"
        case \TestArticle.content: return "content"
        default: return "\(keyPath)"
        }
    }

    static func fieldName(for keyPath: AnyKeyPath) -> String {
        if let partial = keyPath as? PartialKeyPath<TestArticle> {
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
    let maintainer: FullTextIndexMaintainer<TestArticle>
    let kind: FullTextIndexKind

    init(tokenizer: TokenizationStrategy = .simple, storePositions: Bool = false, indexName: String = "TestArticle_content") throws {
        self.database = try FDBClient.openDatabase()
        let testId = UUID().uuidString.prefix(8)
        self.subspace = Subspace(prefix: Tuple("test", "fulltext", String(testId)).pack())
        self.indexSubspace = subspace.subspace("I").subspace(indexName)

        self.kind = FullTextIndexKind(tokenizer: tokenizer, storePositions: storePositions)

        // Expression: content
        let index = Index(
            name: indexName,
            kind: kind,
            rootExpression: FieldKeyExpression(fieldName: "content"),
            subspaceKey: indexName,
            itemTypes: Set(["TestArticle"])
        )

        self.maintainer = FullTextIndexMaintainer<TestArticle>(
            index: index,
            kind: kind,
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
            let termsSubspace = indexSubspace.subspace("terms")
            let (begin, end) = termsSubspace.range()
            var count = 0
            for try await _ in transaction.getRange(begin: begin, end: end, snapshot: true) {
                count += 1
            }
            return count
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

// MARK: - Behavior Tests

@Suite("FullTextIndex Behavior Tests", .tags(.fdb))
struct FullTextIndexBehaviorTests {

    // MARK: - Insert Tests

    @Test("Insert tokenizes and indexes")
    func testInsertTokenizesAndIndexes() async throws {
        try await FDBTestSetup.shared.initialize()
        let ctx = try TestContext()

        let article = TestArticle(id: "a1", title: "Test", content: "Hello world")

        try await ctx.database.withTransaction { transaction in
            try await ctx.maintainer.updateIndex(
                oldItem: nil,
                newItem: article,
                transaction: transaction
            )
        }

        let count = try await ctx.countIndexEntries()
        #expect(count == 2, "Should have 2 term entries (hello, world)")

        try await ctx.cleanup()
    }

    @Test("Multiple documents are indexed")
    func testMultipleDocuments() async throws {
        try await FDBTestSetup.shared.initialize()
        let ctx = try TestContext()

        let articles = [
            TestArticle(id: "a1", title: "Swift", content: "Swift programming language"),
            TestArticle(id: "a2", title: "Python", content: "Python programming language")
        ]

        try await ctx.database.withTransaction { transaction in
            for article in articles {
                try await ctx.maintainer.updateIndex(
                    oldItem: nil,
                    newItem: article,
                    transaction: transaction
                )
            }
        }

        // "programming" and "language" appear in both, so should have shared term entries
        let count = try await ctx.countIndexEntries()
        #expect(count >= 4, "Should have term entries for both documents")

        try await ctx.cleanup()
    }

    // MARK: - Delete Tests

    @Test("Delete removes all tokens")
    func testDeleteRemovesAllTokens() async throws {
        try await FDBTestSetup.shared.initialize()
        let ctx = try TestContext()

        let article = TestArticle(id: "a1", title: "Test", content: "Hello world")

        // Insert
        try await ctx.database.withTransaction { transaction in
            try await ctx.maintainer.updateIndex(
                oldItem: nil,
                newItem: article,
                transaction: transaction
            )
        }

        let countBefore = try await ctx.countIndexEntries()
        #expect(countBefore == 2)

        // Delete
        try await ctx.database.withTransaction { transaction in
            try await ctx.maintainer.updateIndex(
                oldItem: article,
                newItem: nil,
                transaction: transaction
            )
        }

        let countAfter = try await ctx.countIndexEntries()
        #expect(countAfter == 0, "Should have 0 entries after delete")

        try await ctx.cleanup()
    }

    // MARK: - Update Tests

    @Test("Update re-tokenizes")
    func testUpdateReTokenizes() async throws {
        try await FDBTestSetup.shared.initialize()
        let ctx = try TestContext()

        let article = TestArticle(id: "a1", title: "Test", content: "Hello world")

        // Insert
        try await ctx.database.withTransaction { transaction in
            try await ctx.maintainer.updateIndex(
                oldItem: nil,
                newItem: article,
                transaction: transaction
            )
        }

        // Update with different content
        let updatedArticle = TestArticle(id: "a1", title: "Test", content: "Goodbye universe")
        try await ctx.database.withTransaction { transaction in
            try await ctx.maintainer.updateIndex(
                oldItem: article,
                newItem: updatedArticle,
                transaction: transaction
            )
        }

        // Search for old terms
        let helloResults = try await ctx.searchTerm("hello")
        #expect(helloResults.isEmpty, "Should not find 'hello' after update")

        // Search for new terms
        let goodbyeResults = try await ctx.searchTerm("goodbye")
        #expect(goodbyeResults.count == 1, "Should find 'goodbye' after update")

        try await ctx.cleanup()
    }

    // MARK: - Search Tests

    @Test("Simple term search")
    func testSimpleTermSearch() async throws {
        try await FDBTestSetup.shared.initialize()
        let ctx = try TestContext()

        let articles = [
            TestArticle(id: "a1", title: "Swift", content: "Swift is a modern programming language"),
            TestArticle(id: "a2", title: "Python", content: "Python is also a programming language"),
            TestArticle(id: "a3", title: "Rust", content: "Rust is a systems language")
        ]

        try await ctx.database.withTransaction { transaction in
            for article in articles {
                try await ctx.maintainer.updateIndex(
                    oldItem: nil,
                    newItem: article,
                    transaction: transaction
                )
            }
        }

        // Search for "swift"
        let swiftResults = try await ctx.searchTerm("swift")
        #expect(swiftResults.count == 1, "Should find 1 document with 'swift'")

        // Search for "programming" (in 2 documents)
        let programmingResults = try await ctx.searchTerm("programming")
        #expect(programmingResults.count == 2, "Should find 2 documents with 'programming'")

        // Search for "language" (in all 3)
        let languageResults = try await ctx.searchTerm("language")
        #expect(languageResults.count == 3, "Should find 3 documents with 'language'")

        try await ctx.cleanup()
    }

    @Test("Boolean AND query")
    func testBooleanANDQuery() async throws {
        try await FDBTestSetup.shared.initialize()
        let ctx = try TestContext()

        let articles = [
            TestArticle(id: "a1", title: "Swift", content: "Swift is modern and fast"),
            TestArticle(id: "a2", title: "Python", content: "Python is modern but slow"),
            TestArticle(id: "a3", title: "Rust", content: "Rust is fast and safe")
        ]

        try await ctx.database.withTransaction { transaction in
            for article in articles {
                try await ctx.maintainer.updateIndex(
                    oldItem: nil,
                    newItem: article,
                    transaction: transaction
                )
            }
        }

        // Search for "modern" AND "fast" (only Swift)
        let results = try await ctx.searchTermsAND(["modern", "fast"])
        #expect(results.count == 1, "Should find 1 document with both 'modern' and 'fast'")

        try await ctx.cleanup()
    }

    @Test("Boolean OR query")
    func testBooleanORQuery() async throws {
        try await FDBTestSetup.shared.initialize()
        let ctx = try TestContext()

        let articles = [
            TestArticle(id: "a1", title: "Swift", content: "Swift is fast"),
            TestArticle(id: "a2", title: "Python", content: "Python is slow"),
            TestArticle(id: "a3", title: "Rust", content: "Rust is safe")
        ]

        try await ctx.database.withTransaction { transaction in
            for article in articles {
                try await ctx.maintainer.updateIndex(
                    oldItem: nil,
                    newItem: article,
                    transaction: transaction
                )
            }
        }

        // Search for "fast" OR "slow" (Swift and Python)
        let results = try await ctx.searchTermsOR(["fast", "slow"])
        #expect(results.count == 2, "Should find 2 documents with 'fast' or 'slow'")

        try await ctx.cleanup()
    }

    // MARK: - Tokenizer Tests

    @Test("Stemming tokenizer")
    func testStemmingTokenizer() async throws {
        try await FDBTestSetup.shared.initialize()
        let ctx = try TestContext(tokenizer: .stem)

        let article = TestArticle(id: "a1", title: "Test", content: "Running runners run")

        try await ctx.database.withTransaction { transaction in
            try await ctx.maintainer.updateIndex(
                oldItem: nil,
                newItem: article,
                transaction: transaction
            )
        }

        // All forms should match "run" after stemming
        let results = try await ctx.searchTerm("run")
        #expect(results.count >= 1, "Stemmed search should find the document")

        try await ctx.cleanup()
    }

    // MARK: - Scan Tests

    @Test("ScanItem tokenizes and indexes")
    func testScanItemTokenizesAndIndexes() async throws {
        try await FDBTestSetup.shared.initialize()
        let ctx = try TestContext()

        let articles = [
            TestArticle(id: "a1", title: "First", content: "First article content"),
            TestArticle(id: "a2", title: "Second", content: "Second article content")
        ]

        try await ctx.database.withTransaction { transaction in
            for article in articles {
                try await ctx.maintainer.scanItem(
                    article,
                    id: Tuple(article.id),
                    transaction: transaction
                )
            }
        }

        let results = try await ctx.searchTerm("article")
        #expect(results.count == 2, "Should find both articles with 'article'")

        try await ctx.cleanup()
    }

    // MARK: - Edge Cases

    @Test("Case insensitive search")
    func testCaseInsensitiveSearch() async throws {
        try await FDBTestSetup.shared.initialize()
        let ctx = try TestContext()

        let article = TestArticle(id: "a1", title: "Test", content: "Hello WORLD")

        try await ctx.database.withTransaction { transaction in
            try await ctx.maintainer.updateIndex(
                oldItem: nil,
                newItem: article,
                transaction: transaction
            )
        }

        // Search with different cases
        let lowerResults = try await ctx.searchTerm("world")
        let upperResults = try await ctx.searchTerm("WORLD")

        #expect(lowerResults.count == 1, "Should find with lowercase")
        #expect(upperResults.count == 1, "Should find with uppercase")

        try await ctx.cleanup()
    }
}
