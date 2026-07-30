// FullTextIndexBehaviorTests.swift
// Backend-neutral tests for FullTextIndex persistence behavior

import Testing
import Foundation
import StorageKit
import DatabaseKit
import DatabaseTypes
import TestSupport
@testable import DatabaseEngine
@testable import FullTextIndex

// MARK: - Test Model

@Persistable
struct SearchableArticle {
    var id: String
    var title: String
    var content: String
}

// MARK: - Full-Text Index Context

private struct FullTextIndexContext {
    let database: any StorageEngine
    let subspace: Subspace
    let indexSubspace: Subspace
    let maintainer: FullTextIndexMaintainer<SearchableArticle>

    init(tokenizer: TokenizationStrategy = .simple, storePositions: Bool = false, indexName: String = "SearchableArticle_content") async throws {
        self.database = InMemoryEngine()
        let testId = UUID().uuidString.prefix(8)
        self.subspace = Subspace(prefix: Tuple("test", "fulltext", String(testId)).pack())
        self.indexSubspace = subspace.subspace("I").subspace(indexName)

        // Expression: content
        let index = Index(
            name: indexName,
            kind: fullTextIndexMetadata(
                fieldName: "content",
                fieldNumber: 3,
                tokenizer: tokenizer,
                storePositions: storePositions
            ),
            rootExpression: FieldKeyExpression(fieldName: "content"),
            subspaceKey: indexName,
            itemTypes: Set(["SearchableArticle"])
        )

        self.maintainer = FullTextIndexMaintainer<SearchableArticle>(
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

    func countIndexEntries() async throws -> Int {
        try await database.withTransaction { transaction -> Int in
            let termsSubspace = indexSubspace.subspace("terms")
            let (begin, end) = termsSubspace.range()
            return try await transaction.collectRange(
                begin: begin,
                end: end,
                snapshot: true
            ).count
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

    func posting(term: String, id: String) async throws -> ByteString? {
        try await database.withTransaction { transaction in
            let key = indexSubspace
                .subspace("terms")
                .subspace(term)
                .pack(Tuple(id))
            return try await transaction.getValue(for: key, snapshot: true)
        }
    }
}

func fullTextIndexMetadata(
    fieldName: String,
    fieldNumber: Int,
    tokenizer: TokenizationStrategy,
    storePositions: Bool,
    ngramSize: Int = 3,
    minTermLength: Int = 2
) -> IndexKindMetadata {
    IndexKindMetadata(
        identifier: "fulltext",
        subspaceStructure: .hierarchical,
        fields: [
            IndexFieldMetadata(
                identity: FieldIdentity(name: fieldName, number: fieldNumber)
            )
        ],
        metadata: [
            "tokenizer": .string(tokenizer.rawValue),
            "storePositions": .bool(storePositions),
            "ngramSize": .int64(Int64(ngramSize)),
            "minTermLength": .int64(Int64(minTermLength)),
        ]
    )
}

// MARK: - Behavior Tests

@Suite("FullTextIndex Behavior Tests", .serialized, .heartbeat)
struct FullTextIndexBehaviorTests {

    // MARK: - Insert Tests

    @Test("Insert tokenizes and indexes")
    func testInsertTokenizesAndIndexes() async throws {
        let ctx = try await FullTextIndexContext()

        let article = SearchableArticle(id: "a1", title: "Test", content: "Hello world")

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

    @Test("Writer stores canonical frequency-first posting payload")
    func writerStoresCanonicalPostingPayload() async throws {
        let ctx = try await FullTextIndexContext(storePositions: true)
        let article = SearchableArticle(
            id: "a1",
            title: "Test",
            content: "swift database swift"
        )

        try await ctx.database.withTransaction { transaction in
            try await ctx.maintainer.updateIndex(
                oldItem: nil,
                newItem: article,
                transaction: transaction
            )
        }

        let value = try #require(
            try await ctx.posting(term: "swift", id: "a1")
        )
        let posting = try FullTextStorageDecoder.posting(
            from: value,
            positionsStored: true,
            term: "swift"
        )
        #expect(posting.termFrequency == 2)
        #expect(posting.positions == [0, 2])

        try await ctx.cleanup()
    }

    @Test("Multiple documents are indexed")
    func testMultipleDocuments() async throws {
        let ctx = try await FullTextIndexContext()

        let articles = [
            SearchableArticle(id: "a1", title: "Swift", content: "Swift programming language"),
            SearchableArticle(id: "a2", title: "Python", content: "Python programming language")
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
        let ctx = try await FullTextIndexContext()

        let article = SearchableArticle(id: "a1", title: "Test", content: "Hello world")

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
        let ctx = try await FullTextIndexContext()

        let article = SearchableArticle(id: "a1", title: "Test", content: "Hello world")

        // Insert
        try await ctx.database.withTransaction { transaction in
            try await ctx.maintainer.updateIndex(
                oldItem: nil,
                newItem: article,
                transaction: transaction
            )
        }

        // Update with different content
        let updatedArticle = SearchableArticle(id: "a1", title: "Test", content: "Goodbye universe")
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
        let ctx = try await FullTextIndexContext()

        let articles = [
            SearchableArticle(id: "a1", title: "Swift", content: "Swift is a modern programming language"),
            SearchableArticle(id: "a2", title: "Python", content: "Python is also a programming language"),
            SearchableArticle(id: "a3", title: "Rust", content: "Rust is a systems language")
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
        let ctx = try await FullTextIndexContext()

        let articles = [
            SearchableArticle(id: "a1", title: "Swift", content: "Swift is modern and fast"),
            SearchableArticle(id: "a2", title: "Python", content: "Python is modern but slow"),
            SearchableArticle(id: "a3", title: "Rust", content: "Rust is fast and safe")
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
        let ctx = try await FullTextIndexContext()

        let articles = [
            SearchableArticle(id: "a1", title: "Swift", content: "Swift is fast"),
            SearchableArticle(id: "a2", title: "Python", content: "Python is slow"),
            SearchableArticle(id: "a3", title: "Rust", content: "Rust is safe")
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
        let ctx = try await FullTextIndexContext(tokenizer: .stem)

        let article = SearchableArticle(id: "a1", title: "Test", content: "Running runners run")

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
        let ctx = try await FullTextIndexContext()

        let articles = [
            SearchableArticle(id: "a1", title: "First", content: "First article content"),
            SearchableArticle(id: "a2", title: "Second", content: "Second article content")
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
        let ctx = try await FullTextIndexContext()

        let article = SearchableArticle(id: "a1", title: "Test", content: "Hello WORLD")

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
