// FullTextIndexBehaviorTests.swift
// Backend-neutral tests for FullTextIndex persistence behavior

import DatabaseKit
import DatabaseTypes
import Foundation
import StorageKit
import TestSupport
import Testing

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
        let index = try ResolvedIndex(
            for: SearchableArticle.self,
            name: indexName,
            definition: fullTextIndexDefinition(
                fieldName: "content",
                fieldNumber: 3,
                tokenizer: tokenizer,
                storePositions: storePositions
            ),
            rootExpression: FieldKeyExpression(fieldName: "content"),
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

func fullTextIndexDefinition(
    fieldName: String,
    fieldNumber: Int,
    tokenizer: TokenizationStrategy,
    storePositions: Bool,
    ngramSize: Int = 3,
    minTermLength: Int = 2
) -> IndexDefinition<FieldIdentity> {
    .text(
        fields: [FieldIdentity(name: fieldName, number: fieldNumber)],
        mode: .fullText(
            tokenizer: tokenizer,
            storePositions: storePositions,
            ngramSize: ngramSize,
            minimumTermLength: minTermLength
        )
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
            SearchableArticle(id: "a2", title: "Python", content: "Python programming language"),
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

        let oldPosting = try await ctx.posting(term: "hello", id: "a1")
        #expect(oldPosting == nil, "The old term should be removed after update")

        let newPosting = try await ctx.posting(term: "goodbye", id: "a1")
        #expect(newPosting != nil, "The new term should be indexed after update")

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

        // All forms should produce the normalized "run" posting.
        let posting = try await ctx.posting(term: "run", id: "a1")
        #expect(posting != nil, "Stemming should write the normalized posting")

        try await ctx.cleanup()
    }

    // MARK: - Scan Tests

    @Test("ScanItem tokenizes and indexes")
    func testScanItemTokenizesAndIndexes() async throws {
        let ctx = try await FullTextIndexContext()

        let articles = [
            SearchableArticle(id: "a1", title: "First", content: "First article content"),
            SearchableArticle(id: "a2", title: "Second", content: "Second article content"),
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

        let firstPosting = try await ctx.posting(term: "article", id: "a1")
        let secondPosting = try await ctx.posting(term: "article", id: "a2")
        #expect(firstPosting != nil)
        #expect(secondPosting != nil)

        try await ctx.cleanup()
    }
}
