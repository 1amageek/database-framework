// VersionIndexBehaviorTests.swift
// Integration tests for VersionIndex behavior with FDB

import Testing
import Foundation
import FoundationDB
import Core
import TestSupport
@testable import DatabaseEngine
@testable import VersionIndex

// MARK: - Test Model

struct TestDocument: Persistable {
    typealias ID = String

    var id: String
    var title: String
    var content: String
    var version: Int

    init(id: String = UUID().uuidString, title: String, content: String, version: Int = 1) {
        self.id = id
        self.title = title
        self.content = content
        self.version = version
    }

    static var persistableType: String { "TestDocument" }
    static var allFields: [String] { ["id", "title", "content", "version"] }
    static var indexDescriptors: [IndexDescriptor] { [] }

    static func fieldNumber(for fieldName: String) -> Int? { nil }
    static func enumMetadata(for fieldName: String) -> EnumMetadata? { nil }

    subscript(dynamicMember member: String) -> (any Sendable)? {
        switch member {
        case "id": return id
        case "title": return title
        case "content": return content
        case "version": return version
        default: return nil
        }
    }

    static func fieldName<Value>(for keyPath: KeyPath<TestDocument, Value>) -> String {
        switch keyPath {
        case \TestDocument.id: return "id"
        case \TestDocument.title: return "title"
        case \TestDocument.content: return "content"
        case \TestDocument.version: return "version"
        default: return "\(keyPath)"
        }
    }

    static func fieldName(for keyPath: PartialKeyPath<TestDocument>) -> String {
        switch keyPath {
        case \TestDocument.id: return "id"
        case \TestDocument.title: return "title"
        case \TestDocument.content: return "content"
        case \TestDocument.version: return "version"
        default: return "\(keyPath)"
        }
    }

    static func fieldName(for keyPath: AnyKeyPath) -> String {
        if let partial = keyPath as? PartialKeyPath<TestDocument> {
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
    let maintainer: VersionIndexMaintainer<TestDocument>
    let kind: VersionIndexKind<TestDocument>

    init(strategy: VersionHistoryStrategy = .keepAll, indexName: String = "TestDocument_version") throws {
        self.database = try FDBClient.openDatabase()
        let testId = UUID().uuidString.prefix(8)
        self.subspace = Subspace(prefix: Tuple("test", "version", String(testId)).pack())
        self.indexSubspace = subspace.subspace("I").subspace(indexName)

        self.kind = VersionIndexKind<TestDocument>(field: \.id, strategy: strategy)

        let index = Index(
            name: indexName,
            kind: kind,
            rootExpression: FieldKeyExpression(fieldName: "id"),
            subspaceKey: indexName,
            itemTypes: Set(["TestDocument"])
        )

        self.maintainer = VersionIndexMaintainer<TestDocument>(
            index: index,
            strategy: kind.strategy,
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

    func getVersionHistory(primaryKey: [any TupleElement], limit: Int? = nil) async throws -> [(version: Version, data: [UInt8])] {
        try await database.withTransaction { transaction in
            try await maintainer.getVersionHistory(
                primaryKey: primaryKey,
                limit: limit,
                transaction: transaction
            )
        }
    }

    func getLatestVersion(primaryKey: [any TupleElement]) async throws -> [UInt8]? {
        try await database.withTransaction { transaction in
            try await maintainer.getLatestVersion(
                primaryKey: primaryKey,
                transaction: transaction
            )
        }
    }
}

// MARK: - Behavior Tests

@Suite("VersionIndex Behavior Tests", .tags(.fdb), .serialized)
struct VersionIndexBehaviorTests {

    // MARK: - Insert Tests

    @Test("Insert creates version entry")
    func testInsertCreatesVersionEntry() async throws {
        try await FDBTestSetup.shared.initialize()
        let ctx = try TestContext()

        let doc = TestDocument(id: "doc1", title: "Test", content: "Hello", version: 1)

        // Insert document
        try await ctx.database.withTransaction { transaction in
            try await ctx.maintainer.updateIndex(
                oldItem: nil,
                newItem: doc,
                transaction: transaction
            )
        }

        // versionstampedKey is committed, now read in a new transaction
        let count = try await ctx.countIndexEntries()
        #expect(count == 1, "Should have 1 version entry after insert")

        try await ctx.cleanup()
    }

    @Test("Multiple updates create multiple versions")
    func testMultipleUpdatesCreateVersions() async throws {
        try await FDBTestSetup.shared.initialize()
        let ctx = try TestContext()

        let doc1 = TestDocument(id: "doc1", title: "v1", content: "Version 1", version: 1)
        let doc2 = TestDocument(id: "doc1", title: "v2", content: "Version 2", version: 2)
        let doc3 = TestDocument(id: "doc1", title: "v3", content: "Version 3", version: 3)

        // Insert version 1
        try await ctx.database.withTransaction { transaction in
            try await ctx.maintainer.updateIndex(
                oldItem: nil,
                newItem: doc1,
                transaction: transaction
            )
        }

        // Update to version 2
        try await ctx.database.withTransaction { transaction in
            try await ctx.maintainer.updateIndex(
                oldItem: doc1,
                newItem: doc2,
                transaction: transaction
            )
        }

        // Update to version 3
        try await ctx.database.withTransaction { transaction in
            try await ctx.maintainer.updateIndex(
                oldItem: doc2,
                newItem: doc3,
                transaction: transaction
            )
        }

        let count = try await ctx.countIndexEntries()
        #expect(count == 3, "Should have 3 version entries")

        try await ctx.cleanup()
    }

    // MARK: - Version History Tests

    @Test("getVersionHistory returns all versions")
    func testGetVersionHistoryReturnsAllVersions() async throws {
        try await FDBTestSetup.shared.initialize()
        let ctx = try TestContext()

        let doc1 = TestDocument(id: "doc1", title: "v1", content: "Version 1", version: 1)
        let doc2 = TestDocument(id: "doc1", title: "v2", content: "Version 2", version: 2)

        // Insert version 1
        try await ctx.database.withTransaction { transaction in
            try await ctx.maintainer.updateIndex(
                oldItem: nil,
                newItem: doc1,
                transaction: transaction
            )
        }

        // Update to version 2
        try await ctx.database.withTransaction { transaction in
            try await ctx.maintainer.updateIndex(
                oldItem: doc1,
                newItem: doc2,
                transaction: transaction
            )
        }

        let history = try await ctx.getVersionHistory(primaryKey: ["doc1"])
        #expect(history.count == 2, "Should have 2 versions in history")

        try await ctx.cleanup()
    }

    @Test("getVersionHistory with limit returns limited versions")
    func testGetVersionHistoryWithLimit() async throws {
        try await FDBTestSetup.shared.initialize()
        let ctx = try TestContext()

        // Create multiple versions
        for i in 1...5 {
            let doc = TestDocument(id: "doc1", title: "v\(i)", content: "Version \(i)", version: i)
            try await ctx.database.withTransaction { transaction in
                try await ctx.maintainer.updateIndex(
                    oldItem: i > 1 ? TestDocument(id: "doc1", title: "v\(i-1)", content: "Version \(i-1)", version: i-1) : nil,
                    newItem: doc,
                    transaction: transaction
                )
            }
        }

        let history = try await ctx.getVersionHistory(primaryKey: ["doc1"], limit: 3)
        #expect(history.count == 3, "Should return only 3 versions when limited")

        try await ctx.cleanup()
    }

    @Test("getLatestVersion returns most recent data")
    func testGetLatestVersionReturnsMostRecent() async throws {
        try await FDBTestSetup.shared.initialize()
        let ctx = try TestContext()

        let doc1 = TestDocument(id: "doc1", title: "v1", content: "Version 1", version: 1)
        let doc2 = TestDocument(id: "doc1", title: "v2", content: "Latest Version", version: 2)

        // Insert version 1
        try await ctx.database.withTransaction { transaction in
            try await ctx.maintainer.updateIndex(
                oldItem: nil,
                newItem: doc1,
                transaction: transaction
            )
        }

        // Update to version 2
        try await ctx.database.withTransaction { transaction in
            try await ctx.maintainer.updateIndex(
                oldItem: doc1,
                newItem: doc2,
                transaction: transaction
            )
        }

        let latestData = try await ctx.getLatestVersion(primaryKey: ["doc1"])
        #expect(latestData != nil, "Should have latest version data")

        try await ctx.cleanup()
    }

    // MARK: - Delete Tests

    @Test("Delete creates deletion marker")
    func testDeleteCreatesDeletionMarker() async throws {
        try await FDBTestSetup.shared.initialize()
        let ctx = try TestContext()

        let doc = TestDocument(id: "doc1", title: "Test", content: "Hello", version: 1)

        // Insert
        try await ctx.database.withTransaction { transaction in
            try await ctx.maintainer.updateIndex(
                oldItem: nil,
                newItem: doc,
                transaction: transaction
            )
        }

        // Delete
        try await ctx.database.withTransaction { transaction in
            try await ctx.maintainer.updateIndex(
                oldItem: doc,
                newItem: nil,
                transaction: transaction
            )
        }

        // Should have 2 entries: original + deletion marker
        let count = try await ctx.countIndexEntries()
        #expect(count == 2, "Should have 2 entries (original + deletion marker)")

        try await ctx.cleanup()
    }

    // MARK: - Retention Strategy Tests

    @Test("keepLast strategy limits versions")
    func testKeepLastStrategyLimitsVersions() async throws {
        try await FDBTestSetup.shared.initialize()
        let ctx = try TestContext(strategy: .keepLast(3))

        // Create 5 versions
        for i in 1...5 {
            let doc = TestDocument(id: "doc1", title: "v\(i)", content: "Version \(i)", version: i)
            try await ctx.database.withTransaction { transaction in
                try await ctx.maintainer.updateIndex(
                    oldItem: i > 1 ? TestDocument(id: "doc1", title: "v\(i-1)", content: "Version \(i-1)", version: i-1) : nil,
                    newItem: doc,
                    transaction: transaction
                )
            }
        }

        let count = try await ctx.countIndexEntries()
        #expect(count <= 3, "Should keep at most 3 versions with keepLast(3)")

        try await ctx.cleanup()
    }

    // MARK: - Scan Tests

    @Test("ScanItem creates version entry")
    func testScanItemCreatesVersionEntry() async throws {
        try await FDBTestSetup.shared.initialize()
        let ctx = try TestContext()

        let doc = TestDocument(id: "doc1", title: "Scanned", content: "Content", version: 1)

        try await ctx.database.withTransaction { transaction in
            try await ctx.maintainer.scanItem(
                doc,
                id: Tuple(doc.id),
                transaction: transaction
            )
        }

        let count = try await ctx.countIndexEntries()
        #expect(count == 1, "Should have 1 entry after scanItem")

        try await ctx.cleanup()
    }

    // MARK: - Multiple Documents Tests

    @Test("Different documents have separate histories")
    func testDifferentDocumentsHaveSeparateHistories() async throws {
        try await FDBTestSetup.shared.initialize()
        let ctx = try TestContext()

        let doc1v1 = TestDocument(id: "doc1", title: "Doc1 v1", content: "Content", version: 1)
        let doc1v2 = TestDocument(id: "doc1", title: "Doc1 v2", content: "Updated", version: 2)
        let doc2v1 = TestDocument(id: "doc2", title: "Doc2 v1", content: "Other", version: 1)

        // Insert doc1 version 1
        try await ctx.database.withTransaction { transaction in
            try await ctx.maintainer.updateIndex(oldItem: nil, newItem: doc1v1, transaction: transaction)
        }

        // Update doc1 to version 2
        try await ctx.database.withTransaction { transaction in
            try await ctx.maintainer.updateIndex(oldItem: doc1v1, newItem: doc1v2, transaction: transaction)
        }

        // Insert doc2 version 1
        try await ctx.database.withTransaction { transaction in
            try await ctx.maintainer.updateIndex(oldItem: nil, newItem: doc2v1, transaction: transaction)
        }

        // Check histories are separate
        let doc1History = try await ctx.getVersionHistory(primaryKey: ["doc1"])
        let doc2History = try await ctx.getVersionHistory(primaryKey: ["doc2"])

        #expect(doc1History.count == 2, "doc1 should have 2 versions")
        #expect(doc2History.count == 1, "doc2 should have 1 version")

        try await ctx.cleanup()
    }

    // MARK: - Version Ordering Tests

    @Test("Versions are ordered by versionstamp")
    func testVersionsAreOrderedByVersionstamp() async throws {
        try await FDBTestSetup.shared.initialize()
        let ctx = try TestContext()

        // Create versions with delays to ensure different versionstamps
        for i in 1...3 {
            let doc = TestDocument(id: "doc1", title: "v\(i)", content: "Version \(i)", version: i)
            try await ctx.database.withTransaction { transaction in
                try await ctx.maintainer.updateIndex(
                    oldItem: i > 1 ? TestDocument(id: "doc1", title: "v\(i-1)", content: "Version \(i-1)", version: i-1) : nil,
                    newItem: doc,
                    transaction: transaction
                )
            }
        }

        let history = try await ctx.getVersionHistory(primaryKey: ["doc1"])

        // Verify versions are in order (newest first based on implementation)
        #expect(history.count == 3, "Should have 3 versions")

        // Versionstamps should be increasing
        for i in 0..<history.count - 1 {
            let current = history[i].version
            let next = history[i + 1].version
            // Newer versions should have higher versionstamps
            #expect(current > next, "Versions should be in descending order (newest first)")
        }

        try await ctx.cleanup()
    }
}
