// VersionIndexPerformanceTests.swift
// Performance tests for VersionIndex maintainer

import Testing
import Foundation
import FoundationDB
import Core
import TestSupport
@testable import DatabaseEngine
@testable import VersionIndex

// MARK: - Test Model

struct PerfTestDocument: Persistable {
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

    static var persistableType: String { "PerfTestDocument" }
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

    static func fieldName<Value>(for keyPath: KeyPath<PerfTestDocument, Value>) -> String {
        switch keyPath {
        case \PerfTestDocument.id: return "id"
        case \PerfTestDocument.title: return "title"
        case \PerfTestDocument.content: return "content"
        case \PerfTestDocument.version: return "version"
        default: return "\(keyPath)"
        }
    }

    static func fieldName(for keyPath: PartialKeyPath<PerfTestDocument>) -> String {
        switch keyPath {
        case \PerfTestDocument.id: return "id"
        case \PerfTestDocument.title: return "title"
        case \PerfTestDocument.content: return "content"
        case \PerfTestDocument.version: return "version"
        default: return "\(keyPath)"
        }
    }

    static func fieldName(for keyPath: AnyKeyPath) -> String {
        if let partial = keyPath as? PartialKeyPath<PerfTestDocument> {
            return fieldName(for: partial)
        }
        return "\(keyPath)"
    }
}

// MARK: - Benchmark Helper

private struct BenchmarkResult {
    let operation: String
    let itemCount: Int
    let durationMs: Double
    let throughput: Double

    var description: String {
        String(format: "%@ - %d items in %.2fms (%.0f items/sec)",
               operation, itemCount, durationMs, throughput)
    }
}

private func benchmark<T>(
    _ operation: String,
    itemCount: Int,
    _ block: () async throws -> T
) async throws -> (result: T, benchmark: BenchmarkResult) {
    let start = DispatchTime.now()
    let result = try await block()
    let end = DispatchTime.now()

    let nanos = end.uptimeNanoseconds - start.uptimeNanoseconds
    let ms = Double(nanos) / 1_000_000
    let throughput = Double(itemCount) / (ms / 1000)

    return (result, BenchmarkResult(
        operation: operation,
        itemCount: itemCount,
        durationMs: ms,
        throughput: throughput
    ))
}

// MARK: - Test Context Helper

private struct TestContext {
    nonisolated(unsafe) let database: any DatabaseProtocol
    let subspace: Subspace
    let indexSubspace: Subspace
    let maintainer: VersionIndexMaintainer<PerfTestDocument>
    let kind: VersionIndexKind<PerfTestDocument>

    init(strategy: VersionHistoryStrategy = .keepAll, testId: String? = nil) throws {
        self.database = try FDBClient.openDatabase()
        let id = testId ?? String(UUID().uuidString.prefix(8))
        self.subspace = Subspace(prefix: Tuple("test", "version", "perf", id).pack())
        let indexName = "PerfTestDocument_version"
        self.indexSubspace = subspace.subspace("I").subspace(indexName)

        self.kind = VersionIndexKind<PerfTestDocument>(field: \.id, strategy: strategy)

        let index = Index(
            name: indexName,
            kind: kind,
            rootExpression: FieldKeyExpression(fieldName: "id"),
            subspaceKey: indexName,
            itemTypes: Set(["PerfTestDocument"])
        )

        self.maintainer = VersionIndexMaintainer<PerfTestDocument>(
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
}

// MARK: - Performance Tests

@Suite("VersionIndex Performance Tests", .tags(.fdb, .performance), .serialized)
struct VersionIndexPerformanceTests {

    // MARK: - Insert Performance

    @Test("Version insert performance (keepAll)")
    func testVersionInsertPerformanceKeepAll() async throws {
        try await FDBTestSetup.shared.initialize()
        let ctx = try TestContext(strategy: .keepAll)

        let itemCount = 100
        var documents: [PerfTestDocument] = []

        // Generate unique documents
        for i in 0..<itemCount {
            documents.append(PerfTestDocument(
                id: "doc-\(i)",
                title: "Document \(i)",
                content: "Content for document \(i)",
                version: 1
            ))
        }

        // Benchmark bulk insert (one version per document)
        let (_, insertBenchmark) = try await benchmark("Version insert (keepAll)", itemCount: itemCount) {
            for doc in documents {
                try await ctx.database.withTransaction { transaction in
                    try await ctx.maintainer.updateIndex(
                        oldItem: nil,
                        newItem: doc,
                        transaction: transaction
                    )
                }
            }
        }

        print(insertBenchmark.description)
        #expect(insertBenchmark.throughput > 50, "Version insert throughput should be > 50/s")

        // Verify
        let count = try await ctx.countIndexEntries()
        #expect(count == itemCount, "Should have \(itemCount) version entries")

        try await ctx.cleanup()
    }

    @Test("Version insert performance (keepLast)")
    func testVersionInsertPerformanceKeepLast() async throws {
        try await FDBTestSetup.shared.initialize()
        let ctx = try TestContext(strategy: .keepLast(5))

        let itemCount = 50
        var documents: [PerfTestDocument] = []

        for i in 0..<itemCount {
            documents.append(PerfTestDocument(
                id: "doc-\(i)",
                title: "Document \(i)",
                content: "Content for document \(i)",
                version: 1
            ))
        }

        let (_, insertBenchmark) = try await benchmark("Version insert (keepLast)", itemCount: itemCount) {
            for doc in documents {
                try await ctx.database.withTransaction { transaction in
                    try await ctx.maintainer.updateIndex(
                        oldItem: nil,
                        newItem: doc,
                        transaction: transaction
                    )
                }
            }
        }

        print(insertBenchmark.description)
        #expect(insertBenchmark.throughput > 30, "Version insert (keepLast) throughput should be > 30/s")

        try await ctx.cleanup()
    }

    // MARK: - Multiple Versions Performance

    @Test("Multiple versions per document performance")
    func testMultipleVersionsPerformance() async throws {
        try await FDBTestSetup.shared.initialize()
        let ctx = try TestContext(strategy: .keepAll)

        let docCount = 10
        let versionsPerDoc = 10
        let totalVersions = docCount * versionsPerDoc

        // Create documents with multiple versions each
        let (_, versionBenchmark) = try await benchmark("Multiple versions", itemCount: totalVersions) {
            for docIndex in 0..<docCount {
                var previousDoc: PerfTestDocument? = nil

                for versionNum in 1...versionsPerDoc {
                    let doc = PerfTestDocument(
                        id: "doc-\(docIndex)",
                        title: "Doc \(docIndex) v\(versionNum)",
                        content: "Version \(versionNum) content",
                        version: versionNum
                    )

                    try await ctx.database.withTransaction { transaction in
                        try await ctx.maintainer.updateIndex(
                            oldItem: previousDoc,
                            newItem: doc,
                            transaction: transaction
                        )
                    }

                    previousDoc = doc
                }
            }
        }

        print(versionBenchmark.description)
        #expect(versionBenchmark.throughput > 30, "Multiple versions throughput should be > 30/s")

        // Verify total version count
        let count = try await ctx.countIndexEntries()
        #expect(count == totalVersions, "Should have \(totalVersions) version entries")

        try await ctx.cleanup()
    }

    // MARK: - Query Performance

    @Test("Get latest version performance")
    func testGetLatestVersionPerformance() async throws {
        try await FDBTestSetup.shared.initialize()
        let ctx = try TestContext(strategy: .keepAll)

        // Create 20 documents with 10 versions each
        let docCount = 20
        let versionsPerDoc = 10

        for docIndex in 0..<docCount {
            var previousDoc: PerfTestDocument? = nil

            for versionNum in 1...versionsPerDoc {
                let doc = PerfTestDocument(
                    id: "doc-\(docIndex)",
                    title: "Doc \(docIndex) v\(versionNum)",
                    content: "Version \(versionNum)",
                    version: versionNum
                )

                try await ctx.database.withTransaction { transaction in
                    try await ctx.maintainer.updateIndex(
                        oldItem: previousDoc,
                        newItem: doc,
                        transaction: transaction
                    )
                }

                previousDoc = doc
            }
        }

        // Benchmark getLatestVersion
        let queryCount = 100
        let (_, queryBenchmark) = try await benchmark("Get latest version", itemCount: queryCount) {
            for i in 0..<queryCount {
                _ = try await ctx.database.withTransaction { transaction in
                    try await ctx.maintainer.getLatestVersion(
                        primaryKey: ["doc-\(i % docCount)"],
                        transaction: transaction
                    )
                }
            }
        }

        print(queryBenchmark.description)
        #expect(queryBenchmark.throughput > 50, "Get latest version throughput should be > 50/s")

        try await ctx.cleanup()
    }

    @Test("Get version history performance")
    func testGetVersionHistoryPerformance() async throws {
        try await FDBTestSetup.shared.initialize()
        let ctx = try TestContext(strategy: .keepAll)

        // Create 10 documents with 20 versions each
        let docCount = 10
        let versionsPerDoc = 20

        for docIndex in 0..<docCount {
            var previousDoc: PerfTestDocument? = nil

            for versionNum in 1...versionsPerDoc {
                let doc = PerfTestDocument(
                    id: "doc-\(docIndex)",
                    title: "Doc \(docIndex) v\(versionNum)",
                    content: "Version \(versionNum) with some content",
                    version: versionNum
                )

                try await ctx.database.withTransaction { transaction in
                    try await ctx.maintainer.updateIndex(
                        oldItem: previousDoc,
                        newItem: doc,
                        transaction: transaction
                    )
                }

                previousDoc = doc
            }
        }

        // Benchmark getVersionHistory (full)
        let queryCount = 20
        let (_, fullHistoryBenchmark) = try await benchmark("Get full history", itemCount: queryCount) {
            for i in 0..<queryCount {
                _ = try await ctx.database.withTransaction { transaction in
                    try await ctx.maintainer.getVersionHistory(
                        primaryKey: ["doc-\(i % docCount)"],
                        limit: nil,
                        transaction: transaction
                    )
                }
            }
        }

        print(fullHistoryBenchmark.description)
        #expect(fullHistoryBenchmark.throughput > 10, "Get full history throughput should be > 10/s")

        // Benchmark getVersionHistory (limited)
        let (_, limitedHistoryBenchmark) = try await benchmark("Get limited history (5)", itemCount: queryCount) {
            for i in 0..<queryCount {
                _ = try await ctx.database.withTransaction { transaction in
                    try await ctx.maintainer.getVersionHistory(
                        primaryKey: ["doc-\(i % docCount)"],
                        limit: 5,
                        transaction: transaction
                    )
                }
            }
        }

        print(limitedHistoryBenchmark.description)
        #expect(limitedHistoryBenchmark.throughput > 20, "Get limited history throughput should be > 20/s")

        try await ctx.cleanup()
    }

    // MARK: - Retention Strategy Performance

    @Test("KeepLast retention cleanup performance")
    func testKeepLastRetentionPerformance() async throws {
        try await FDBTestSetup.shared.initialize()
        let ctx = try TestContext(strategy: .keepLast(5))

        let docId = "retention-test-doc"
        let totalUpdates = 20
        var previousDoc: PerfTestDocument? = nil

        // Create many versions (retention cleanup happens on each update)
        let (_, retentionBenchmark) = try await benchmark("KeepLast retention", itemCount: totalUpdates) {
            for versionNum in 1...totalUpdates {
                let doc = PerfTestDocument(
                    id: docId,
                    title: "Doc v\(versionNum)",
                    content: "Version \(versionNum) content",
                    version: versionNum
                )

                try await ctx.database.withTransaction { transaction in
                    try await ctx.maintainer.updateIndex(
                        oldItem: previousDoc,
                        newItem: doc,
                        transaction: transaction
                    )
                }

                previousDoc = doc
            }
        }

        print(retentionBenchmark.description)
        #expect(retentionBenchmark.throughput > 20, "KeepLast retention throughput should be > 20/s")

        // Verify only 5 versions remain
        let history = try await ctx.database.withTransaction { transaction in
            try await ctx.maintainer.getVersionHistory(
                primaryKey: [docId],
                limit: nil,
                transaction: transaction
            )
        }
        #expect(history.count == 5, "Should have exactly 5 versions with keepLast(5)")

        try await ctx.cleanup()
    }

    @Test("KeepForDuration retention performance")
    func testKeepForDurationRetentionPerformance() async throws {
        try await FDBTestSetup.shared.initialize()
        // Very short duration for testing (retention won't actually clean up recent items)
        let ctx = try TestContext(strategy: .keepForDuration(3600)) // 1 hour

        let docId = "duration-test-doc"
        let totalUpdates = 20
        var previousDoc: PerfTestDocument? = nil

        let (_, retentionBenchmark) = try await benchmark("KeepForDuration retention", itemCount: totalUpdates) {
            for versionNum in 1...totalUpdates {
                let doc = PerfTestDocument(
                    id: docId,
                    title: "Doc v\(versionNum)",
                    content: "Version \(versionNum) content",
                    version: versionNum
                )

                try await ctx.database.withTransaction { transaction in
                    try await ctx.maintainer.updateIndex(
                        oldItem: previousDoc,
                        newItem: doc,
                        transaction: transaction
                    )
                }

                previousDoc = doc
            }
        }

        print(retentionBenchmark.description)
        #expect(retentionBenchmark.throughput > 15, "KeepForDuration retention throughput should be > 15/s")

        // All versions should remain (within 1 hour window)
        let history = try await ctx.database.withTransaction { transaction in
            try await ctx.maintainer.getVersionHistory(
                primaryKey: [docId],
                limit: nil,
                transaction: transaction
            )
        }
        #expect(history.count == totalUpdates, "All versions should remain within duration window")

        try await ctx.cleanup()
    }

    // MARK: - Scan Performance

    @Test("ScanItem performance")
    func testScanItemPerformance() async throws {
        try await FDBTestSetup.shared.initialize()
        let ctx = try TestContext(strategy: .keepAll)

        let itemCount = 100
        var documents: [PerfTestDocument] = []

        for i in 0..<itemCount {
            documents.append(PerfTestDocument(
                id: "doc-\(i)",
                title: "Document \(i)",
                content: "Content for document \(i)",
                version: 1
            ))
        }

        // Benchmark scanItem (used by OnlineIndexer)
        let (_, scanBenchmark) = try await benchmark("ScanItem", itemCount: itemCount) {
            for doc in documents {
                try await ctx.database.withTransaction { transaction in
                    try await ctx.maintainer.scanItem(
                        doc,
                        id: Tuple(doc.id),
                        transaction: transaction
                    )
                }
            }
        }

        print(scanBenchmark.description)
        #expect(scanBenchmark.throughput > 50, "ScanItem throughput should be > 50/s")

        try await ctx.cleanup()
    }

    // MARK: - Delete Performance

    @Test("Delete marker performance")
    func testDeleteMarkerPerformance() async throws {
        try await FDBTestSetup.shared.initialize()
        let ctx = try TestContext(strategy: .keepAll)

        let itemCount = 50
        var documents: [PerfTestDocument] = []

        // First, insert documents
        for i in 0..<itemCount {
            let doc = PerfTestDocument(
                id: "doc-\(i)",
                title: "Document \(i)",
                content: "Content",
                version: 1
            )
            documents.append(doc)

            try await ctx.database.withTransaction { transaction in
                try await ctx.maintainer.updateIndex(
                    oldItem: nil,
                    newItem: doc,
                    transaction: transaction
                )
            }
        }

        // Benchmark delete markers
        let (_, deleteBenchmark) = try await benchmark("Delete markers", itemCount: itemCount) {
            for doc in documents {
                try await ctx.database.withTransaction { transaction in
                    try await ctx.maintainer.updateIndex(
                        oldItem: doc,
                        newItem: nil,
                        transaction: transaction
                    )
                }
            }
        }

        print(deleteBenchmark.description)
        #expect(deleteBenchmark.throughput > 50, "Delete marker throughput should be > 50/s")

        // Verify both original and deletion marker exist
        let count = try await ctx.countIndexEntries()
        #expect(count == itemCount * 2, "Should have \(itemCount * 2) entries (original + deletion markers)")

        try await ctx.cleanup()
    }

    // MARK: - Scale Tests

    @Test("Large history scale test")
    func testLargeHistoryScaleTest() async throws {
        try await FDBTestSetup.shared.initialize()
        let ctx = try TestContext(strategy: .keepAll)

        let docId = "scale-test-doc"
        let versionCount = 100
        var previousDoc: PerfTestDocument? = nil

        // Create many versions of a single document
        let (_, scaleBenchmark) = try await benchmark("Large history creation", itemCount: versionCount) {
            for versionNum in 1...versionCount {
                let doc = PerfTestDocument(
                    id: docId,
                    title: "Doc v\(versionNum)",
                    content: String(repeating: "Content ", count: 10), // ~80 bytes content
                    version: versionNum
                )

                try await ctx.database.withTransaction { transaction in
                    try await ctx.maintainer.updateIndex(
                        oldItem: previousDoc,
                        newItem: doc,
                        transaction: transaction
                    )
                }

                previousDoc = doc
            }
        }

        print(scaleBenchmark.description)
        #expect(scaleBenchmark.throughput > 50, "Large history creation throughput should be > 50/s")

        // Benchmark querying large history
        let (history, queryBenchmark) = try await benchmark("Query large history", itemCount: 1) {
            try await ctx.database.withTransaction { transaction in
                try await ctx.maintainer.getVersionHistory(
                    primaryKey: [docId],
                    limit: nil,
                    transaction: transaction
                )
            }
        }

        print("Query \(history.count) versions: \(queryBenchmark.durationMs)ms")
        #expect(history.count == versionCount, "Should have \(versionCount) versions")

        // Benchmark limited query on large history
        let (_, limitedQueryBenchmark) = try await benchmark("Query large history (limited)", itemCount: 10) {
            for _ in 0..<10 {
                _ = try await ctx.database.withTransaction { transaction in
                    try await ctx.maintainer.getVersionHistory(
                        primaryKey: [docId],
                        limit: 10,
                        transaction: transaction
                    )
                }
            }
        }

        print(limitedQueryBenchmark.description)
        #expect(limitedQueryBenchmark.throughput > 20, "Limited query throughput should be > 20/s")

        try await ctx.cleanup()
    }

    // MARK: - Concurrent Access Test

    @Test("Concurrent version updates")
    func testConcurrentVersionUpdates() async throws {
        try await FDBTestSetup.shared.initialize()
        let ctx = try TestContext(strategy: .keepAll)

        let docCount = 20
        let versionsPerDoc = 5

        // Create documents concurrently
        let (_, concurrentBenchmark) = try await benchmark("Concurrent updates", itemCount: docCount * versionsPerDoc) {
            try await withThrowingTaskGroup(of: Void.self) { group in
                for docIndex in 0..<docCount {
                    group.addTask {
                        var previousDoc: PerfTestDocument? = nil

                        for versionNum in 1...versionsPerDoc {
                            let doc = PerfTestDocument(
                                id: "concurrent-doc-\(docIndex)",
                                title: "Doc \(docIndex) v\(versionNum)",
                                content: "Version \(versionNum)",
                                version: versionNum
                            )

                            try await ctx.database.withTransaction { transaction in
                                try await ctx.maintainer.updateIndex(
                                    oldItem: previousDoc,
                                    newItem: doc,
                                    transaction: transaction
                                )
                            }

                            previousDoc = doc
                        }
                    }
                }

                try await group.waitForAll()
            }
        }

        print(concurrentBenchmark.description)
        #expect(concurrentBenchmark.throughput > 30, "Concurrent updates throughput should be > 30/s")

        // Verify all versions were created
        let totalCount = try await ctx.countIndexEntries()
        #expect(totalCount == docCount * versionsPerDoc, "Should have all \(docCount * versionsPerDoc) versions")

        try await ctx.cleanup()
    }
}
