// PolymorphableTests.swift
// Tests for Polymorphable protocol and dual-write functionality

import Testing
import Foundation
import Core
import DatabaseEngine
import FoundationDB
@testable import TestSupport

// MARK: - Test Models

/// Polymorphable protocol for documents
@Polymorphable
protocol TestDocument: Polymorphable {
    var id: String { get }
    var title: String { get }

    #Directory<TestDocument>("test", "documents")
}

/// Article - conforms to TestDocument with its own directory (dual-write)
@Persistable
struct TestArticle: TestDocument {
    var id: String = ULID().ulidString
    var title: String
    var content: String

    #Directory<TestArticle>("test", "articles")
}

/// Report - conforms to TestDocument without its own directory (single write to protocol directory)
@Persistable
struct TestReport: TestDocument {
    var id: String = ULID().ulidString
    var title: String
    var data: Data
}

/// Memo - regular Persistable without Polymorphable conformance
@Persistable
struct TestMemo {
    var id: String = ULID().ulidString
    var text: String

    #Directory<TestMemo>("test", "memos")
}

// MARK: - Tests

@Suite("Polymorphable Tests", .tags(.fdb), .serialized)
struct PolymorphableTests {

    init() async throws {
        try await FDBTestSetup.shared.initialize()
    }

    // MARK: - Protocol Conformance Tests

    @Test("Polymorphable types have correct polymorphableType")
    func testPolymorphableTypeInheritance() {
        // TestArticle conforms to TestDocument, should have polymorphableType "TestDocument"
        #expect(TestArticle.polymorphableType == "TestDocument")

        // TestReport also conforms to TestDocument
        #expect(TestReport.polymorphableType == "TestDocument")

        // Verify they return the same polymorphableType
        #expect(TestArticle.polymorphableType == TestReport.polymorphableType)
    }

    @Test("Non-polymorphic types are not detected as Polymorphable")
    func testNonPolymorphicType() {
        // TestMemo does not conform to any Polymorphable protocol
        let memoType = TestMemo.self as Any
        let isPolymorphic = memoType is any Polymorphable.Type
        #expect(!isPolymorphic)
    }

    @Test("Polymorphic types can be detected via conformance check")
    func testPolymorphicConformanceCheck() {
        // TestArticle conforms to Polymorphable (via TestDocument)
        let articleType = TestArticle.self as Any
        let isPolymorphic = articleType is any Polymorphable.Type
        #expect(isPolymorphic)

        // TestReport also conforms
        let reportType = TestReport.self as Any
        #expect(reportType is any Polymorphable.Type)
    }

    @Test("typeCode is consistent for the same type name")
    func testTypeCodeConsistency() {
        // Use typeCode(for:) static method from Polymorphable
        let code1 = TestArticle.typeCode(for: "TestArticle")
        let code2 = TestArticle.typeCode(for: "TestArticle")
        #expect(code1 == code2)

        // Different types should have different codes
        let code3 = TestArticle.typeCode(for: "TestReport")
        #expect(code1 != code3)
    }

    // MARK: - Directory Resolution Tests

    @Test("Different directories are correctly identified")
    func testDirectoryDifference() {
        // TestArticle has its own directory (type-specific)
        let articleDir = TestArticle.directoryPathComponents.map { "\($0)" }.joined(separator: "/")
        // Access polymorphic shared directory via polymorphicDirectoryPathComponents
        let protoDir = TestArticle.polymorphicDirectoryPathComponents.map { "\($0)" }.joined(separator: "/")

        #expect(articleDir != protoDir)
        #expect(articleDir.contains("articles"))
        #expect(protoDir.contains("documents"))
    }

    @Test("Type without own directory uses default path")
    func testNoOwnDirectory() {
        // TestReport doesn't have its own #Directory, so it uses the default (persistableType)
        // In this implementation, @Persistable generates default directoryPathComponents
        let reportDir = TestReport.directoryPathComponents.map { "\($0)" }.joined(separator: "/")

        // The default is [Path(persistableType)] = "TestReport"
        #expect(reportDir.contains("TestReport"))
    }

    @Test("All conforming types share the same polymorphicDirectoryPathComponents")
    func testSharedPolymorphicDirectory() {
        // Both TestArticle and TestReport should have the same polymorphicDirectoryPathComponents
        let articlePolyDir = TestArticle.polymorphicDirectoryPathComponents.map { "\($0)" }.joined(separator: "/")
        let reportPolyDir = TestReport.polymorphicDirectoryPathComponents.map { "\($0)" }.joined(separator: "/")

        #expect(articlePolyDir == reportPolyDir)
        #expect(articlePolyDir.contains("documents"))
    }

    // MARK: - Runtime Type Access Tests

    @Test("Polymorphic properties accessible from Persistable type at runtime")
    func testRuntimePolymorphicAccess() {
        let type: any Persistable.Type = TestArticle.self

        // Check if type conforms to Polymorphable
        if let polyType = type as? any Polymorphable.Type {
            // Access polymorphic properties
            #expect(polyType.polymorphableType == "TestDocument")

            let polyDir = polyType.polymorphicDirectoryPathComponents.map { "\($0)" }.joined(separator: "/")
            #expect(polyDir.contains("documents"))

            let typeCode = polyType.typeCode(for: type.persistableType)
            #expect(typeCode > 0)
        } else {
            Issue.record("TestArticle should conform to Polymorphable")
        }
    }

    @Test("Non-polymorphic types return nil for polymorphic cast")
    func testNonPolymorphicRuntimeCheck() {
        let type: any Persistable.Type = TestMemo.self

        // TestMemo does not conform to Polymorphable
        // Use negated `is` check instead of storing the cast result to avoid potential runtime issues
        let isPolymorphic = type is any Polymorphable.Type
        #expect(!isPolymorphic)
    }

    // MARK: - Dual-Write Detection Tests

    @Test("Dual-write detection works correctly")
    func testDualWriteDetection() {
        // TestArticle: has own directory AND polymorphic directory → dual-write
        let articleOwnDir = TestArticle.directoryPathComponents.map { "\($0)" }.joined(separator: "/")
        let articlePolyDir = TestArticle.polymorphicDirectoryPathComponents.map { "\($0)" }.joined(separator: "/")
        let articleNeedsDualWrite = articleOwnDir != articlePolyDir
        #expect(articleNeedsDualWrite == true)

        // TestReport: only has default directory (persistableType), different from polymorphic
        let reportOwnDir = TestReport.directoryPathComponents.map { "\($0)" }.joined(separator: "/")
        let reportPolyDir = TestReport.polymorphicDirectoryPathComponents.map { "\($0)" }.joined(separator: "/")
        let reportNeedsDualWrite = reportOwnDir != reportPolyDir
        #expect(reportNeedsDualWrite == true)  // "TestReport" != "test/documents"
    }
}

// MARK: - FDB Integration Tests

@Suite("Polymorphable FDB Integration Tests", .tags(.fdb), .serialized)
struct PolymorphableFDBTests {

    init() async throws {
        try await FDBTestSetup.shared.initialize()
    }

    // MARK: - Dual-Write Tests

    @Test("Dual-write saves data to both type-specific and polymorphic directories")
    func testDualWriteOnSave() async throws {
        try await FDBTestSetup.shared.withSerializedAccess {
            let database = try FDBClient.openDatabase()
            let schema = Schema([TestArticle.self, TestReport.self])
            let container = FDBContainer(database: database, schema: schema, security: .disabled)
            let context = container.newContext()

            // Create article (has own directory - triggers dual-write)
            let article = TestArticle(title: "Test Article", content: "Article content")

            // Save using normal save (should trigger dual-write)
            context.insert(article)
            try await context.save()

            // Verify data exists in type-specific directory
            let typeSubspace = try await container.resolveDirectory(for: TestArticle.self)
            let typeItemSubspace = typeSubspace.subspace(SubspaceKey.items)
            let typeKey = typeItemSubspace.subspace(TestArticle.persistableType).pack(Tuple([article.id]))

            let typeData = try await database.withTransaction { transaction in
                return try await transaction.getValue(for: typeKey, snapshot: true)
            }
            #expect(typeData != nil, "Data should exist in type-specific directory")

            // Verify data exists in polymorphic directory
            // Use TestArticle to resolve polymorphic directory (all conforming types share the same directory)
            let polySubspace = try await container.resolvePolymorphicDirectory(for: TestArticle.self)
            let polyItemSubspace = polySubspace.subspace(SubspaceKey.items)
            let typeCode = TestArticle.typeCode(for: TestArticle.persistableType)
            let polyKey = polyItemSubspace.subspace(Tuple([typeCode])).pack(Tuple([article.id]))

            let polyData = try await database.withTransaction { transaction in
                return try await transaction.getValue(for: polyKey, snapshot: true)
            }
            #expect(polyData != nil, "Data should exist in polymorphic directory")

            // Clean up
            context.delete(article)
            try await context.save()
        }
    }

    @Test("Dual-delete removes data from both directories")
    func testDualDeleteOnDelete() async throws {
        try await FDBTestSetup.shared.withSerializedAccess {
            let database = try FDBClient.openDatabase()
            let schema = Schema([TestArticle.self, TestReport.self])
            let container = FDBContainer(database: database, schema: schema, security: .disabled)
            let context = container.newContext()

            // Create and save article
            let article = TestArticle(title: "Delete Test", content: "To be deleted")
            context.insert(article)
            try await context.save()

            // Verify data exists in both directories before delete
            let typeSubspace = try await container.resolveDirectory(for: TestArticle.self)
            let typeItemSubspace = typeSubspace.subspace(SubspaceKey.items)
            let typeKey = typeItemSubspace.subspace(TestArticle.persistableType).pack(Tuple([article.id]))

            // Use TestArticle to resolve polymorphic directory
            let polySubspace = try await container.resolvePolymorphicDirectory(for: TestArticle.self)
            let polyItemSubspace = polySubspace.subspace(SubspaceKey.items)
            let typeCode = TestArticle.typeCode(for: TestArticle.persistableType)
            let polyKey = polyItemSubspace.subspace(Tuple([typeCode])).pack(Tuple([article.id]))

            let typeDataBefore = try await database.withTransaction { transaction in
                return try await transaction.getValue(for: typeKey, snapshot: true)
            }
            let polyDataBefore = try await database.withTransaction { transaction in
                return try await transaction.getValue(for: polyKey, snapshot: true)
            }
            #expect(typeDataBefore != nil, "Data should exist before delete")
            #expect(polyDataBefore != nil, "Polymorphic data should exist before delete")

            // Delete article
            context.delete(article)
            try await context.save()

            // Verify data removed from both directories
            let typeDataAfter = try await database.withTransaction { transaction in
                return try await transaction.getValue(for: typeKey, snapshot: true)
            }
            let polyDataAfter = try await database.withTransaction { transaction in
                return try await transaction.getValue(for: polyKey, snapshot: true)
            }
            #expect(typeDataAfter == nil, "Data should be removed from type-specific directory")
            #expect(polyDataAfter == nil, "Data should be removed from polymorphic directory")
        }
    }

    // MARK: - Polymorphic Fetch Tests

    @Test("fetchPolymorphic returns all conforming types")
    func testFetchPolymorphicReturnsAllTypes() async throws {
        try await FDBTestSetup.shared.withSerializedAccess {
            let database = try FDBClient.openDatabase()
            let schema = Schema([TestArticle.self, TestReport.self])
            let container = FDBContainer(database: database, schema: schema, security: .disabled)
            let context = container.newContext()

            // Clean up any leftover data from previous test runs
            // Clear polymorphic directory to ensure test isolation
            let polySubspace = try await container.resolvePolymorphicDirectory(for: TestArticle.self)
            let polyItemSubspace = polySubspace.subspace(SubspaceKey.items)
            try await database.withTransaction { transaction in
                let (begin, end) = polyItemSubspace.range()
                transaction.clearRange(beginKey: begin, endKey: end)
            }

            // Create article and report
            let article = TestArticle(title: "Article Title", content: "Article content")
            let report = TestReport(title: "Report Title", data: Data("Report data".utf8))

            // Save both
            context.insert(article)
            context.insert(report)
            try await context.save()

            // Fetch all documents using polymorphic query
            // Use concrete type (TestArticle) since protocol types cannot conform to Polymorphable
            let allDocuments = try await context.fetchPolymorphic(TestArticle.self)

            // Verify both types are returned
            #expect(allDocuments.count == 2, "Should return both article and report")

            let articleFound = allDocuments.contains { item in
                if let a = item as? TestArticle {
                    return a.id == article.id
                }
                return false
            }
            let reportFound = allDocuments.contains { item in
                if let r = item as? TestReport {
                    return r.id == report.id
                }
                return false
            }

            #expect(articleFound, "Article should be in results")
            #expect(reportFound, "Report should be in results")

            // Clean up
            context.delete(article)
            context.delete(report)
            try await context.save()
        }
    }

    @Test("fetchPolymorphic by ID returns correct item")
    func testFetchPolymorphicByID() async throws {
        try await FDBTestSetup.shared.withSerializedAccess {
            let database = try FDBClient.openDatabase()
            let schema = Schema([TestArticle.self, TestReport.self])
            let container = FDBContainer(database: database, schema: schema, security: .disabled)
            let context = container.newContext()

            // Create article and report
            let article = TestArticle(title: "Find Me Article", content: "Content")
            let report = TestReport(title: "Find Me Report", data: Data())

            context.insert(article)
            context.insert(report)
            try await context.save()

            // Fetch article by ID from polymorphic directory
            // Use concrete type (TestArticle) for polymorphic query
            let foundArticle = try await context.fetchPolymorphic(TestArticle.self, id: article.id)
            #expect(foundArticle != nil, "Article should be found by ID")
            if let found = foundArticle as? TestArticle {
                #expect(found.title == "Find Me Article")
                #expect(found.content == "Content")
            } else {
                Issue.record("Found item should be TestArticle type")
            }

            // Fetch report by ID from polymorphic directory
            // Use TestArticle to query the shared polymorphic directory
            let foundReport = try await context.fetchPolymorphic(TestArticle.self, id: report.id)
            #expect(foundReport != nil, "Report should be found by ID")
            if let found = foundReport as? TestReport {
                #expect(found.title == "Find Me Report")
            } else {
                Issue.record("Found item should be TestReport type")
            }

            // Fetch non-existent ID
            let notFound = try await context.fetchPolymorphic(TestArticle.self, id: "non-existent-id")
            #expect(notFound == nil, "Non-existent ID should return nil")

            // Clean up
            context.delete(article)
            context.delete(report)
            try await context.save()
        }
    }

    @Test("Non-polymorphic type does not trigger dual-write")
    func testNonPolymorphicTypeNoDualWrite() async throws {
        try await FDBTestSetup.shared.withSerializedAccess {
            let database = try FDBClient.openDatabase()
            let schema = Schema([TestArticle.self, TestReport.self, TestMemo.self])
            let container = FDBContainer(database: database, schema: schema, security: .disabled)
            let context = container.newContext()

            // Create memo (not polymorphic)
            let memo = TestMemo(text: "Simple memo")
            context.insert(memo)
            try await context.save()

            // Verify memo exists in its own directory
            let memoSubspace = try await container.resolveDirectory(for: TestMemo.self)
            let memoItemSubspace = memoSubspace.subspace(SubspaceKey.items)
            let memoKey = memoItemSubspace.subspace(TestMemo.persistableType).pack(Tuple([memo.id]))

            let memoData = try await database.withTransaction { transaction in
                return try await transaction.getValue(for: memoKey, snapshot: true)
            }
            #expect(memoData != nil, "Memo should exist in its directory")

            // Verify memo does NOT exist in polymorphic directory
            // (TestMemo doesn't conform to TestDocument, so no dual-write)
            // Use TestArticle to resolve the polymorphic directory
            let polySubspace = try await container.resolvePolymorphicDirectory(for: TestArticle.self)
            let polyItemSubspace = polySubspace.subspace(SubspaceKey.items)

            // Scan all type codes - memo should not be there
            var polyDataFound = false
            try await database.withTransaction { transaction in
                let (begin, end) = polyItemSubspace.range()
                for try await (key, _) in transaction.getRange(begin: begin, end: end, snapshot: true) {
                    // Check if any key contains memo.id
                    let keyString = String(describing: key)
                    if keyString.contains(memo.id) {
                        polyDataFound = true
                    }
                }
            }
            #expect(!polyDataFound, "Non-polymorphic memo should not be in polymorphic directory")

            // Clean up
            context.delete(memo)
            try await context.save()
        }
    }

    @Test("Report without own directory still writes to polymorphic directory")
    func testReportSingleWriteToPolymorphicDirectory() async throws {
        try await FDBTestSetup.shared.withSerializedAccess {
            let database = try FDBClient.openDatabase()
            let schema = Schema([TestArticle.self, TestReport.self])
            let container = FDBContainer(database: database, schema: schema, security: .disabled)
            let context = container.newContext()

            // Create report (no own directory - single write to polymorphic directory)
            let report = TestReport(title: "Single Write Report", data: Data("Data".utf8))
            context.insert(report)
            try await context.save()

            // Verify data exists in type-specific directory
            let typeSubspace = try await container.resolveDirectory(for: TestReport.self)
            let typeItemSubspace = typeSubspace.subspace(SubspaceKey.items)
            let typeKey = typeItemSubspace.subspace(TestReport.persistableType).pack(Tuple([report.id]))

            let typeData = try await database.withTransaction { transaction in
                return try await transaction.getValue(for: typeKey, snapshot: true)
            }
            #expect(typeData != nil, "Report should exist in type-specific directory")

            // Verify data exists in polymorphic directory via dual-write
            // (TestReport has different directoryPathComponents from polymorphicDirectoryPathComponents)
            // Use TestReport to resolve the polymorphic directory
            let polySubspace = try await container.resolvePolymorphicDirectory(for: TestReport.self)
            let polyItemSubspace = polySubspace.subspace(SubspaceKey.items)
            let typeCode = TestReport.typeCode(for: TestReport.persistableType)
            let polyKey = polyItemSubspace.subspace(Tuple([typeCode])).pack(Tuple([report.id]))

            let polyData = try await database.withTransaction { transaction in
                return try await transaction.getValue(for: polyKey, snapshot: true)
            }
            #expect(polyData != nil, "Report should also exist in polymorphic directory via dual-write")

            // Clean up
            context.delete(report)
            try await context.save()
        }
    }
}
