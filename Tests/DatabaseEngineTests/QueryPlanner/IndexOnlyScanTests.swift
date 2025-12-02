// IndexOnlyScanTests.swift
// Tests for Index-Only Scan (Covering Index) functionality

import Testing
import Foundation
import FoundationDB
@testable import DatabaseEngine
@testable import ScalarIndex
@testable import Core

// MARK: - Test Model for Index-Only Scan

/// Simple user model that conforms to both Persistable and Codable
/// for testing IndexEntryDecoder
struct IOSTestUser: Persistable, Codable, Sendable {
    typealias ID = String

    var id: String
    var name: String
    var email: String
    var age: Int

    init(id: String = UUID().uuidString, name: String, email: String, age: Int) {
        self.id = id
        self.name = name
        self.email = email
        self.age = age
    }

    static var persistableType: String { "IOSTestUser" }

    static var allFields: [String] {
        ["id", "name", "email", "age"]
    }

    static func fieldNumber(for fieldName: String) -> Int? { nil }
    static func enumMetadata(for fieldName: String) -> EnumMetadata? { nil }

    subscript(dynamicMember member: String) -> (any Sendable)? {
        switch member {
        case "id": return id
        case "name": return name
        case "email": return email
        case "age": return age
        default: return nil
        }
    }

    static func fieldName<Value>(for keyPath: KeyPath<IOSTestUser, Value>) -> String {
        switch keyPath {
        case \IOSTestUser.id: return "id"
        case \IOSTestUser.name: return "name"
        case \IOSTestUser.email: return "email"
        case \IOSTestUser.age: return "age"
        default: return "\(keyPath)"
        }
    }

    static func fieldName(for keyPath: PartialKeyPath<IOSTestUser>) -> String {
        switch keyPath {
        case \IOSTestUser.id: return "id"
        case \IOSTestUser.name: return "name"
        case \IOSTestUser.email: return "email"
        case \IOSTestUser.age: return "age"
        default: return "\(keyPath)"
        }
    }

    static func fieldName(for keyPath: AnyKeyPath) -> String {
        if let partial = keyPath as? PartialKeyPath<IOSTestUser> {
            return fieldName(for: partial)
        }
        return "\(keyPath)"
    }
}

// MARK: - CoveringIndexMetadata Tests

@Suite("CoveringIndexMetadata Tests")
struct CoveringIndexMetadataTests {

    @Test("Build metadata for fully covering index")
    func testBuildMetadataFullyCovering() {
        // Create an index that covers ALL fields of IOSTestUser
        // Fields: id, name, email, age
        // Index keys: name, email, age (+ id is always included)
        let scalarKind = ScalarIndexKind()
        let coveringIndex = IndexDescriptor(
            name: "idx_covering",
            keyPaths: [\IOSTestUser.name, \IOSTestUser.email, \IOSTestUser.age],
            kind: scalarKind
        )

        let metadata = CoveringIndexMetadata.build(for: coveringIndex, type: IOSTestUser.self)

        #expect(metadata.keyFields == ["name", "email", "age"])
        #expect(metadata.isFullyCovering == true)
        #expect(metadata.allFields.contains("id"))
        #expect(metadata.allFields.contains("name"))
        #expect(metadata.allFields.contains("email"))
        #expect(metadata.allFields.contains("age"))
    }

    @Test("Build metadata for partial covering index")
    func testBuildMetadataPartialCovering() {
        // Create an index that only covers some fields
        let scalarKind = ScalarIndexKind()
        let partialIndex = IndexDescriptor(
            name: "idx_partial",
            keyPaths: [\IOSTestUser.email],
            kind: scalarKind
        )

        let metadata = CoveringIndexMetadata.build(for: partialIndex, type: IOSTestUser.self)

        #expect(metadata.keyFields == ["email"])
        #expect(metadata.isFullyCovering == false)
        #expect(metadata.allFields.contains("id"))
        #expect(metadata.allFields.contains("email"))
        #expect(!metadata.allFields.contains("name"))
        #expect(!metadata.allFields.contains("age"))
    }

    @Test("All fields includes id automatically")
    func testAllFieldsIncludesId() {
        let scalarKind = ScalarIndexKind()
        let index = IndexDescriptor(
            name: "idx_test",
            keyPaths: [\IOSTestUser.name],
            kind: scalarKind
        )

        let metadata = CoveringIndexMetadata.build(for: index, type: IOSTestUser.self)

        #expect(metadata.allFields.contains("id"))
    }
}

// MARK: - IndexEntryDecoder Tests

@Suite("IndexEntryDecoder Tests")
struct IndexEntryDecoderTests {

    @Test("Decode record from index entry with all fields")
    func testDecodeFullRecord() throws {
        // Create covering index metadata
        let metadata = CoveringIndexMetadata(
            keyFields: ["name", "email", "age"],
            storedFields: [],
            isFullyCovering: true
        )

        let decoder = IndexEntryDecoder<IOSTestUser>(metadata: metadata)

        // Create an index entry with all field values
        let entry = IndexEntry(
            itemID: Tuple(["user-123" as any TupleElement]),
            keyValues: Tuple([
                "John Doe" as any TupleElement,   // name
                "john@example.com" as any TupleElement,  // email
                30 as any TupleElement            // age
            ])
        )

        let user = try decoder.decode(from: entry)

        #expect(user.id == "user-123")
        #expect(user.name == "John Doe")
        #expect(user.email == "john@example.com")
        #expect(user.age == 30)
    }

    @Test("Decoder reports correct coverage status")
    func testDecoderCoverageStatus() {
        let fullyCoveringMetadata = CoveringIndexMetadata(
            keyFields: ["name", "email", "age"],
            storedFields: [],
            isFullyCovering: true
        )

        let partialMetadata = CoveringIndexMetadata(
            keyFields: ["email"],
            storedFields: [],
            isFullyCovering: false
        )

        let fullDecoder = IndexEntryDecoder<IOSTestUser>(metadata: fullyCoveringMetadata)
        let partialDecoder = IndexEntryDecoder<IOSTestUser>(metadata: partialMetadata)

        #expect(fullDecoder.canFullyDecode == true)
        #expect(partialDecoder.canFullyDecode == false)
    }

    @Test("Decode handles Int conversion correctly")
    func testDecodeIntConversion() throws {
        let metadata = CoveringIndexMetadata(
            keyFields: ["name", "email", "age"],
            storedFields: [],
            isFullyCovering: true
        )

        let decoder = IndexEntryDecoder<IOSTestUser>(metadata: metadata)

        // Test with Int64 value (common from databases)
        let entry = IndexEntry(
            itemID: Tuple(["user-456" as any TupleElement]),
            keyValues: Tuple([
                "Jane" as any TupleElement,
                "jane@example.com" as any TupleElement,
                Int64(25) as any TupleElement  // Int64 instead of Int
            ])
        )

        let user = try decoder.decode(from: entry)

        #expect(user.age == 25)
    }
}

// MARK: - IndexOnlyScanAnalyzer Tests

@Suite("IndexOnlyScanAnalyzer Tests")
struct IndexOnlyScanAnalyzerTests {

    @Test("Analyzer detects fully covering index")
    func testAnalyzerDetectsFullyCovering() throws {
        let analyzer = IndexOnlyScanAnalyzer<IOSTestUser>()
        let queryAnalyzer = QueryAnalyzer<IOSTestUser>()

        // Create a covering index (all fields in key)
        let scalarKind = ScalarIndexKind()
        let coveringIndex = IndexDescriptor(
            name: "idx_covering",
            keyPaths: [\IOSTestUser.name, \IOSTestUser.email, \IOSTestUser.age],
            kind: scalarKind
        )

        var query = Query<IOSTestUser>()
        query = query.where(\IOSTestUser.name == "John")
        let analysis = try queryAnalyzer.analyze(query)

        let result = analyzer.analyze(
            query: query,
            analysis: analysis,
            index: coveringIndex
        )

        #expect(result.canUseIndexOnlyScan == true)
        #expect(result.metadata.isFullyCovering == true)
        #expect(result.uncoveredFields.isEmpty)
    }

    @Test("Analyzer detects partial covering index")
    func testAnalyzerDetectsPartialCovering() throws {
        let analyzer = IndexOnlyScanAnalyzer<IOSTestUser>()
        let queryAnalyzer = QueryAnalyzer<IOSTestUser>()

        // Create a partial index (only some fields)
        let scalarKind = ScalarIndexKind()
        let partialIndex = IndexDescriptor(
            name: "idx_email_only",
            keyPaths: [\IOSTestUser.email],
            kind: scalarKind
        )

        var query = Query<IOSTestUser>()
        query = query.where(\IOSTestUser.email == "test@example.com")
        let analysis = try queryAnalyzer.analyze(query)

        let result = analyzer.analyze(
            query: query,
            analysis: analysis,
            index: partialIndex
        )

        #expect(result.canUseIndexOnlyScan == false)
        #expect(result.metadata.isFullyCovering == false)
        #expect(result.uncoveredFields.contains("name"))
        #expect(result.uncoveredFields.contains("age"))
    }

    @Test("Analyzer provides estimated savings for covering index")
    func testAnalyzerEstimatedSavings() throws {
        let analyzer = IndexOnlyScanAnalyzer<IOSTestUser>()
        let queryAnalyzer = QueryAnalyzer<IOSTestUser>()

        let scalarKind = ScalarIndexKind()
        let coveringIndex = IndexDescriptor(
            name: "idx_covering",
            keyPaths: [\IOSTestUser.name, \IOSTestUser.email, \IOSTestUser.age],
            kind: scalarKind
        )

        let query = Query<IOSTestUser>()
        let analysis = try queryAnalyzer.analyze(query)

        let result = analyzer.analyze(
            query: query,
            analysis: analysis,
            index: coveringIndex
        )

        // Covering index should have significant savings
        #expect(result.estimatedSavings > 0.5)
    }
}

// MARK: - IndexOnlyScanOperator Tests

@Suite("IndexOnlyScanOperator Tests")
struct IndexOnlyScanOperatorTests {

    @Test("Operator stores metadata correctly")
    func testOperatorStoresMetadata() {
        let metadata = CoveringIndexMetadata(
            keyFields: ["name", "email", "age"],
            storedFields: [],
            isFullyCovering: true
        )

        let scalarKind = ScalarIndexKind()
        let index = IndexDescriptor(
            name: "idx_test",
            keyPaths: [\IOSTestUser.name],
            kind: scalarKind
        )

        let bounds = IndexScanBounds(
            start: [.init(value: AnySendable("A"), inclusive: true)],
            end: [.init(value: AnySendable("Z"), inclusive: true)]
        )

        let op = IndexOnlyScanOperator<IOSTestUser>(
            index: index,
            metadata: metadata,
            bounds: bounds,
            reverse: false,
            projectedFields: Set(["name", "email", "age", "id"]),
            satisfiedConditions: [],
            estimatedEntries: 100
        )

        #expect(op.metadata.isFullyCovering == true)
        #expect(op.metadata.keyFields == ["name", "email", "age"])
        #expect(op.estimatedEntries == 100)
    }
}

// MARK: - CoveringIndexSuggester Tests

@Suite("CoveringIndexSuggester Tests")
struct CoveringIndexSuggesterTests {

    @Test("Suggester returns nil when covering index exists")
    func testSuggesterReturnsNilWhenCoveringExists() throws {
        let suggester = CoveringIndexSuggester<IOSTestUser>()
        let queryAnalyzer = QueryAnalyzer<IOSTestUser>()

        // Create a covering index
        let scalarKind = ScalarIndexKind()
        let coveringIndex = IndexDescriptor(
            name: "idx_covering",
            keyPaths: [\IOSTestUser.name, \IOSTestUser.email, \IOSTestUser.age],
            kind: scalarKind
        )

        var query = Query<IOSTestUser>()
        query = query.where(\IOSTestUser.name == "John")
        let analysis = try queryAnalyzer.analyze(query)

        let suggestion = suggester.suggest(
            query: query,
            analysis: analysis,
            existingIndexes: [coveringIndex]
        )

        #expect(suggestion == nil)
    }

    @Test("Suggester recommends extending existing index")
    func testSuggesterRecommendsExtension() throws {
        let suggester = CoveringIndexSuggester<IOSTestUser>()
        let queryAnalyzer = QueryAnalyzer<IOSTestUser>()

        // Create a partial index
        let scalarKind = ScalarIndexKind()
        let partialIndex = IndexDescriptor(
            name: "idx_email",
            keyPaths: [\IOSTestUser.email],
            kind: scalarKind
        )

        var query = Query<IOSTestUser>()
        query = query.where(\IOSTestUser.email == "test@example.com")
        let analysis = try queryAnalyzer.analyze(query)

        let suggestion = suggester.suggest(
            query: query,
            analysis: analysis,
            existingIndexes: [partialIndex]
        )

        #expect(suggestion != nil)
        #expect(suggestion?.type == .extendExisting)
        #expect(suggestion?.indexName == "idx_email")
        // Should suggest adding missing fields
        #expect(!suggestion!.storedFields.isEmpty)
    }

    @Test("Suggester recommends new index when no suitable exists")
    func testSuggesterRecommendsNewIndex() throws {
        let suggester = CoveringIndexSuggester<IOSTestUser>()
        let queryAnalyzer = QueryAnalyzer<IOSTestUser>()

        var query = Query<IOSTestUser>()
        query = query.where(\IOSTestUser.name == "John")
        let analysis = try queryAnalyzer.analyze(query)

        // No existing indexes
        let suggestion = suggester.suggest(
            query: query,
            analysis: analysis,
            existingIndexes: []
        )

        #expect(suggestion != nil)
        #expect(suggestion?.type == .newIndex)
    }
}

// MARK: - CostEstimator Index-Only Tests

@Suite("CostEstimator Index-Only Scan Tests")
struct CostEstimatorIndexOnlyTests {

    @Test("Index-only scan has zero record fetches")
    func testIndexOnlyScanZeroRecordFetches() throws {
        let statistics = MockStatisticsProvider(rowCount: 10000)
        let costEstimator = CostEstimator<IOSTestUser>(statistics: statistics)
        let queryAnalyzer = QueryAnalyzer<IOSTestUser>()

        let metadata = CoveringIndexMetadata(
            keyFields: ["name", "email", "age"],
            storedFields: [],
            isFullyCovering: true
        )

        let scalarKind = ScalarIndexKind()
        let index = IndexDescriptor(
            name: "idx_covering",
            keyPaths: [\IOSTestUser.name, \IOSTestUser.email, \IOSTestUser.age],
            kind: scalarKind
        )

        let bounds = IndexScanBounds(
            start: [],
            end: []
        )

        let indexOnlyOp = IndexOnlyScanOperator<IOSTestUser>(
            index: index,
            metadata: metadata,
            bounds: bounds,
            reverse: false,
            projectedFields: Set(["id", "name", "email", "age"]),
            satisfiedConditions: [],
            estimatedEntries: 100
        )

        let query = Query<IOSTestUser>()
        let analysis = try queryAnalyzer.analyze(query)

        let cost = costEstimator.estimate(
            plan: .indexOnlyScan(indexOnlyOp),
            analysis: analysis
        )

        // Key assertion: no record fetches for index-only scan
        #expect(cost.recordFetches == 0)
        #expect(cost.indexReads > 0)
    }

    @Test("Index-only scan costs less than regular index scan")
    func testIndexOnlyScanCostsLessThanRegularScan() throws {
        let statistics = MockStatisticsProvider(rowCount: 10000)
        let costEstimator = CostEstimator<IOSTestUser>(statistics: statistics)
        let queryAnalyzer = QueryAnalyzer<IOSTestUser>()

        let scalarKind = ScalarIndexKind()
        let index = IndexDescriptor(
            name: "idx_test",
            keyPaths: [\IOSTestUser.name, \IOSTestUser.email, \IOSTestUser.age],
            kind: scalarKind
        )

        let metadata = CoveringIndexMetadata(
            keyFields: ["name", "email", "age"],
            storedFields: [],
            isFullyCovering: true
        )

        let bounds = IndexScanBounds(start: [], end: [])

        // Create index-only scan operator
        let indexOnlyOp = IndexOnlyScanOperator<IOSTestUser>(
            index: index,
            metadata: metadata,
            bounds: bounds,
            reverse: false,
            projectedFields: Set(["id", "name", "email", "age"]),
            satisfiedConditions: [],
            estimatedEntries: 100
        )

        // Create regular index scan operator
        let indexScanOp = IndexScanOperator<IOSTestUser>(
            index: index,
            bounds: bounds,
            reverse: false,
            satisfiedConditions: [],
            estimatedEntries: 100
        )

        let query = Query<IOSTestUser>()
        let analysis = try queryAnalyzer.analyze(query)

        let indexOnlyCost = costEstimator.estimate(
            plan: .indexOnlyScan(indexOnlyOp),
            analysis: analysis
        )

        let indexScanCost = costEstimator.estimate(
            plan: .indexScan(indexScanOp),
            analysis: analysis
        )

        // Index-only should be cheaper
        #expect(indexOnlyCost.totalCost < indexScanCost.totalCost)
    }
}

// MARK: - PlanEnumerator Index-Only Tests

@Suite("PlanEnumerator Index-Only Scan Tests")
struct PlanEnumeratorIndexOnlyTests {

    @Test("Enumerator generates index-only plan for covering index")
    func testEnumeratorGeneratesIndexOnlyPlan() throws {
        let statistics = MockStatisticsProvider(rowCount: 10000)

        // Create a covering index (all fields)
        let scalarKind = ScalarIndexKind()
        let coveringIndex = IndexDescriptor(
            name: "idx_covering",
            keyPaths: [\IOSTestUser.name, \IOSTestUser.email, \IOSTestUser.age],
            kind: scalarKind
        )

        let enumerator = PlanEnumerator<IOSTestUser>(
            indexes: [coveringIndex],
            statistics: statistics
        )

        let queryAnalyzer = QueryAnalyzer<IOSTestUser>()
        var query = Query<IOSTestUser>()
        query = query.where(\IOSTestUser.name == "John")
        let analysis = try queryAnalyzer.analyze(query)

        let candidates = enumerator.enumerate(analysis: analysis)

        // Should have an index-only scan candidate
        let hasIndexOnlyScan = candidates.contains { plan in
            if case .indexOnlyScan = plan {
                return true
            } else if case .filter(let filterOp) = plan,
                      case .indexOnlyScan = filterOp.input {
                return true
            }
            return false
        }

        #expect(hasIndexOnlyScan)
    }

    @Test("Enumerator does not generate index-only plan for partial index")
    func testEnumeratorDoesNotGenerateIndexOnlyForPartial() throws {
        let statistics = MockStatisticsProvider(rowCount: 10000)

        // Create a partial index (only one field)
        let scalarKind = ScalarIndexKind()
        let partialIndex = IndexDescriptor(
            name: "idx_email",
            keyPaths: [\IOSTestUser.email],
            kind: scalarKind
        )

        let enumerator = PlanEnumerator<IOSTestUser>(
            indexes: [partialIndex],
            statistics: statistics
        )

        let queryAnalyzer = QueryAnalyzer<IOSTestUser>()
        var query = Query<IOSTestUser>()
        query = query.where(\IOSTestUser.email == "test@example.com")
        let analysis = try queryAnalyzer.analyze(query)

        let candidates = enumerator.enumerate(analysis: analysis)

        // Should NOT have an index-only scan candidate
        let hasIndexOnlyScan = candidates.contains { plan in
            if case .indexOnlyScan = plan { return true }
            return false
        }

        #expect(!hasIndexOnlyScan)
    }
}

// MARK: - Mock Statistics Provider

private struct MockStatisticsProvider: StatisticsProvider {
    let rowCount: Int

    func estimatedRowCount<T: Persistable>(for type: T.Type) -> Int {
        rowCount
    }

    func estimatedDistinctValues<T: Persistable>(field: String, type: T.Type) -> Int? {
        rowCount / 10
    }

    func equalitySelectivity<T: Persistable>(field: String, type: T.Type) -> Double? {
        0.01
    }

    func rangeSelectivity<T: Persistable>(field: String, range: RangeBound, type: T.Type) -> Double? {
        0.3
    }

    func nullSelectivity<T: Persistable>(field: String, type: T.Type) -> Double? {
        0.05
    }

    func estimatedIndexEntries(index: IndexDescriptor) -> Int? {
        rowCount
    }
}

// MARK: - Mock Storage Reader

/// A mock execution reader for testing PlanExecutor without FDB dependency
///
/// **Usage**:
/// ```swift
/// let mockContext = MockExecutionContext<IOSTestUser>()
/// mockContext.items = [user1, user2, user3]
/// mockContext.indexEntries["idx_name"] = [entry1, entry2]
///
/// let executor = PlanExecutor<IOSTestUser>(context: context, executionContext: mockContext)
/// let results = try await executor.execute(plan: plan)
/// ```
final class MockExecutionContext<T: Persistable & Codable>: QueryExecutionContext, @unchecked Sendable {
    /// Items to return from scanRecords/fetchItem
    var items: [T] = []

    /// Index entries to return from IndexSearcher (by index name)
    var indexEntries: [String: [IndexEntry]] = [:]

    /// Mock storage reader for index access
    private(set) lazy var storageReader: StorageReader = MockStorageReader(context: self)

    func scanRecords<R: Persistable & Codable>(type: R.Type) async throws -> [R] {
        guard R.self == T.self else { return [] }
        return items as! [R]
    }

    func streamRecords<R: Persistable & Codable>(type: R.Type) -> AsyncThrowingStream<R, Error> {
        let items = self.items
        return AsyncThrowingStream { continuation in
            Task {
                guard R.self == T.self else {
                    continuation.finish()
                    return
                }
                for item in items {
                    continuation.yield(item as! R)
                }
                continuation.finish()
            }
        }
    }

    func fetchItem<R: Persistable & Codable>(id: Tuple, type: R.Type) async throws -> R? {
        guard R.self == T.self else { return nil }
        // Find item by ID using first element of tuple
        guard let idValue = id[0] else { return nil }
        let idString = "\(idValue)"
        for item in items {
            if "\(item.id)" == idString {
                return item as? R
            }
        }
        return nil
    }
}

/// Mock storage reader that provides index subspace and scan operations
final class MockStorageReader: StorageReader, @unchecked Sendable {
    private let _indexSubspace: Subspace

    init(context: any Sendable) {
        // Create a simple subspace for testing
        self._indexSubspace = Subspace(prefix: [0x02])
    }

    var indexSubspace: Subspace {
        _indexSubspace
    }

    func fetchItem<T: Persistable & Codable>(id: any TupleElement, type: T.Type) async throws -> T? {
        nil
    }

    func scanItems<T: Persistable & Codable>(type: T.Type) -> AsyncThrowingStream<T, Error> {
        AsyncThrowingStream { $0.finish() }
    }

    func scanRange(
        subspace: Subspace,
        start: Tuple?,
        end: Tuple?,
        startInclusive: Bool,
        endInclusive: Bool,
        reverse: Bool
    ) -> AsyncThrowingStream<(key: [UInt8], value: [UInt8]), Error> {
        AsyncThrowingStream { $0.finish() }
    }

    func getValue(key: [UInt8]) async throws -> [UInt8]? {
        nil
    }
}
