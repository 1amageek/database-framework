#if FOUNDATION_DB
import Testing
import Foundation
import Core
import StorageKit
import TestSupport
import DatabaseEngine
import ScalarIndex
import AggregationIndex

struct PlainBenchmarkRecord: Persistable {
    typealias ID = String

    var id: String
    var runID: String
    var category: String
    var age: Int
    var score: Double

    init(
        id: String = UUID().uuidString,
        runID: String = "",
        category: String = "",
        age: Int = 0,
        score: Double = 0
    ) {
        self.id = id
        self.runID = runID
        self.category = category
        self.age = age
        self.score = score
    }

    static var persistableType: String { "PlainBenchmarkRecord" }

    static var allFields: [String] {
        ["id", "runID", "category", "age", "score"]
    }

    static var directoryPathComponents: [any DirectoryPathElement] {
        [Path("test"), Path("performance"), Path("plain-records")]
    }

    subscript(dynamicMember member: String) -> (any Sendable)? {
        switch member {
        case "id": return id
        case "runID": return runID
        case "category": return category
        case "age": return age
        case "score": return score
        default: return nil
        }
    }

    static func fieldName<Value>(for keyPath: KeyPath<PlainBenchmarkRecord, Value>) -> String {
        switch keyPath {
        case \PlainBenchmarkRecord.id: return "id"
        case \PlainBenchmarkRecord.runID: return "runID"
        case \PlainBenchmarkRecord.category: return "category"
        case \PlainBenchmarkRecord.age: return "age"
        case \PlainBenchmarkRecord.score: return "score"
        default: return "\(keyPath)"
        }
    }

    static func fieldName(for keyPath: PartialKeyPath<PlainBenchmarkRecord>) -> String {
        switch keyPath {
        case \PlainBenchmarkRecord.id: return "id"
        case \PlainBenchmarkRecord.runID: return "runID"
        case \PlainBenchmarkRecord.category: return "category"
        case \PlainBenchmarkRecord.age: return "age"
        case \PlainBenchmarkRecord.score: return "score"
        default: return "\(keyPath)"
        }
    }

    static func fieldName(for keyPath: AnyKeyPath) -> String {
        if let partial = keyPath as? PartialKeyPath<PlainBenchmarkRecord> {
            return fieldName(for: partial)
        }
        return "\(keyPath)"
    }
}

struct SingleIndexBenchmarkRecord: Persistable {
    typealias ID = String

    var id: String
    var runID: String
    var category: String
    var age: Int
    var score: Double

    init(
        id: String = UUID().uuidString,
        runID: String = "",
        category: String = "",
        age: Int = 0,
        score: Double = 0
    ) {
        self.id = id
        self.runID = runID
        self.category = category
        self.age = age
        self.score = score
    }

    static var persistableType: String { "SingleIndexBenchmarkRecord" }

    static var allFields: [String] {
        ["id", "runID", "category", "age", "score"]
    }

    static var directoryPathComponents: [any DirectoryPathElement] {
        [Path("test"), Path("performance"), Path("single-index-records")]
    }

    static var _persistableDescriptors: [any Descriptor] {
        [
            IndexDescriptor(
                name: "single_category",
                keyPaths: [\SingleIndexBenchmarkRecord.runID, \SingleIndexBenchmarkRecord.category],
                kind: ScalarIndexKind<SingleIndexBenchmarkRecord>(fields: [\.runID, \.category])
            )
        ]
    }

    subscript(dynamicMember member: String) -> (any Sendable)? {
        switch member {
        case "id": return id
        case "runID": return runID
        case "category": return category
        case "age": return age
        case "score": return score
        default: return nil
        }
    }

    static func fieldName<Value>(for keyPath: KeyPath<SingleIndexBenchmarkRecord, Value>) -> String {
        switch keyPath {
        case \SingleIndexBenchmarkRecord.id: return "id"
        case \SingleIndexBenchmarkRecord.runID: return "runID"
        case \SingleIndexBenchmarkRecord.category: return "category"
        case \SingleIndexBenchmarkRecord.age: return "age"
        case \SingleIndexBenchmarkRecord.score: return "score"
        default: return "\(keyPath)"
        }
    }

    static func fieldName(for keyPath: PartialKeyPath<SingleIndexBenchmarkRecord>) -> String {
        switch keyPath {
        case \SingleIndexBenchmarkRecord.id: return "id"
        case \SingleIndexBenchmarkRecord.runID: return "runID"
        case \SingleIndexBenchmarkRecord.category: return "category"
        case \SingleIndexBenchmarkRecord.age: return "age"
        case \SingleIndexBenchmarkRecord.score: return "score"
        default: return "\(keyPath)"
        }
    }

    static func fieldName(for keyPath: AnyKeyPath) -> String {
        if let partial = keyPath as? PartialKeyPath<SingleIndexBenchmarkRecord> {
            return fieldName(for: partial)
        }
        return "\(keyPath)"
    }
}

struct TripleIndexBenchmarkRecord: Persistable {
    typealias ID = String

    var id: String
    var runID: String
    var category: String
    var age: Int
    var score: Double

    init(
        id: String = UUID().uuidString,
        runID: String = "",
        category: String = "",
        age: Int = 0,
        score: Double = 0
    ) {
        self.id = id
        self.runID = runID
        self.category = category
        self.age = age
        self.score = score
    }

    static var persistableType: String { "TripleIndexBenchmarkRecord" }

    static var allFields: [String] {
        ["id", "runID", "category", "age", "score"]
    }

    static var directoryPathComponents: [any DirectoryPathElement] {
        [Path("test"), Path("performance"), Path("triple-index-records")]
    }

    static var _persistableDescriptors: [any Descriptor] {
        [
            IndexDescriptor(
                name: "triple_category",
                keyPaths: [\TripleIndexBenchmarkRecord.runID, \TripleIndexBenchmarkRecord.category],
                kind: ScalarIndexKind<TripleIndexBenchmarkRecord>(fields: [\.runID, \.category])
            ),
            IndexDescriptor(
                name: "triple_age",
                keyPaths: [\TripleIndexBenchmarkRecord.runID, \TripleIndexBenchmarkRecord.age],
                kind: ScalarIndexKind<TripleIndexBenchmarkRecord>(fields: [\.runID, \.age])
            ),
            IndexDescriptor(
                name: "triple_score_by_category",
                keyPaths: [\TripleIndexBenchmarkRecord.runID, \TripleIndexBenchmarkRecord.category],
                kind: SumIndexKind<TripleIndexBenchmarkRecord, Double>(groupBy: [\.runID, \.category], value: \.score)
            )
        ]
    }

    subscript(dynamicMember member: String) -> (any Sendable)? {
        switch member {
        case "id": return id
        case "runID": return runID
        case "category": return category
        case "age": return age
        case "score": return score
        default: return nil
        }
    }

    static func fieldName<Value>(for keyPath: KeyPath<TripleIndexBenchmarkRecord, Value>) -> String {
        switch keyPath {
        case \TripleIndexBenchmarkRecord.id: return "id"
        case \TripleIndexBenchmarkRecord.runID: return "runID"
        case \TripleIndexBenchmarkRecord.category: return "category"
        case \TripleIndexBenchmarkRecord.age: return "age"
        case \TripleIndexBenchmarkRecord.score: return "score"
        default: return "\(keyPath)"
        }
    }

    static func fieldName(for keyPath: PartialKeyPath<TripleIndexBenchmarkRecord>) -> String {
        switch keyPath {
        case \TripleIndexBenchmarkRecord.id: return "id"
        case \TripleIndexBenchmarkRecord.runID: return "runID"
        case \TripleIndexBenchmarkRecord.category: return "category"
        case \TripleIndexBenchmarkRecord.age: return "age"
        case \TripleIndexBenchmarkRecord.score: return "score"
        default: return "\(keyPath)"
        }
    }

    static func fieldName(for keyPath: AnyKeyPath) -> String {
        if let partial = keyPath as? PartialKeyPath<TripleIndexBenchmarkRecord> {
            return fieldName(for: partial)
        }
        return "\(keyPath)"
    }
}

private struct IndexedBenchmarkContext: Sendable {
    let engine: any StorageEngine
    let container: DBContainer

    let plainRunID: String
    let singleRunID: String
    let tripleRunID: String

    init() async throws {
        self.engine = try await FDBTestSetup.shared.makeEngine()
        self.plainRunID = "plain-\(UUID().uuidString.prefix(8))"
        self.singleRunID = "single-\(UUID().uuidString.prefix(8))"
        self.tripleRunID = "triple-\(UUID().uuidString.prefix(8))"

        let schema = Schema(
            [PlainBenchmarkRecord.self, SingleIndexBenchmarkRecord.self, TripleIndexBenchmarkRecord.self],
            version: .init(1, 0, 0)
        )
        self.container = try await DBContainer(
            for: schema,
            configuration: .init(backend: .custom(engine)),
            security: .disabled
        )
        try await cleanup()
    }

    func cleanup() async throws {
        for path in [
            ["test", "performance", "plain-records"],
            ["test", "performance", "single-index-records"],
            ["test", "performance", "triple-index-records"],
        ] {
            do {
                try await engine.directoryService.remove(path: path)
            } catch {
                // Ignore missing directory when the strategy did not materialize it.
            }
        }
    }

    func makePlainRecord(seed: Int) -> PlainBenchmarkRecord {
        let suffix = String(UUID().uuidString.prefix(6))
        let id = "plain-\(seed)-\(suffix)"
        let category = seed.isMultiple(of: 10) ? "hot" : "cold-\(seed % 6)"
        return PlainBenchmarkRecord(
            id: id,
            runID: plainRunID,
            category: category,
            age: 18 + (seed % 50),
            score: Double(100 + (seed % 200))
        )
    }

    func makeSingleIndexRecord(seed: Int) -> SingleIndexBenchmarkRecord {
        let suffix = String(UUID().uuidString.prefix(6))
        let id = "single-\(seed)-\(suffix)"
        let category = seed.isMultiple(of: 10) ? "hot" : "cold-\(seed % 6)"
        return SingleIndexBenchmarkRecord(
            id: id,
            runID: singleRunID,
            category: category,
            age: 18 + (seed % 50),
            score: Double(100 + (seed % 200))
        )
    }

    func makeTripleIndexRecord(seed: Int) -> TripleIndexBenchmarkRecord {
        let suffix = String(UUID().uuidString.prefix(6))
        let id = "triple-\(seed)-\(suffix)"
        let category = seed.isMultiple(of: 10) ? "hot" : "cold-\(seed % 6)"
        return TripleIndexBenchmarkRecord(
            id: id,
            runID: tripleRunID,
            category: category,
            age: 18 + (seed % 50),
            score: Double(100 + (seed % 200))
        )
    }

    func insertPlain(_ record: PlainBenchmarkRecord) async throws {
        let store = try await container.store(for: PlainBenchmarkRecord.self)
        try await store.executeBatch(inserts: [record], deletes: [])
    }

    func insertSingle(_ record: SingleIndexBenchmarkRecord) async throws {
        let store = try await container.store(for: SingleIndexBenchmarkRecord.self)
        try await store.executeBatch(inserts: [record], deletes: [])
    }

    func insertTriple(_ record: TripleIndexBenchmarkRecord) async throws {
        let store = try await container.store(for: TripleIndexBenchmarkRecord.self)
        try await store.executeBatch(inserts: [record], deletes: [])
    }

    func indexedLookup(category: String) async throws -> [SingleIndexBenchmarkRecord] {
        try await FDBContext(container: container)
            .fetch(SingleIndexBenchmarkRecord.self)
            .where(\.runID == singleRunID)
            .where(\.category == category)
            .execute()
    }

    func scannedLookup(category: String) async throws -> [PlainBenchmarkRecord] {
        let all = try await FDBContext(container: container)
            .fetch(PlainBenchmarkRecord.self)
            .execute()
        return all.filter { $0.runID == plainRunID && $0.category == category }
    }
}

private func withIndexedBenchmarkContext<T: Sendable>(
    _ body: (IndexedBenchmarkContext) async throws -> T
) async throws -> T {
    let context = try await IndexedBenchmarkContext()
    do {
        let result = try await body(context)
        try await context.cleanup()
        return result
    } catch {
        do {
            try await context.cleanup()
        } catch {
            print("IndexedBenchmark cleanup failed: \(error)")
        }
        throw error
    }
}

@Suite("Indexed Query And Write Benchmarks", .tags(.fdb, .performance), .serialized, .heartbeat)
struct IndexedQueryAndWriteBenchmarkTests {
    private let queryDatasetSize = 2_000

    @Test("write amplification across index counts", .timeLimit(.minutes(1)))
    func testWriteAmplificationAcrossIndexCounts() async throws {
        try await FDBTestSetup.shared.withSerializedAccess {
            try await withIndexedBenchmarkContext { context in
                var plainSeed = 0
                let plainMeasurement = try await measureBenchmark(name: "0 indexes") {
                    plainSeed += 1
                    try await context.insertPlain(context.makePlainRecord(seed: plainSeed))
                }

                var singleSeed = 0
                let singleMeasurement = try await measureBenchmark(name: "1 index") {
                    singleSeed += 1
                    try await context.insertSingle(context.makeSingleIndexRecord(seed: singleSeed))
                }

                var tripleSeed = 0
                let tripleMeasurement = try await measureBenchmark(name: "3 indexes") {
                    tripleSeed += 1
                    try await context.insertTriple(context.makeTripleIndexRecord(seed: tripleSeed))
                }

                let measurements = [plainMeasurement, singleMeasurement, tripleMeasurement]
                printBenchmarkReport(title: "Indexed Write Amplification", measurements: measurements)

                #expect(measurements.allSatisfy { $0.opsPerSecond > 0 })
                #expect(measurements.allSatisfy { !$0.samplesMs.isEmpty })
            }
        }
    }

    @Test("indexed equality query versus full scan", .timeLimit(.minutes(1)))
    func testIndexedEqualityQueryVersusFullScan() async throws {
        try await FDBTestSetup.shared.withSerializedAccess {
            try await withIndexedBenchmarkContext { context in
                for seed in 0..<queryDatasetSize {
                    try await context.insertPlain(context.makePlainRecord(seed: seed))
                    try await context.insertSingle(context.makeSingleIndexRecord(seed: seed))
                }

                let expectedIndexed = try await context.indexedLookup(category: "hot")
                let expectedScanned = try await context.scannedLookup(category: "hot")

                #expect(expectedIndexed.count == expectedScanned.count)
                #expect(!expectedIndexed.isEmpty)

                let scannedMeasurement = try await measureBenchmark(name: "Full scan + filter", measurementIterations: 10) {
                    let records = try await context.scannedLookup(category: "hot")
                    #expect(records.count == expectedScanned.count)
                }

                let indexedMeasurement = try await measureBenchmark(name: "ScalarIndex equality", measurementIterations: 10) {
                    let records = try await context.indexedLookup(category: "hot")
                    #expect(records.count == expectedIndexed.count)
                }

                printBenchmarkReport(
                    title: "Indexed Equality Query Versus Full Scan",
                    measurements: [scannedMeasurement, indexedMeasurement]
                )

                #expect(scannedMeasurement.opsPerSecond > 0)
                #expect(indexedMeasurement.opsPerSecond > 0)
            }
        }
    }
}
#endif
