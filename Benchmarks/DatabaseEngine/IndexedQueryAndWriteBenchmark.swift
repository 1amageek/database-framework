#if FOUNDATION_DB
import Testing
import Foundation
import Core
import DatabaseValue
import StorageKit
import TestSupport
import DatabaseEngine
import DatabaseRuntime
import ScalarIndex
import AggregationIndex

struct PlainBenchmarkEntity: Persistable {
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

    static var persistableType: String { "PlainBenchmarkEntity" }

    static var allFields: [String] {
        ["id", "runID", "category", "age", "score"]
    }

    static var directoryPathComponents: [DirectoryPathComponent] {
        [.staticPath("test"), .staticPath("performance"), .staticPath("plain-entities")]
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

    static func fieldName<Value>(for keyPath: KeyPath<PlainBenchmarkEntity, Value>) -> String {
        switch keyPath {
        case \PlainBenchmarkEntity.id: return "id"
        case \PlainBenchmarkEntity.runID: return "runID"
        case \PlainBenchmarkEntity.category: return "category"
        case \PlainBenchmarkEntity.age: return "age"
        case \PlainBenchmarkEntity.score: return "score"
        default: return "\(keyPath)"
        }
    }

    static func fieldName(for keyPath: PartialKeyPath<PlainBenchmarkEntity>) -> String {
        switch keyPath {
        case \PlainBenchmarkEntity.id: return "id"
        case \PlainBenchmarkEntity.runID: return "runID"
        case \PlainBenchmarkEntity.category: return "category"
        case \PlainBenchmarkEntity.age: return "age"
        case \PlainBenchmarkEntity.score: return "score"
        default: return "\(keyPath)"
        }
    }

    static func fieldName(for keyPath: AnyKeyPath) -> String {
        if let partial = keyPath as? PartialKeyPath<PlainBenchmarkEntity> {
            return fieldName(for: partial)
        }
        return "\(keyPath)"
    }
}

struct SingleIndexBenchmarkEntity: Persistable {
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

    static var persistableType: String { "SingleIndexBenchmarkEntity" }

    static var allFields: [String] {
        ["id", "runID", "category", "age", "score"]
    }

    static var directoryPathComponents: [DirectoryPathComponent] {
        [.staticPath("test"), .staticPath("performance"), .staticPath("single-index-entities")]
    }

    static var _persistableDescriptors: [any Descriptor] {
        [
            IndexDescriptor(
                name: "single_category",
                keyPaths: [\SingleIndexBenchmarkEntity.runID, \SingleIndexBenchmarkEntity.category],
                kind: ScalarIndexKind<SingleIndexBenchmarkEntity>(fields: [\.runID, \.category])
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

    static func fieldName<Value>(for keyPath: KeyPath<SingleIndexBenchmarkEntity, Value>) -> String {
        switch keyPath {
        case \SingleIndexBenchmarkEntity.id: return "id"
        case \SingleIndexBenchmarkEntity.runID: return "runID"
        case \SingleIndexBenchmarkEntity.category: return "category"
        case \SingleIndexBenchmarkEntity.age: return "age"
        case \SingleIndexBenchmarkEntity.score: return "score"
        default: return "\(keyPath)"
        }
    }

    static func fieldName(for keyPath: PartialKeyPath<SingleIndexBenchmarkEntity>) -> String {
        switch keyPath {
        case \SingleIndexBenchmarkEntity.id: return "id"
        case \SingleIndexBenchmarkEntity.runID: return "runID"
        case \SingleIndexBenchmarkEntity.category: return "category"
        case \SingleIndexBenchmarkEntity.age: return "age"
        case \SingleIndexBenchmarkEntity.score: return "score"
        default: return "\(keyPath)"
        }
    }

    static func fieldName(for keyPath: AnyKeyPath) -> String {
        if let partial = keyPath as? PartialKeyPath<SingleIndexBenchmarkEntity> {
            return fieldName(for: partial)
        }
        return "\(keyPath)"
    }
}

struct TripleIndexBenchmarkEntity: Persistable {
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

    static var persistableType: String { "TripleIndexBenchmarkEntity" }

    static var allFields: [String] {
        ["id", "runID", "category", "age", "score"]
    }

    static var directoryPathComponents: [DirectoryPathComponent] {
        [.staticPath("test"), .staticPath("performance"), .staticPath("triple-index-entities")]
    }

    static var _persistableDescriptors: [any Descriptor] {
        [
            IndexDescriptor(
                name: "triple_category",
                keyPaths: [\TripleIndexBenchmarkEntity.runID, \TripleIndexBenchmarkEntity.category],
                kind: ScalarIndexKind<TripleIndexBenchmarkEntity>(fields: [\.runID, \.category])
            ),
            IndexDescriptor(
                name: "triple_age",
                keyPaths: [\TripleIndexBenchmarkEntity.runID, \TripleIndexBenchmarkEntity.age],
                kind: ScalarIndexKind<TripleIndexBenchmarkEntity>(fields: [\.runID, \.age])
            ),
            IndexDescriptor(
                name: "triple_score_by_category",
                keyPaths: [\TripleIndexBenchmarkEntity.runID, \TripleIndexBenchmarkEntity.category],
                kind: SumIndexKind<TripleIndexBenchmarkEntity, Double>(groupBy: [\.runID, \.category], value: \.score)
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

    static func fieldName<Value>(for keyPath: KeyPath<TripleIndexBenchmarkEntity, Value>) -> String {
        switch keyPath {
        case \TripleIndexBenchmarkEntity.id: return "id"
        case \TripleIndexBenchmarkEntity.runID: return "runID"
        case \TripleIndexBenchmarkEntity.category: return "category"
        case \TripleIndexBenchmarkEntity.age: return "age"
        case \TripleIndexBenchmarkEntity.score: return "score"
        default: return "\(keyPath)"
        }
    }

    static func fieldName(for keyPath: PartialKeyPath<TripleIndexBenchmarkEntity>) -> String {
        switch keyPath {
        case \TripleIndexBenchmarkEntity.id: return "id"
        case \TripleIndexBenchmarkEntity.runID: return "runID"
        case \TripleIndexBenchmarkEntity.category: return "category"
        case \TripleIndexBenchmarkEntity.age: return "age"
        case \TripleIndexBenchmarkEntity.score: return "score"
        default: return "\(keyPath)"
        }
    }

    static func fieldName(for keyPath: AnyKeyPath) -> String {
        if let partial = keyPath as? PartialKeyPath<TripleIndexBenchmarkEntity> {
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
        self.engine = try await FoundationDBScenarioCoordinator.shared.makeEngine()
        self.plainRunID = "plain-\(UUID().uuidString.prefix(8))"
        self.singleRunID = "single-\(UUID().uuidString.prefix(8))"
        self.tripleRunID = "triple-\(UUID().uuidString.prefix(8))"

        let schema = Schema(
            [PlainBenchmarkEntity.self, SingleIndexBenchmarkEntity.self, TripleIndexBenchmarkEntity.self],
            version: .init(1, 0, 0)
        )
        self.container = try await DBContainer.open(
            for: schema,
            configuration: .init(backend: .custom(engine)),
            runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(),
            security: .disabled
        )
        try await cleanup()
    }

    func cleanup() async throws {
        for path in [
            ["test", "performance", "plain-entities"],
            ["test", "performance", "single-index-entities"],
            ["test", "performance", "triple-index-entities"],
        ] {
            do {
                try await engine.removeDirectory(path: path)
            } catch {
                // Ignore missing directory when the strategy did not materialize it.
            }
        }
    }

    func makePlainEntity(seed: Int) -> PlainBenchmarkEntity {
        let suffix = String(UUID().uuidString.prefix(6))
        let id = "plain-\(seed)-\(suffix)"
        let category = seed.isMultiple(of: 10) ? "hot" : "cold-\(seed % 6)"
        return PlainBenchmarkEntity(
            id: id,
            runID: plainRunID,
            category: category,
            age: 18 + (seed % 50),
            score: Double(100 + (seed % 200))
        )
    }

    func makeSingleIndexEntity(seed: Int) -> SingleIndexBenchmarkEntity {
        let suffix = String(UUID().uuidString.prefix(6))
        let id = "single-\(seed)-\(suffix)"
        let category = seed.isMultiple(of: 10) ? "hot" : "cold-\(seed % 6)"
        return SingleIndexBenchmarkEntity(
            id: id,
            runID: singleRunID,
            category: category,
            age: 18 + (seed % 50),
            score: Double(100 + (seed % 200))
        )
    }

    func makeTripleIndexEntity(seed: Int) -> TripleIndexBenchmarkEntity {
        let suffix = String(UUID().uuidString.prefix(6))
        let id = "triple-\(seed)-\(suffix)"
        let category = seed.isMultiple(of: 10) ? "hot" : "cold-\(seed % 6)"
        return TripleIndexBenchmarkEntity(
            id: id,
            runID: tripleRunID,
            category: category,
            age: 18 + (seed % 50),
            score: Double(100 + (seed % 200))
        )
    }

    func insertPlain(_ entity: PlainBenchmarkEntity) async throws {
        let store = try await container.store(for: PlainBenchmarkEntity.self)
        try await store.executeBatch(inserts: [entity], deletes: [])
    }

    func insertSingle(_ entity: SingleIndexBenchmarkEntity) async throws {
        let store = try await container.store(for: SingleIndexBenchmarkEntity.self)
        try await store.executeBatch(inserts: [entity], deletes: [])
    }

    func insertTriple(_ entity: TripleIndexBenchmarkEntity) async throws {
        let store = try await container.store(for: TripleIndexBenchmarkEntity.self)
        try await store.executeBatch(inserts: [entity], deletes: [])
    }

    func indexedLookup(category: String) async throws -> [SingleIndexBenchmarkEntity] {
        try await DatabaseContext(container: container)
            .fetch(SingleIndexBenchmarkEntity.self)
            .where(\.runID == singleRunID)
            .where(\.category == category)
            .execute()
    }

    func scannedLookup(category: String) async throws -> [PlainBenchmarkEntity] {
        let all = try await DatabaseContext(container: container)
            .fetch(PlainBenchmarkEntity.self)
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
        try await FoundationDBScenarioCoordinator.shared.withSerializedAccess {
            try await withIndexedBenchmarkContext { context in
                var plainSeed = 0
                let plainMeasurement = try await measureBenchmark(name: "0 indexes") {
                    plainSeed += 1
                    try await context.insertPlain(context.makePlainEntity(seed: plainSeed))
                }

                var singleSeed = 0
                let singleMeasurement = try await measureBenchmark(name: "1 index") {
                    singleSeed += 1
                    try await context.insertSingle(context.makeSingleIndexEntity(seed: singleSeed))
                }

                var tripleSeed = 0
                let tripleMeasurement = try await measureBenchmark(name: "3 indexes") {
                    tripleSeed += 1
                    try await context.insertTriple(context.makeTripleIndexEntity(seed: tripleSeed))
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
        try await FoundationDBScenarioCoordinator.shared.withSerializedAccess {
            try await withIndexedBenchmarkContext { context in
                for seed in 0..<queryDatasetSize {
                    try await context.insertPlain(context.makePlainEntity(seed: seed))
                    try await context.insertSingle(context.makeSingleIndexEntity(seed: seed))
                }

                let expectedIndexed = try await context.indexedLookup(category: "hot")
                let expectedScanned = try await context.scannedLookup(category: "hot")

                #expect(expectedIndexed.count == expectedScanned.count)
                #expect(!expectedIndexed.isEmpty)

                let scannedMeasurement = try await measureBenchmark(name: "Full scan + filter", measurementIterations: 10) {
                    let entities = try await context.scannedLookup(category: "hot")
                    #expect(entities.count == expectedScanned.count)
                }

                let indexedMeasurement = try await measureBenchmark(name: "ScalarIndex equality", measurementIterations: 10) {
                    let entities = try await context.indexedLookup(category: "hot")
                    #expect(entities.count == expectedIndexed.count)
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
