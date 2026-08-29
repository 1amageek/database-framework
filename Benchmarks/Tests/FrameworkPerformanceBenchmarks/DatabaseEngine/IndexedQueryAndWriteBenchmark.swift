#if FOUNDATION_DB
// Benchmark-only target; excluded from the database-framework test graph.
import Testing
import Foundation
import DatabaseKit
import DatabaseTypes
import StorageKit
@_spi(Benchmarking) import DatabaseEngine
import DatabaseRuntime
import ScalarIndex
import AggregationIndex

@Persistable
struct PlainBenchmarkEntity {
    #Directory<PlainBenchmarkEntity>(
        "test",
        "performance",
        "plain-entities"
    )

    var id: String = UUID().uuidString
    var runID: String = ""
    var category: String = ""
    var age: Int64 = 0
    var score: Double = 0
}

@Persistable
struct SingleIndexBenchmarkEntity {
    #Directory<SingleIndexBenchmarkEntity>(
        "test",
        "performance",
        "single-index-entities"
    )
    #Index(
        .ordered(
            name: "single_category",
            keys: [
                .ascending(\SingleIndexBenchmarkEntity.runID),
                .ascending(\SingleIndexBenchmarkEntity.category),
            ]
        )
    )

    var id: String = UUID().uuidString
    var runID: String = ""
    var category: String = ""
    var age: Int64 = 0
    var score: Double = 0
}

@Persistable
struct TripleIndexBenchmarkEntity {
    #Directory<TripleIndexBenchmarkEntity>(
        "test",
        "performance",
        "triple-index-entities"
    )
    #Index(
        .ordered(
            name: "triple_category",
            keys: [
                .ascending(\TripleIndexBenchmarkEntity.runID),
                .ascending(\TripleIndexBenchmarkEntity.category),
            ]
        )
    )
    #Index(
        .ordered(
            name: "triple_age",
            keys: [
                .ascending(\TripleIndexBenchmarkEntity.runID),
                .ascending(\TripleIndexBenchmarkEntity.age),
            ]
        )
    )
    #Index(
        .aggregate(
            name: "triple_score_by_category",
            function: .sum,
        groupBy: [
                .ascending(\TripleIndexBenchmarkEntity.runID),
                .ascending(\TripleIndexBenchmarkEntity.category),
            ],
        value: \TripleIndexBenchmarkEntity.score
        )
    )

    var id: String = UUID().uuidString
    var runID: String = ""
    var category: String = ""
    var age: Int64 = 0
    var score: Double = 0
}

private struct IndexedBenchmarkContext: Sendable {
    let engine: any StorageEngine
    let container: DBContainer

    let plainRunID: String
    let singleRunID: String
    let tripleRunID: String

    init() async throws {
        self.engine = try await FoundationDBBenchmarkEnvironment.shared.makeEngine()
        self.plainRunID = "plain-\(UUID().uuidString.prefix(8))"
        self.singleRunID = "single-\(UUID().uuidString.prefix(8))"
        self.tripleRunID = "triple-\(UUID().uuidString.prefix(8))"

        let schema = try Schema(
            entities: [
                try PlainBenchmarkEntity.schemaEntity,
                try SingleIndexBenchmarkEntity.schemaEntity,
                try TripleIndexBenchmarkEntity.schemaEntity,
            ],
            version: .init(1, 0, 0)
        )
        self.container = try await DBContainer.open(
            for: schema,
            configuration: .benchmarking(storageEngine: engine),
            runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(
                executionIdentity: DatabaseExecutionRuntimeIdentity(
                    identifier: "database-tests",
                    revision: 1
                ),
                entityRuntimes: [try DatabaseFrameworkRuntime.entity(PlainBenchmarkEntity.self), try DatabaseFrameworkRuntime.entity(SingleIndexBenchmarkEntity.self), try DatabaseFrameworkRuntime.entity(TripleIndexBenchmarkEntity.self),
                ]
            ),
            security: .benchmarkingDisabled
        )
        try await cleanup()
    }

    func cleanup() async throws {
        // Clearing the resolved Directory contents keeps the Directory nodes and
        // their allocated prefixes in place, which is what a repeated benchmark
        // run needs: the same layout is reused and only stored data is dropped.
        let subspaces = [
            try await container.resolveDirectory(for: PlainBenchmarkEntity.self),
            try await container.resolveDirectory(for: SingleIndexBenchmarkEntity.self),
            try await container.resolveDirectory(for: TripleIndexBenchmarkEntity.self),
        ]
        for subspace in subspaces {
            try await engine.withTransaction { transaction in
                let (begin, end) = subspace.range()
                try transaction.clearRange(beginKey: begin, endKey: end)
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
            age: Int64(18 + (seed % 50)),
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
            age: Int64(18 + (seed % 50)),
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
            age: Int64(18 + (seed % 50)),
            score: Double(100 + (seed % 200))
        )
    }

    func insertPlain(_ entity: PlainBenchmarkEntity) async throws {
        let store = try await DataStoreBenchmarkProbe.openDataStore(
            for: PlainBenchmarkEntity.self,
            in: container
        )
        try await store.executeBatch(
            inserts: [try PersistedModel(entity)],
            deletes: []
        )
    }

    func insertSingle(_ entity: SingleIndexBenchmarkEntity) async throws {
        let store = try await DataStoreBenchmarkProbe.openDataStore(
            for: SingleIndexBenchmarkEntity.self,
            in: container
        )
        try await store.executeBatch(
            inserts: [try PersistedModel(entity)],
            deletes: []
        )
    }

    func insertTriple(_ entity: TripleIndexBenchmarkEntity) async throws {
        let store = try await DataStoreBenchmarkProbe.openDataStore(
            for: TripleIndexBenchmarkEntity.self,
            in: container
        )
        try await store.executeBatch(
            inserts: [try PersistedModel(entity)],
            deletes: []
        )
    }

    func indexedLookup(category: String) async throws -> [SingleIndexBenchmarkEntity] {
        try await container.benchmarkContext()
            .fetch(SingleIndexBenchmarkEntity.self)
            .where(SingleIndexBenchmarkEntity.fields.runID == singleRunID)
            .where(SingleIndexBenchmarkEntity.fields.category == category)
            .execute()
    }

    func scannedLookup(category: String) async throws -> [PlainBenchmarkEntity] {
        let all = try await container.benchmarkContext()
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
struct IndexedQueryAndWriteBenchmarks {
    private let queryDatasetSize = 2_000

    @Test("write amplification across index counts", .timeLimit(.minutes(1)))
    func testWriteAmplificationAcrossIndexCounts() async throws {
        try await FoundationDBBenchmarkEnvironment.shared.withExclusiveAccess {
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
        try await FoundationDBBenchmarkEnvironment.shared.withExclusiveAccess {
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
