#if FOUNDATION_DB
import Testing
import Foundation
import Core
import StorageKit
import TestSupport
import DatabaseEngine

struct CRUDBenchmarkRecord: Persistable {
    typealias ID = String

    var id: String
    var runID: String
    var name: String
    var age: Int
    var score: Double

    init(
        id: String = UUID().uuidString,
        runID: String = "",
        name: String = "",
        age: Int = 0,
        score: Double = 0
    ) {
        self.id = id
        self.runID = runID
        self.name = name
        self.age = age
        self.score = score
    }

    static var persistableType: String { "CRUDBenchmarkRecord" }

    static var allFields: [String] {
        ["id", "runID", "name", "age", "score"]
    }

    static var directoryPathComponents: [any DirectoryPathElement] {
        [Path("test"), Path("performance"), Field<CRUDBenchmarkRecord>(\.runID), Path("crud-records")]
    }

    static var directoryLayer: DirectoryLayer { .partition }

    subscript(dynamicMember member: String) -> (any Sendable)? {
        switch member {
        case "id": return id
        case "runID": return runID
        case "name": return name
        case "age": return age
        case "score": return score
        default: return nil
        }
    }

    static func fieldName<Value>(for keyPath: KeyPath<CRUDBenchmarkRecord, Value>) -> String {
        switch keyPath {
        case \CRUDBenchmarkRecord.id: return "id"
        case \CRUDBenchmarkRecord.runID: return "runID"
        case \CRUDBenchmarkRecord.name: return "name"
        case \CRUDBenchmarkRecord.age: return "age"
        case \CRUDBenchmarkRecord.score: return "score"
        default: return "\(keyPath)"
        }
    }

    static func fieldName(for keyPath: PartialKeyPath<CRUDBenchmarkRecord>) -> String {
        switch keyPath {
        case \CRUDBenchmarkRecord.id: return "id"
        case \CRUDBenchmarkRecord.runID: return "runID"
        case \CRUDBenchmarkRecord.name: return "name"
        case \CRUDBenchmarkRecord.age: return "age"
        case \CRUDBenchmarkRecord.score: return "score"
        default: return "\(keyPath)"
        }
    }

    static func fieldName(for keyPath: AnyKeyPath) -> String {
        if let partial = keyPath as? PartialKeyPath<CRUDBenchmarkRecord> {
            return fieldName(for: partial)
        }
        return "\(keyPath)"
    }
}

private struct CRUDBenchmarkContext: Sendable {
    let engine: any StorageEngine
    let container: DBContainer
    let runID: String
    let path: DirectoryPath<CRUDBenchmarkRecord>
    let rawSubspace: Subspace

    init(runID: String = "crud-\(UUID().uuidString.prefix(8))") async throws {
        self.engine = try await FDBTestSetup.shared.makeEngine()
        self.runID = runID

        var path = DirectoryPath<CRUDBenchmarkRecord>()
        path.set(\.runID, to: runID)
        self.path = path
        self.rawSubspace = Subspace(prefix: Tuple(["test", "performance", "raw-crud", runID]).pack())

        let schema = Schema([CRUDBenchmarkRecord.self], version: .init(1, 0, 0))
        self.container = try await DBContainer(
            for: schema,
            configuration: .init(backend: .custom(engine)),
            security: .disabled
        )
    }

    func cleanup() async throws {
        do {
            try await engine.directoryService.remove(path: ["test", "performance", runID, "crud-records"])
        } catch {
            // Ignore missing directory for empty/failed runs.
        }
        try await engine.withTransaction { transaction in
            let (begin, end) = rawSubspace.range()
            transaction.clearRange(beginKey: begin, endKey: end)
        }
    }

    func makeRecord(id: String = UUID().uuidString, seed: Int = 0) -> CRUDBenchmarkRecord {
        let name = "user-\(seed)"
        return CRUDBenchmarkRecord(
            id: id,
            runID: runID,
            name: name,
            age: 20 + (seed % 50),
            score: Double(60 + (seed % 40))
        )
    }

    func frameworkLayout() async throws -> (itemSubspace: Subspace, blobsSubspace: Subspace) {
        let subspace = try await container.resolveDirectory(for: CRUDBenchmarkRecord.self, path: path)
        return (
            itemSubspace: subspace.subspace(SubspaceKey.items).subspace(CRUDBenchmarkRecord.persistableType),
            blobsSubspace: subspace.subspace(SubspaceKey.blobs)
        )
    }

    func rawWrite(id: String) async throws {
        let value = Array(repeating: UInt8(0x42), count: 72)
        let key = rawSubspace.pack(Tuple([id]))
        try await engine.withAutoCommit { transaction in
            transaction.setValue(value, for: key)
        }
    }

    func frameworkLayoutWrite(_ record: CRUDBenchmarkRecord, isNewRecord: Bool) async throws {
        let layout = try await frameworkLayout()
        let key = layout.itemSubspace.pack(Tuple([record.id]))
        let data = try DataAccess.serialize(record)
        try await engine.withAutoCommit { transaction in
            let storage = ItemStorage(transaction: transaction, blobsSubspace: layout.blobsSubspace)
            try await storage.write(data, for: key, isNewRecord: isNewRecord)
        }
    }

    func rawFrameworkKeyRead(id: String) async throws -> Bool {
        let layout = try await frameworkLayout()
        let key = layout.itemSubspace.pack(Tuple([id]))
        return try await engine.withAutoCommit { transaction in
            try await transaction.getValue(for: key, snapshot: false) != nil
        }
    }

    func frameworkLayoutRead(id: String) async throws -> CRUDBenchmarkRecord? {
        let layout = try await frameworkLayout()
        let key = layout.itemSubspace.pack(Tuple([id]))
        return try await engine.withAutoCommit { transaction in
            let storage = ItemStorage(transaction: transaction, blobsSubspace: layout.blobsSubspace)
            guard let data = try await storage.read(for: key, snapshot: false) else {
                return nil
            }
            let decoded: CRUDBenchmarkRecord = try DataAccess.deserialize(data)
            return decoded
        }
    }

    func dataStoreWrite(_ record: CRUDBenchmarkRecord) async throws {
        let store = try await container.store(for: CRUDBenchmarkRecord.self, path: path)
        try await store.executeBatch(inserts: [record], deletes: [])
    }

    func dataStoreRead(id: String) async throws -> CRUDBenchmarkRecord? {
        let store = try await container.store(for: CRUDBenchmarkRecord.self, path: path)
        return try await store.withAutoCommit { transaction in
            try await store.fetchByIdInTransaction(CRUDBenchmarkRecord.self, id: id, transaction: transaction)
        }
    }

    func frameworkWrite(_ record: CRUDBenchmarkRecord) async throws {
        let context = FDBContext(container: container)
        context.insert(record)
        try await context.save()
    }

    func frameworkRead(id: String) async throws -> CRUDBenchmarkRecord? {
        let context = FDBContext(container: container)
        return try await context.model(for: id, as: CRUDBenchmarkRecord.self, partition: path)
    }
}

private func withCRUDBenchmarkContext<T: Sendable>(
    _ body: (CRUDBenchmarkContext) async throws -> T
) async throws -> T {
    let context = try await CRUDBenchmarkContext()
    do {
        let result = try await body(context)
        try await context.cleanup()
        return result
    } catch {
        do {
            try await context.cleanup()
        } catch {
            print("CRUDBenchmark cleanup failed: \(error)")
        }
        throw error
    }
}

@Suite("FDB Framework CRUD Benchmarks", .tags(.fdb, .performance), .serialized, .heartbeat)
struct FDBFrameworkCRUDBenchmarkTests {

    @Test("write path layer comparison", .timeLimit(.minutes(1)))
    func testWritePathLayerComparison() async throws {
        try await FDBTestSetup.shared.withSerializedAccess {
            try await withCRUDBenchmarkContext { context in
                let rawMeasurement = try await measureBenchmark(name: "L1 Raw KV") {
                    try await context.rawWrite(id: UUID().uuidString)
                }

                let storageMeasurement = try await measureBenchmark(name: "L2 ItemStorage") {
                    try await context.frameworkLayoutWrite(context.makeRecord(seed: Int.random(in: 0...1_000_000)), isNewRecord: true)
                }

                let dataStoreMeasurement = try await measureBenchmark(name: "L3 DataStore") {
                    try await context.dataStoreWrite(context.makeRecord(seed: Int.random(in: 0...1_000_000)))
                }

                let frameworkMeasurement = try await measureBenchmark(name: "L4 FDBContext") {
                    try await context.frameworkWrite(context.makeRecord(seed: Int.random(in: 0...1_000_000)))
                }

                let measurements = [rawMeasurement, storageMeasurement, dataStoreMeasurement, frameworkMeasurement]
                printBenchmarkReport(title: "FDB Write Path Layer Comparison", measurements: measurements)

                #expect(measurements.allSatisfy { $0.opsPerSecond > 0 })
                #expect(measurements.allSatisfy { !$0.samplesMs.isEmpty })
            }
        }
    }

    @Test("read path layer comparison", .timeLimit(.minutes(1)))
    func testReadPathLayerComparison() async throws {
        try await FDBTestSetup.shared.withSerializedAccess {
            try await withCRUDBenchmarkContext { context in
                let records: [(String, CRUDBenchmarkRecord)] = (0..<128).map { index in
                    let id = "record-\(index)"
                    let record = context.makeRecord(id: id, seed: index)
                    return (id, record)
                }

                for (_, record) in records {
                    try await context.frameworkLayoutWrite(record, isNewRecord: true)
                }

                var cursor = 0
                func nextID() -> String {
                    defer { cursor = (cursor + 1) % records.count }
                    return records[cursor].0
                }

                let rawMeasurement = try await measureBenchmark(name: "L1 Framework Key Read") {
                    let found = try await context.rawFrameworkKeyRead(id: nextID())
                    #expect(found)
                }

                let storageMeasurement = try await measureBenchmark(name: "L2 ItemStorage Decode") {
                    let record = try await context.frameworkLayoutRead(id: nextID())
                    #expect(record != nil)
                }

                let dataStoreMeasurement = try await measureBenchmark(name: "L3 DataStore Fetch") {
                    let record = try await context.dataStoreRead(id: nextID())
                    #expect(record != nil)
                }

                let frameworkMeasurement = try await measureBenchmark(name: "L4 FDBContext Model") {
                    let record = try await context.frameworkRead(id: nextID())
                    #expect(record != nil)
                }

                let measurements = [rawMeasurement, storageMeasurement, dataStoreMeasurement, frameworkMeasurement]
                printBenchmarkReport(title: "FDB Read Path Layer Comparison", measurements: measurements)

                #expect(measurements.allSatisfy { $0.opsPerSecond > 0 })
                #expect(measurements.allSatisfy { !$0.samplesMs.isEmpty })
            }
        }
    }

    @Test("benchmark partitions remain isolated", .timeLimit(.minutes(1)))
    func testBenchmarkPartitionsRemainIsolated() async throws {
        try await FDBTestSetup.shared.withSerializedAccess {
            let first = try await CRUDBenchmarkContext(runID: "crud-a-\(UUID().uuidString.prefix(8))")
            let second = try await CRUDBenchmarkContext(runID: "crud-b-\(UUID().uuidString.prefix(8))")

            do {
                let firstRecord = first.makeRecord(id: "shared-id", seed: 1)
                let secondRecord = second.makeRecord(id: "shared-id", seed: 2)

                try await first.frameworkWrite(firstRecord)
                try await second.frameworkWrite(secondRecord)

                let firstFetch = try await first.frameworkRead(id: "shared-id")
                let secondFetch = try await second.frameworkRead(id: "shared-id")

                #expect(firstFetch?.runID == first.runID)
                #expect(secondFetch?.runID == second.runID)
                #expect(firstFetch?.name != secondFetch?.name)
            } catch {
                do {
                    try await first.cleanup()
                } catch {
                    print("First CRUD isolation cleanup failed: \(error)")
                }
                do {
                    try await second.cleanup()
                } catch {
                    print("Second CRUD isolation cleanup failed: \(error)")
                }
                throw error
            }

            try await first.cleanup()
            try await second.cleanup()
        }
    }
}
#endif
