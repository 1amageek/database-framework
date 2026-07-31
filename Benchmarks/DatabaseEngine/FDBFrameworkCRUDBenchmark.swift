#if FOUNDATION_DB
import Testing
import Foundation
import DatabaseKit
import DatabaseTypes
import StorageKit
import TestSupport
import DatabaseEngine
import DatabaseRuntime

@Persistable
struct CRUDBenchmarkEntity {
    #Directory<CRUDBenchmarkEntity>(
        "test",
        "performance",
        \CRUDBenchmarkEntity.runID,
        "crud-entities",
        layer: .partition
    )

    var id: String = UUID().uuidString
    var runID: String = ""
    var name: String = ""
    var age: Int64 = 0
    var score: Double = 0
}

private struct CRUDBenchmarkContext: Sendable {
    let engine: any StorageEngine
    let container: DBContainer
    let runID: String
    let path: DirectoryPath<CRUDBenchmarkEntity>
    let rawSubspace: Subspace

    init(runID: String = "crud-\(UUID().uuidString.prefix(8))") async throws {
        self.engine = try await FoundationDBScenarioCoordinator.shared.makeEngine()
        self.runID = runID

        var path = DirectoryPath<CRUDBenchmarkEntity>()
        path.set(CRUDBenchmarkEntity.fields.runID, to: runID)
        self.path = path
        self.rawSubspace = Subspace(prefix: Tuple(["test", "performance", "raw-crud", runID]).pack())

        let schema = try Schema(
            entities: [try CRUDBenchmarkEntity.schemaEntity],
            version: .init(1, 0, 0)
        )
        self.container = try await DBContainer.open(
            for: schema,
            configuration: .testing(storageEngine: engine),
            runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(
                entityRuntimes: [try DatabaseFrameworkRuntime.entity(CRUDBenchmarkEntity.self)]
            ),
            security: .disabled
        )
    }

    func cleanup() async throws {
        do {
            try await engine.removeNamespace(path: ["test", "performance", runID, "crud-entities"])
        } catch {
            // Ignore missing directory for empty/failed runs.
        }
        try await engine.withTransaction { transaction in
            let (begin, end) = rawSubspace.range()
            try transaction.clearRange(beginKey: begin, endKey: end)
        }
    }

    func makeEntity(id: String = UUID().uuidString, seed: Int = 0) -> CRUDBenchmarkEntity {
        let name = "user-\(seed)"
        return CRUDBenchmarkEntity(
            id: id,
            runID: runID,
            name: name,
            age: Int64(20 + (seed % 50)),
            score: Double(60 + (seed % 40))
        )
    }

    func frameworkLayout() async throws -> (itemSubspace: Subspace, blobsSubspace: Subspace) {
        let subspace = try await container.resolveDirectory(for: CRUDBenchmarkEntity.self, path: path)
        return (
            itemSubspace: subspace.subspace(SubspaceKey.items).subspace(CRUDBenchmarkEntity.persistableType),
            blobsSubspace: subspace.subspace(SubspaceKey.blobs)
        )
    }

    func rawWrite(id: String) async throws {
        let value = ByteString(repeating: 0x42, count: 72)
        let key = rawSubspace.pack(Tuple([id]))
        try await engine.withTransaction { transaction in
            try transaction.setValue(value, for: key)
        }
    }

    func frameworkLayoutWrite(_ entity: CRUDBenchmarkEntity) async throws {
        let layout = try await frameworkLayout()
        let key = layout.itemSubspace.pack(Tuple([entity.id]))
        let data = try DataAccess.serialize(entity)
        try await engine.withTransaction { transaction in
            let storage = ItemStorage(transaction: transaction, blobsSubspace: layout.blobsSubspace, configuration: .v1)
            try await storage.write(data, for: key)
        }
    }

    func rawFrameworkKeyRead(id: String) async throws -> Bool {
        let layout = try await frameworkLayout()
        let key = layout.itemSubspace.pack(Tuple([id]))
        return try await engine.withTransaction { transaction in
            try await transaction.getValue(for: key, snapshot: false) != nil
        }
    }

    func frameworkLayoutRead(id: String) async throws -> CRUDBenchmarkEntity? {
        let layout = try await frameworkLayout()
        let key = layout.itemSubspace.pack(Tuple([id]))
        return try await engine.withTransaction { transaction in
            let storage = ItemStorage(transaction: transaction, blobsSubspace: layout.blobsSubspace, configuration: .v1)
            guard let data = try await storage.read(for: key, snapshot: false) else {
                return nil
            }
            let decoded: CRUDBenchmarkEntity = try DataAccess.deserialize(data)
            return decoded
        }
    }

    func dataStoreWrite(_ entity: CRUDBenchmarkEntity) async throws {
        let store = try await container.store(for: CRUDBenchmarkEntity.self, path: path)
        try await store.executeBatch(
            inserts: [try PersistedModel(entity)],
            deletes: []
        )
    }

    func dataStoreRead(id: String) async throws -> CRUDBenchmarkEntity? {
        let store = try await container.store(for: CRUDBenchmarkEntity.self, path: path)
        return try await store.withTransaction(configuration: .readOnly) { transaction in
            try await transaction.fetch(
                CRUDBenchmarkEntity.self,
                identifiedBy: id,
                in: path,
                consistency: .serializable
            )
        }
    }

    func frameworkWrite(_ entity: CRUDBenchmarkEntity) async throws {
        let context = DatabaseContext(container: container)
        try context.insert(entity)
        try await context.save()
    }

    func frameworkRead(id: String) async throws -> CRUDBenchmarkEntity? {
        let context = DatabaseContext(container: container)
        return try await context.model(for: id, as: CRUDBenchmarkEntity.self, partition: path)
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
        try await FoundationDBScenarioCoordinator.shared.withSerializedAccess {
            try await withCRUDBenchmarkContext { context in
                let rawMeasurement = try await measureBenchmark(name: "L1 Raw KV") {
                    try await context.rawWrite(id: UUID().uuidString)
                }

                let storageMeasurement = try await measureBenchmark(name: "L2 ItemStorage") {
                    try await context.frameworkLayoutWrite(context.makeEntity(seed: Int.random(in: 0...1_000_000)))
                }

                let dataStoreMeasurement = try await measureBenchmark(name: "L3 DataStore") {
                    try await context.dataStoreWrite(context.makeEntity(seed: Int.random(in: 0...1_000_000)))
                }

                let frameworkMeasurement = try await measureBenchmark(name: "L4 DatabaseContext") {
                    try await context.frameworkWrite(context.makeEntity(seed: Int.random(in: 0...1_000_000)))
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
        try await FoundationDBScenarioCoordinator.shared.withSerializedAccess {
            try await withCRUDBenchmarkContext { context in
                let entities: [(String, CRUDBenchmarkEntity)] = (0..<128).map { index in
                    let id = "entity-\(index)"
                    let entity = context.makeEntity(id: id, seed: index)
                    return (id, entity)
                }

                for (_, entity) in entities {
                    try await context.frameworkLayoutWrite(entity)
                }

                var cursor = 0
                func nextID() -> String {
                    defer { cursor = (cursor + 1) % entities.count }
                    return entities[cursor].0
                }

                let rawMeasurement = try await measureBenchmark(name: "L1 Framework Key Read") {
                    let found = try await context.rawFrameworkKeyRead(id: nextID())
                    #expect(found)
                }

                let storageMeasurement = try await measureBenchmark(name: "L2 ItemStorage Decode") {
                    let entity = try await context.frameworkLayoutRead(id: nextID())
                    #expect(entity != nil)
                }

                let dataStoreMeasurement = try await measureBenchmark(name: "L3 DataStore Fetch") {
                    let entity = try await context.dataStoreRead(id: nextID())
                    #expect(entity != nil)
                }

                let frameworkMeasurement = try await measureBenchmark(name: "L4 DatabaseContext Model") {
                    let entity = try await context.frameworkRead(id: nextID())
                    #expect(entity != nil)
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
        try await FoundationDBScenarioCoordinator.shared.withSerializedAccess {
            let first = try await CRUDBenchmarkContext(runID: "crud-a-\(UUID().uuidString.prefix(8))")
            let second = try await CRUDBenchmarkContext(runID: "crud-b-\(UUID().uuidString.prefix(8))")

            do {
                let firstEntity = first.makeEntity(id: "shared-id", seed: 1)
                let secondEntity = second.makeEntity(id: "shared-id", seed: 2)

                try await first.frameworkWrite(firstEntity)
                try await second.frameworkWrite(secondEntity)

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
