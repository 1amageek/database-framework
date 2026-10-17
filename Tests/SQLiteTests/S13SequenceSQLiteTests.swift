#if SQLITE
import Database
import DatabaseKit
import DatabaseRuntime
import DatabaseTypes
import StorageKit
import TestHeartbeat
import TestSupport
import Testing
@_spi(DatabaseExecution) @testable import DatabaseEngine

@Persistable
private struct S13SequenceAnchor {
    #Directory<S13SequenceAnchor>("s13-sequence-anchor")
    var id: String = ""
}

@Suite("S13 durable SQLite sequences", .timeLimit(.minutes(1)), .heartbeat)
struct S13SequenceSQLiteTests {
    @Test("Independent consumption survives whole-request failure and file-backed restart")
    func consumedValueSurvivesRollbackAndRestart() async throws {
        let database = try SQLiteTestDatabase(prefix: "s13-sequence-rollback")
        defer { database.remove() }
        let first = try await open(database)
        defer { await first.shutdown() }
        _ = try await execute(first, [.sequence(.create(sequence()))])
        await #expect(throws: SQLBlockExecutionError.unknownCheckpoint("missing")) {
            try await execute(first, [next(), .rollbackTo("missing")])
        }
        await first.shutdown()
        let restored = try await restore(database)
        defer { await restored.shutdown() }
        let result = try await execute(restored, [next()])
        #expect(try value(result, at: 0) == 2)
    }

    @Test("An interrupted request's atomic recovery record cannot cause duplicate allocation after restart")
    func durableRecoveryCellAndTransactionalRestart() async throws {
        let database = try SQLiteTestDatabase(prefix: "s13-sequence-recovery")
        defer { database.remove() }
        let first = try await open(database)
        defer { await first.shutdown() }
        _ = try await execute(first, [.sequence(.create(sequence())), next()])
        // Persist exactly the allocator transaction's atomic cell + recovery pair,
        // leaving its completed marker as if the process stopped before request close.
        let markerRequest = ByteString([83, 49, 51])
        try await first.testBaseContext().withTransaction { transaction in
            let storage = try first.testBaseContext().executionStorage()
            let store = SQLSequenceStore(root: storage.systemRoot)
            let head = try #require(try await transaction.storageAccess.getValue(
                for: store.headKey("counter"), snapshot: false, maximumByteCount: 34))
            try transaction.storageAccess.setValue(SQLSequenceStore.encode(77, called: true),
                for: store.cellKey("counter", head: head))
            try transaction.storageAccess.setValue(SQLSequenceStore.encode(77, called: true),
                for: store.markerKey(request: markerRequest, event: 0))
        }
        await first.shutdown()
        let restored = try await restore(database)
        defer { await restored.shutdown() }
        let recovered = try await execute(restored, [next()])
        #expect(try value(recovered, at: 0) == 78)
        let reset = try await execute(restored, [.savepoint("s"), .sequence(.alter(sequence(), restart: 5)),
            next(), .rollbackTo("s"), next()])
        #expect(try value(reset, at: 2) == 5)
        #expect(try value(reset, at: 4) == 79)
    }

    private func open(_ database: SQLiteTestDatabase) async throws -> DBContainer {
        try await DBContainer.open(for: Schema(entities: [S13SequenceAnchor.schemaEntity]),
            configuration: .testing(storageEngine: SQLiteStorageEngine(configuration: .file(database.path))),
            runtimeConfiguration: DatabaseFrameworkRuntime.configuration(
                executionIdentity: .init(identifier: "s13-sequence-restart", revision: 1),
                entityRuntimes: [DatabaseFrameworkRuntime.entity(S13SequenceAnchor.self)]), security: .testingDisabled)
    }

    private func restore(_ database: SQLiteTestDatabase) async throws -> DBContainer {
        try await DBContainer.openRestoringSchema(configuration: .testing(storageEngine:
            SQLiteStorageEngine(configuration: .file(database.path))), security: .testingDisabled) { schema in
            try DatabaseFrameworkRuntime.configuration(executionIdentity:
                .init(identifier: "s13-sequence-restart", revision: 1), schema: schema)
        }
    }

    private func sequence() throws -> SQLSequence {
        try SQLSequence(id: "counter", namespaceID: "public", name: "counter", start: 1,
            increment: 1, minimum: 1, maximum: 100)
    }

    private func next() -> SQLBlockStep {
        .statement(.select(SelectQuery(projection: [.expression(.function(FunctionCall(name: "nextval",
            arguments: [.string("counter")])))], source: .values([[]]))))
    }

    private func execute(_ container: DBContainer, _ steps: [SQLBlockStep]) async throws -> SQLBlockResult {
        try await SQLBlockExecutor(context: container.testBaseContext()).execute(SQLBlock(steps: steps),
            options: SQLBlockExecutionOptions(checkpointLimits: TransactionCheckpointLimits(maximumEntries: 10_000,
                maximumDepth: 16, maximumRetainedBytes: 4 * 1_024 * 1_024,
                maximumWorkUnits: 1_000_000, maximumReadPageRows: 128)))
    }

    private func value(_ result: SQLBlockResult, at index: Int) throws -> Int64 {
        guard case .rows(let response) = result.results[index],
              case .primitive(.int64(let value)) = response.rows.first?.values.first else {
            throw SQLSequenceExecutionError.invalidPersistedState
        }
        return value
    }
}
#endif
