#if SQLITE
import Database
import DatabaseEngine
import DatabaseFoundation
import DatabaseKit
import DatabaseRuntime
import DatabaseWireRuntime
import DatabaseTypes
import DatabaseWire
import TestSupport
import Testing

@Persistable
private struct SQLiteContinuationEntity {
    #Directory<SQLiteContinuationEntity>("sqlite-continuation")

    var id: String = ""
}

@Suite("SQLite query continuation semantics")
struct DatabaseQueryContinuationSQLiteTests {
    @Test("SQLite resumes pagination from one durable fixed-result snapshot")
    func opaqueReadPointContinuationUsesDurableSnapshot() async throws {
        let container = try await makeContainer()
        defer { await container.shutdown() }
        let endpoint = try makeEndpoint(container: container)
        let context = container.testBaseContext()
        try context.insert(SQLiteContinuationEntity(id: "first"))
        try context.insert(SQLiteContinuationEntity(id: "second"))
        try await context.save()
        let query = SelectQuery(
            projection: .all,
            source: .table(TableRef(SQLiteContinuationEntity.persistableType))
        )

        let first = try await execute(
            request(query: query, continuation: nil),
            requestID: 1,
            endpoint: endpoint
        )
        let firstPage: QueryRowPage
        switch first {
        case .success(.rows(let page)):
            firstPage = page
        case .success(let response):
            Issue.record("Expected rows, received \(response)")
            return
        case .failure(let error):
            Issue.record(
                "Expected the first SQLite page to succeed, received \(error.code): \(error.message)"
            )
            return
        }
        let continuation = try #require(firstPage.continuation)
        guard case .transactional(let readPoint) = firstPage.consistency else {
            Issue.record("Expected one transactional SQLite read point")
            return
        }
        guard case .opaque = readPoint.position else {
            Issue.record("SQLite must advertise a non-restorable opaque read point")
            return
        }

        try context.insert(SQLiteContinuationEntity(id: "third"))
        try await context.save()

        let second = try await execute(
            request(query: query, continuation: continuation),
            requestID: 2,
            endpoint: endpoint
        )
        guard case .success(.rows(let secondPage)) = second else {
            Issue.record(
                "Expected SQLite to resume the durable query snapshot"
            )
            return
        }
        #expect(secondPage.rowCount == 1)
        #expect(secondPage.continuation == nil)
        guard case .transactional(let secondReadPoint) = secondPage.consistency
        else {
            Issue.record("Expected one transactional SQLite read point")
            return
        }
        guard case .opaque = secondReadPoint.position else {
            Issue.record("SQLite must advertise its per-page opaque read point")
            return
        }
    }

    private func makeContainer() async throws -> DBContainer {
        let engine = try SQLiteStorageEngine(configuration: .inMemory)
        return try await DBContainer.open(
            for: try Schema(
                entities: [try SQLiteContinuationEntity.schemaEntity],
                version: Schema.Version(1, 0, 0)
            ),
            configuration: .testing(storageEngine: engine),
            runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(
                entityRuntimes: [
                    try DatabaseFrameworkRuntime.entity(
                        SQLiteContinuationEntity.self
                    ),
                ]
            ),
            security: .testingDisabled
        )
    }

    private func makeEndpoint(container: DBContainer) throws -> DatabaseEndpoint {
        let snapshotStore = DatabaseQuerySnapshotStore(
            container: container,
            clock: AnyDatabaseWallClock(RealtimeDatabaseWallClock()),
            identifierGenerator: AnyDatabaseUUIDGenerator(
                RandomDatabaseUUIDGenerator()
            ),
            scheduler: AnyDatabaseJobScheduler(
                SQLiteQuerySnapshotScheduler()
            ),
            wireLimits: .default
        )
        let registry = try DatabaseOperationRegistry(
            handlers: [
                AnyDatabaseOperationHandler(
                    QueryExecuteHandler(
                        runtimeLimits: .default,
                        querySnapshotStore: snapshotStore
                    )
                ),
            ],
            requiredOperations: [.queryExecute]
        )
        return DatabaseEndpoint(
            container: container,
            registry: registry,
            admissionPolicy: AnyDatabaseOperationAdmissionPolicy(
                UnrestrictedDatabaseOperationAdmissionPolicy()
            )
        )
    }

    private func request(
        query: SelectQuery,
        continuation: ByteString?
    ) -> QueryExecuteOperation.Request {
        QueryExecuteOperation.Request(
            input: .ir(.select(query)),
            page: QueryExecuteOperation.Page(
                limit: 1,
                continuation: continuation
            )
        )
    }

    private func execute(
        _ request: QueryExecuteOperation.Request,
        requestID: UInt64,
        endpoint: DatabaseEndpoint
    ) async throws -> Result<QueryExecuteOperation.Response, RemoteOperationError> {
        let encoder = DatabaseWireEncoder()
        let requestFrame = try encoder.encodeRequest(
            DatabaseOperations.queryExecute,
            requestID: requestID,
            target: operationTarget(),
            metadata: OperationRequestMetadata(),
            request: request
        )
        let responseFrame = try await endpoint.execute(
            requestFrame,
            context: DatabaseRequestExecutionContext(
                authorization: TestBaseEnvironment.authorization
            )
        )
        return try DatabaseWireDecoder().decodeResponse(
            DatabaseOperations.queryExecute,
            from: responseFrame,
            matching: requestID
        )
    }

    private func operationTarget() throws -> DatabaseOperationTarget {
        #if MultipleBases
        .base(try TestBaseEnvironment.id())
        #else
        .database
        #endif
    }
}

private actor SQLiteQuerySnapshotScheduler: DatabaseJobScheduler {
    func ensureWakeUp(noLaterThan: Timestamp) async throws {
        _ = noLaterThan
    }
}
#endif
