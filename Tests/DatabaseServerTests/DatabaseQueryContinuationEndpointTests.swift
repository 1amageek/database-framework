import DatabaseKit
import TestSupport
import DatabaseEngine
import DatabaseRuntime
import DatabaseServer
import DatabaseTypes
import DatabaseWire
import StorageKit
import Testing

@Suite("Canonical query continuation endpoint", .serialized)
struct DatabaseQueryContinuationEndpointTests {
    @Test("row pages traverse the binary endpoint without gaps or duplicates")
    func rowPagesRoundTripWithoutGapsOrDuplicates() async throws {
        let endpoint = try await makeEndpoint()
        let query = valuesQuery()

        let first = try await successfulPage(
            request(query: query, pageLimit: 2),
            requestID: 1,
            endpoint: endpoint
        )
        let continuation = try #require(first.continuation)
        let second = try await successfulPage(
            request(
                query: query,
                pageLimit: 2,
                continuation: continuation
            ),
            requestID: 2,
            endpoint: endpoint
        )

        let rows = try first.materializedRows(maximumCount: 2)
            + second.materializedRows(maximumCount: 2)
        let identifiers = try rows.map {
            try identifier(from: $0, columns: first.columns)
        }
        #expect(identifiers == ["entity-0", "entity-1", "entity-2", "entity-3"])
        #expect(Set(identifiers).count == identifiers.count)
        #expect(second.continuation == nil)
    }

    @Test("a continuation is rejected for a different canonical QueryIR")
    func continuationRejectsDifferentQueryIR() async throws {
        let endpoint = try await makeEndpoint()
        let first = try await successfulPage(
            request(query: valuesQuery(), pageLimit: 1),
            requestID: 10,
            endpoint: endpoint
        )
        let continuation = try #require(first.continuation)

        let error = try await remoteFailure(
            request(
                query: valuesQuery(distinct: true),
                pageLimit: 1,
                continuation: continuation
            ),
            requestID: 11,
            endpoint: endpoint
        )

        expectInvalidContinuation(error)
    }

    @Test("a continuation is rejected for a different partition scope")
    func continuationRejectsDifferentPartitionScope() async throws {
        let endpoint = try await makeEndpoint()
        let first = try await successfulPage(
            request(
                query: valuesQuery(),
                graphPartitions: try partition("calendar-a"),
                pageLimit: 1
            ),
            requestID: 20,
            endpoint: endpoint
        )
        let continuation = try #require(first.continuation)

        let error = try await remoteFailure(
            request(
                query: valuesQuery(),
                graphPartitions: try partition("calendar-b"),
                pageLimit: 1,
                continuation: continuation
            ),
            requestID: 21,
            endpoint: endpoint
        )

        expectInvalidContinuation(error)
    }

    @Test("a continuation is rejected after the materialized result changes")
    func continuationRejectsChangedResult() async throws {
        let container = try await makeContainer(seedCount: 3)
        let endpoint = try makeEndpoint(container: container)
        let query = tableQuery()
        let first = try await successfulPage(
            request(query: query, pageLimit: 1),
            requestID: 30,
            endpoint: endpoint
        )
        let continuation = try #require(first.continuation)

        let context = container.newContext()
        var inserted = DatabaseEndpointEntity()
        inserted.id = "entity-added"
        inserted.title = "Added after the first page"
        inserted.priority = 100
        try context.insert(inserted)
        try await context.save()

        let error = try await remoteFailure(
            request(
                query: query,
                pageLimit: 1,
                continuation: continuation
            ),
            requestID: 31,
            endpoint: endpoint
        )

        expectInvalidContinuation(error)
    }

    @Test("malformed binary continuation bytes are rejected")
    func malformedContinuationFrameIsRejected() async throws {
        let endpoint = try await makeEndpoint()

        let error = try await remoteFailure(
            request(
                query: valuesQuery(),
                pageLimit: 1,
                continuation: [0x43, 0x51, 0x50]
            ),
            requestID: 40,
            endpoint: endpoint
        )

        expectInvalidContinuation(error)
    }

    private func makeEndpoint() async throws -> DatabaseEndpoint {
        let container = try await makeContainer()
        return try makeEndpoint(container: container)
    }

    private func makeEndpoint(container: DBContainer) throws -> DatabaseEndpoint {
        let registry = try DatabaseOperationRegistry(
            handlers: [AnyDatabaseOperationHandler(QueryExecuteHandler())],
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

    private func makeContainer(seedCount: Int = 0) async throws -> DBContainer {
        let container = try await DBContainer.open(
            for: try Schema(
                entities: [
                    try DatabaseEndpointEntity.schemaEntity,
                ],
                version: Schema.Version(1, 0, 0)
            ),
            configuration: DBConfiguration.testing(backend: .custom(InMemoryEngine())),
            runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(
            entityRuntimes: [try DatabaseFrameworkRuntime.entity(DatabaseEndpointEntity.self)]
            ),
            security: .disabled
        )
        guard seedCount > 0 else {
            return container
        }
        let context = container.newContext()
        for index in 0..<seedCount {
            var entity = DatabaseEndpointEntity()
            entity.id = "entity-\(index)"
            entity.title = "Title \(index)"
            entity.priority = Int64(index)
            try context.insert(entity)
        }
        try await context.save()
        return container
    }

    private func valuesQuery(distinct: Bool = false) -> SelectQuery {
        SelectQuery(
            projection: .all,
            source: .values(
                (0..<4).map { [.string("entity-\($0)")] },
                columnNames: ["id"]
            ),
            distinct: distinct
        )
    }

    private func tableQuery() -> SelectQuery {
        SelectQuery(
            projection: .all,
            source: .table(TableRef(DatabaseEndpointEntity.persistableType))
        )
    }

    private func request(
        query: SelectQuery,
        graphPartitions: FieldObject = FieldObject(),
        pageLimit: UInt32,
        continuation: ByteString? = nil
    ) -> QueryExecuteOperation.Request {
        QueryExecuteOperation.Request(
            input: .ir(.select(query)),
            graphPartitions: graphPartitions,
            page: QueryExecuteOperation.Page(
                limit: pageLimit,
                continuation: continuation
            )
        )
    }

    private func partition(_ value: String) throws -> FieldObject {
        try FieldObject([
            (key: "calendar", value: .string(value)),
        ])
    }

    private func successfulPage(
        _ request: QueryExecuteOperation.Request,
        requestID: UInt64,
        endpoint: DatabaseEndpoint
    ) async throws -> QueryRowPage {
        let response = try await execute(
            request,
            requestID: requestID,
            endpoint: endpoint
        )
        switch response {
        case .success(let value):
            let response = value
            guard case .rows(let page) = response else {
                Issue.record("Expected a row page response")
                throw ContinuationEndpointAssertionError.unexpectedResponse
            }
            return page
        case .failure(let error):
            Issue.record("Expected success, received \(error.code): \(error.message)")
            throw ContinuationEndpointAssertionError.unexpectedResponse
        }
    }

    private func remoteFailure(
        _ request: QueryExecuteOperation.Request,
        requestID: UInt64,
        endpoint: DatabaseEndpoint
    ) async throws -> RemoteOperationError {
        let response = try await execute(
            request,
            requestID: requestID,
            endpoint: endpoint
        )
        switch response {
        case .failure(let error):
            return error
        case .success:
            Issue.record("Expected the endpoint to return a remote failure")
            throw ContinuationEndpointAssertionError.unexpectedResponse
        }
    }

    private func execute(
        _ request: QueryExecuteOperation.Request,
        requestID: UInt64,
        endpoint: DatabaseEndpoint
    ) async throws -> Result<
        QueryExecuteOperation.Response,
        RemoteOperationError
    > {
        let requestFrame = try DatabaseWireEncoder().encodeRequest(
            DatabaseOperations.queryExecute,
            requestID: requestID,
            metadata: OperationRequestMetadata(),
            request: request
        )
        let responseFrame = try await endpoint.execute(requestFrame)
        let decoder = DatabaseWireDecoder()
        let header = try decoder.decodeResponseHeader(responseFrame)
        #expect(header.requestID == requestID)
        #expect(header.operation == .queryExecute)
        return try decoder.decodeResponse(
            DatabaseOperations.queryExecute,
            from: responseFrame,
            matching: requestID
        )
    }

    private func identifier(
        from row: DatabaseWire.QueryRow,
        columns: [QueryColumn]
    ) throws -> String {
        guard let index = columns.firstIndex(where: { $0.name == "id" }),
              row.values.indices.contains(index),
              case .string(let identifier) = row.values[index] else {
            Issue.record("Expected each row to contain a string id")
            throw ContinuationEndpointAssertionError.missingIdentifier
        }
        return identifier
    }

    private func expectInvalidContinuation(_ error: RemoteOperationError) {
        #expect(error.category == .invalidRequest)
        #expect(error.code == "INVALID_CONTINUATION")
        #expect(error.retryability == .never)
    }
}

private enum ContinuationEndpointAssertionError: Error {
    case missingIdentifier
    case unexpectedResponse
}
