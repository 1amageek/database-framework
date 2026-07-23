import Core
import DatabaseEngine
import DatabaseRuntime
import DatabaseServer
import DatabaseValue
import DatabaseWire
import QueryIR
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

        let identifiers = try (first.rows + second.rows).map {
            try identifier(from: $0)
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
                graphPartitions: [partition("calendar-a")],
                pageLimit: 1
            ),
            requestID: 20,
            endpoint: endpoint
        )
        let continuation = try #require(first.continuation)

        let error = try await remoteFailure(
            request(
                query: valuesQuery(),
                graphPartitions: [partition("calendar-b")],
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
            for: Schema(
                [DatabaseEndpointEntity.self],
                version: Schema.Version(1, 0, 0)
            ),
            configuration: DBConfiguration(backend: .custom(InMemoryEngine())),
            runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(),
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
            entity.priority = index
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
        graphPartitions: [DatabaseObjectField] = [],
        pageLimit: UInt32,
        continuation: DatabaseBytes? = nil
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

    private func partition(_ value: String) -> DatabaseObjectField {
        DatabaseObjectField(
            number: 1,
            name: "calendar",
            value: .string(value)
        )
    }

    private func successfulPage(
        _ request: QueryExecuteOperation.Request,
        requestID: UInt64,
        endpoint: DatabaseEndpoint
    ) async throws -> QueryExecuteOperation.RowPage {
        let envelope = try await execute(
            request,
            requestID: requestID,
            endpoint: endpoint
        )
        switch envelope.payload {
        case .success(let payload):
            let response = try DatabaseEnvelopeCodec.decode(
                QueryExecuteOperation.Response.self,
                from: payload
            )
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
    ) async throws -> DatabaseRemoteError {
        let envelope = try await execute(
            request,
            requestID: requestID,
            endpoint: endpoint
        )
        switch envelope.payload {
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
    ) async throws -> DatabaseWireResponseEnvelope {
        let requestFrame = try DatabaseEnvelopeCodec.encodeRequest(
            QueryExecuteOperation.self,
            requestID: requestID,
            metadata: DatabaseRequestMetadata(),
            request: request
        )
        let responseFrame = try await endpoint.execute(requestFrame)
        let envelope = try DatabaseEnvelopeCodec.decodeResponse(responseFrame)
        #expect(envelope.requestID == requestID)
        #expect(envelope.operation == .queryExecute)
        return envelope
    }

    private func identifier(
        from row: QueryExecuteOperation.Row
    ) throws -> String {
        guard let value = row.values.first(where: { $0.name == "id" })?.value,
              case .string(let identifier) = value else {
            Issue.record("Expected each row to contain a string id")
            throw ContinuationEndpointAssertionError.missingIdentifier
        }
        return identifier
    }

    private func expectInvalidContinuation(_ error: DatabaseRemoteError) {
        #expect(error.category == .invalidRequest)
        #expect(error.code == "INVALID_CONTINUATION")
        #expect(error.retryability == .never)
    }
}

private enum ContinuationEndpointAssertionError: Error {
    case missingIdentifier
    case unexpectedResponse
}
