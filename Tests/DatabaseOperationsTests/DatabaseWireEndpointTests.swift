import DatabaseKit
import TestSupport
import DatabaseRuntime
import DatabaseEngine
import DatabaseOperations
import DatabaseWireAdapter
import DatabaseTypes
import DatabaseWire
import StorageKit
import Testing

@Suite("Canonical database endpoint", .serialized)
struct DatabaseWireEndpointTests {
    @Test("canonical request and response round-trip preserves identity and metadata")
    func canonicalRoundTrip() async throws {
        let container = try await makeContainer()
        let handler = DatabaseOperationRoute<CapabilitiesDescribeOperation> {
            _, context in
            CapabilitiesDescribeOperation.Response(
                runtimeVersion: "request-\(context.requestID)-\(context.metadata.traceID ?? "none")",
                features: [
                    CapabilitiesDescribeOperation.Feature(
                        identifier: "canonical-wire",
                        version: 1
                    )
                ],
                jobOperations: []
            )
        }
        let registry = try DatabaseOperationRegistry(
            handlers: [AnyDatabaseOperationHandler(handler)],
            requiredOperations: [.capabilitiesDescribe]
        )
        let endpoint = DatabaseWireEndpoint(
            container: container,
            registry: registry,
            admissionPolicy: Self.unrestrictedAdmissionPolicy
        )
        let request = try makeRequest(
            operation: DatabaseOperationCatalog.capabilitiesDescribe,
            requestID: 9_223_372_036_854_775_001,
            metadata: OperationRequestMetadata(
                traceID: "trace-canonical",
                idempotencyKey: "request-canonical"
            ),
            payload: EmptyOperationPayload()
        )

        let responseBytes = try await endpoint.execute(
            request,
            context: DatabaseRequestExecutionContext(authorization: .anonymous)
        )
        let header = try DatabaseWireDecoder().decodeResponseHeader(
            responseBytes
        )
        let decoded = try successfulResponse(
            DatabaseOperationCatalog.capabilitiesDescribe,
            responseBytes: responseBytes,
            requestID: 9_223_372_036_854_775_001
        )

        #expect(header.requestID == 9_223_372_036_854_775_001)
        #expect(header.operation == .capabilitiesDescribe)
        #expect(decoded.runtimeVersion == "request-9223372036854775001-trace-canonical")
        #expect(decoded.features == [
            CapabilitiesDescribeOperation.Feature(
                identifier: "canonical-wire",
                version: 1
            )
        ])
    }

    @Test("middleware observes the canonical request context")
    func middlewareRunsAroundTypedHandler() async throws {
        let container = try await makeContainer()
        let middleware = RecordingMiddleware()
        let handler = DatabaseOperationRoute<CapabilitiesDescribeOperation> {
            _, _ in
            CapabilitiesDescribeOperation.Response(
                runtimeVersion: "middleware",
                features: [],
                jobOperations: []
            )
        }
        let registry = try DatabaseOperationRegistry(
            handlers: [AnyDatabaseOperationHandler(handler)],
            requiredOperations: [.capabilitiesDescribe]
        )
        let endpoint = DatabaseWireEndpoint(
            container: container,
            registry: registry,
            admissionPolicy: Self.unrestrictedAdmissionPolicy,
            middlewares: [AnyDatabaseRequestMiddleware(middleware)]
        )
        let request = try makeRequest(
            operation: DatabaseOperationCatalog.capabilitiesDescribe,
            requestID: 42,
            metadata: OperationRequestMetadata(traceID: "trace-middleware"),
            payload: EmptyOperationPayload()
        )

        _ = try await endpoint.execute(
            request,
            context: DatabaseRequestExecutionContext(authorization: .anonymous)
        )
        let invocationCount = await middleware.invocationCount
        let traceID = await middleware.traceID
        let requestID = await middleware.requestID

        #expect(invocationCount == 1)
        #expect(traceID == "trace-middleware")
        #expect(requestID == 42)
    }

    @Test("handler cancellation propagates without a failure envelope")
    func cancellationPropagates() async throws {
        let container = try await makeContainer()
        let handler = DatabaseOperationRoute<CapabilitiesDescribeOperation> {
            _, _ in
            throw CancellationError()
        }
        let registry = try DatabaseOperationRegistry(
            handlers: [AnyDatabaseOperationHandler(handler)],
            requiredOperations: [.capabilitiesDescribe]
        )
        let endpoint = DatabaseWireEndpoint(
            container: container,
            registry: registry,
            admissionPolicy: Self.unrestrictedAdmissionPolicy
        )
        let request = try makeRequest(
            operation: DatabaseOperationCatalog.capabilitiesDescribe,
            requestID: 43,
            payload: EmptyOperationPayload()
        )

        await #expect(throws: CancellationError.self) {
            _ = try await endpoint.execute(
                request,
                context: DatabaseRequestExecutionContext(authorization: .anonymous)
            )
        }
    }

    @Test("uncompiled operations return a typed unavailable failure")
    func uncompiledOperationIsUnavailable() async throws {
        let endpoint = try await makeDescribeEndpoint()
        let request = try makeRequest(
            operation: DatabaseOperationCatalog.schemaDescribe,
            requestID: 45,
            payload: EmptyOperationPayload()
        )

        let responseBytes = try await endpoint.execute(
            request,
            context: DatabaseRequestExecutionContext(authorization: .anonymous)
        )
        let response = try DatabaseWireDecoder().decodeResponse(
            DatabaseOperationCatalog.schemaDescribe,
            from: responseBytes,
            matching: 45
        )

        guard case .failure(let error) = response else {
            Issue.record("Expected an unavailable operation failure")
            return
        }
        #expect(error.category == .unavailable)
        #expect(error.code == "OPERATION_UNAVAILABLE")
        #expect(error.retryability == .never)
        #expect(
            error.details["operation"]
                == .uint64(
                    UInt64(DatabaseOperationIdentifier.schemaDescribe.rawValue)
                )
        )
    }

    @Test("Oversized mapped details reduce to an encodable typed failure")
    func oversizedFailureDetailsUseBoundedFallback() async throws {
        let container = try await makeContainer()
        let handler = DatabaseOperationRoute<CapabilitiesDescribeOperation> {
            _, _ in
            throw EndpointInvocationFailure.remoteFailure
        }
        let registry = try DatabaseOperationRegistry(
            handlers: [AnyDatabaseOperationHandler(handler)],
            requiredOperations: [.capabilitiesDescribe]
        )
        let limits = try DatabaseWireLimits(
            maximumFrameBytes: 4_096,
            maximumStringBytes: 64,
            maximumByteStringBytes: 4_096,
            maximumCollectionCount: 4,
            maximumNestingDepth: 8,
            maximumObjectCount: 16
        )
        let endpoint = DatabaseWireEndpoint(
            container: container,
            registry: registry,
            admissionPolicy: Self.unrestrictedAdmissionPolicy,
            responseLimits: limits,
            errorMapper: try OversizedEndpointErrorMapper()
        )
        let request = try makeRequest(
            operation: DatabaseOperationCatalog.capabilitiesDescribe,
            requestID: 44,
            payload: EmptyOperationPayload()
        )

        let responseBytes = try await endpoint.execute(
            request,
            context: DatabaseRequestExecutionContext(authorization: .anonymous)
        )
        let decoded = try DatabaseWireDecoder(limits: limits).decodeResponse(
            DatabaseOperationCatalog.capabilitiesDescribe,
            from: responseBytes,
            matching: 44
        )
        guard case .failure(let error) = decoded else {
            Issue.record("Expected a failure response")
            return
        }
        #expect(error.category == .internalFailure)
        #expect(error.code == "OVERSIZED_TEST_FAILURE")
        #expect(error.retryability == .never)
        #expect(error.details.isEmpty)
    }

    @Test("request and response limits remain independent")
    func asymmetricWireLimitsRemainIndependent() async throws {
        let container = try await makeContainer()
        let handler = DatabaseOperationRoute<BaseExecuteOperation> { _, _ in
            .bases([])
        }
        let registry = try DatabaseOperationRegistry(
            handlers: [AnyDatabaseOperationHandler(handler)],
            requiredOperations: [.baseExecute]
        )
        let requestLimits = try DatabaseWireLimits(
            maximumFrameBytes: 4_096,
            maximumStringBytes: 256,
            maximumByteStringBytes: 4_096,
            maximumCollectionCount: 8,
            maximumNestingDepth: 8,
            maximumObjectCount: 32
        )
        let responseLimits = try DatabaseWireLimits(
            maximumFrameBytes: 256,
            maximumStringBytes: 128,
            maximumByteStringBytes: 256,
            maximumCollectionCount: 4,
            maximumNestingDepth: 8,
            maximumObjectCount: 16
        )
        let endpoint = DatabaseWireEndpoint(
            container: container,
            registry: registry,
            admissionPolicy: Self.unrestrictedAdmissionPolicy,
            requestLimits: requestLimits,
            responseLimits: responseLimits
        )
        let baseID = try Base.ID(String(repeating: "b", count: 120))
        let request = try DatabaseWireEncoder(limits: requestLimits)
            .encodeRequest(
                DatabaseOperationCatalog.baseExecute,
                requestID: 46,
                target: .database,
                metadata: OperationRequestMetadata(
                    idempotencyKey: String(repeating: "i", count: 120)
                ),
                request: BaseExecuteOperation.Request(
                    invocation: .create(
                        baseID: baseID,
                        placementID: try Base.Placement.ID(
                            String(repeating: "p", count: 120)
                        ),
                        initialGrants: [
                            Security.Grant(
                                subject: .principal(
                                    String(repeating: "s", count: 120)
                                ),
                                resource: .base(baseID),
                                access: .all
                            )
                        ],
                        expectedRevision: 0,
                        idempotencyKey: String(repeating: "i", count: 120)
                    )
                )
            )
        #expect(request.count > responseLimits.maximumFrameBytes)

        let response = try await endpoint.execute(
            request,
            context: DatabaseRequestExecutionContext(authorization: .anonymous)
        )
        #expect(response.count <= responseLimits.maximumFrameBytes)
        let decoded = try DatabaseWireDecoder(limits: responseLimits)
            .decodeResponse(
                DatabaseOperationCatalog.baseExecute,
                from: response,
                matching: 46
            )
        #expect(decoded == .success(.bases([])))
    }

    @Test("truncated request frame is rejected before dispatch")
    func truncatedFrameIsRejected() async throws {
        let endpoint = try await makeDescribeEndpoint()
        let valid = try makeRequest(
            operation: DatabaseOperationCatalog.capabilitiesDescribe,
            requestID: 7,
            payload: EmptyOperationPayload()
        )

        await expectInvalidRequestFrame(
            valid[0..<(valid.count - 1)],
            endpoint: endpoint
        )
    }

    @Test("invalid request magic is rejected before dispatch")
    func invalidMagicIsRejected() async throws {
        let endpoint = try await makeDescribeEndpoint()
        var invalid = try makeRequest(
            operation: DatabaseOperationCatalog.capabilitiesDescribe,
            requestID: 8,
            payload: EmptyOperationPayload()
        ).copyBytes()
        invalid[0] = 0

        await expectInvalidRequestFrame(
            ByteString(invalid),
            endpoint: endpoint
        )
    }

    @Test("capabilities and schema handlers describe the compiled runtime")
    func describesCapabilitiesAndSchema() async throws {
        let container = try await makeContainer()
        try await container.grantTestDatabaseAccess(
            to: .principal("test-runner"),
            access: .read
        )
        let identity = DatabaseOperationIdentity(version: "3.2.1")
        let registry = try DatabaseOperationRegistry(
            handlers: [
                AnyDatabaseOperationHandler(
                    CapabilitiesDescribeHandler(
                        identity: identity,
                        jobOperations: [
                            try JobOperationIdentifier(
                                family: .commandExecute,
                                kind: "calendar.import.validate"
                            ),
                        ]
                    )
                ),
                AnyDatabaseOperationHandler(
                    SchemaDescribeHandler()
                ),
            ],
            requiredOperations: [.capabilitiesDescribe, .schemaDescribe]
        )
        let endpoint = DatabaseWireEndpoint(
            container: container,
            registry: registry,
            admissionPolicy: Self.unrestrictedAdmissionPolicy
        )

        let capabilities: CapabilitiesDescribeOperation.Response = try await invoke(
            DatabaseOperationCatalog.capabilitiesDescribe,
            requestID: 100,
            endpoint: endpoint,
            authorization: TestBaseEnvironment.authorization
        )
        let schema: SchemaDescribeOperation.Response = try await invoke(
            DatabaseOperationCatalog.schemaDescribe,
            requestID: 101,
            endpoint: endpoint,
            authorization: TestBaseEnvironment.authorization
        )

        #expect(capabilities.runtimeVersion == "3.2.1")
        #expect(
            capabilities.features.map(\.identifier) == [
                "base.execute",
                "capabilities.describe",
                "command.execute",
                "composition.execute",
                "composition.query.aggregate.avg",
                "composition.query.aggregate.count",
                "composition.query.aggregate.max",
                "composition.query.aggregate.min",
                "composition.query.aggregate.sum",
                "composition.query.distinct-provenance",
                "composition.query.global-order-window",
                "composition.query.scan-filter-project",
                "composition.query.sparql-ask",
                "composition.query.sparql-select",
                "composition.query.vector",
                "grant.execute",
                "graph.algorithm",
                "job.cancel",
                "job.result",
                "job.start",
                "job.status",
                "maintenance.execute",
                "mutation.execute",
                "ontology.execute",
                "query.execute",
                "schema.describe",
                "shacl.execute",
            ]
        )
        #expect(capabilities.features.allSatisfy { $0.version == 1 })
        #expect(
            capabilities.jobOperations == [
                try JobOperationIdentifier(
                    family: .commandExecute,
                    kind: "calendar.import.validate"
                ),
            ]
        )
        #expect(schema.version == container.schema.version)
        #expect(schema.entities.count == 1)
        #expect(schema.entities[0].name == DatabaseEndpointEntity.persistableType)
        #expect(schema.entities[0].fields.map(\.name) == ["id", "title", "priority"])
        #expect(schema.entities[0].fields.map(\.type) == [.string, .string, .int64])
    }

    private func makeContainer() async throws -> DBContainer {
        let schema = try Schema(
            entities: [try DatabaseEndpointEntity.schemaEntity],
            version: Schema.Version(1, 0, 0)
        )
        return try await DBContainer.open(
            for: schema,
            configuration: DBConfiguration.testing(storageEngine: InMemoryEngine()),
            runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(
            entityRuntimes: [try DatabaseFrameworkRuntime.entity(DatabaseEndpointEntity.self)]
            ),
            security: .testingDisabled
        )
    }

    private func makeDescribeEndpoint() async throws -> DatabaseWireEndpoint {
        let container = try await makeContainer()
        let handler = DatabaseOperationRoute<CapabilitiesDescribeOperation> {
            _, _ in
            CapabilitiesDescribeOperation.Response(
                runtimeVersion: "test",
                features: [],
                jobOperations: []
            )
        }
        let registry = try DatabaseOperationRegistry(
            handlers: [AnyDatabaseOperationHandler(handler)],
            requiredOperations: [.capabilitiesDescribe]
        )
        return DatabaseWireEndpoint(
            container: container,
            registry: registry,
            admissionPolicy: Self.unrestrictedAdmissionPolicy
        )
    }

    private static var unrestrictedAdmissionPolicy:
        AnyDatabaseOperationAdmissionPolicy {
        AnyDatabaseOperationAdmissionPolicy(
            UnrestrictedDatabaseOperationAdmissionPolicy()
        )
    }

    private func makeRequest<Request, Response>(
        operation: DatabaseOperation<Request, Response>,
        requestID: UInt64,
        metadata: OperationRequestMetadata = OperationRequestMetadata(),
        payload: Request
    ) throws -> ByteString {
        try DatabaseWireEncoder().encodeRequest(
            operation,
            requestID: requestID,
            target: .database,
            metadata: metadata,
            request: payload
        )
    }

    private func invoke<Response>(
        _ operation: DatabaseOperation<EmptyOperationPayload, Response>,
        requestID: UInt64,
        endpoint: DatabaseWireEndpoint,
        authorization: AuthorizationContext = .anonymous
    ) async throws -> Response {
        let request = try makeRequest(
            operation: operation,
            requestID: requestID,
            payload: EmptyOperationPayload()
        )
        let responseBytes = try await endpoint.execute(
            request,
            context: DatabaseRequestExecutionContext(
                authorization: authorization
            )
        )
        let header = try DatabaseWireDecoder().decodeResponseHeader(
            responseBytes
        )
        #expect(header.requestID == requestID)
        #expect(header.operation == operation.identifier)
        return try successfulResponse(
            operation,
            responseBytes: responseBytes,
            requestID: requestID
        )
    }

    private func successfulResponse<Request, Response>(
        _ operation: DatabaseOperation<Request, Response>,
        responseBytes: ByteString,
        requestID: UInt64
    ) throws -> Response {
        switch try DatabaseWireDecoder().decodeResponse(
            operation,
            from: responseBytes,
            matching: requestID
        ) {
        case .success(let response):
            return response
        case .failure(let error):
            Issue.record("Expected success, received \(error.code): \(error.message)")
            throw EndpointInvocationFailure.remoteFailure
        }
    }

    private func expectInvalidRequestFrame(
        _ request: ByteString,
        endpoint: DatabaseWireEndpoint
    ) async {
        do {
            _ = try await endpoint.execute(
                request,
                context: DatabaseRequestExecutionContext(authorization: .anonymous)
            )
            Issue.record("Expected an invalid request frame error")
        } catch DatabaseWireAdapterError.invalidRequestFrame {
        } catch {
            Issue.record("Unexpected error: \(error)")
        }
    }
}

private struct OversizedEndpointErrorMapper: DatabaseErrorMapper {
    private let details: FieldObject

    init() throws {
        details = try FieldObject(
            (0..<100).map { index in
                (
                    key: "detail\(index)",
                    value: FieldValue.string("value")
                )
            }
        )
    }

    func remoteError(
        for error: any Error,
        context: DatabaseOperationContext,
        limits: DatabaseWireLimits
    ) -> RemoteOperationError {
        RemoteOperationError(
            category: .internalFailure,
            code: "OVERSIZED_TEST_FAILURE",
            message: "Mapped failure",
            retryability: .never,
            details: details
        )
    }
}
