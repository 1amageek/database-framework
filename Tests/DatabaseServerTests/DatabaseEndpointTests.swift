import Core
import DatabaseRuntime
import DatabaseEngine
import DatabaseServer
import DatabaseValue
import DatabaseWire
import StorageKit
import Testing

@Suite("Canonical database endpoint", .serialized)
struct DatabaseEndpointTests {
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
        let endpoint = DatabaseEndpoint(
            container: container,
            registry: registry,
            admissionPolicy: Self.unrestrictedAdmissionPolicy
        )
        let request = try makeRequest(
            operation: CapabilitiesDescribeOperation.self,
            requestID: 9_223_372_036_854_775_001,
            metadata: DatabaseRequestMetadata(
                traceID: "trace-canonical",
                idempotencyKey: "request-canonical"
            ),
            payload: DatabaseEmpty()
        )

        let responseBytes = try await endpoint.execute(request)
        let response = try DatabaseEnvelopeCodec.decodeResponse(responseBytes)
        let payload = try successPayload(response)
        let decoded = try DatabaseEnvelopeCodec.decode(
            CapabilitiesDescribeOperation.Response.self,
            from: payload
        )

        #expect(response.requestID == 9_223_372_036_854_775_001)
        #expect(response.operation == .capabilitiesDescribe)
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
        let endpoint = DatabaseEndpoint(
            container: container,
            registry: registry,
            admissionPolicy: Self.unrestrictedAdmissionPolicy,
            middlewares: [AnyDatabaseRequestMiddleware(middleware)]
        )
        let request = try makeRequest(
            operation: CapabilitiesDescribeOperation.self,
            requestID: 42,
            metadata: DatabaseRequestMetadata(traceID: "trace-middleware"),
            payload: DatabaseEmpty()
        )

        _ = try await endpoint.execute(request)
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
        let endpoint = DatabaseEndpoint(
            container: container,
            registry: registry,
            admissionPolicy: Self.unrestrictedAdmissionPolicy
        )
        let request = try makeRequest(
            operation: CapabilitiesDescribeOperation.self,
            requestID: 43,
            payload: DatabaseEmpty()
        )

        await #expect(throws: CancellationError.self) {
            _ = try await endpoint.execute(request)
        }
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
        let endpoint = DatabaseEndpoint(
            container: container,
            registry: registry,
            admissionPolicy: Self.unrestrictedAdmissionPolicy,
            limits: limits,
            errorMapper: OversizedEndpointErrorMapper()
        )
        let request = try makeRequest(
            operation: CapabilitiesDescribeOperation.self,
            requestID: 44,
            payload: DatabaseEmpty()
        )

        let responseBytes = try await endpoint.execute(request)
        let response = try DatabaseEnvelopeCodec.decodeResponse(
            responseBytes,
            limits: limits
        )
        guard case .failure(let error) = response.payload else {
            Issue.record("Expected a failure response")
            return
        }
        #expect(error.category == .internalFailure)
        #expect(error.code == "OVERSIZED_TEST_FAILURE")
        #expect(error.retryability == .never)
        #expect(error.details.isEmpty)
    }

    @Test("truncated request frame is rejected before dispatch")
    func truncatedFrameIsRejected() async throws {
        let endpoint = try await makeDescribeEndpoint()
        let valid = try makeRequest(
            operation: CapabilitiesDescribeOperation.self,
            requestID: 7,
            payload: DatabaseEmpty()
        )

        await expectInvalidRequestFrame(
            valid.slice(0..<(valid.count - 1)),
            endpoint: endpoint
        )
    }

    @Test("invalid request magic is rejected before dispatch")
    func invalidMagicIsRejected() async throws {
        let endpoint = try await makeDescribeEndpoint()
        var invalid = try makeRequest(
            operation: CapabilitiesDescribeOperation.self,
            requestID: 8,
            payload: DatabaseEmpty()
        ).contiguousArray()
        invalid[0] = 0

        await expectInvalidRequestFrame(
            DatabaseBytes(invalid),
            endpoint: endpoint
        )
    }

    @Test("capabilities and schema handlers describe the compiled runtime")
    func describesCapabilitiesAndSchema() async throws {
        let container = try await makeContainer()
        let identity = DatabaseRuntimeIdentity(version: "3.2.1")
        let registry = try DatabaseOperationRegistry(
            handlers: [
                AnyDatabaseOperationHandler(
                    CapabilitiesDescribeHandler(
                        identity: identity,
                        jobOperations: [
                            try DatabaseJobOperationIdentifier(
                                family: .commandWrite,
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
        let endpoint = DatabaseEndpoint(
            container: container,
            registry: registry,
            admissionPolicy: Self.unrestrictedAdmissionPolicy
        )

        let capabilities: CapabilitiesDescribeOperation.Response = try await invoke(
            CapabilitiesDescribeOperation.self,
            requestID: 100,
            endpoint: endpoint
        )
        let schema: SchemaDescribeOperation.Response = try await invoke(
            SchemaDescribeOperation.self,
            requestID: 101,
            endpoint: endpoint
        )

        #expect(capabilities.runtimeVersion == "3.2.1")
        #expect(
            capabilities.features.map(\.identifier) == [
                "capabilities.describe",
                "schema.describe",
                "query.execute",
                "mutation.execute",
                "graph.algorithm",
                "ontology.execute",
                "shacl.execute",
                "command.read",
                "command.write",
                "maintenance.execute",
                "job.start",
                "job.status",
                "job.result",
                "job.cancel",
            ]
        )
        #expect(capabilities.features.allSatisfy { $0.version == 1 })
        #expect(
            capabilities.jobOperations == [
                try DatabaseJobOperationIdentifier(
                    family: .commandWrite,
                    kind: "calendar.import.validate"
                ),
            ]
        )
        #expect(schema.version == container.schema.version)
        #expect(schema.entities.count == 1)
        #expect(schema.entities[0].name == DatabaseEndpointRecord.persistableType)
        #expect(schema.entities[0].fields.map(\.name) == ["id", "title", "priority"])
        #expect(schema.entities[0].fields.map(\.type) == [.string, .string, .int64])
    }

    private func makeContainer() async throws -> DBContainer {
        let schema = Schema(
            [DatabaseEndpointRecord.self],
            version: Schema.Version(1, 0, 0)
        )
        return try await DBContainer.open(
            for: schema,
            configuration: DBConfiguration(backend: .custom(InMemoryEngine())),
            runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(),
            security: .disabled
        )
    }

    private func makeDescribeEndpoint() async throws -> DatabaseEndpoint {
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
        return DatabaseEndpoint(
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

    private func makeRequest<Operation: DatabaseOperation>(
        operation: Operation.Type,
        requestID: UInt64,
        metadata: DatabaseRequestMetadata = DatabaseRequestMetadata(),
        payload: Operation.Request
    ) throws -> DatabaseBytes {
        try DatabaseEnvelopeCodec.encodeRequest(
            operation,
            requestID: requestID,
            metadata: metadata,
            request: payload
        )
    }

    private func invoke<Operation: DatabaseOperation>(
        _ operation: Operation.Type,
        requestID: UInt64,
        endpoint: DatabaseEndpoint
    ) async throws -> Operation.Response where Operation.Request == DatabaseEmpty {
        let request = try makeRequest(
            operation: operation,
            requestID: requestID,
            payload: DatabaseEmpty()
        )
        let responseBytes = try await endpoint.execute(request)
        let response = try DatabaseEnvelopeCodec.decodeResponse(responseBytes)
        #expect(response.requestID == requestID)
        #expect(response.operation == Operation.identifier)
        return try DatabaseEnvelopeCodec.decode(
            Operation.Response.self,
            from: successPayload(response)
        )
    }

    private func successPayload(
        _ response: DatabaseWireResponseEnvelope
    ) throws -> DatabaseBytes {
        switch response.payload {
        case .success(let payload):
            return payload
        case .failure(let error):
            Issue.record("Expected success, received \(error.code): \(error.message)")
            throw EndpointInvocationFailure.remoteFailure
        }
    }

    private func expectInvalidRequestFrame(
        _ request: DatabaseBytes,
        endpoint: DatabaseEndpoint
    ) async {
        do {
            _ = try await endpoint.execute(request)
            Issue.record("Expected an invalid request frame error")
        } catch DatabaseEndpointError.invalidRequestFrame {
        } catch {
            Issue.record("Unexpected error: \(error)")
        }
    }
}

private struct OversizedEndpointErrorMapper: DatabaseErrorMapper {
    func remoteError(
        for error: any Error,
        context: DatabaseOperationContext,
        limits: DatabaseWireLimits
    ) -> DatabaseRemoteError {
        DatabaseRemoteError(
            category: .internalFailure,
            code: "OVERSIZED_TEST_FAILURE",
            message: "Mapped failure",
            retryability: .never,
            details: (0..<100).map { index in
                DatabaseObjectField(
                    number: UInt32(index + 1),
                    name: "detail",
                    value: .string("value")
                )
            }
        )
    }
}
