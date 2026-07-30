import DatabaseKit
import TestSupport
import DatabaseEngine
import DatabaseRuntime
import DatabaseServer
import DatabaseTypes
import DatabaseWire
import StorageKit
import Testing

@Suite("Database operation admission policy")
struct DatabaseOperationAdmissionPolicyTests {
    @Test("Admission denial precedes middleware and operation dispatch")
    func denialPrecedesExtensibleDispatch() async throws {
        let container = try await makeContainer()
        let middleware = RecordingMiddleware()
        let handler = DatabaseOperationRoute<CapabilitiesDescribeOperation> {
            _, _ in
            CapabilitiesDescribeOperation.Response(
                runtimeVersion: "denied-handler",
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
            admissionPolicy: AnyDatabaseOperationAdmissionPolicy(
                DenyingAdmissionPolicy()
            ),
            middlewares: [AnyDatabaseRequestMiddleware(middleware)]
        )
        let request = try DatabaseWireEncoder().encodeRequest(
            DatabaseOperations.capabilitiesDescribe,
            requestID: 700,
            metadata: OperationRequestMetadata(traceID: "admission-test"),
            request: EmptyOperationPayload()
        )

        let responseBytes = try await endpoint.execute(request)
        let decoder = DatabaseWireDecoder()
        let header = try decoder.decodeResponseHeader(responseBytes)
        let response = try decoder.decodeResponse(
            DatabaseOperations.capabilitiesDescribe,
            from: responseBytes,
            matching: 700
        )
        guard case .failure(let error) = response else {
            Issue.record("Expected admission denial")
            return
        }
        let middlewareInvocationCount = await middleware.invocationCount

        #expect(header.requestID == 700)
        #expect(header.operation == .capabilitiesDescribe)
        #expect(error.category == .authorization)
        #expect(error.code == "OPERATION_DENIED")
        #expect(error.message == "Operation denied")
        #expect(error.retryability == .never)
        #expect(middlewareInvocationCount == 0)
    }

    private func makeContainer() async throws -> DBContainer {
        try await DBContainer.open(
            for: try Schema(
                entities: [
                    try DatabaseEndpointEntity.schemaEntity,
                ],
                version: Schema.Version(1, 0, 0)
            ),
            configuration: DBConfiguration.testing(
                backend: .custom(InMemoryEngine())
            ),
            runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(
            entityRuntimes: [try DatabaseFrameworkRuntime.entity(DatabaseEndpointEntity.self)]
            ),
            security: .disabled
        )
    }

    private struct DenyingAdmissionPolicy: DatabaseOperationAdmissionPolicy {
        func decision(
            for request: DatabaseOperationAdmissionRequest
        ) -> DatabaseOperationAdmissionDecision {
            #expect(request.requestID == 700)
            #expect(request.operation == .capabilitiesDescribe)
            #expect(request.metadata.traceID == "admission-test")
            return .deny(
                DatabaseOperationAdmissionDenial(
                    code: "OPERATION_DENIED",
                    message: "Operation denied",
                    retryability: .never
                )
            )
        }
    }
}
