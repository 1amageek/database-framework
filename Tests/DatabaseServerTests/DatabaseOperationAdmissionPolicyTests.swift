import Core
import DatabaseEngine
import DatabaseRuntime
import DatabaseServer
import DatabaseValue
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
        let request = try DatabaseEnvelopeCodec.encodeRequest(
            CapabilitiesDescribeOperation.self,
            requestID: 700,
            metadata: DatabaseRequestMetadata(traceID: "admission-test"),
            request: DatabaseEmpty()
        )

        let responseBytes = try await endpoint.execute(request)
        let response = try DatabaseEnvelopeCodec.decodeResponse(responseBytes)
        guard case .failure(let error) = response.payload else {
            Issue.record("Expected admission denial")
            return
        }
        let middlewareInvocationCount = await middleware.invocationCount

        #expect(response.requestID == 700)
        #expect(response.operation == .capabilitiesDescribe)
        #expect(error.category == .authorization)
        #expect(error.code == "OPERATION_DENIED")
        #expect(error.message == "Operation denied")
        #expect(error.retryability == .never)
        #expect(middlewareInvocationCount == 0)
    }

    private func makeContainer() async throws -> DBContainer {
        try await DBContainer.open(
            for: Schema(
                [DatabaseEndpointRecord.self],
                version: Schema.Version(1, 0, 0)
            ),
            configuration: DBConfiguration(
                backend: .custom(InMemoryEngine())
            ),
            runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(),
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
