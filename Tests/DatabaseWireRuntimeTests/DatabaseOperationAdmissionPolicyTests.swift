import DatabaseKit
import TestSupport
import DatabaseEngine
import DatabaseRuntime
import DatabaseWireRuntime
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
            target: .database,
            metadata: OperationRequestMetadata(traceID: "admission-test"),
            request: EmptyOperationPayload()
        )

        let authorization = AuthorizationContext.authenticated(
            Principal(identifier: "admission-user", roles: ["reader"])
        )
        let responseBytes = try await endpoint.execute(
            request,
            context: DatabaseRequestExecutionContext(
                authorization: authorization
            )
        )
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

    @Test("Authorization context remains isolated across concurrent requests")
    func authorizationContextIsRequestLocal() async throws {
        let container = try await makeContainer()
        let handler = DatabaseOperationRoute<CapabilitiesDescribeOperation> {
            _, context in
            await Task.yield()
            let taskPrincipal = RequestAuthorization.context.principal?.identifier
            let operationPrincipal = context.authorization.principal?.identifier
            #expect(taskPrincipal == operationPrincipal)
            return CapabilitiesDescribeOperation.Response(
                runtimeVersion: taskPrincipal ?? "anonymous",
                features: [],
                jobOperations: []
            )
        }
        let endpoint = DatabaseEndpoint(
            container: container,
            registry: try DatabaseOperationRegistry(
                handlers: [AnyDatabaseOperationHandler(handler)],
                requiredOperations: [.capabilitiesDescribe]
            ),
            admissionPolicy: AnyDatabaseOperationAdmissionPolicy(
                UnrestrictedDatabaseOperationAdmissionPolicy()
            )
        )
        let encoder = DatabaseWireEncoder()
        let aliceRequest = try encoder.encodeRequest(
            DatabaseOperations.capabilitiesDescribe,
            requestID: 701,
            target: .database,
            request: EmptyOperationPayload()
        )
        let bobRequest = try encoder.encodeRequest(
            DatabaseOperations.capabilitiesDescribe,
            requestID: 702,
            target: .database,
            request: EmptyOperationPayload()
        )

        async let aliceResponse = endpoint.execute(
            aliceRequest,
            context: DatabaseRequestExecutionContext(
                authorization: .authenticated(
                    Principal(identifier: "alice")
                )
            )
        )
        async let bobResponse = endpoint.execute(
            bobRequest,
            context: DatabaseRequestExecutionContext(
                authorization: .authenticated(
                    Principal(identifier: "bob")
                )
            )
        )
        let responses = try await (aliceResponse, bobResponse)
        let decoder = DatabaseWireDecoder()
        let alice = try decoder.decodeResponse(
            DatabaseOperations.capabilitiesDescribe,
            from: responses.0,
            matching: 701
        )
        let bob = try decoder.decodeResponse(
            DatabaseOperations.capabilitiesDescribe,
            from: responses.1,
            matching: 702
        )

        #expect(try alice.get().runtimeVersion == "alice")
        #expect(try bob.get().runtimeVersion == "bob")
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
                storageEngine: InMemoryEngine()
            ),
            runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(
            entityRuntimes: [try DatabaseFrameworkRuntime.entity(DatabaseEndpointEntity.self)]
            ),
            security: .testingDisabled
        )
    }

    private struct DenyingAdmissionPolicy: DatabaseOperationAdmissionPolicy {
        func decision(
            for request: DatabaseOperationAdmissionRequest
        ) -> DatabaseOperationAdmissionDecision {
            #expect(request.requestID == 700)
            #expect(request.operation == .capabilitiesDescribe)
            #expect(request.metadata.traceID == "admission-test")
            #expect(
                request.authorization.principal?.identifier
                    == "admission-user"
            )
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
