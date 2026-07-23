import Core
import DatabaseEngine
import DatabaseRuntime
import DatabaseServer
import DatabaseValue
import DatabaseWire
import StorageKit
import Testing

@Suite("Database operation authorization policy")
struct DatabaseOperationAuthorizationPolicyTests {
    @Test("Authorization runs before middleware and operation dispatch")
    func authorizationPrecedesExtensibleDispatch() async throws {
        let container = try await makeContainer()
        let middleware = RecordingMiddleware()
        let handler = DatabaseOperationRoute<CapabilitiesDescribeOperation> {
            _, _ in
            CapabilitiesDescribeOperation.Response(
                runtimeVersion: "unauthorized-handler",
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
            authorizationPolicy: AnyDatabaseOperationAuthorizationPolicy(
                DenyingAuthorizationPolicy()
            ),
            middlewares: [AnyDatabaseRequestMiddleware(middleware)],
            errorMapper: AuthorizationErrorMapper()
        )
        let request = try DatabaseEnvelopeCodec.encodeRequest(
            CapabilitiesDescribeOperation.self,
            requestID: 700,
            metadata: DatabaseRequestMetadata(),
            request: DatabaseEmpty()
        )

        let responseBytes = try await endpoint.execute(request)
        let response = try DatabaseEnvelopeCodec.decodeResponse(responseBytes)
        guard case .failure(let error) = response.payload else {
            Issue.record("Expected authorization failure")
            return
        }
        let middlewareInvocationCount = await middleware.invocationCount

        #expect(response.requestID == 700)
        #expect(response.operation == .capabilitiesDescribe)
        #expect(error.category == .authorization)
        #expect(error.code == "OPERATION_DENIED")
        #expect(error.retryability == .never)
        #expect(middlewareInvocationCount == 0)
    }

    private func makeContainer() async throws -> DBContainer {
        try await DBContainer(
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

    private struct DenyingAuthorizationPolicy:
        DatabaseOperationAuthorizationPolicy {
        func authorize(
            request: DatabaseWireRequestEnvelope,
            context: DatabaseOperationContext
        ) async throws {
            _ = request
            _ = context
            throw AuthorizationTestError.denied
        }
    }

    private struct AuthorizationErrorMapper: DatabaseErrorMapper {
        func remoteError(
            for error: any Error,
            context: DatabaseOperationContext,
            limits: DatabaseWireLimits
        ) -> DatabaseRemoteError {
            _ = error
            _ = context
            _ = limits
            return DatabaseRemoteError(
                category: .authorization,
                code: "OPERATION_DENIED",
                message: "Operation denied",
                retryability: .never
            )
        }
    }

    private enum AuthorizationTestError: Error {
        case denied
    }
}
