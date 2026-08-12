import DatabaseMaintenanceOperations
import DatabaseSchemaOperations
import DatabaseJobRuntime
import DatabaseGraphOperations
import DatabaseMutationOperations
import DatabaseQueryOperations
import DatabaseCommandOperations
import DatabaseOperationCore
@_spi(DatabaseOperations) import DatabaseWire

public struct AnyDatabaseOperationHandler: Sendable {
    public let identifier: DatabaseOperationIdentifier

    private let prepareOperation: @Sendable (
        DatabaseWireRequestEnvelope,
        DatabaseWireLimits
    ) throws -> PreparedDatabaseOperation

    public init<Handler: DatabaseOperationHandler>(_ handler: Handler) {
        self.identifier = Handler.Operation.operation.identifier
        self.prepareOperation = { envelope, limits in
            let request = try DatabaseWireDecoder(limits: limits).decodeRequest(
                Handler.Operation.operation,
                from: envelope
            )
            let requirement = try handler.requirement(for: request)
            return PreparedDatabaseOperation(
                requirement: requirement,
                invoke: { context in
                    let response = try await handler.handle(
                        request,
                        context: context
                    )
                    return DatabaseOperationResult(
                        Handler.Operation.self,
                        response: response
                    )
                }
            )
        }
    }

    public init<Handler: DatabaseOperationEndpointHandler>(_ handler: Handler) {
        self.identifier = Handler.Operation.operation.identifier
        self.prepareOperation = { envelope, requestLimits in
            let request = try DatabaseWireDecoder(limits: requestLimits).decodeRequest(
                Handler.Operation.operation,
                from: envelope
            )
            let requirement = try handler.requirement(for: request)
            return PreparedDatabaseOperation(
                requirement: requirement,
                invoke: { context in
                    try await handler.invoke(
                        request: request,
                        context: context,
                        limits: context.wireLimits
                    )
                }
            )
        }
    }

    func prepare(
        envelope: DatabaseWireRequestEnvelope,
        limits: DatabaseWireLimits
    ) throws -> PreparedDatabaseOperation {
        try prepareOperation(envelope, limits)
    }
}
