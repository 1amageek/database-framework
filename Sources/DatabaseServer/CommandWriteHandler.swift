import DatabaseEngine
import DatabaseValue
import DatabaseWire

public struct CommandWriteHandler: DatabaseOperationEndpointHandler {
    public let identifier = DatabaseOperationIdentifier.commandWrite

    private let registry: DatabaseWriteCommandRegistry
    private let coordinator: DatabaseTransactionalOperationCoordinator
    private let runtimeLimits: DatabaseRuntimeLimits
    private let wireLimits: DatabaseWireLimits

    public init(
        registry: DatabaseWriteCommandRegistry,
        coordinator: DatabaseTransactionalOperationCoordinator,
        runtimeLimits: DatabaseRuntimeLimits = .default,
        wireLimits: DatabaseWireLimits = .default
    ) {
        self.registry = registry
        self.coordinator = coordinator
        self.runtimeLimits = runtimeLimits
        self.wireLimits = wireLimits
    }

    public func invoke(
        payload: DatabaseBytes,
        context: DatabaseOperationContext,
        limits: DatabaseWireLimits
    ) async throws -> DatabaseOperationResult {
        let request = try DatabaseEnvelopeCodec.decode(
            DatabaseCommandRequest.self,
            from: payload,
            limits: limits
        )
        try runtimeLimits.validate(request.budget)
        let command = try registry.resolve(request.command)
        let requestPayload = context.requestPayload

        return try await coordinator.execute(
            operation: .commandWrite,
            requestPayload: requestPayload,
            context: context,
            timeoutMilliseconds: request.budget.timeoutMilliseconds
        ) { transactionContext in
            try await command.execute(
                input: request.input,
                context: DatabaseWriteCommandContext(
                    operation: context,
                    transaction: transactionContext,
                    budget: request.budget
                ),
                limits: wireLimits
            )
        } makeResponse: { result, commitVersion in
            DatabaseOperationResponseEncoder {
                (writer: inout DatabaseWireWriter) throws(DatabaseWireError) in
                try result.encodeLengthPrefixedOutput(into: &writer)
                writer.writeUInt64(commitVersion)
                try writer.writeOptionalBytes(result.continuation)
            }
        }.result
    }
}
