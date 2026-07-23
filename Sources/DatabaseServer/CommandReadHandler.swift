import DatabaseEngine
import DatabaseValue
import DatabaseWire

public struct CommandReadHandler: DatabaseOperationEndpointHandler {
    public let identifier = DatabaseOperationIdentifier.commandRead

    private let registry: DatabaseReadCommandRegistry
    private let runtimeLimits: DatabaseRuntimeLimits
    private let wireLimits: DatabaseWireLimits

    public init(
        registry: DatabaseReadCommandRegistry,
        runtimeLimits: DatabaseRuntimeLimits = .default,
        wireLimits: DatabaseWireLimits = .default
    ) {
        self.registry = registry
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

        let encoder = try await DatabaseExecutionTimeout.run(
            milliseconds: request.budget.timeoutMilliseconds,
            clock: context.container.engine.monotonicClock
        ) {
            let databaseContext = context.container.newContext()
            return try await databaseContext.withTransaction(
                configuration: .readOnly
            ) { transactionContext in
                let result = try await command.execute(
                    input: request.input,
                    context: DatabaseReadCommandContext(
                        operation: context,
                        transaction: transactionContext.rawTransaction,
                        budget: request.budget
                    ),
                    limits: wireLimits
                )
                return DatabaseOperationResponseEncoder {
                    (writer: inout DatabaseWireWriter) throws(DatabaseWireError) in
                    try result.encodeLengthPrefixedOutput(into: &writer)
                    try writer.writeOptionalBytes(result.continuation)
                }
            }
        }
        return DatabaseOperationResult(
            operation: .commandRead,
            encoder: encoder
        )
    }
}
