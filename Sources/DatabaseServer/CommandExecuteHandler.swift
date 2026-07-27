import DatabaseEngine
import DatabaseTypes
@_spi(DatabaseServer) import DatabaseWire

public struct CommandExecuteHandler: DatabaseOperationEndpointHandler {
    public typealias Operation = CommandExecuteOperation

    private let readRegistry: DatabaseReadCommandRegistry
    private let writeRegistry: DatabaseWriteCommandRegistry
    private let coordinator: DatabaseTransactionalOperationCoordinator
    private let runtimeLimits: DatabaseRuntimeLimits

    public init(
        readRegistry: DatabaseReadCommandRegistry,
        writeRegistry: DatabaseWriteCommandRegistry,
        coordinator: DatabaseTransactionalOperationCoordinator,
        runtimeLimits: DatabaseRuntimeLimits = .default
    ) {
        self.readRegistry = readRegistry
        self.writeRegistry = writeRegistry
        self.coordinator = coordinator
        self.runtimeLimits = runtimeLimits
    }

    public func invoke(
        request: CommandRequest,
        context: DatabaseOperationContext,
        limits: DatabaseWireLimits
    ) async throws -> DatabaseOperationResult {
        try runtimeLimits.validate(request.budget)
        switch request.command.access {
        case .readOnly:
            let command = try readRegistry.resolve(
                request.command.identifier
            )
            let result = try await DatabaseExecutionTimeout.run(
                milliseconds: request.budget.timeoutMilliseconds,
                clock: context.container.engine.monotonicClock
            ) {
                let databaseContext = context.container.newContext()
                return try await databaseContext.withTransaction(
                    configuration: .readOnly
                ) { transactionContext in
                    try await command.execute(
                        input: request.input,
                        context: DatabaseReadCommandContext(
                            operation: context,
                            transaction: transactionContext,
                            budget: request.budget
                        )
                    )
                }
            }
            return DatabaseOperationResult(
                CommandExecuteOperation.self,
                response: .read(
                    output: result.output,
                    continuation: result.continuation
                )
            )

        case .readWrite:
            let command = try writeRegistry.resolve(
                request.command.identifier
            )
            return try await coordinator.execute(
                CommandExecuteOperation.self,
                requestPayload: context.requestPayload,
                context: context,
                timeoutMilliseconds: request.budget.timeoutMilliseconds
            ) { transactionContext in
                try await command.execute(
                    input: request.input,
                    context: DatabaseWriteCommandContext(
                        operation: context,
                        transaction: transactionContext,
                        budget: request.budget
                    )
                )
            } makeResponse: { result, commitVersion in
                .write(
                    output: result.output,
                    commitVersion: commitVersion,
                    continuation: result.continuation
                )
            }.operationResult
        }
    }
}
