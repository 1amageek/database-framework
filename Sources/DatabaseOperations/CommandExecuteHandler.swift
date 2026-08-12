import DatabaseMaintenanceOperations
import DatabaseSchemaOperations
import DatabaseJobRuntime
import DatabaseGraphOperations
import DatabaseMutationOperations
import DatabaseQueryOperations
import DatabaseCommandOperations
import DatabaseOperationCore
import DatabaseEngine
import DatabaseTypes
@_spi(DatabaseOperations) import DatabaseWire

public struct CommandExecuteHandler: DatabaseOperationEndpointHandler {
    public typealias Operation = CommandExecuteOperation

    private let readRegistry: DatabaseReadCommandRegistry
    private let writeRegistry: DatabaseWriteCommandRegistry
    private let coordinator: DatabaseTransactionalOperationCoordinator
    private let runtimeLimits: DatabaseOperationLimits

    public init(
        readRegistry: DatabaseReadCommandRegistry,
        writeRegistry: DatabaseWriteCommandRegistry,
        coordinator: DatabaseTransactionalOperationCoordinator,
        runtimeLimits: DatabaseOperationLimits = .default
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
                clock: context.executor.monotonicClock
            ) {
                let databaseContext = try context.requireDataContext()
                return try await databaseContext.withTransaction(
                    requiredAccess: .read,
                    configuration: .readOnly
                ) { transactionContext in
                    try await command.execute(
                        input: request.input,
                        context: DatabaseReadCommandContext(
                            requestID: context.requestID,
                            metadata: context.metadata,
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
                operation: CommandExecuteOperation.identifier,
                requestPayload: context.requestPayload,
                context: context,
                timeoutMilliseconds: request.budget.timeoutMilliseconds
            ) { transactionContext in
                try await command.execute(
                    input: request.input,
                    context: DatabaseWriteCommandContext(
                        requestID: context.requestID,
                        metadata: context.metadata,
                        transaction: transactionContext,
                        budget: request.budget
                    )
                )
            } makeResponse: { result, commitVersion in
                DatabaseOperationResponseEncoder(
                    CommandExecuteOperation.self,
                    response: .write(
                        output: result.output,
                        commitVersion: commitVersion,
                        continuation: result.continuation
                    )
                )
            }.result
        }
    }
}
