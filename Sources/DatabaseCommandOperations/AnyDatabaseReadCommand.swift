import DatabaseOperationCore
import DatabaseKit
import DatabaseTypes

public struct AnyDatabaseReadCommand: Sendable {
    public let identifier: CommandIdentifier

    private let executeCommand: @Sendable (
        FieldObject,
        DatabaseReadCommandContext,
    ) async throws -> DatabaseCommandResult

    public init<Command: DatabaseReadCommand>(_ command: Command) {
        self.identifier = command.identifier
        self.executeCommand = { input, context in
            try await command.execute(input: input, context: context)
        }
    }

    package func execute(
        input: FieldObject,
        context: DatabaseReadCommandContext
    ) async throws -> DatabaseCommandResult {
        try await executeCommand(input, context)
    }
}
