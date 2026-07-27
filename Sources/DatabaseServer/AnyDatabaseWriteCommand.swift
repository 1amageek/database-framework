import DatabaseKit
import DatabaseTypes

public struct AnyDatabaseWriteCommand: Sendable {
    public let identifier: CommandIdentifier

    private let executeCommand: @Sendable (
        FieldObject,
        DatabaseWriteCommandContext,
    ) async throws -> DatabaseCommandResult

    public init<Command: DatabaseWriteCommand>(_ command: Command) {
        self.identifier = command.identifier
        self.executeCommand = { input, context in
            try await command.execute(input: input, context: context)
        }
    }

    func execute(
        input: FieldObject,
        context: DatabaseWriteCommandContext
    ) async throws -> DatabaseCommandResult {
        try await executeCommand(input, context)
    }
}
