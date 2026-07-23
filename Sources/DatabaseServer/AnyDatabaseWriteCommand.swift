import DatabaseValue
import DatabaseWire

public struct AnyDatabaseWriteCommand: Sendable {
    public let identifier: String

    private let executeCommand: @Sendable (
        DatabaseBytes,
        DatabaseWriteCommandContext,
        DatabaseWireLimits
    ) async throws -> AnyDatabaseCommandResult

    public init<Command: DatabaseWriteCommand>(_ command: Command) {
        self.identifier = command.identifier
        self.executeCommand = { input, context, limits in
            let decoded = try DatabaseEnvelopeCodec.decode(
                Command.Descriptor.Input.self,
                from: input,
                limits: limits
            )
            return AnyDatabaseCommandResult(
                try await command.execute(input: decoded, context: context)
            )
        }
    }

    func execute(
        input: DatabaseBytes,
        context: DatabaseWriteCommandContext,
        limits: DatabaseWireLimits
    ) async throws -> AnyDatabaseCommandResult {
        try await executeCommand(input, context, limits)
    }
}
