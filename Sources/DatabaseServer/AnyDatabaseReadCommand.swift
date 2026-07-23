import DatabaseValue
import DatabaseWire

public struct AnyDatabaseReadCommand: Sendable {
    public let identifier: String

    private let executeCommand: @Sendable (
        DatabaseBytes,
        DatabaseReadCommandContext,
        DatabaseWireLimits
    ) async throws -> AnyDatabaseCommandResult

    public init<Command: DatabaseReadCommand>(_ command: Command) {
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
        context: DatabaseReadCommandContext,
        limits: DatabaseWireLimits
    ) async throws -> AnyDatabaseCommandResult {
        try await executeCommand(input, context, limits)
    }
}
