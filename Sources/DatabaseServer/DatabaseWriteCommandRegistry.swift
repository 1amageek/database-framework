import DatabaseKit

public struct DatabaseWriteCommandRegistry: Sendable {
    private let commands: [AnyDatabaseWriteCommand]

    public init(commands: [AnyDatabaseWriteCommand]) throws {
        try Self.validate(commands.map(\.identifier))
        self.commands = commands.sorted { $0.identifier < $1.identifier }
    }

    public var identifiers: [CommandIdentifier] {
        commands.map(\.identifier)
    }

    public func merging(
        _ additionalRegistry: DatabaseWriteCommandRegistry
    ) throws -> DatabaseWriteCommandRegistry {
        try DatabaseWriteCommandRegistry(
            commands: commands + additionalRegistry.commands
        )
    }

    func resolve(
        _ identifier: CommandIdentifier
    ) throws -> AnyDatabaseWriteCommand {
        guard let command = commands.first(where: {
            $0.identifier == identifier
        }) else {
            throw DatabaseCommandRegistryError.commandNotFound(identifier)
        }
        return command
    }

    private static func validate(_ identifiers: [CommandIdentifier]) throws {
        let sorted = identifiers.sorted()
        guard sorted.count > 1 else {
            return
        }
        for index in 1..<sorted.count {
            guard sorted[index - 1] != sorted[index] else {
                throw DatabaseCommandRegistryError.duplicate(sorted[index])
            }
        }
    }
}
