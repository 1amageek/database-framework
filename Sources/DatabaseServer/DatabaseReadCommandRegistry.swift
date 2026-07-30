import DatabaseKit

public struct DatabaseReadCommandRegistry: Sendable {
    private let commands: [AnyDatabaseReadCommand]

    public init(commands: [AnyDatabaseReadCommand]) throws {
        try Self.validate(commands.map { $0.identifier })
        self.commands = commands.sorted { $0.identifier < $1.identifier }
    }

    public var identifiers: [CommandIdentifier] {
        commands.map { $0.identifier }
    }

    public func merging(
        _ additionalRegistry: DatabaseReadCommandRegistry
    ) throws -> DatabaseReadCommandRegistry {
        try DatabaseReadCommandRegistry(
            commands: commands + additionalRegistry.commands
        )
    }

    func resolve(
        _ identifier: CommandIdentifier
    ) throws -> AnyDatabaseReadCommand {
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
