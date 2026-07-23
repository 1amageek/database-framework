public struct DatabaseReadCommandRegistry: Sendable {
    private let commands: [AnyDatabaseReadCommand]

    public init(commands: [AnyDatabaseReadCommand]) throws {
        try Self.validate(commands.map(\.identifier))
        self.commands = commands.sorted { $0.identifier < $1.identifier }
    }

    public var identifiers: [String] {
        commands.map(\.identifier)
    }

    func resolve(_ identifier: String) throws -> AnyDatabaseReadCommand {
        guard let command = commands.first(where: {
            $0.identifier == identifier
        }) else {
            throw DatabaseCommandRegistryError.commandNotFound(identifier)
        }
        return command
    }

    private static func validate(_ identifiers: [String]) throws {
        guard identifiers.allSatisfy({ !$0.isEmpty }) else {
            throw DatabaseCommandRegistryError.emptyIdentifier
        }
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
