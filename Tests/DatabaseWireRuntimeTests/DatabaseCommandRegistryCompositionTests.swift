import DatabaseWireRuntime
import DatabaseKit
import DatabaseTypes
import DatabaseWire
import Testing

@Suite("Database command registry composition")
struct DatabaseCommandRegistryCompositionTests {
    @Test("Read and write registries preserve both command sets")
    func mergesDistinctCommands() throws {
        let read = try DatabaseReadCommandRegistry(
            commands: [AnyDatabaseReadCommand(try FirstReadCommand())]
        ).merging(
            DatabaseReadCommandRegistry(
                commands: [AnyDatabaseReadCommand(try SecondReadCommand())]
            )
        )
        let write = try DatabaseWriteCommandRegistry(
            commands: [AnyDatabaseWriteCommand(try FirstWriteCommand())]
        ).merging(
            DatabaseWriteCommandRegistry(
                commands: [AnyDatabaseWriteCommand(try SecondWriteCommand())]
            )
        )

        #expect(
            read.identifiers.map(\.rawValue)
                == ["test.read.first", "test.read.second"]
        )
        #expect(
            write.identifiers.map(\.rawValue)
                == ["test.write.first", "test.write.second"]
        )
    }

    @Test("Composition rejects duplicate command identifiers")
    func rejectsDuplicates() throws {
        let read = try DatabaseReadCommandRegistry(
            commands: [AnyDatabaseReadCommand(try FirstReadCommand())]
        )
        do {
            _ = try read.merging(
                DatabaseReadCommandRegistry(
                    commands: [
                        AnyDatabaseReadCommand(try FirstReadCommand())
                    ]
                )
            )
            Issue.record("Expected duplicate read command rejection")
        } catch DatabaseCommandRegistryError.duplicate(let identifier) {
            #expect(identifier.rawValue == "test.read.first")
        }
    }
}

private struct FirstReadCommand: DatabaseReadCommand {
    let declaration: CommandDeclaration

    init() throws {
        declaration = CommandDeclaration(
            identifier: try CommandIdentifier("test.read.first"),
            access: .readOnly
        )
    }

    func execute(
        input: FieldObject,
        context: DatabaseReadCommandContext
    ) async throws -> DatabaseCommandResult {
        _ = input
        _ = context
        throw RegistryCommandTestError.invoked
    }
}

private struct SecondReadCommand: DatabaseReadCommand {
    let declaration: CommandDeclaration

    init() throws {
        declaration = CommandDeclaration(
            identifier: try CommandIdentifier("test.read.second"),
            access: .readOnly
        )
    }

    func execute(
        input: FieldObject,
        context: DatabaseReadCommandContext
    ) async throws -> DatabaseCommandResult {
        _ = input
        _ = context
        throw RegistryCommandTestError.invoked
    }
}

private struct FirstWriteCommand: DatabaseWriteCommand {
    let declaration: CommandDeclaration

    init() throws {
        declaration = CommandDeclaration(
            identifier: try CommandIdentifier("test.write.first"),
            access: .readWrite
        )
    }

    func execute(
        input: FieldObject,
        context: DatabaseWriteCommandContext
    ) async throws -> DatabaseCommandResult {
        _ = input
        _ = context
        throw RegistryCommandTestError.invoked
    }
}

private struct SecondWriteCommand: DatabaseWriteCommand {
    let declaration: CommandDeclaration

    init() throws {
        declaration = CommandDeclaration(
            identifier: try CommandIdentifier("test.write.second"),
            access: .readWrite
        )
    }

    func execute(
        input: FieldObject,
        context: DatabaseWriteCommandContext
    ) async throws -> DatabaseCommandResult {
        _ = input
        _ = context
        throw RegistryCommandTestError.invoked
    }
}

private enum RegistryCommandTestError: Error {
    case invoked
}
