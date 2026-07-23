import DatabaseServer
import DatabaseWire
import Testing

@Suite("Database command registry composition")
struct DatabaseCommandRegistryCompositionTests {
    @Test("Read and write registries preserve both command sets")
    func mergesDistinctCommands() throws {
        let read = try DatabaseReadCommandRegistry(
            commands: [AnyDatabaseReadCommand(FirstReadCommand())]
        ).merging(
            DatabaseReadCommandRegistry(
                commands: [AnyDatabaseReadCommand(SecondReadCommand())]
            )
        )
        let write = try DatabaseWriteCommandRegistry(
            commands: [AnyDatabaseWriteCommand(FirstWriteCommand())]
        ).merging(
            DatabaseWriteCommandRegistry(
                commands: [AnyDatabaseWriteCommand(SecondWriteCommand())]
            )
        )

        #expect(read.identifiers == ["test.read.first", "test.read.second"])
        #expect(write.identifiers == ["test.write.first", "test.write.second"])
    }

    @Test("Composition rejects duplicate command identifiers")
    func rejectsDuplicates() throws {
        let read = try DatabaseReadCommandRegistry(
            commands: [AnyDatabaseReadCommand(FirstReadCommand())]
        )
        do {
            _ = try read.merging(
                DatabaseReadCommandRegistry(
                    commands: [AnyDatabaseReadCommand(FirstReadCommand())]
                )
            )
            Issue.record("Expected duplicate read command rejection")
        } catch DatabaseCommandRegistryError.duplicate(let identifier) {
            #expect(identifier == "test.read.first")
        }
    }
}

private enum FirstReadDescriptor: DatabaseReadCommandDescriptor {
    typealias Input = DatabaseEmpty
    typealias Output = DatabaseEmpty
    static let identifier = "test.read.first"
}

private enum SecondReadDescriptor: DatabaseReadCommandDescriptor {
    typealias Input = DatabaseEmpty
    typealias Output = DatabaseEmpty
    static let identifier = "test.read.second"
}

private enum FirstWriteDescriptor: DatabaseWriteCommandDescriptor {
    typealias Input = DatabaseEmpty
    typealias Output = DatabaseEmpty
    static let identifier = "test.write.first"
}

private enum SecondWriteDescriptor: DatabaseWriteCommandDescriptor {
    typealias Input = DatabaseEmpty
    typealias Output = DatabaseEmpty
    static let identifier = "test.write.second"
}

private struct FirstReadCommand: DatabaseReadCommand {
    typealias Descriptor = FirstReadDescriptor

    func execute(
        input: DatabaseEmpty,
        context: DatabaseReadCommandContext
    ) async throws -> DatabaseCommandResult<DatabaseEmpty> {
        _ = input
        _ = context
        throw RegistryCommandTestError.invoked
    }
}

private struct SecondReadCommand: DatabaseReadCommand {
    typealias Descriptor = SecondReadDescriptor

    func execute(
        input: DatabaseEmpty,
        context: DatabaseReadCommandContext
    ) async throws -> DatabaseCommandResult<DatabaseEmpty> {
        _ = input
        _ = context
        throw RegistryCommandTestError.invoked
    }
}

private struct FirstWriteCommand: DatabaseWriteCommand {
    typealias Descriptor = FirstWriteDescriptor

    func execute(
        input: DatabaseEmpty,
        context: DatabaseWriteCommandContext
    ) async throws -> DatabaseCommandResult<DatabaseEmpty> {
        _ = input
        _ = context
        throw RegistryCommandTestError.invoked
    }
}

private struct SecondWriteCommand: DatabaseWriteCommand {
    typealias Descriptor = SecondWriteDescriptor

    func execute(
        input: DatabaseEmpty,
        context: DatabaseWriteCommandContext
    ) async throws -> DatabaseCommandResult<DatabaseEmpty> {
        _ = input
        _ = context
        throw RegistryCommandTestError.invoked
    }
}

private enum RegistryCommandTestError: Error {
    case invoked
}
