import DatabaseWire

public protocol DatabaseReadCommand:
    DatabaseCommand where Descriptor: DatabaseReadCommandDescriptor {
    func execute(
        input: Descriptor.Input,
        context: DatabaseReadCommandContext
    ) async throws -> DatabaseCommandResult<Descriptor.Output>
}
