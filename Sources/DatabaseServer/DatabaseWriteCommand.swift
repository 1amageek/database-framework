import DatabaseWire

public protocol DatabaseWriteCommand:
    DatabaseCommand where Descriptor: DatabaseWriteCommandDescriptor {
    func execute(
        input: Descriptor.Input,
        context: DatabaseWriteCommandContext
    ) async throws -> DatabaseCommandResult<Descriptor.Output>
}
