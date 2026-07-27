import DatabaseTypes

public protocol DatabaseWriteCommand: DatabaseCommand {
    func execute(
        input: FieldObject,
        context: DatabaseWriteCommandContext
    ) async throws -> DatabaseCommandResult
}
