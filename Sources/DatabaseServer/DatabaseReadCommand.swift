import DatabaseTypes

public protocol DatabaseReadCommand: DatabaseCommand {
    func execute(
        input: FieldObject,
        context: DatabaseReadCommandContext
    ) async throws -> DatabaseCommandResult
}
