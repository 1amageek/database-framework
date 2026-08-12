import DatabaseMaintenanceOperations
import DatabaseSchemaOperations
import DatabaseJobRuntime
import DatabaseGraphOperations
import DatabaseMutationOperations
import DatabaseQueryOperations
import DatabaseCommandOperations
import DatabaseOperationCore
@_spi(DatabaseOperations) import DatabaseWire

public protocol DatabaseOperationHandler: Sendable {
    associatedtype Operation: DatabaseOperationDeclaration

    func handle(
        _ request: Operation.Request,
        context: DatabaseOperationContext
    ) async throws -> Operation.Response

    func requirement(
        for request: Operation.Request
    ) throws -> DatabaseOperationRequirement
}

extension DatabaseOperationHandler {
    public func requirement(
        for request: Operation.Request
    ) throws -> DatabaseOperationRequirement {
        _ = request
        return .canonical(for: Operation.operation.identifier)
    }
}
