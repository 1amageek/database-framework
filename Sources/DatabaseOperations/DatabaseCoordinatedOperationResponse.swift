import DatabaseMaintenanceOperations
import DatabaseSchemaOperations
import DatabaseJobRuntime
import DatabaseGraphOperations
import DatabaseMutationOperations
import DatabaseQueryOperations
import DatabaseCommandOperations
import DatabaseOperationCore
@_spi(DatabaseOperations) import DatabaseWire

public struct DatabaseCoordinatedOperationResponse: Sendable {
    public let result: DatabaseOperationResult
    let successPayload: DatabaseSuccessPayload

    init(
        result: DatabaseOperationResult,
        successPayload: DatabaseSuccessPayload
    ) {
        self.result = result
        self.successPayload = successPayload
    }

    func decodeResponse<Operation: DatabaseOperationDeclaration>(
        _ operation: Operation.Type,
        limits: DatabaseWireLimits
    ) throws -> Operation.Response {
        try DatabaseWireDecoder(limits: limits).decodeResponsePayload(
            Operation.operation,
            from: successPayload.bytes
        )
    }
}
