import DatabaseMaintenanceOperations
import DatabaseSchemaOperations
import DatabaseJobRuntime
import DatabaseGraphOperations
import DatabaseMutationOperations
import DatabaseQueryOperations
import DatabaseCommandOperations
import DatabaseOperationCore
@_spi(DatabaseOperations) import DatabaseWire

public struct JobStartExecutionResult: Sendable {
    public let response: JobStartOperation.Response
    let operationResult: DatabaseOperationResult

    init(
        coordinated: DatabaseCoordinatedOperationResponse,
        limits: DatabaseWireLimits
    ) throws {
        self.response = try coordinated.decodeResponse(
            JobStartOperation.self,
            limits: limits
        )
        self.operationResult = coordinated.result
    }
}
