import DatabaseMaintenanceOperations
import DatabaseSchemaOperations
import DatabaseJobRuntime
import DatabaseGraphOperations
import DatabaseMutationOperations
import DatabaseQueryOperations
import DatabaseCommandOperations
import DatabaseOperationCore
import DatabaseTypes
@_spi(DatabaseOperations) import DatabaseWire

public struct DatabaseSuccessPayload: Sendable, Hashable {
    public let operation: DatabaseOperationIdentifier
    public let bytes: ByteString

    init(
        operation: DatabaseOperationIdentifier,
        bytes: ByteString,
        limits: DatabaseWireLimits
    ) throws(DatabaseWireError) {
        try DatabaseWireEncoder(limits: limits).validateSuccessPayload(bytes)
        self.operation = operation
        self.bytes = bytes
    }
}
