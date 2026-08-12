import DatabaseMaintenanceOperations
import DatabaseSchemaOperations
import DatabaseJobRuntime
import DatabaseGraphOperations
import DatabaseMutationOperations
import DatabaseQueryOperations
import DatabaseCommandOperations
import DatabaseOperationCore
@_spi(DatabaseOperations) import DatabaseWire

public struct DatabasePreparedResumableJob<Plan, State>: Sendable
where Plan: PersistentJobPayload, State: PersistentJobPayload {
    public let plan: Plan
    public let initialState: State
    public let sliceTimeoutMilliseconds: UInt32

    public init(
        plan: Plan,
        initialState: State,
        sliceTimeoutMilliseconds: UInt32
    ) {
        self.plan = plan
        self.initialState = initialState
        self.sliceTimeoutMilliseconds = sliceTimeoutMilliseconds
    }
}
