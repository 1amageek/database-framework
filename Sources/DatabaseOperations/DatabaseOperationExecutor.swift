import DatabaseMaintenanceOperations
import DatabaseSchemaOperations
import DatabaseJobRuntime
import DatabaseGraphOperations
import DatabaseMutationOperations
import DatabaseQueryOperations
import DatabaseCommandOperations
import DatabaseOperationCore
import DatabaseEngine
import DatabaseKit
@_spi(DatabaseOperations) import DatabaseWire
import StorageKit

package enum DatabaseOperationExecutor: Sendable {
    case control(DatabaseControlExecutor)
    #if DATABASE_OPERATIONS_MULTIPLE_BASES
    case base(BaseOperationExecutor)
    case composition(CompositionReadExecutor)
    #endif

    package var monotonicClock: any StorageMonotonicClock {
        switch self {
        case .control(let executor): executor.monotonicClock
        #if DATABASE_OPERATIONS_MULTIPLE_BASES
        case .base(let executor): executor.monotonicClock
        case .composition(let executor): executor.monotonicClock
        #endif
        }
    }

    package var schema: Schema {
        switch self {
        case .control(let executor): executor.schema
        #if DATABASE_OPERATIONS_MULTIPLE_BASES
        case .base(let executor): executor.schema
        case .composition(let executor): executor.schema
        #endif
        }
    }

    package var schemaGeneration: UInt64 {
        switch self {
        case .control(let executor): executor.schemaGeneration
        #if DATABASE_OPERATIONS_MULTIPLE_BASES
        case .base(let executor): executor.schemaGeneration
        case .composition(let executor): executor.schemaGeneration
        #endif
        }
    }

    package var runtimeConfiguration: DatabaseRuntimeConfiguration {
        switch self {
        case .control(let executor): executor.runtimeConfiguration
        #if DATABASE_OPERATIONS_MULTIPLE_BASES
        case .base(let executor): executor.runtimeConfiguration
        case .composition(let executor): executor.runtimeConfiguration
        #endif
        }
    }

    package var containerIdentity: ObjectIdentifier {
        switch self {
        case .control(let executor):
            executor.containerIdentity
        #if DATABASE_OPERATIONS_MULTIPLE_BASES
        case .base(let executor):
            executor.containerIdentity
        case .composition(let executor):
            executor.containerIdentity
        #endif
        }
    }
}
