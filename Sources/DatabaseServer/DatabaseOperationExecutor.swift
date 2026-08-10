import DatabaseEngine
import DatabaseKit
@_spi(DatabaseServer) import DatabaseWire
import StorageKit

package enum DatabaseOperationExecutor: Sendable {
    case control(DatabaseControlExecutor)
    case base(BaseOperationExecutor)
    case composition(CompositionReadExecutor)

    package var monotonicClock: any StorageMonotonicClock {
        switch self {
        case .control(let executor): executor.monotonicClock
        case .base(let executor): executor.monotonicClock
        case .composition(let executor): executor.monotonicClock
        }
    }

    package var schema: Schema {
        switch self {
        case .control(let executor): executor.schema
        case .base(let executor): executor.schema
        case .composition(let executor): executor.schema
        }
    }

    package var schemaGeneration: UInt64 {
        switch self {
        case .control(let executor): executor.schemaGeneration
        case .base(let executor): executor.schemaGeneration
        case .composition(let executor): executor.schemaGeneration
        }
    }

    package var runtimeConfiguration: DatabaseRuntimeConfiguration {
        switch self {
        case .control(let executor): executor.runtimeConfiguration
        case .base(let executor): executor.runtimeConfiguration
        case .composition(let executor): executor.runtimeConfiguration
        }
    }

    package func validateMutationStateStore(
        _ stateStore: DatabaseMutationStateStore,
        target: DatabaseOperationTarget
    ) throws {
        switch self {
        case .control(let executor):
            try executor.validateMutationStateStore(stateStore)
        case .base(let executor):
            try executor.validateMutationStateStore(stateStore)
        case .composition:
            throw DatabaseEndpointError.targetKindNotAccepted(target)
        }
    }
}
