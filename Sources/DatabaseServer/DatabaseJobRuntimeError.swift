import DatabaseValue
import DatabaseWire

public enum DatabaseJobRuntimeError: Error, CustomStringConvertible, Equatable {
    case invalidConfiguration(String)
    case invalidRetryPolicy
    case requestPayloadTooLarge(actual: Int, maximum: Int)
    case specificationTooLarge(actual: Int, maximum: Int)
    case planTooLarge(actual: Int, maximum: Int)
    case stateTooLarge(actual: Int, maximum: Int)
    case jobNotFound(DatabaseUUID)
    case jobOperationMismatch(
        expected: DatabaseJobOperationIdentifier,
        actual: DatabaseJobOperationIdentifier
    )
    case resultNotReady(DatabaseUUID)
    case corruptedSpecification
    case corruptedPlan
    case corruptedState
    case corruptedResult
    case resultChunkMissing(jobID: DatabaseUUID, index: UInt32)
    case invalidResultContinuation
    case invalidStateTransition
    case stateRevisionOverflow
    case workUnitOverflow
    case sliceExceededBudget(actual: UInt64, maximum: UInt64)
    case responseTooLarge(actual: Int, maximum: Int)
    case duplicateJobIdentifier(DatabaseUUID)
    case commitModelMismatch

    public var description: String {
        switch self {
        case .invalidConfiguration(let detail):
            return "Invalid job runtime configuration: \(detail)"
        case .invalidRetryPolicy:
            return "Invalid job retry policy"
        case .requestPayloadTooLarge(let actual, let maximum):
            return "Job request payload is too large: \(actual) > \(maximum)"
        case .specificationTooLarge(let actual, let maximum):
            return "Job specification is too large: \(actual) > \(maximum)"
        case .planTooLarge(let actual, let maximum):
            return "Job plan is too large: \(actual) > \(maximum)"
        case .stateTooLarge(let actual, let maximum):
            return "Job state is too large: \(actual) > \(maximum)"
        case .jobNotFound(let jobID):
            return "Job not found: \(jobID)"
        case .jobOperationMismatch(let expected, let actual):
            return "Job operation mismatch: expected \(expected.family.rawValue):\(expected.kind), actual \(actual.family.rawValue):\(actual.kind)"
        case .resultNotReady(let jobID):
            return "Job result is not ready: \(jobID)"
        case .corruptedSpecification:
            return "Persistent job specification is corrupted"
        case .corruptedPlan:
            return "Persistent job plan is corrupted"
        case .corruptedState:
            return "Persistent job state is corrupted"
        case .corruptedResult:
            return "Persistent job result manifest is corrupted"
        case .resultChunkMissing(let jobID, let index):
            return "Persistent job result chunk is missing: \(jobID) chunk \(index)"
        case .invalidResultContinuation:
            return "Persistent job result continuation is invalid"
        case .invalidStateTransition:
            return "Invalid persistent job state transition"
        case .stateRevisionOverflow:
            return "Persistent job state revision overflowed"
        case .workUnitOverflow:
            return "Persistent job work unit counter overflowed"
        case .sliceExceededBudget(let actual, let maximum):
            return "Job slice exceeded its work budget: \(actual) > \(maximum)"
        case .responseTooLarge(let actual, let maximum):
            return "Job response is too large: \(actual) > \(maximum)"
        case .duplicateJobIdentifier(let jobID):
            return "Generated duplicate job identifier: \(jobID)"
        case .commitModelMismatch:
            return "The resumable operation was invoked with the wrong commit model"
        }
    }
}
