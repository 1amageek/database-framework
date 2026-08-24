import DatabaseKit
import DatabaseTypes
import StorageKit

/// Typed failures produced by canonical Fusion execution.
public enum FusionExecutionError: Error, Sendable, Equatable {
    case invalidPlan(FusionPlanValidationError)
    case unsupportedSource
    case indexNotFound(entity: String, name: String)
    case indexTypeMismatch(
        entity: String,
        name: String,
        expected: IndexType,
        actual: IndexType
    )
    case indexMatchNotFound(entity: String, type: IndexType)
    case ambiguousIndexMatch(entity: String, type: IndexType, names: [String])
    case indexExecutorNotRegistered(IndexType)
    case indexNotReadable(entity: String, name: String)
    case inputLimitOutOfRange(UInt64)
    case requiredCandidates(stage: Int, input: Int)
    case invalidInputScoring(FusionScoring?)
    case reservedAnnotationCollision(String)
    case invalidIndexInput(indexType: IndexType, parameter: String)
    case corruptedIndex(IndexType)
    case invalidIndexScore(IndexType)
    case executionContractViolation
}

/// Internal invariant and physical-runtime failures. These details are not a
/// stable public API and never expose packed storage keys to library clients.
enum FusionExecutionContractError: Error, Sendable, Equatable {
    case missingIdentity(field: String)
    case duplicateIdentity(FieldValue)
    case duplicatePrimaryKey(ByteString)
    case candidateDomainViolation(ByteString)
    case duplicateMatch(ByteString)
    case matchLimitExceeded(maximum: Int)
    case matchSinkInvalidated
    case invalidScoreSignal
    case missingScoreSignal(stage: Int, input: Int)
    case missingCandidateRow(ByteString)
    case entityReadCountMismatch(expected: Int, actual: Int)
    case candidateCountOverflow
    case invalidInputCoverage
    case inconsistentPayload(ByteString)
    case scoreOverflow(ByteString)
    case missingFusedScore(ByteString)
    case indexReadSessionInvalidated(index: String)
    case concurrentIndexReadSessionOperation(index: String)
    case indexReadOutsideAdmittedSubspace(index: String)
    case invalidIndexReadRange(index: String)
    case indexCursorEscapedAdmittedSubspace(index: String)
}

func sanitizedFusionExecutionError(
    _ error: any Error
) -> any Error {
    if error is FusionExecutionContractError {
        return FusionExecutionError.executionContractViolation
    }
    if let cleanup = error as? StorageRangeTerminalCleanupError {
        return StorageRangeTerminalCleanupError(
            cleanupError: sanitizedFusionExecutionError(
                cleanup.cleanupError
            )
        )
    }
    if let cleanup = error as? StorageRangeCleanupError {
        return StorageRangeCleanupError(
            iterationError: sanitizedFusionExecutionError(
                cleanup.iterationError
            ),
            cleanupError: sanitizedFusionExecutionError(
                cleanup.cleanupError
            )
        )
    }
    if let cleanup = error as? StorageTransactionCleanupError,
       let firstCancellationError = cleanup.cancellationErrors.first {
        var sanitized = StorageTransactionCleanupError(
            operationError: sanitizedFusionExecutionError(
                cleanup.operationError
            ),
            cancellationError: sanitizedFusionExecutionError(
                firstCancellationError
            )
        )
        for cancellationError in cleanup.cancellationErrors.dropFirst() {
            sanitized = sanitized.addingCancellationError(
                sanitizedFusionExecutionError(cancellationError)
            )
        }
        return sanitized
    }
    return error
}
