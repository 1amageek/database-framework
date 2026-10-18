import DatabaseKit

package enum SQLMutationError: Error, Sendable, Equatable {
    case invalidStatement(String)
    case cardinalityViolation
    case invalidConflictTarget
    case deferrableConflictTarget
    case unsupportedConflictInference
    case unsupportedHiddenIdentifier(String)
    case identityRequestRequired
    case identityReplayDiverged
}
