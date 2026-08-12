import DatabaseOperationCore
import DatabaseTypes
@_spi(DatabaseOperations) import DatabaseWire

public struct DatabaseJobUnsuccessfulOutcomeCommitError:
    Error,
    CustomStringConvertible {
    public let jobID: DatabaseTypes.UUID
    public let outcome: DatabaseJobUnsuccessfulOutcome
    public let underlyingError: any Error

    public init(
        jobID: DatabaseTypes.UUID,
        outcome: DatabaseJobUnsuccessfulOutcome,
        underlyingError: any Error
    ) {
        self.jobID = jobID
        self.outcome = outcome
        self.underlyingError = underlyingError
    }

    public var description: String {
        "Persistent job unsuccessful outcome commit failed for \(jobID)"
    }
}
