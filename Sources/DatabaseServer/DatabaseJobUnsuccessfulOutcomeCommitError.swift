import DatabaseValue
import DatabaseWire

public struct DatabaseJobUnsuccessfulOutcomeCommitError:
    Error,
    CustomStringConvertible {
    public let jobID: DatabaseUUID
    public let outcome: DatabaseJobUnsuccessfulOutcome
    public let underlyingError: any Error

    public init(
        jobID: DatabaseUUID,
        outcome: DatabaseJobUnsuccessfulOutcome,
        underlyingError: any Error
    ) {
        self.jobID = jobID
        self.outcome = outcome
        self.underlyingError = underlyingError
    }

    public var description: String {
        "Persistent job unsuccessful outcome commit failed for \(jobID): \(underlyingError)"
    }
}
