import DatabaseOperationCore
public struct DatabaseJobRuntimeConfiguration: Sendable, Hashable {
    public let leaseDurationMilliseconds: UInt32
    public let maximumJobsPerRun: Int
    public let maximumSliceAttempts: UInt32
    public let maximumSliceRetryBackoffMilliseconds: UInt32
    public let unsuccessfulOutcomeCommitInitialBackoffMilliseconds: UInt32
    public let unsuccessfulOutcomeCommitMaximumBackoffMilliseconds: UInt32
    public let leaseSafetyMarginMilliseconds: UInt32

    public init(
        leaseDurationMilliseconds: UInt32 = 60_000,
        maximumJobsPerRun: Int = 8,
        maximumSliceAttempts: UInt32 = 32,
        maximumSliceRetryBackoffMilliseconds: UInt32 = 3_600_000,
        unsuccessfulOutcomeCommitInitialBackoffMilliseconds: UInt32 = 100,
        unsuccessfulOutcomeCommitMaximumBackoffMilliseconds: UInt32 = 3_600_000,
        leaseSafetyMarginMilliseconds: UInt32 = 5_000
    ) {
        self.leaseDurationMilliseconds = leaseDurationMilliseconds
        self.maximumJobsPerRun = maximumJobsPerRun
        self.maximumSliceAttempts = maximumSliceAttempts
        self.maximumSliceRetryBackoffMilliseconds =
            maximumSliceRetryBackoffMilliseconds
        self.unsuccessfulOutcomeCommitInitialBackoffMilliseconds =
            unsuccessfulOutcomeCommitInitialBackoffMilliseconds
        self.unsuccessfulOutcomeCommitMaximumBackoffMilliseconds =
            unsuccessfulOutcomeCommitMaximumBackoffMilliseconds
        self.leaseSafetyMarginMilliseconds = leaseSafetyMarginMilliseconds
    }

    public func validate() throws {
        guard leaseDurationMilliseconds > 0 else {
            throw DatabaseJobRuntimeError.invalidConfiguration(
                "leaseDurationMilliseconds must be greater than zero"
            )
        }
        guard leaseSafetyMarginMilliseconds < leaseDurationMilliseconds else {
            throw DatabaseJobRuntimeError.invalidConfiguration(
                "leaseSafetyMarginMilliseconds must be less than the lease duration"
            )
        }
        guard maximumJobsPerRun > 0 else {
            throw DatabaseJobRuntimeError.invalidConfiguration(
                "maximumJobsPerRun must be greater than zero"
            )
        }
        guard maximumSliceAttempts > 0 else {
            throw DatabaseJobRuntimeError.invalidConfiguration(
                "maximumSliceAttempts must be greater than zero"
            )
        }
        guard maximumSliceRetryBackoffMilliseconds > 0 else {
            throw DatabaseJobRuntimeError.invalidConfiguration(
                "maximumSliceRetryBackoffMilliseconds must be greater than zero"
            )
        }
        guard unsuccessfulOutcomeCommitInitialBackoffMilliseconds > 0,
              unsuccessfulOutcomeCommitInitialBackoffMilliseconds
                <= unsuccessfulOutcomeCommitMaximumBackoffMilliseconds else {
            throw DatabaseJobRuntimeError.invalidConfiguration(
                "Unsuccessful outcome backoff limits are inconsistent"
            )
        }
    }

    package func validate(sliceTimeoutMilliseconds: UInt32) throws {
        guard sliceTimeoutMilliseconds > 0 else {
            throw DatabaseJobRuntimeError.invalidConfiguration(
                "sliceTimeoutMilliseconds must be greater than zero"
            )
        }
        let requiredLease = sliceTimeoutMilliseconds.addingReportingOverflow(
            leaseSafetyMarginMilliseconds
        )
        guard !requiredLease.overflow,
              requiredLease.partialValue <= leaseDurationMilliseconds else {
            throw DatabaseJobRuntimeError.invalidConfiguration(
                "The job lease must exceed the slice timeout by the configured safety margin"
            )
        }
    }
}
