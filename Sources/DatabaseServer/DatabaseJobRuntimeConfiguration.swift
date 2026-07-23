public struct DatabaseJobRuntimeConfiguration: Sendable, Hashable {
    public let leaseDurationMilliseconds: UInt32
    public let maximumJobsPerRun: Int
    public let maximumAttempts: UInt32
    public let maximumBackoffMilliseconds: UInt32
    public let leaseSafetyMarginMilliseconds: UInt32

    public init(
        leaseDurationMilliseconds: UInt32 = 60_000,
        maximumJobsPerRun: Int = 8,
        maximumAttempts: UInt32 = 32,
        maximumBackoffMilliseconds: UInt32 = 3_600_000,
        leaseSafetyMarginMilliseconds: UInt32 = 5_000
    ) {
        self.leaseDurationMilliseconds = leaseDurationMilliseconds
        self.maximumJobsPerRun = maximumJobsPerRun
        self.maximumAttempts = maximumAttempts
        self.maximumBackoffMilliseconds = maximumBackoffMilliseconds
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
        guard maximumAttempts > 0 else {
            throw DatabaseJobRuntimeError.invalidConfiguration(
                "maximumAttempts must be greater than zero"
            )
        }
        guard maximumBackoffMilliseconds > 0 else {
            throw DatabaseJobRuntimeError.invalidConfiguration(
                "maximumBackoffMilliseconds must be greater than zero"
            )
        }
    }

    func validate(sliceTimeoutMilliseconds: UInt32) throws {
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
