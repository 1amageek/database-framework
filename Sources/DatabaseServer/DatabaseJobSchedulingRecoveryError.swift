public struct DatabaseJobSchedulingRecoveryError: Error, CustomStringConvertible {
    public let processingError: any Error
    public let schedulingError: any Error

    public init(
        processingError: any Error,
        schedulingError: any Error
    ) {
        self.processingError = processingError
        self.schedulingError = schedulingError
    }

    public var description: String {
        "Job processing failed with \(processingError); rescheduling also failed with \(schedulingError)"
    }
}
