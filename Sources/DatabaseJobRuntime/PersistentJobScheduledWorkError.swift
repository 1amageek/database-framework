import DatabaseOperationCore
/// A failure in one phase of persistent job scheduled-work execution.
///
/// Task cancellation is propagated as `CancellationError` and is never wrapped
/// by this error.
public enum PersistentJobScheduledWorkError: Error, CustomStringConvertible {
    /// Loading the bounded set of due jobs failed before processing began.
    case loadingDueJobs(any Error)

    /// At least one due job failed after due jobs were loaded.
    case processingJob(any Error)

    /// Persisting the next required scheduler wake-up failed.
    case schedulingNextWakeUp(any Error)

    /// Job processing and subsequent wake-up persistence both failed.
    case processingJobAndSchedulingNextWakeUp(
        processingError: any Error,
        schedulingError: any Error
    )

    public var description: String {
        switch self {
        case .loadingDueJobs:
            return "Loading due database jobs failed"
        case .processingJob:
            return "Scheduled database job processing failed"
        case .schedulingNextWakeUp:
            return "Scheduling the next database job wake-up failed"
        case .processingJobAndSchedulingNextWakeUp:
            return "Scheduled database job processing and wake-up scheduling failed"
        }
    }
}
