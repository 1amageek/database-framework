/// A failure that prevents an online index build from completing its lifecycle.
public enum OnlineIndexBuildError:
    Error,
    CustomStringConvertible,
    Equatable,
    Sendable
{
    case uniquenessViolationsDetected(
        indexName: String,
        violationCount: Int,
        totalConflictingEntities: Int
    )
    case invalidBatchSize(Int)
    case invalidThrottleDelayMilliseconds(Int)
    case invalidMaximumConcurrency(Int)
    case invalidChunkSizeBytes(Int)
    case unsupportedCustomBuildStrategy(indexName: String)
    case unsupportedUniqueCustomBuildStrategy(indexName: String)
    case unsupportedUniquenessConstraint(indexName: String)
    case requiresSymmetricConfiguration
    case corruptedProgress

    public var description: String {
        switch self {
        case .uniquenessViolationsDetected(
            let indexName,
            let violationCount,
            let totalEntities
        ):
            return """
            Unique index '\(indexName)' has violations: \
            \(violationCount) duplicate value(s) affecting \(totalEntities) entity(s). \
            Index remains in write-only state. \
            Use scanUniquenessViolations() to review and resolve duplicates.
            """
        case .invalidBatchSize(let value):
            return "Online index batch size must be greater than zero; received \(value)"
        case .invalidThrottleDelayMilliseconds(let value):
            return "Online index throttle delay must not be negative; received \(value)"
        case .invalidMaximumConcurrency(let value):
            return "Online index maximum concurrency must be greater than zero; received \(value)"
        case .invalidChunkSizeBytes(let value):
            return "Online index chunk size must be greater than zero; received \(value)"
        case .unsupportedCustomBuildStrategy(let indexName):
            return "Online index target '\(indexName)' requires a custom strategy that this builder cannot execute"
        case .unsupportedUniqueCustomBuildStrategy(let indexName):
            return "Unique online index '\(indexName)' cannot use an opaque custom build strategy"
        case .unsupportedUniquenessConstraint(let indexName):
            return "Online index '\(indexName)' requires a uniqueness-capable maintainer"
        case .requiresSymmetricConfiguration:
            return "A symmetric index builder requires a symmetric index configuration"
        case .corruptedProgress:
            return "Online index progress is corrupted"
        }
    }
}
