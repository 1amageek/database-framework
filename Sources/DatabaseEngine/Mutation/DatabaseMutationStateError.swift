@_spi(DatabaseExecution)
public enum DatabaseMutationStateError:
    Error,
    Sendable,
    Equatable,
    CustomStringConvertible {
    case invalidLimits
    case idempotencyKeyRequired
    case idempotencyKeyTooLarge(actual: Int, maximum: Int)
    case idempotencyKeyConflict
    case invalidDiscriminator
    case invalidFingerprint
    case outcomeTooLarge(actual: Int, maximum: Int)
    case corruptedState
    case logicalVersionOverflow
    case containerMismatch

    public var description: String {
        switch self {
        case .invalidLimits:
            return "Mutation state limits are invalid"
        case .idempotencyKeyRequired:
            return "A mutation requires an idempotency key"
        case .idempotencyKeyTooLarge(let actual, let maximum):
            return "Idempotency key contains \(actual) UTF-8 bytes, exceeding the limit of \(maximum)"
        case .idempotencyKeyConflict:
            return "The idempotency key is associated with a different mutation"
        case .invalidDiscriminator:
            return "Mutation discriminator is invalid"
        case .invalidFingerprint:
            return "Mutation fingerprint is invalid"
        case .outcomeTooLarge(let actual, let maximum):
            return "Mutation outcome contains \(actual) bytes, exceeding the limit of \(maximum)"
        case .corruptedState:
            return "Persisted mutation state is corrupted"
        case .logicalVersionOverflow:
            return "The logical mutation version reached UInt64.max"
        case .containerMismatch:
            return "Mutation state and transaction use different containers"
        }
    }
}
