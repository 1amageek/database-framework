import DatabaseKit
@_spi(DatabaseServer) import DatabaseWire

/// Typed failures shared by Base, Composition, and Grant administration.
public enum DatabaseAdministrationError: Error, Sendable, Equatable {
    case targetMismatch(DatabaseOperationTarget)
    case grantResourceMismatch(
        expected: Security.Resource,
        actual: Security.Resource
    )
    case idempotencyKeyMismatch
    case unsupportedLifecycleAction
}
