import StorageKit

/// Requests cancellation from the admitted attempt's sole lifecycle owner.
enum ActiveTransactionCancellation {
    @TaskLocal static var request: (@Sendable (any Error) async -> StorageTransactionCleanupError?)?
}
