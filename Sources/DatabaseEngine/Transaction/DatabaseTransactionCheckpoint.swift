import StorageKit

/// An opaque logical checkpoint belonging to one admitted transaction attempt.
public struct DatabaseTransactionCheckpoint: Sendable, Hashable {
    let physical: TransactionCheckpoint
}
