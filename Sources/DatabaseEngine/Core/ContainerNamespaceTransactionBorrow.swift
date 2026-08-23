import StorageKit

/// Retains the container operation that owns a backend namespace transaction
/// while the backend performs an asynchronous namespace operation.
struct ContainerNamespaceTransactionBorrow {
    let transaction: any TransactionAccess

    private let operationLease: DatabaseStorageOperationLease

    init(
        transaction: any TransactionAccess,
        operationLease: DatabaseStorageOperationLease
    ) {
        self.transaction = transaction
        self.operationLease = operationLease
    }

    /// Ends the lexical borrow after the asynchronous backend call. The
    /// identity check is an internal invariant and makes the retained owner an
    /// observable use after the suspension point on every target.
    func end(for lifecycle: DatabaseStorageLifecycle) {
        precondition(
            operationLease.belongs(to: lifecycle),
            "A namespace transaction borrow must retain its container operation"
        )
    }
}
