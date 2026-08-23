import StorageKit

/// Non-forgeable package capability for borrowing only the backend read
/// transaction used by namespace metadata. It deliberately has no mutation
/// counterpart and is implemented only by container-level transactions;
/// data-root and index projections must never forward namespace resolution.
protocol ContainerNamespaceReadTransactionBorrowing: Sendable {
    func namespaceReadTransactionBorrow(
        for lifecycle: DatabaseStorageLifecycle
    ) throws -> ContainerNamespaceReadTransactionBorrow
}

/// Retains one write-capable backend namespace transaction for the duration
/// of an asynchronous namespace mutation.
struct ContainerNamespaceWriteTransactionBorrow {
    let transaction: any TransactionAccess

    private let operationLease: DatabaseStorageOperationLease

    init(
        transaction: any TransactionAccess,
        operationLease: DatabaseStorageOperationLease
    ) {
        self.transaction = transaction
        self.operationLease = operationLease
    }

    func readOnly() -> ContainerNamespaceReadTransactionBorrow {
        ContainerNamespaceReadTransactionBorrow(
            transaction: DataRootTransactionAccess.admitted(
                transaction,
                dataRoot: Subspace()
            ).readProjection(),
            operationLease: operationLease
        )
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

/// Retains one read-only backend namespace transaction while namespace
/// metadata is resolved outside the selected application data root.
struct ContainerNamespaceReadTransactionBorrow {
    let transaction: any TransactionReadAccess

    private let operationLease: DatabaseStorageOperationLease

    init(
        transaction: any TransactionReadAccess,
        operationLease: DatabaseStorageOperationLease
    ) {
        self.transaction = transaction
        self.operationLease = operationLease
    }

    func end(for lifecycle: DatabaseStorageLifecycle) {
        precondition(
            operationLease.belongs(to: lifecycle),
            "A namespace transaction borrow must retain its container operation"
        )
    }
}
