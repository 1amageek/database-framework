import StorageKit

/// Retains the container operation that owns a backend transaction while the
/// backend performs an asynchronous Directory operation.
struct ContainerDirectoryTransactionBorrow {
    /// The backend transaction the container admitted. Directory operations must
    /// receive this value rather than the container wrapper, because a backend
    /// Directory capability resolves its own concrete transaction type.
    let transaction: any TransactionAccess
    private let operationLease: DatabaseStorageOperationLease
    /// Read-scope operations retained by this borrow, in acquisition order. An
    /// attenuated read scope resolves through the scope that admitted it, so a
    /// nested resolution contributes one lease per level and every level must
    /// stay active until the backend call returns.
    private let readScopeOperationLeases: [DatabaseReadScopeOperationLease]

    init(
        transaction: any TransactionAccess,
        operationLease: DatabaseStorageOperationLease,
        readScopeOperationLeases: [DatabaseReadScopeOperationLease] = []
    ) {
        self.transaction = transaction
        self.operationLease = operationLease
        self.readScopeOperationLeases = readScopeOperationLeases
    }

    /// Adds one read-scope operation to the borrow without releasing the scopes
    /// a deeper resolution already retained.
    func retainingReadScope(
        _ lease: DatabaseReadScopeOperationLease?
    ) -> ContainerDirectoryTransactionBorrow {
        guard let lease else { return self }
        return ContainerDirectoryTransactionBorrow(
            transaction: transaction,
            operationLease: operationLease,
            readScopeOperationLeases: readScopeOperationLeases + [lease]
        )
    }

    /// Ends the lexical borrow after the asynchronous backend call. The identity
    /// check is an internal invariant and makes the retained owner an observable
    /// use after the suspension point on every target.
    func end(for lifecycle: DatabaseStorageLifecycle) {
        precondition(
            operationLease.belongs(to: lifecycle),
            "A Directory transaction borrow must retain its container operation"
        )
        for lease in readScopeOperationLeases.reversed() {
            lease.end()
        }
    }
}
