import StorageKit

/// A transaction this database container admitted and can therefore lend to a
/// backend Directory capability.
///
/// Directory resolution needs the backend transaction, the container operation
/// that keeps the storage lifecycle active for the whole asynchronous call, and
/// the authority to mutate the Directory catalog. Those three facts belong to
/// the transaction that the container produced, so admission is expressed as a
/// contract the admitted transactions conform to rather than as a list of
/// concrete types held by the Directory access. A caller-owned transaction from
/// another container, or a raw backend transaction, carries no container
/// operation lease and is therefore not admissible.
protocol ContainerAdmittedTransaction {
    /// Whether this transaction may create, move, or remove Directories. A
    /// read-only capability refuses a mutating Directory operation before the
    /// backend observes it.
    var admitsDirectoryMutation: Bool { get }

    /// Borrows the backend transaction while retaining every operation lease
    /// that keeps it valid. The borrow must be ended after the backend call.
    ///
    /// - Throws: `StorageError.invalidOperation` when the transaction was
    ///   admitted by a different container lifecycle.
    func directoryTransactionBorrow(
        for lifecycle: DatabaseStorageLifecycle
    ) throws -> ContainerDirectoryTransactionBorrow
}
