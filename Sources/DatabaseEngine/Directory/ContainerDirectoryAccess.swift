import StorageKit

/// The database container's Directory capability.
///
/// The container never hands a backend Directory capability its own transaction
/// wrapper: a backend resolves the concrete transaction type it created, so a
/// wrapper would be rejected as belonging to a different storage engine. Every
/// operation therefore admits the caller's transaction, takes a borrow that
/// retains the container operation across the asynchronous backend call, and
/// forwards the backend transaction the wrapper holds.
final class ContainerDirectoryAccess: DirectoryAccess, Sendable {
    private let lifecycle: DatabaseStorageLifecycle

    init(lifecycle: DatabaseStorageLifecycle) {
        self.lifecycle = lifecycle
    }

    /// The backend capability that owns Directory existence and enumeration.
    private var backendAccess: any DirectoryAccess {
        lifecycle.underlyingStorageEngine.directoryAccess
    }

    /// Forwarded unchanged: a `Directory` value carries the domain that created
    /// it, and every backend operation compares that domain by identity.
    var transactionDomain: StorageTransactionDomain {
        backendAccess.transactionDomain
    }

    var backend: StorageBackend {
        backendAccess.backend
    }

    // MARK: - Read operations

    func openRoot(transaction: any TransactionReadAccess) async throws -> Directory? {
        let borrow = try admit(transaction, requiresMutation: false)
        defer { borrow.end(for: lifecycle) }
        return try await backendAccess.openRoot(transaction: borrow.transaction)
    }

    func open(
        _ name: String,
        expecting expected: LayerTag?,
        in parent: Directory,
        transaction: any TransactionReadAccess
    ) async throws -> Directory? {
        let borrow = try admit(transaction, requiresMutation: false)
        defer { borrow.end(for: lifecycle) }
        return try await backendAccess.open(
            name,
            expecting: expected,
            in: parent,
            transaction: borrow.transaction
        )
    }

    func listChildren(
        in parent: Directory,
        after: String?,
        limit: Int,
        transaction: any TransactionReadAccess
    ) async throws -> [DirectoryEntry] {
        let borrow = try admit(transaction, requiresMutation: false)
        defer { borrow.end(for: lifecycle) }
        return try await backendAccess.listChildren(
            in: parent,
            after: after,
            limit: limit,
            transaction: borrow.transaction
        )
    }

    // MARK: - Mutating operations

    func openOrInitializeRoot(transaction: any TransactionAccess) async throws -> Directory {
        let borrow = try admit(transaction, requiresMutation: true)
        defer { borrow.end(for: lifecycle) }
        return try await backendAccess.openOrInitializeRoot(transaction: borrow.transaction)
    }

    func openOrCreate(
        _ name: String,
        layer: LayerTag,
        in parent: Directory,
        transaction: any TransactionAccess
    ) async throws -> Directory {
        let borrow = try admit(transaction, requiresMutation: true)
        defer { borrow.end(for: lifecycle) }
        return try await backendAccess.openOrCreate(
            name,
            layer: layer,
            in: parent,
            transaction: borrow.transaction
        )
    }

    func move(
        _ name: String,
        in source: Directory,
        to newName: String,
        in destination: Directory,
        transaction: any TransactionAccess
    ) async throws -> Directory {
        let borrow = try admit(transaction, requiresMutation: true)
        defer { borrow.end(for: lifecycle) }
        return try await backendAccess.move(
            name,
            in: source,
            to: newName,
            in: destination,
            transaction: borrow.transaction
        )
    }

    func remove(
        _ name: String,
        in parent: Directory,
        transaction: any TransactionAccess
    ) async throws {
        let borrow = try admit(transaction, requiresMutation: true)
        defer { borrow.end(for: lifecycle) }
        try await backendAccess.remove(name, in: parent, transaction: borrow.transaction)
    }

    // MARK: - Admission

    /// Admits a transaction this container produced and borrows the backend
    /// transaction it wraps. A read-only capability refuses a mutating
    /// Directory operation before the backend observes it, and before any
    /// operation lease is taken for a call that cannot succeed.
    private func admit(
        _ transaction: any TransactionReadAccess,
        requiresMutation: Bool
    ) throws -> ContainerDirectoryTransactionBorrow {
        if let admitted = transaction as? ReadAuthorizedTransactionAccess {
            guard !requiresMutation else {
                throw DatabaseReadTransactionError.mutationRequiresWriteAccess
            }
            return try admitted.directoryTransactionBorrow(for: lifecycle)
        }
        if let admitted = transaction as? DatabaseReadTransaction {
            guard !requiresMutation else {
                throw DatabaseReadTransactionError.mutationRequiresWriteAccess
            }
            return try admitted.directoryTransactionBorrow(for: lifecycle)
        }
        if let admitted = transaction as? ContainerTransactionAccess {
            return try admitted.directoryTransactionBorrow(for: lifecycle)
        }
        if let admitted = transaction as? ContainerTransaction {
            return try admitted.directoryTransactionBorrow(for: lifecycle)
        }
        // The rejected transaction type is not reported: `type(of:)` on an
        // existential requires runtime type metadata, which Embedded Swift
        // does not provide. Naming it here breaks the Embedded WASM runtime.
        throw StorageError.invalidOperation(
            """
            Directory operations require a transaction admitted by this \
            database container
            """
        )
    }
}
