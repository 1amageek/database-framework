import StorageKit

/// Storage access whose operations are admitted by one database container's
/// terminal lifecycle.
final class ContainerStorageEngine: StorageEngine, Sendable {
    struct Configuration: Sendable {
        let lifecycle: DatabaseStorageLifecycle
    }

    typealias TransactionType = ContainerTransaction

    private let lifecycle: DatabaseStorageLifecycle
    private let containerDirectoryAccess: ContainerDirectoryAccess

    init(lifecycle: DatabaseStorageLifecycle) {
        self.lifecycle = lifecycle
        self.containerDirectoryAccess = ContainerDirectoryAccess(lifecycle: lifecycle)
    }

    init(configuration: Configuration) async throws {
        self.lifecycle = configuration.lifecycle
        self.containerDirectoryAccess = ContainerDirectoryAccess(
            lifecycle: configuration.lifecycle
        )
    }

    /// Forwarded from the backend engine so that a `Directory` created by the
    /// backend and a transaction admitted here compare equal by identity.
    var transactionDomain: StorageTransactionDomain {
        lifecycle.underlyingStorageEngine.transactionDomain
    }

    var directoryAccess: any DirectoryAccess {
        containerDirectoryAccess
    }

    func createTransaction() throws -> ContainerTransaction {
        let lease = try lifecycle.beginOperation()
        do {
            let transaction = try lifecycle
                .underlyingStorageEngine
                .createOwnedTransaction()
            return ContainerTransaction(
                transaction: transaction,
                operationLease: lease
            )
        } catch {
            lease.finish()
            throw error
        }
    }

    func createOwnedTransaction() throws -> any Transaction {
        try createTransaction()
    }

    func requestShutdown() {
        lifecycle.requestShutdown()
    }

    func waitUntilShutdown() async {
        await lifecycle.shutdown()
    }

    func executeTransaction(
        _ operation: @escaping @Sendable (
            any TransactionAccess
        ) async throws -> Void
    ) async throws {
        let lease = try lifecycle.beginOperation()
        defer { withExtendedLifetime(lease) {} }
        try await lifecycle.underlyingStorageEngine.executeTransaction(
            { transaction in
                try await operation(
                    ContainerTransactionAccess(
                        transaction: transaction,
                        operationLease: lease
                    )
                )
            }
        )
    }
}
