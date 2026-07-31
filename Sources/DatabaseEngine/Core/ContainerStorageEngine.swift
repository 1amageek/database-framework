import StorageKit

/// Storage access whose operations are admitted by one database container's
/// terminal lifecycle.
final class ContainerStorageEngine: StorageEngine, Sendable {
    struct Configuration: Sendable {
        let lifecycle: DatabaseStorageLifecycle
    }

    typealias TransactionType = ContainerTransaction

    private let lifecycle: DatabaseStorageLifecycle

    init(lifecycle: DatabaseStorageLifecycle) {
        self.lifecycle = lifecycle
    }

    init(configuration: Configuration) async throws {
        self.lifecycle = configuration.lifecycle
    }

    var namespaceResolver: any NamespaceResolver {
        lifecycle.underlyingStorageEngine.namespaceResolver
    }

    var namespaceCatalog: (any NamespaceCatalog)? {
        lifecycle.underlyingStorageEngine.namespaceCatalog
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

    func resolveOrCreateNamespace(path: [String]) async throws -> Subspace {
        let lease = try lifecycle.beginOperation()
        defer { lease.finish() }
        return try await lifecycle
            .underlyingStorageEngine
            .resolveOrCreateNamespace(path: path)
    }

    func resolveExistingNamespace(path: [String]) async throws -> Subspace {
        let lease = try lifecycle.beginOperation()
        defer { lease.finish() }
        return try await lifecycle
            .underlyingStorageEngine
            .resolveExistingNamespace(path: path)
    }

    func listNamespaces(path: [String]) async throws -> [String] {
        let lease = try lifecycle.beginOperation()
        defer { lease.finish() }
        return try await lifecycle
            .underlyingStorageEngine
            .listNamespaces(path: path)
    }

    func removeNamespace(path: [String]) async throws {
        let lease = try lifecycle.beginOperation()
        defer { lease.finish() }
        try await lifecycle
            .underlyingStorageEngine
            .removeNamespace(path: path)
    }

    func namespaceExists(path: [String]) async throws -> Bool {
        let lease = try lifecycle.beginOperation()
        defer { lease.finish() }
        return try await lifecycle
            .underlyingStorageEngine
            .namespaceExists(path: path)
    }
}
