import StorageKit
import Synchronization

final class TransactionCountingInMemoryEngine: StorageEngine, Sendable {
    struct Configuration: Sendable {
        init() {}
    }

    typealias TransactionType = InMemoryTransaction

    private let engine: InMemoryEngine
    private let transactionCountState = Mutex(0)

    init(configuration: Configuration = .init()) {
        _ = configuration
        self.engine = InMemoryEngine()
    }

    func createTransaction() throws -> InMemoryTransaction {
        let transaction = try engine.createTransaction()
        transactionCountState.withLock { $0 += 1 }
        return transaction
    }

    var namespaceResolver: any NamespaceResolver {
        engine.namespaceResolver
    }

    var namespaceCatalog: (any NamespaceCatalog)? {
        engine.namespaceCatalog
    }

    func shutdown() {
        engine.shutdown()
    }

    var transactionCount: Int {
        transactionCountState.withLock { $0 }
    }

    var keyCount: Int {
        engine.count
    }
}
