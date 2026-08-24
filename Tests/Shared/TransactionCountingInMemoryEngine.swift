import StorageKit
import Synchronization

public final class TransactionCountingInMemoryEngine: StorageEngine, Sendable {
    public struct Configuration: Sendable {
        public init() {}
    }

    public typealias TransactionType = InMemoryTransaction

    private let underlying = InMemoryEngine()
    private let transactionCounter = Mutex(0)

    public init() {}

    public init(configuration: Configuration) async throws {}

    public var transactionCount: Int {
        transactionCounter.withLock { $0 }
    }

    public var namespaceResolver: any NamespaceResolver {
        underlying.namespaceResolver
    }

    public var namespaceCatalog: (any NamespaceCatalog)? {
        underlying.namespaceCatalog
    }

    public func createTransaction() throws -> InMemoryTransaction {
        transactionCounter.withLock { $0 += 1 }
        return try underlying.createTransaction()
    }

    public func requestShutdown() {
        underlying.requestShutdown()
    }

    public func waitUntilShutdown() async {
        await underlying.waitUntilShutdown()
    }
}
