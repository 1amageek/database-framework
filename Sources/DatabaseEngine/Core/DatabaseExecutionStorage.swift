import DatabaseKit
import StorageKit

/// Transaction and namespace selected for one database execution path.
///
/// The lightweight runtime resolves directly to its one injected engine. The
/// `MultipleBases` runtime resolves the operation-bound Base lease. This value
/// does not perform authorization; persisted Grant evaluation remains in the
/// optional Base transaction path.
@_spi(DatabaseExecution)
public struct DatabaseExecutionStorage: Sendable {
    public let engine: any StorageEngine
    public let transactionExecutor: StorageTransactionExecutor
    public let root: Subspace
    #if DATABASE_MULTIPLE_BASES
    public let resource: Security.Resource
    #endif
    public let generation: UInt64
    public let domainIdentifier: String

    #if DATABASE_MULTIPLE_BASES
    package init(
        engine: any StorageEngine,
        transactionExecutor: StorageTransactionExecutor,
        root: Subspace,
        resource: Security.Resource,
        generation: UInt64,
        domainIdentifier: String
    ) {
        self.engine = engine
        self.transactionExecutor = transactionExecutor
        self.root = root
        self.resource = resource
        self.generation = generation
        self.domainIdentifier = domainIdentifier
    }
    #else
    package init(
        engine: any StorageEngine,
        transactionExecutor: StorageTransactionExecutor,
        root: Subspace,
        generation: UInt64,
        domainIdentifier: String
    ) {
        self.engine = engine
        self.transactionExecutor = transactionExecutor
        self.root = root
        self.generation = generation
        self.domainIdentifier = domainIdentifier
    }
    #endif
}
