import DatabaseKit
import StorageKit

/// Transaction and Directory roots selected for one database execution path.
///
/// The lightweight runtime resolves directly to its one injected engine. The
/// `MultiBase` runtime resolves the operation-bound Base lease. This value
/// does not perform authorization; persisted Grant evaluation remains in the
/// optional Base transaction path.
///
/// The two roots are separate because Section 13 of the product architecture
/// separates the keyspaces they address: Framework metadata derives only from
/// `systemRoot`, application entity data, indexes, and relationships only from
/// `dataRoot`. A caller that needs one must not receive the other.
@_spi(DatabaseExecution)
public struct DatabaseExecutionStorage: Sendable {
    public let engine: any StorageEngine
    public let transactionExecutor: StorageTransactionExecutor

    /// `system/database-framework` of the bound Tenant Partition.
    public let systemRoot: Subspace

    /// `data` of the bound Tenant Partition.
    public let dataRoot: Subspace

    #if DATABASE_MULTI_BASE
    public let resource: Security.Resource
    #endif
    public let generation: UInt64
    public let domainIdentifier: String

    #if DATABASE_MULTI_BASE
    package init(
        engine: any StorageEngine,
        transactionExecutor: StorageTransactionExecutor,
        systemRoot: Subspace,
        dataRoot: Subspace,
        resource: Security.Resource,
        generation: UInt64,
        domainIdentifier: String
    ) {
        self.engine = engine
        self.transactionExecutor = transactionExecutor
        self.systemRoot = systemRoot
        self.dataRoot = dataRoot
        self.resource = resource
        self.generation = generation
        self.domainIdentifier = domainIdentifier
    }
    #else
    package init(
        engine: any StorageEngine,
        transactionExecutor: StorageTransactionExecutor,
        systemRoot: Subspace,
        dataRoot: Subspace,
        generation: UInt64,
        domainIdentifier: String
    ) {
        self.engine = engine
        self.transactionExecutor = transactionExecutor
        self.systemRoot = systemRoot
        self.dataRoot = dataRoot
        self.generation = generation
        self.domainIdentifier = domainIdentifier
    }
    #endif
}
