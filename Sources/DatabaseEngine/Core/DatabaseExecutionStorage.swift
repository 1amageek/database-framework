import DatabaseKit
import StorageKit

/// Non-authoritative identity of the storage root selected for one database
/// execution path.
///
/// The lightweight runtime resolves directly to its one injected engine. The
/// `MultiBase` runtime resolves the operation-bound Base lease. This value
/// intentionally contains no engine or transaction executor: retaining
/// metadata must never retain storage authority beyond the operation that
/// admitted it.
@_spi(DatabaseExecution)
public struct DatabaseExecutionStorage: Sendable, Equatable {
    public let root: Subspace
    #if DATABASE_MULTI_BASE
    public let resource: Security.Resource
    #endif
    public let generation: UInt64
    public let domainIdentifier: String

    #if DATABASE_MULTI_BASE
    package init(
        root: Subspace,
        resource: Security.Resource,
        generation: UInt64,
        domainIdentifier: String
    ) {
        self.root = root
        self.resource = resource
        self.generation = generation
        self.domainIdentifier = domainIdentifier
    }
    #else
    package init(
        root: Subspace,
        generation: UInt64,
        domainIdentifier: String
    ) {
        self.root = root
        self.generation = generation
        self.domainIdentifier = domainIdentifier
    }
    #endif
}
