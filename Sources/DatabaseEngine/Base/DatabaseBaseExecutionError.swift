import DatabaseKit

/// Fail-closed target resolution and lifecycle errors for Base operations.
public enum DatabaseBaseExecutionError: Error, Sendable, Equatable {
    case baseTargetRequired
    case baseNotFound(Base.ID)
    case baseUnavailable(Base.ID, lifecycle: UInt8)
    case storageDomainUnavailable(DatabaseStorageDomain.ID)
    case placementRootMissing(Base.ID)
    case leaseCountOverflow(Base.ID)
}
