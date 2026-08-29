#if DATABASE_MULTI_BASE
import StorageKit

/// Immutable Base placement retained by every admitted operation.
package struct DatabaseBaseGeneration: Sendable {
    package let record: DatabaseBaseRecord
    package let domain: DatabaseStorageDomainRuntime
    package let tenant: DatabaseTenantDirectories
}

#endif
