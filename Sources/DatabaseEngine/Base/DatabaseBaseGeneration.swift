#if DATABASE_MULTIPLE_BASES
import StorageKit

/// Immutable Base placement retained by every admitted operation.
package struct DatabaseBaseGeneration: Sendable {
    package let record: DatabaseBaseRecord
    package let domain: DatabaseStorageDomainRuntime
    package let root: Subspace
}

#endif
