import StorageKit

/// Selects the Partition a data operation is entitled to touch, and issues the
/// StorageKit lease that carries that entitlement.
///
/// A resolved address names where a node lives; it does not authorize reading
/// or writing there (SPEC 9.1). Authority is the noncopyable `PartitionLease`
/// StorageKit issues, so this type holds only what is needed to ask for one:
/// the engine owning the keyspace and the Partition to lease.
///
/// `Sources/DatabaseEngine/Directory/DESIGN.md` owns the per-operation ordering
/// this type participates in and the reason the container's other two "lease"
/// types are not authority.
package struct DatabasePartitionAuthority: Sendable {
    /// Engine owning the keyspace the Partition lives in.
    package let engine: any StorageEngine

    /// Partition every primary, index, and per-operation metadata Subspace of
    /// the operation resolves below.
    package let partition: Partition

    package init(engine: any StorageEngine, partition: Partition) {
        self.engine = engine
        self.partition = partition
    }

    /// Leases the Partition inside `transaction`.
    ///
    /// The lease is acquired in the transaction that will use it because
    /// `leasePartition` re-resolves the Partition there: a Partition removed
    /// and recreated at the same address has a different prefix, and the stale
    /// value is rejected rather than bound to the new keyspace.
    package func lease(
        in transaction: any TransactionReadAccess
    ) async throws -> PartitionLease {
        try await engine.leasePartition(partition, transaction: transaction)
    }
}
