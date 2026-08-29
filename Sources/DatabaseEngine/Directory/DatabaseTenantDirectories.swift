import StorageKit

/// The resolved Directory handles of one Tenant Partition.
///
/// A Tenant Partition is the Default Partition of a database root or one
/// `bases/<Base.ID>` Partition. Both carry the same reserved children, so both
/// are described by this value.
///
/// The two handles are deliberately separate: Framework metadata is addressed
/// only from `system`, application entity data, indexes, and relationships only
/// from `data`. Handing an application path binder the `data` handle alone is
/// what keeps the two keyspaces disjoint, so this value is never flattened into
/// a single root Subspace.
package struct DatabaseTenantDirectories: Sendable {
    /// The Partition node itself.
    package let partition: Partition

    /// `system/database-framework`: the only Directory holding Framework state.
    package let system: Directory

    /// `data`: the only Directory application `#Directory` binding may address.
    package let data: Directory

    package init(partition: Partition, system: Directory, data: Directory) {
        self.partition = partition
        self.system = system
        self.data = data
    }

    /// Subspace Framework metadata derives its keys from.
    package var systemRoot: Subspace { system.root }

    /// Stable identity of the Partition itself.
    ///
    /// A Partition removed and recreated at the same address receives a new
    /// prefix, so comparing this value detects a stale binding.
    package var partitionRoot: Subspace { partition.root.root }
}
