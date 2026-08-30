/// Failures of the Framework-owned database Directory layout.
///
/// StorageKit owns the layout-marker state machine and reports an unusable
/// store as `StorageError.incompatibleStorageLayout`. This type reports only
/// violations of the layout the Framework itself commits: the reserved
/// Directories of a Tenant Partition and the configured database root path.
public enum DatabaseDirectoryLayoutError: Error, Sendable, Hashable {
    /// A Tenant Partition exists without a reserved child. The Partition and
    /// its reserved children commit in one transaction, so an existing
    /// Partition missing one of them is a corrupted layout rather than an
    /// absent Tenant.
    ///
    /// `presentChildren` reports the names found beside the missing one, so a
    /// Partition that holds no child at all is distinguishable from one that
    /// lost a single node. The listing is bounded and may be truncated.
    case missingReservedDirectory(
        partition: [String],
        name: String,
        presentChildren: [String]
    )

    /// A child of `bases` is stored under a layer other than `.partition`.
    /// Every Base is created as a Partition, so a child of another layer is a
    /// structure this layout did not commit and cannot address as a Base.
    case nonPartitionBase(name: String)

    /// The configured database root path contains an empty component.
    case emptyRootPathComponent(index: Int)
}

extension DatabaseDirectoryLayoutError: CustomStringConvertible {
    public var description: String {
        switch self {
        case .missingReservedDirectory(let partition, let name, let presentChildren):
            let address = partition.joined(separator: "/")
            let present = presentChildren.isEmpty
                ? "none"
                : presentChildren.joined(separator: ", ")
            return "Partition /\(address) is missing its reserved \(name) "
                + "Directory (present children: \(present))"
        case .nonPartitionBase(let name):
            return "Base '\(name)' is not stored as a Partition"
        case .emptyRootPathComponent(let index):
            return "Database root path component \(index) is empty"
        }
    }
}
