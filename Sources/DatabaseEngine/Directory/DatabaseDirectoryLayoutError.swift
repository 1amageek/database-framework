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
    case missingReservedDirectory(partition: [String], name: String)

    /// The configured database root path contains an empty component.
    case emptyRootPathComponent(index: Int)
}

extension DatabaseDirectoryLayoutError: CustomStringConvertible {
    public var description: String {
        switch self {
        case .missingReservedDirectory(let partition, let name):
            let address = partition.joined(separator: "/")
            return "Partition /\(address) is missing its reserved \(name) Directory"
        case .emptyRootPathComponent(let index):
            return "Database root path component \(index) is empty"
        }
    }
}
