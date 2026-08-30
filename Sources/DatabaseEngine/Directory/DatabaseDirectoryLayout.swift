import StorageKit

/// The fixed Directory layout of one database root.
///
/// ```text
/// database root Directory                  plain, configured path
/// ├── default                              Partition: the Default Partition
/// │   ├── system
/// │   │   └── database-framework
/// │   └── data
/// │       └── application #Directory hierarchy
/// └── bases                                plain, present only with MultiBase
///     └── <Base.ID>                        Partition: one per Base
///         ├── system
///         │   └── database-framework
///         └── data
///             └── application #Directory hierarchy
/// ```
///
/// The names below are reserved by the Framework at the positions shown. They
/// are Framework-owned nodes, so the dynamic-component rule that governs
/// `#Directory` declarations does not apply to them.
///
/// Every operation here takes the caller's transaction and creates nothing on a
/// read path. The layout-marker state machine runs inside StorageKit's root
/// operations: an unknown marker, or a non-empty keyspace with no marker,
/// fails with `StorageError.incompatibleStorageLayout`, which this type
/// propagates unchanged rather than reinterpreting.
package enum DatabaseDirectoryLayout {
    // MARK: - Reserved names

    /// Default Partition of a database root.
    package static let defaultPartitionName = "default"

    /// Plain Directory holding one Partition per Base.
    package static let basesDirectoryName = "bases"

    /// Framework-owned subtree of a Tenant Partition.
    package static let systemDirectoryName = "system"

    /// This framework's node below `system`.
    package static let frameworkDirectoryName = "database-framework"

    /// Application-owned subtree of a Tenant Partition.
    package static let dataDirectoryName = "data"

    /// Names no node other than the Framework's own may take at a database root.
    package static let reservedRootNames: Set<String> = [
        defaultPartitionName,
        basesDirectoryName,
    ]

    /// Names no node other than the Framework's own may take at a Tenant root.
    package static let reservedTenantNames: Set<String> = [
        systemDirectoryName,
        dataDirectoryName,
    ]

    // MARK: - Database root

    /// Opens the database root Directory, creating the store layout and every
    /// path component that is absent.
    package static func openOrInitializeDatabaseRoot(
        path: [String],
        access: any DirectoryAccess,
        transaction: any TransactionAccess
    ) async throws -> Directory {
        var current = try await access.openOrInitializeRoot(transaction: transaction)
        for component in path {
            current = try await access.openOrCreate(
                component,
                layer: .default,
                in: current,
                transaction: transaction
            )
        }
        return current
    }

    /// Opens the database root Directory without creating anything. `nil` means
    /// the store is uninitialized or the configured path does not exist yet.
    package static func openDatabaseRoot(
        path: [String],
        access: any DirectoryAccess,
        transaction: any TransactionReadAccess
    ) async throws -> Directory? {
        guard var current = try await access.openRoot(transaction: transaction) else {
            return nil
        }
        for component in path {
            guard let next = try await access.open(
                component,
                expecting: .default,
                in: current,
                transaction: transaction
            ) else {
                return nil
            }
            current = next
        }
        return current
    }

    // MARK: - Tenant Partitions

    /// Opens or creates the Default Partition and its reserved children.
    package static func openOrCreateDefaultTenant(
        in databaseRoot: Directory,
        access: any DirectoryAccess,
        transaction: any TransactionAccess
    ) async throws -> DatabaseTenantDirectories {
        let partition = try await access.openOrCreatePartition(
            defaultPartitionName,
            in: databaseRoot,
            transaction: transaction
        )
        return try await openOrCreateTenantChildren(
            of: partition,
            access: access,
            transaction: transaction
        )
    }

    /// Opens the Default Partition and its reserved children, creating nothing.
    package static func openDefaultTenant(
        in databaseRoot: Directory,
        access: any DirectoryAccess,
        transaction: any TransactionReadAccess
    ) async throws -> DatabaseTenantDirectories? {
        guard let partition = try await access.openPartition(
            defaultPartitionName,
            in: databaseRoot,
            transaction: transaction
        ) else {
            return nil
        }
        return try await openTenantChildren(
            of: partition,
            access: access,
            transaction: transaction
        )
    }

    /// Opens or creates `bases/<name>` and its reserved children.
    package static func openOrCreateBaseTenant(
        _ name: String,
        in databaseRoot: Directory,
        access: any DirectoryAccess,
        transaction: any TransactionAccess
    ) async throws -> DatabaseTenantDirectories {
        let bases = try await access.openOrCreateDirectory(
            basesDirectoryName,
            in: databaseRoot,
            transaction: transaction
        )
        let partition = try await access.openOrCreatePartition(
            name,
            in: bases,
            transaction: transaction
        )
        return try await openOrCreateTenantChildren(
            of: partition,
            access: access,
            transaction: transaction
        )
    }

    /// Opens `bases/<name>` and its reserved children, creating nothing.
    package static func openBaseTenant(
        _ name: String,
        in databaseRoot: Directory,
        access: any DirectoryAccess,
        transaction: any TransactionReadAccess
    ) async throws -> DatabaseTenantDirectories? {
        guard let bases = try await access.openDirectory(
            basesDirectoryName,
            in: databaseRoot,
            transaction: transaction
        ) else {
            return nil
        }
        guard let partition = try await access.openPartition(
            name,
            in: bases,
            transaction: transaction
        ) else {
            return nil
        }
        return try await openTenantChildren(
            of: partition,
            access: access,
            transaction: transaction
        )
    }

    /// Lists the Base Partitions of a database root. An absent `bases`
    /// Directory means no Base has been provisioned, which is an empty listing
    /// rather than a failure.
    package static func listBaseTenantNames(
        in databaseRoot: Directory,
        after: String?,
        limit: Int,
        access: any DirectoryAccess,
        transaction: any TransactionReadAccess
    ) async throws -> [String] {
        guard let bases = try await access.openDirectory(
            basesDirectoryName,
            in: databaseRoot,
            transaction: transaction
        ) else {
            return []
        }
        let entries = try await access.listChildren(
            in: bases,
            after: after,
            limit: limit,
            transaction: transaction
        )
        // Skipping a child of another layer would report a shorter listing as
        // a complete one and hide a node that occupies a Base address. Every
        // Base is committed as a Partition, so a child of another layer is a
        // corrupted layout the caller must see.
        return try entries.map { entry in
            guard entry.isPartition else {
                throw DatabaseDirectoryLayoutError.nonPartitionBase(
                    name: entry.name
                )
            }
            return entry.name
        }
    }

    /// Empties a Tenant Partition while leaving the Partition node in place.
    ///
    /// Both reserved subtrees are removed recursively and recreated in the same
    /// transaction, so the Tenant is never observable as a corrupt layout. The
    /// Partition itself survives, which is what lets a lifecycle job that has
    /// already destroyed the Tenant's Framework metadata still address it.
    ///
    /// The recreated children receive fresh prefixes; only the Partition
    /// identity is preserved.
    package static func clearTenantContents(
        _ tenant: DatabaseTenantDirectories,
        access: any DirectoryAccess,
        transaction: any TransactionAccess
    ) async throws -> DatabaseTenantDirectories {
        try await access.remove(
            systemDirectoryName,
            in: tenant.partition.root,
            transaction: transaction
        )
        try await access.remove(
            dataDirectoryName,
            in: tenant.partition.root,
            transaction: transaction
        )
        return try await openOrCreateTenantChildren(
            of: tenant.partition,
            access: access,
            transaction: transaction
        )
    }

    /// Removes the Base Partition `bases/<name>` and its whole subtree.
    ///
    /// Only a Partition is admitted at the address. A node stored under
    /// another layer is a structure this layout never committed there, and
    /// removing it would destroy a subtree this call has no contract over, so
    /// the layer mismatch `openPartition` reports propagates unchanged.
    package static func removeBaseTenant(
        _ name: String,
        in databaseRoot: Directory,
        access: any DirectoryAccess,
        transaction: any TransactionAccess
    ) async throws {
        guard let bases = try await access.openDirectory(
            basesDirectoryName,
            in: databaseRoot,
            transaction: transaction
        ) else {
            return
        }
        // Absence is the state this call establishes, so a resumed deletion or
        // move-cleanup slice observing it has succeeded rather than failed.
        guard try await access.openPartition(
            name,
            in: bases,
            transaction: transaction
        ) != nil else {
            return
        }
        try await access.remove(name, in: bases, transaction: transaction)
    }

    // MARK: - Reserved children

    private static func openOrCreateTenantChildren(
        of partition: Partition,
        access: any DirectoryAccess,
        transaction: any TransactionAccess
    ) async throws -> DatabaseTenantDirectories {
        let system = try await access.openOrCreateDirectory(
            systemDirectoryName,
            in: partition.root,
            transaction: transaction
        )
        let framework = try await access.openOrCreateDirectory(
            frameworkDirectoryName,
            in: system,
            transaction: transaction
        )
        let data = try await access.openOrCreateDirectory(
            dataDirectoryName,
            in: partition.root,
            transaction: transaction
        )
        return DatabaseTenantDirectories(
            partition: partition,
            system: framework,
            data: data
        )
    }

    /// A Partition whose reserved children are absent is a corrupt layout, not
    /// an absent Tenant: the three nodes commit atomically with the Partition.
    private static func openTenantChildren(
        of partition: Partition,
        access: any DirectoryAccess,
        transaction: any TransactionReadAccess
    ) async throws -> DatabaseTenantDirectories {
        guard let system = try await access.openDirectory(
            systemDirectoryName,
            in: partition.root,
            transaction: transaction
        ) else {
            throw try await missingReservedChild(
                systemDirectoryName,
                of: partition,
                in: partition.root,
                access: access,
                transaction: transaction
            )
        }
        guard let framework = try await access.openDirectory(
            frameworkDirectoryName,
            in: system,
            transaction: transaction
        ) else {
            throw try await missingReservedChild(
                frameworkDirectoryName,
                of: partition,
                in: system,
                access: access,
                transaction: transaction
            )
        }
        guard let data = try await access.openDirectory(
            dataDirectoryName,
            in: partition.root,
            transaction: transaction
        ) else {
            throw try await missingReservedChild(
                dataDirectoryName,
                of: partition,
                in: partition.root,
                access: access,
                transaction: transaction
            )
        }
        return DatabaseTenantDirectories(
            partition: partition,
            system: framework,
            data: data
        )
    }

    /// Bound on the children reported beside a missing reserved node. The
    /// report separates an unpopulated Partition from one that lost a single
    /// node, which the reserved names already decide; it is not a subtree dump.
    private static let reservedChildReportLimit = 8

    private static func missingReservedChild(
        _ name: String,
        of partition: Partition,
        in parent: Directory,
        access: any DirectoryAccess,
        transaction: any TransactionReadAccess
    ) async throws -> DatabaseDirectoryLayoutError {
        let entries = try await access.listChildren(
            in: parent,
            after: nil,
            limit: reservedChildReportLimit,
            transaction: transaction
        )
        return .missingReservedDirectory(
            partition: partition.address.components,
            name: name,
            presentChildren: entries.map(\.name)
        )
    }
}
