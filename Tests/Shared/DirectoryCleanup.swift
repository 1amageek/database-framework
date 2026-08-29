import DatabaseEngine
import StorageKit

/// Directory path of the database root `DBConfiguration.testing` configures.
///
/// Cleanup helpers resolve the same root the container binds, so a test that
/// purges leftover state addresses exactly the Directories the container owns.
public func testingDatabaseRootPath(
    databaseIdentifier: String? = nil
) -> [String] {
    #if MultiBase
    return ["database", databaseIdentifier ?? "test"]
    #else
    return databaseIdentifier.map { ["test-database", $0] } ?? []
    #endif
}

/// Removes an application Directory below the Tenant the testing
/// configuration binds, and does nothing when any component is absent.
///
/// Absence is the state this call establishes, so a store that was never
/// initialized, a database root that does not exist, and a partially created
/// path all succeed. Every other storage failure propagates.
public func ensureDirectoryRemoved(
    from engine: any StorageEngine,
    databaseIdentifier: String? = nil,
    path: [String]
) async throws {
    guard let name = path.last else { return }
    let parentComponents = Array(path.dropLast())
    let access = engine.directoryAccess
    try await engine.executeTransaction { transaction in
        guard let tenant = try await openTestingTenant(
            access: access,
            databaseIdentifier: databaseIdentifier,
            transaction: transaction
        ) else {
            return
        }
        var parent = tenant.data
        for component in parentComponents {
            guard let next = try await access.open(
                component,
                expecting: nil,
                in: parent,
                transaction: transaction
            ) else {
                return
            }
            parent = next
        }
        // The stored layer tag does not decide whether leftover test state may
        // be removed: whatever occupies the address goes with its subtree.
        guard try await access.open(
            name,
            expecting: nil,
            in: parent,
            transaction: transaction
        ) != nil else {
            return
        }
        try await access.remove(name, in: parent, transaction: transaction)
    }
}

/// Reports whether an application Directory exists below the Tenant the
/// testing configuration binds, creating nothing on the way.
public func applicationDirectoryExists(
    in engine: any StorageEngine,
    databaseIdentifier: String? = nil,
    path: [String]
) async throws -> Bool {
    let access = engine.directoryAccess
    return try await engine.withTransaction { transaction in
        guard let tenant = try await openTestingTenant(
            access: access,
            databaseIdentifier: databaseIdentifier,
            transaction: transaction
        ) else {
            return false
        }
        var current = tenant.data
        for component in path {
            guard let next = try await access.open(
                component,
                expecting: nil,
                in: current,
                transaction: transaction
            ) else {
                return false
            }
            current = next
        }
        return true
    }
}

/// Removes every Tenant of the testing database root, leaving the root itself.
///
/// This is the cleanup a scenario needs when it must also discard Framework
/// metadata, which lives below `system/database-framework` of each Tenant and
/// is not reachable from an application Directory path.
public func ensureTestingDatabaseCleared(
    from engine: any StorageEngine,
    databaseIdentifier: String? = nil
) async throws {
    let access = engine.directoryAccess
    let rootPath = testingDatabaseRootPath(databaseIdentifier: databaseIdentifier)
    try await engine.executeTransaction { transaction in
        guard let root = try await DatabaseDirectoryLayout.openDatabaseRoot(
            path: rootPath,
            access: access,
            transaction: transaction
        ) else {
            return
        }
        for name in [
            DatabaseDirectoryLayout.defaultPartitionName,
            DatabaseDirectoryLayout.basesDirectoryName,
        ] {
            guard try await access.open(
                name,
                expecting: nil,
                in: root,
                transaction: transaction
            ) != nil else {
                continue
            }
            try await access.remove(name, in: root, transaction: transaction)
        }
    }
}

/// Opens the Tenant Partition the testing configuration binds application data
/// to, creating nothing. `nil` means the store, the database root, or the
/// Tenant does not exist yet.
private func openTestingTenant(
    access: any DirectoryAccess,
    databaseIdentifier: String?,
    transaction: any TransactionReadAccess
) async throws -> DatabaseTenantDirectories? {
    guard let root = try await DatabaseDirectoryLayout.openDatabaseRoot(
        path: testingDatabaseRootPath(databaseIdentifier: databaseIdentifier),
        access: access,
        transaction: transaction
    ) else {
        return nil
    }
    #if MultiBase
    return try await DatabaseDirectoryLayout.openBaseTenant(
        TestBaseEnvironment.name,
        in: root,
        access: access,
        transaction: transaction
    )
    #else
    return try await DatabaseDirectoryLayout.openDefaultTenant(
        in: root,
        access: access,
        transaction: transaction
    )
    #endif
}
