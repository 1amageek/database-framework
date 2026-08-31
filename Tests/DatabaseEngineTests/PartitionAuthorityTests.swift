import DatabaseKit
import DatabaseRuntime
import DatabaseTypes
import StorageKit
import TestSupport
import Testing
@_spi(DatabaseExecution) @_spi(Testing) @testable import DatabaseEngine

@Persistable
private struct PartitionAuthorityProbeEntity {
    #Directory<PartitionAuthorityProbeEntity>("partition-authority", "entities")

    var id: String = ""
    var value: Int32 = 0
}

/// Structural preconditions of the probe itself, kept distinct from the
/// storage failures the cases assert so a broken fixture never reads as a
/// passing exclusion.
private enum PartitionAuthorityProbeFailure: Error {
    case databaseRootMissing
    case tenantParentMissing
}

/// A resolved address names where a Tenant Partition lives; it does not
/// entitle the operation that resolved it to read or write there. Authority is
/// the StorageKit `PartitionLease` a transaction attempt takes on the Partition
/// it selected.
///
/// Holding a lease does not stop anyone from removing that Partition -- whether
/// a removal is admissible is a database-operation decision made above
/// StorageKit. What a lease guarantees is that the operation holding it fails
/// instead of landing somewhere else, because the attempt resolves the
/// Partition in its own transaction: a removed Partition is refused as
/// `staleLease`, and a removal that commits concurrently conflicts with the
/// attempt that read the node.
///
/// These cases are built for both trait configurations deliberately. The trait
/// selects which Partition an operation leases -- the Default Partition without
/// `MultiBase`, `bases/<Base.ID>` with it -- while the assertions below are the
/// same in both, which is what makes the exclusion a property of the kernel
/// rather than of one build.
@Suite("Partition authority", .serialized)
struct PartitionAuthorityTests {
    @Test("A Partition removed under a live lease fails that operation instead of taking its write")
    func concurrentTenantPartitionRemovalFailsTheLeasingOperation() async throws {
        let engine = InMemoryEngine()
        let container = try await Self.makeContainer(engine: engine)
        let context = container.testBaseContext()
        try context.insert(PartitionAuthorityProbeEntity(id: "one", value: 1))
        try await context.save()

        // The removal runs in its own transaction on the same storage domain
        // and is admitted: a lease does not exclude it. The write staged
        // before it must not survive that removal, because the attempt read
        // the Partition node when it took its lease, so the removal conflicts
        // with this attempt and the replay finds no Partition to lease.
        let raised = await #expect(throws: StorageError.self) {
            try await context.withTransaction { transaction in
                try await transaction.save(
                    PartitionAuthorityProbeEntity(id: "two", value: 2),
                    precondition: .notExists
                )
                try await Self.removeTenantPartition(from: engine)
            }
        }
        let stale = try #require(raised)
        #expect(stale.code == .staleLease)
        // A replay would re-resolve the same absent Partition, so the runner
        // must propagate this instead of retrying it.
        #expect(stale.retryDisposition == .never)

        await container.shutdown()
    }

    #if !MultiBase
    @Test("A Partition recreated at the Tenant address rejects the retained handle")
    func recreatedTenantPartitionRejectsRetainedHandle() async throws {
        let engine = InMemoryEngine()
        let container = try await Self.makeContainer(engine: engine)
        let context = container.testBaseContext()
        try context.insert(PartitionAuthorityProbeEntity(id: "one", value: 1))
        try await context.save()

        // The container retains the Default Partition for its whole lifetime,
        // so the handle now names an address whose keyspace belongs to a
        // different Partition.
        try await Self.removeTenantPartition(from: engine)
        try await Self.createDefaultTenantPartition(in: engine)

        let raised = await #expect(throws: StorageError.self) {
            try await context.withTransaction { _ in }
        }
        let stale = try #require(raised)
        #expect(stale.code == .staleLease)
        #expect(stale.retryDisposition == .never)

        await container.shutdown()
    }
    #endif

    // MARK: - Helpers

    /// Removes the Tenant Partition this container's data operations address.
    ///
    /// The trait selects the address, not the shape of the call.
    private static func removeTenantPartition(
        from engine: InMemoryEngine
    ) async throws {
        let access = engine.directoryAccess
        try await engine.withTransaction { transaction in
            guard let root = try await DatabaseDirectoryLayout.openDatabaseRoot(
                path: testingDatabaseRootPath(),
                access: access,
                transaction: transaction
            ) else {
                throw PartitionAuthorityProbeFailure.databaseRootMissing
            }
            #if MultiBase
            guard let bases = try await access.openDirectory(
                DatabaseDirectoryLayout.basesDirectoryName,
                in: root,
                transaction: transaction
            ) else {
                throw PartitionAuthorityProbeFailure.tenantParentMissing
            }
            try await access.remove(
                TestBaseEnvironment.name,
                in: bases,
                transaction: transaction
            )
            #else
            try await access.remove(
                DatabaseDirectoryLayout.defaultPartitionName,
                in: root,
                transaction: transaction
            )
            #endif
        }
    }

    #if !MultiBase
    /// Puts a fresh Partition at the Default Partition's address. It receives a
    /// new prefix, so a retained handle for the removed Partition is stale
    /// rather than absent.
    private static func createDefaultTenantPartition(
        in engine: InMemoryEngine
    ) async throws {
        let access = engine.directoryAccess
        try await engine.withTransaction { transaction in
            let root = try await DatabaseDirectoryLayout
                .openOrInitializeDatabaseRoot(
                    path: testingDatabaseRootPath(),
                    access: access,
                    transaction: transaction
                )
            _ = try await access.openOrCreatePartition(
                DatabaseDirectoryLayout.defaultPartitionName,
                in: root,
                transaction: transaction
            )
        }
    }
    #endif

    private static func makeContainer(
        engine: InMemoryEngine
    ) async throws -> DBContainer {
        let schema = try Schema(
            entities: [try PartitionAuthorityProbeEntity.schemaEntity],
            version: Schema.Version(1, 0, 0)
        )
        return try await DBContainer.open(
            testing: schema,
            configuration: .testing(storageEngine: engine),
            runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(
                executionIdentity: DatabaseExecutionRuntimeIdentity(
                    identifier: "database-tests",
                    revision: 1
                ),
                entityRuntimes: [
                    try DatabaseFrameworkRuntime.entity(
                        PartitionAuthorityProbeEntity.self
                    )
                ]
            ),
            security: .testingDisabled
        )
    }
}
