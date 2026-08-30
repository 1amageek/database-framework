#if !MultiBase
import DatabaseKit
import DatabaseRuntime
import DatabaseTypes
import StorageKit
import TestSupport
import Testing
@_spi(DatabaseExecution) @_spi(Testing) @testable import DatabaseEngine

@Persistable
private struct LayerRetirementProbeEntity {
    #Directory<LayerRetirementProbeEntity>(
        "layer-retirement",
        \LayerRetirementProbeEntity.tenantID,
        "entities",
        layer: .partition
    )

    var id: String = ""
    var tenantID: String = ""
    var value: Int32 = 0
}

@Persistable
private struct LayerCursorProbeEntityA {
    #Directory<LayerCursorProbeEntityA>(
        "layer-cursor-a",
        \LayerCursorProbeEntityA.tenantID,
        "entities"
    )

    var id: String = ""
    var tenantID: String = ""
}

@Persistable
private struct LayerCursorProbeEntityB {
    #Directory<LayerCursorProbeEntityB>(
        "layer-cursor-b",
        \LayerCursorProbeEntityB.tenantID,
        "entities"
    )

    var id: String = ""
    var tenantID: String = ""
}

/// A node's layer is part of its identity, so every position this framework
/// addresses must be opened under the layer its declaration assigned.
///
/// The cases below cover the three places where the address outlives the
/// declaration that issued it: durable index retirement work, which runs
/// against a path the published schema may no longer declare; the reserved
/// `bases` Directory, whose children are Base Partitions; and a Partition
/// enumeration cursor, which is handed back by a caller and must not be
/// accepted by a walk that never issued it.
///
/// The whole file is built for the standard configuration. `DatabaseDirectory-
/// Layout` is not conditionally compiled, so its evidence does not need to be
/// produced twice, and the container cases resolve the Default Partition's
/// data root, which MultiBase rebinds per Base.
@Suite("Directory layer identity", .serialized)
struct DirectoryLayerIdentityTests {
    private static let authorization = AuthorizationContext.authenticated(
        Principal(identifier: "directory-layer-identity")
    )

    private static let tenant = "tenant-a"

    // MARK: - Durable index retirement

    @Test("A staged retirement records the layer of every source component")
    func stagedRetirementRecordsSourceLayers() async throws {
        let engine = InMemoryEngine()
        let schema = try Self.schema()
        let container = try await Self.makeContainer(
            engine: engine,
            schema: schema
        )

        let entity = try #require(
            schema.entity(named: LayerRetirementProbeEntity.persistableType)
        )
        try await Self.stage(
            Self.retirement(for: entity),
            schema: schema,
            in: container
        )

        let pending = try await Self.pending(schema: schema, in: container)
        let record = try #require(pending.first)
        #expect(pending.count == 1)
        // The declaration types `entities` as a Partition and everything above
        // it as a plain Directory.
        #expect(record.directoryLayers == [.default, .default, .partition])

        await container.shutdown()
    }

    @Test("A retirement is refused when its source node carries another layer")
    func retirementRefusesRelayeredSourceNode() async throws {
        let engine = InMemoryEngine()
        let schema = try Self.schema()
        let container = try await Self.makeContainer(
            engine: engine,
            schema: schema
        )
        try await Self.writeProbe(to: container)

        let entity = try #require(
            schema.entity(named: LayerRetirementProbeEntity.persistableType)
        )
        try await Self.stage(
            Self.retirement(for: entity),
            schema: schema,
            in: container
        )
        let record = try #require(
            try await Self.pending(schema: schema, in: container).first
        )

        // The write created `entities` as the Partition the declaration
        // assigns. Replacing it with a plain Directory leaves a node at the
        // same address holding storage this record never described.
        try await Self.replaceLeafWithPlainDirectory(in: engine)

        await #expect(throws: StorageError.self) {
            try await container.withControlTransaction(
                authorization: Self.authorization
            ) { transaction in
                try await container.retireSchemaIndexStorage(
                    record,
                    partitions: try Self.probePartitions(),
                    transaction: transaction.executionStorageAccess
                )
            }
        }

        await container.shutdown()
    }

    @Test("A retirement the schema cannot type records no layer and still runs")
    func retirementOfUndeclaredScopeRecordsNoLayers() async throws {
        let engine = InMemoryEngine()
        let schema = try Self.schema()
        let container = try await Self.makeContainer(
            engine: engine,
            schema: schema
        )

        // No generation of this schema declares `removed-entity`, so there is
        // no declaration to take a layer from. The marker records none rather
        // than inventing one, which is exactly the state a marker written
        // before layers were recorded is read back in.
        let scope = DatabaseIndexStorageScope.entity(
            name: "removed-entity",
            directoryComponents: [
                .staticPath("layer-removed"),
                .staticPath("entities"),
            ]
        )
        try await Self.stage(
            DatabasePendingIndexRetirement(
                scope: scope,
                identity: try Self.identity()
            ),
            schema: schema,
            in: container
        )

        let record = try #require(
            try await Self.pending(schema: schema, in: container).first
        )
        #expect(record.directoryLayers == nil)

        // Its path was never created, so the unverified open reports absence
        // and the marker is cleared without touching storage.
        try await container.withControlTransaction(
            authorization: Self.authorization
        ) { transaction in
            try await container.retireSchemaIndexStorage(
                record,
                partitions: FieldObject(),
                transaction: transaction.executionStorageAccess
            )
            try container.completeSchemaIndexRetirement(
                record,
                transaction: transaction.executionStorageAccess
            )
        }
        #expect(try await Self.pending(schema: schema, in: container).isEmpty)

        await container.shutdown()
    }

    // MARK: - Partition enumeration cursor

    @Test("A partition cursor is refused by a declaration that did not issue it")
    func partitionCursorIsBoundToItsDeclaration() async throws {
        let engine = InMemoryEngine()
        let schema = try Self.schema()
        let container = try await Self.makeContainer(
            engine: engine,
            schema: schema
        )

        let context = container.newContext(authorization: Self.authorization)
        for tenant in ["tenant-1", "tenant-2"] {
            var model = LayerCursorProbeEntityA()
            model.id = tenant
            model.tenantID = tenant
            try context.insert(model)
        }
        var other = LayerCursorProbeEntityB()
        other.id = "tenant-1"
        other.tenantID = "tenant-1"
        try context.insert(other)
        try await context.save()

        let first = try await Self.partitionPage(
            entity: LayerCursorProbeEntityA.persistableType,
            continuation: nil,
            in: container
        )
        #expect(first.entries.count == 1)
        let cursor = try #require(first.continuation)

        // Both declarations have three components in the same shape, so a
        // cursor that only typed its encoding would resume the other walk at a
        // position it never described.
        await #expect(throws: DatabasePartitionCatalogError.invalidContinuation) {
            _ = try await Self.partitionPage(
                entity: LayerCursorProbeEntityB.persistableType,
                continuation: cursor,
                in: container
            )
        }

        let second = try await Self.partitionPage(
            entity: LayerCursorProbeEntityA.persistableType,
            continuation: cursor,
            in: container
        )
        #expect(second.entries.count == 1)
        let visited = (first.entries + second.entries).compactMap { item in
            item.partitions.fields.first { $0.key == "tenantID" }?.value
        }
        #expect(visited == [.string("tenant-1"), .string("tenant-2")])

        let third = try await Self.partitionPage(
            entity: LayerCursorProbeEntityA.persistableType,
            continuation: try #require(second.continuation),
            in: container
        )
        #expect(third.entries.isEmpty)
        #expect(third.continuation == nil)

        await container.shutdown()
    }

    // MARK: - Base addresses

    @Test("Listing Bases reports a child stored under another layer")
    func listingBasesRejectsNonPartitionChild() async throws {
        let engine = InMemoryEngine()
        let access = engine.directoryAccess

        try await Self.withDatabaseRoot(of: engine) { root, transaction in
            _ = try await DatabaseDirectoryLayout.openOrCreateBaseTenant(
                "base-a",
                in: root,
                access: access,
                transaction: transaction
            )
        }
        try await Self.withDatabaseRoot(of: engine) { root, transaction in
            let names = try await DatabaseDirectoryLayout.listBaseTenantNames(
                in: root,
                after: nil,
                limit: 16,
                access: access,
                transaction: transaction
            )
            #expect(names == ["base-a"])
        }

        try await Self.withDatabaseRoot(of: engine) { root, transaction in
            let bases = try #require(
                try await access.openDirectory(
                    DatabaseDirectoryLayout.basesDirectoryName,
                    in: root,
                    transaction: transaction
                )
            )
            _ = try await access.openOrCreateDirectory(
                "intruder",
                in: bases,
                transaction: transaction
            )
        }

        // A child of another layer occupies a Base address without being one,
        // so reporting the shorter listing would present a partial view as a
        // complete one.
        try await Self.withDatabaseRoot(of: engine) { root, transaction in
            await #expect(
                throws: DatabaseDirectoryLayoutError.nonPartitionBase(
                    name: "intruder"
                )
            ) {
                _ = try await DatabaseDirectoryLayout.listBaseTenantNames(
                    in: root,
                    after: nil,
                    limit: 16,
                    access: access,
                    transaction: transaction
                )
            }
        }

        engine.requestShutdown()
        await engine.waitUntilShutdown()
    }

    @Test("Removing a Base refuses a node stored under another layer")
    func removingBaseRejectsNonPartitionNode() async throws {
        let engine = InMemoryEngine()
        let access = engine.directoryAccess

        try await Self.withDatabaseRoot(of: engine) { root, transaction in
            _ = try await DatabaseDirectoryLayout.openOrCreateBaseTenant(
                "base-a",
                in: root,
                access: access,
                transaction: transaction
            )
            let bases = try #require(
                try await access.openDirectory(
                    DatabaseDirectoryLayout.basesDirectoryName,
                    in: root,
                    transaction: transaction
                )
            )
            _ = try await access.openOrCreateDirectory(
                "intruder",
                in: bases,
                transaction: transaction
            )
        }

        // Removing the node would destroy a subtree this layout never
        // committed there, so the mismatch stops the deletion.
        try await Self.withDatabaseRoot(of: engine) { root, transaction in
            await #expect(throws: StorageError.self) {
                try await DatabaseDirectoryLayout.removeBaseTenant(
                    "intruder",
                    in: root,
                    access: access,
                    transaction: transaction
                )
            }
        }
        #expect(try await Self.baseChildNames(of: engine) == ["base-a", "intruder"])

        // Absence is the state the call establishes, and a Base Partition is
        // still removed.
        try await Self.withDatabaseRoot(of: engine) { root, transaction in
            try await DatabaseDirectoryLayout.removeBaseTenant(
                "absent",
                in: root,
                access: access,
                transaction: transaction
            )
            try await DatabaseDirectoryLayout.removeBaseTenant(
                "base-a",
                in: root,
                access: access,
                transaction: transaction
            )
        }
        #expect(try await Self.baseChildNames(of: engine) == ["intruder"])

        engine.requestShutdown()
        await engine.waitUntilShutdown()
    }

    // MARK: - Helpers

    private static func withDatabaseRoot(
        of engine: InMemoryEngine,
        _ body: @escaping @Sendable (
            Directory,
            any TransactionAccess
        ) async throws -> Void
    ) async throws {
        let access = engine.directoryAccess
        try await engine.withTransaction { transaction in
            let root = try await DatabaseDirectoryLayout
                .openOrInitializeDatabaseRoot(
                    path: [],
                    access: access,
                    transaction: transaction
                )
            try await body(root, transaction)
        }
    }

    private static func baseChildNames(
        of engine: InMemoryEngine
    ) async throws -> [String] {
        let access = engine.directoryAccess
        return try await engine.withTransaction { transaction in
            guard let root = try await access.openRoot(
                transaction: transaction
            ),
            let bases = try await access.openDirectory(
                DatabaseDirectoryLayout.basesDirectoryName,
                in: root,
                transaction: transaction
            ) else {
                return [String]()
            }
            return try await access.listChildren(
                in: bases,
                after: nil,
                limit: 16,
                transaction: transaction
            ).map(\.name)
        }
    }

    private static func retirement(
        for entity: Schema.Entity
    ) throws -> DatabasePendingIndexRetirement {
        DatabasePendingIndexRetirement(
            scope: .entity(
                name: entity.name,
                directoryComponents: entity.directoryComponents
            ),
            identity: try identity()
        )
    }

    /// A generation no declaration of the test schema owns, so staging retains
    /// it instead of recognizing it as active work.
    private static func identity() throws -> DatabaseIndexStorageIdentity {
        try DatabaseIndexStorageIdentity(
            name: "retired-index",
            definitionFingerprint: try SchemaFingerprint(
                ByteString(
                    repeating: 0x11,
                    count: SchemaFingerprint.byteCount
                )
            ),
            layoutFingerprint: ByteString(
                repeating: 0x22,
                count: SHA256Accumulator.digestByteCount
            )
        )
    }

    private static func probePartitions() throws -> FieldObject {
        try FieldObject([(key: "tenantID", value: .string(tenant))])
    }

    private static func stage(
        _ retirement: DatabasePendingIndexRetirement,
        schema: Schema,
        in container: DBContainer
    ) async throws {
        try await container.withControlTransaction(
            authorization: authorization
        ) { transaction in
            try await container.stageSchemaIndexRetirements(
                [retirement],
                validFor: schema,
                indexPhysicalLayouts: container.indexPhysicalLayouts,
                transaction: transaction.executionStorageAccess
            )
        }
    }

    private static func pending(
        schema: Schema,
        in container: DBContainer
    ) async throws -> [DatabasePendingIndexRetirement] {
        try await container.withControlTransaction(
            authorization: authorization
        ) { transaction in
            try await container.pendingSchemaIndexRetirements(
                validFor: schema,
                transaction: transaction.executionStorageAccess
            )
        }
    }

    private static func partitionPage(
        entity: String,
        continuation: ByteString?,
        in container: DBContainer
    ) async throws -> DatabasePartitionCatalogPage {
        try await container.withControlTransaction(
            authorization: authorization
        ) { transaction in
            try await container.executionPartitionCatalogPage(
                entity: entity,
                continuation: continuation,
                limit: 1,
                transaction: transaction.executionStorageAccess
            )
        }
    }

    private static func writeProbe(to container: DBContainer) async throws {
        let context = container.newContext(authorization: authorization)
        var model = LayerRetirementProbeEntity()
        model.id = "probe"
        model.tenantID = tenant
        model.value = 1
        try context.insert(model)
        try await context.save()
    }

    /// Removes the Partition the declaration assigned to `entities` and puts a
    /// plain Directory at the same address.
    private static func replaceLeafWithPlainDirectory(
        in engine: InMemoryEngine
    ) async throws {
        let access = engine.directoryAccess
        try await engine.withTransaction { transaction in
            let storeRoot = try #require(
                try await access.openRoot(transaction: transaction)
            )
            let partition = try #require(
                try await access.openPartition(
                    DatabaseDirectoryLayout.defaultPartitionName,
                    in: storeRoot,
                    transaction: transaction
                )
            )
            let data = try #require(
                try await access.openDirectory(
                    DatabaseDirectoryLayout.dataDirectoryName,
                    in: partition.root,
                    transaction: transaction
                )
            )
            let declared = try #require(
                try await access.openDirectory(
                    "layer-retirement",
                    in: data,
                    transaction: transaction
                )
            )
            let children = try await access.listChildren(
                in: declared,
                after: nil,
                limit: 16,
                transaction: transaction
            )
            let tenantName = try #require(children.first?.name)
            #expect(children.count == 1)
            let tenantDirectory = try #require(
                try await access.openDirectory(
                    tenantName,
                    in: declared,
                    transaction: transaction
                )
            )
            let leaves = try await access.listChildren(
                in: tenantDirectory,
                after: nil,
                limit: 16,
                transaction: transaction
            )
            #expect(leaves.map(\.isPartition) == [true])
            try await access.remove(
                "entities",
                in: tenantDirectory,
                transaction: transaction
            )
            _ = try await access.openOrCreateDirectory(
                "entities",
                in: tenantDirectory,
                transaction: transaction
            )
        }
    }

    private static func makeContainer(
        engine: any StorageEngine,
        schema: Schema
    ) async throws -> DBContainer {
        try await DBContainer.open(
            for: schema,
            configuration: DBConfiguration(
                storageEngine: engine,
                monotonicClock: TestProcessMonotonicClock(),
                wallClock: FixedTestWallClock()
            ),
            runtimeConfiguration: try runtimeConfiguration(),
            security: .testingDisabled
        )
    }

    private static func schema() throws -> Schema {
        try Schema(
            entities: [
                try LayerRetirementProbeEntity.schemaEntity,
                try LayerCursorProbeEntityA.schemaEntity,
                try LayerCursorProbeEntityB.schemaEntity,
            ],
            version: Schema.Version(1, 0, 0)
        )
    }

    private static func runtimeConfiguration() throws
        -> DatabaseRuntimeConfiguration {
        try DatabaseFrameworkRuntime.configuration(
            executionIdentity: DatabaseExecutionRuntimeIdentity(
                identifier: "database-tests",
                revision: 1
            ),
            entityRuntimes: [
                try DatabaseFrameworkRuntime.entity(
                    LayerRetirementProbeEntity.self
                ),
                try DatabaseFrameworkRuntime.entity(
                    LayerCursorProbeEntityA.self
                ),
                try DatabaseFrameworkRuntime.entity(
                    LayerCursorProbeEntityB.self
                ),
            ]
        )
    }
}
#endif
