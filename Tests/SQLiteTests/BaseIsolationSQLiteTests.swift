#if SQLITE && MultiBase
import Database
@_spi(DatabaseExecution) import DatabaseEngine
import DatabaseRuntime
import TestHeartbeat
import TestSupport
import Testing

@Persistable
private struct BaseIsolationDocument {
    #Directory<BaseIsolationDocument>("base-isolation", "documents")

    var id: String = ""
    var value: String = ""
}

private enum BaseIsolationSchemaV1: VersionedSchema {
    static let versionIdentifier = Schema.Version(1, 0, 0)
    static var entities: [Schema.Entity] {
        get throws(SchemaEntityError) {
            [try BaseIsolationDocument.schemaEntity]
        }
    }
}

private enum BaseIsolationSchemaV2: VersionedSchema {
    static let versionIdentifier = Schema.Version(2, 0, 0)
    static var entities: [Schema.Entity] {
        get throws(SchemaEntityError) {
            [try BaseIsolationDocument.schemaEntity]
        }
    }
}

private enum BaseIsolationMigrationPlan: SchemaMigrationPlan {
    static var schemas: [any VersionedSchema.Type] {
        [BaseIsolationSchemaV1.self, BaseIsolationSchemaV2.self]
    }

    static var stages: [MigrationStage] {
        [
            .lightweight(
                fromVersion: BaseIsolationSchemaV1.self,
                toVersion: BaseIsolationSchemaV2.self
            )
        ]
    }
}

private enum BaseIsolationOperationOwnershipError: Error {
    case leaseMismatch
}

@Suite("Base isolation and persisted Grants", .heartbeat)
struct BaseIsolationSQLiteTests {
    @Test("Composition reads across two SQLite storage domains")
    func compositionReadsAcrossTwoSQLiteDomains() async throws {
        let controlDomainID = try DatabaseStorageDomain.ID("sqlite-control")
        let secondaryDomainID = try DatabaseStorageDomain.ID("sqlite-secondary")
        let primaryPlacementID = try Base.Placement.ID("sqlite-primary")
        let secondaryPlacementID = try Base.Placement.ID("sqlite-secondary")
        let topology = try DatabaseStorageTopology(
            controlDomainID: controlDomainID,
            domains: [
                try DatabaseStorageDomain(
                    id: controlDomainID,
                    namespacePath: ["sqlite", "control"],
                    storageEngine: try SQLiteStorageEngine(
                        configuration: .inMemory
                    )
                ),
                try DatabaseStorageDomain(
                    id: secondaryDomainID,
                    namespacePath: ["sqlite", "secondary"],
                    storageEngine: try SQLiteStorageEngine(
                        configuration: .inMemory
                    )
                ),
            ],
            placements: [
                try DatabaseStoragePlacement(
                    id: primaryPlacementID,
                    domainID: controlDomainID,
                    path: ["bases"]
                ),
                try DatabaseStoragePlacement(
                    id: secondaryPlacementID,
                    domainID: secondaryDomainID,
                    path: ["bases"]
                ),
            ],
            defaultPlacementID: primaryPlacementID
        )
        let container = try await DBContainer.open(
            for: try Schema(
                entities: [try BaseIsolationDocument.schemaEntity]
            ),
            configuration: DBConfiguration(
                name: "sqlite-cross-domain",
                storageTopology: topology,
                monotonicClock: TestProcessMonotonicClock(),
                wallClock: FixedTestWallClock()
            ),
            runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(
                executionIdentity: DatabaseExecutionRuntimeIdentity(
                    identifier: "database-tests",
                    revision: 1
                ),
                entityRuntimes: [
                    try DatabaseFrameworkRuntime.entity(
                        BaseIsolationDocument.self
                    )
                ]
            ),
            security: .testingDisabled
        )
        defer { await container.shutdown() }
        let primaryBaseID = try Base.ID("sqlite-a")
        let secondaryBaseID = try Base.ID("sqlite-b")
        for (baseID, placementID) in [
            (primaryBaseID, primaryPlacementID),
            (secondaryBaseID, secondaryPlacementID),
        ] {
            _ = try await container.provisionBase(
                baseID,
                placementID: placementID,
                initialGrants: [
                    Security.Grant(
                        subject: .principal("test-runner"),
                        resource: .base(baseID),
                        access: .all
                    )
                ],
                expectedRevision: 0
            )
        }
        let compositionID = try Base.Composition.ID("sqlite-shared")
        _ = try await container.withControlMetadataTransaction { transaction in
            try await container.compositionCatalog.create(
                try Base.Composition(
                    id: compositionID,
                    bases: [primaryBaseID, secondaryBaseID]
                ),
                expectedRevision: 0,
                transaction: transaction.storageAccess
            )
        }
        for (baseID, value) in [
            (primaryBaseID, "primary"),
            (secondaryBaseID, "secondary"),
        ] {
            let context = container.session(
                authorization: TestBaseEnvironment.authorization
            ).base(baseID).newContext()
            try context.insert(
                BaseIsolationDocument(id: "same", value: value)
            )
            try await context.save()
        }

        let secondaryContext = container.session(
            authorization: TestBaseEnvironment.authorization
        ).base(secondaryBaseID).newContext()
        await #expect(
            throws: DatabaseControlMetadataTransactionError
                .storageDomainMismatch
        ) {
            try await secondaryContext
                .withExecutionDataAndControlMetadataTransaction(
                    requiredAccess: .administer
                ) { _, _ in () }
        }

        let results = try await container.session(
            authorization: TestBaseEnvironment.authorization
        ).composition(compositionID)
            .query(BaseIsolationDocument.self)
            .execute()

        #expect(Set(results.map(\.value.value)) == ["primary", "secondary"])
        #expect(Set(results.map(\.origin)).count == 2)
    }

    @Test("The same entity identity is isolated by Base root")
    func sameIdentityIsIsolatedByBase() async throws {
        let container = try await makeContainer()
        defer { await container.shutdown() }
        let otherBaseID = try Base.ID("other")
        _ = try await provision(
            otherBaseID,
            grants: [
                Security.Grant(
                    subject: .principal("test-runner"),
                    resource: .base(otherBaseID),
                    access: .all
                )
            ],
            in: container
        )

        let primary = container.testBaseContext()
        let other = container.session(
            authorization: TestBaseEnvironment.authorization
        ).base(otherBaseID).newContext()
        try primary.insert(
            BaseIsolationDocument(id: "same", value: "primary")
        )
        try other.insert(
            BaseIsolationDocument(id: "same", value: "other")
        )
        try await primary.save()
        try await other.save()

        let controlMarkerKey = container.controlStorage().root
            .subspace("base-isolation-tests")
            .pack(Tuple("atomic-control-marker"))
        try await primary.withExecutionDataAndControlMetadataTransaction(
            requiredAccess: .administer
        ) { transaction, controlMetadata in
            try await transaction.save(
                BaseIsolationDocument(id: "atomic", value: "data"),
                precondition: .notExists
            )
            try controlMetadata.setValue(
                ByteString(utf8: "control"),
                for: controlMarkerKey
            )
        }

        let controlMarker = try await container.withControlMetadataTransaction(
            configuration: .readOnly
        ) { transaction in
            try await transaction.executionStorageAccess.getValue(
                for: controlMarkerKey,
                snapshot: true
            )
        }
        #expect(controlMarker == ByteString(utf8: "control"))
        #expect(
            try await primary.model(
                for: "atomic",
                as: BaseIsolationDocument.self
            )?.value == "data"
        )

        let ownerMarkerKey = container.controlStorage().root
            .subspace("base-isolation-tests")
            .pack(Tuple("operation-owner"))
        try await container.withControlMetadataTransaction { transaction in
            try transaction.executionStorageAccess.setValue(
                ByteString(utf8: "leased"),
                for: ownerMarkerKey
            )
        }
        let operationOwned = container.session(
            authorization: .anonymous
        ).base(try TestBaseEnvironment.id()).newContext()
        await #expect(throws: BaseIsolationOperationOwnershipError.leaseMismatch) {
            try await operationOwned
                .withExecutionOperationOwnedDataAndControlMetadataTransaction(
                    validateOwnership: { _ in
                        throw BaseIsolationOperationOwnershipError.leaseMismatch
                    }
                ) { _, transaction, _ in
                    try await transaction.save(
                        BaseIsolationDocument(
                            id: "invalid-operation-owner",
                            value: "must-not-commit"
                        ),
                        precondition: .notExists
                    )
                }
        }
        #expect(
            try await primary.model(
                for: "invalid-operation-owner",
                as: BaseIsolationDocument.self
            ) == nil
        )
        try await operationOwned
            .withExecutionOperationOwnedDataAndControlMetadataTransaction(
                validateOwnership: { controlMetadata in
                    guard try await controlMetadata.getValue(
                        for: ownerMarkerKey,
                        snapshot: false
                    ) == ByteString(utf8: "leased") else {
                        throw BaseIsolationOperationOwnershipError.leaseMismatch
                    }
                }
            ) { _, transaction, controlMetadata in
                try await transaction.save(
                    BaseIsolationDocument(
                        id: "operation-owned",
                        value: "committed"
                    ),
                    precondition: .notExists
                )
                try controlMetadata.setValue(
                    ByteString(utf8: "completed"),
                    for: ownerMarkerKey
                )
            }
        #expect(
            try await primary.model(
                for: "operation-owned",
                as: BaseIsolationDocument.self
            )?.value == "committed"
        )

        let primaryValue = try await primary.model(
            for: "same",
            as: BaseIsolationDocument.self
        )
        let otherValue = try await other.model(
            for: "same",
            as: BaseIsolationDocument.self
        )
        #expect(primaryValue?.value == "primary")
        #expect(otherValue?.value == "other")
    }

    @Test("Principal and role Grants union independent access bits")
    func principalAndRoleGrantsUnion() async throws {
        let container = try await makeContainer()
        defer { await container.shutdown() }
        let baseID = try Base.ID("grant-union")
        _ = try await provision(
            baseID,
            grants: [
                Security.Grant(
                    subject: .principalRole("reader"),
                    resource: .base(baseID),
                    access: .read
                ),
                Security.Grant(
                    subject: .principal("alice"),
                    resource: .base(baseID),
                    access: [.write, .administer]
                ),
            ],
            in: container
        )
        let alice = AuthorizationContext.authenticated(
            Principal(identifier: "alice", roles: ["reader"])
        )
        let bob = AuthorizationContext.authenticated(
            Principal(identifier: "bob", roles: ["reader"])
        )
        let aliceContext = container.session(authorization: alice)
            .base(baseID).newContext()
        let bobContext = container.session(authorization: bob)
            .base(baseID).newContext()

        try aliceContext.insert(
            BaseIsolationDocument(id: "alice", value: "visible")
        )
        try await aliceContext.save()
        let visible = try await aliceContext.model(
            for: "alice",
            as: BaseIsolationDocument.self
        )
        #expect(visible?.value == "visible")

        try bobContext.insert(
            BaseIsolationDocument(id: "bob", value: "denied")
        )
        await #expect(throws: DatabaseGrantAuthorizationError.self) {
            try await bobContext.save()
        }
    }

    @Test("Schema migration metadata is isolated by Base root")
    func schemaMigrationMetadataIsIsolatedByBase() async throws {
        let container = try await makeMigrationContainer()
        defer { await container.shutdown() }
        let otherBaseID = try Base.ID("migration-other")
        _ = try await provision(
            otherBaseID,
            grants: [
                Security.Grant(
                    subject: .principal("test-runner"),
                    resource: .base(otherBaseID),
                    access: .all
                )
            ],
            in: container
        )
        let otherAdmin = container.session(
            authorization: TestBaseEnvironment.authorization
        ).base(otherBaseID).admin()

        try await container.installTestBaseSchemaSnapshot(
            for: BaseIsolationSchemaV1.versionIdentifier
        )
        try await container.installTestBaseSchemaSnapshot(
            for: BaseIsolationSchemaV2.versionIdentifier,
            baseID: otherBaseID
        )

        #expect(
            try await container.testBaseAdmin().migrationStatus()
                .currentVersion == BaseIsolationSchemaV1.versionIdentifier
        )
        #expect(
            try await otherAdmin.migrationStatus().currentVersion
                == BaseIsolationSchemaV2.versionIdentifier
        )

        try await container.overwriteTestBaseSchemaFingerprint(
            ByteString(repeating: 0, count: 32),
            baseID: otherBaseID
        )
        try await container.testBaseAdmin().migrateIfNeeded()

        await #expect(
            throws: DatabaseMigrationAdmissionError.migrationRequired
        ) {
            _ = try await container.testBaseContext()
                .fetch(BaseIsolationDocument.self)
                .execute()
        }

        try await container.installTestBaseSchemaSnapshot(
            for: BaseIsolationSchemaV2.versionIdentifier,
            baseID: otherBaseID
        )
        try await otherAdmin.migrateIfNeeded()

        #expect(
            try await container.testBaseAdmin().migrationStatus()
                .currentVersion == BaseIsolationSchemaV2.versionIdentifier
        )
        #expect(
            try await otherAdmin.migrationStatus().currentVersion
                == BaseIsolationSchemaV2.versionIdentifier
        )
        _ = try await container.testBaseContext()
            .fetch(BaseIsolationDocument.self)
            .execute()
    }

    private func makeContainer() async throws -> DBContainer {
        let engine = try SQLiteStorageEngine(configuration: .inMemory)
        let schema = try Schema(
            entities: [try BaseIsolationDocument.schemaEntity]
        )
        return try await DBContainer.open(
            for: schema,
            configuration: .testing(storageEngine: engine),
            runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(
                executionIdentity: DatabaseExecutionRuntimeIdentity(
                    identifier: "database-tests",
                    revision: 1
                ),
                entityRuntimes: [
                    try DatabaseFrameworkRuntime.entity(
                        BaseIsolationDocument.self
                    )
                ]
            ),
            security: .testingDisabled
        )
    }

    private func makeMigrationContainer() async throws -> DBContainer {
        let engine = try SQLiteStorageEngine(configuration: .inMemory)
        return try await DBContainer.open(
            for: BaseIsolationSchemaV2.self,
            migrationPlan: BaseIsolationMigrationPlan.self,
            configuration: .testing(storageEngine: engine),
            runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(
                executionIdentity: DatabaseExecutionRuntimeIdentity(
                    identifier: "database-tests",
                    revision: 1
                ),
                entityRuntimes: [
                    try DatabaseFrameworkRuntime.entity(
                        BaseIsolationDocument.self
                    )
                ]
            ),
            security: .testingDisabled
        )
    }

    private func provision(
        _ id: Base.ID,
        grants: [Security.Grant],
        in container: DBContainer
    ) async throws -> DatabaseBaseRecord {
        try await container.provisionBase(
            id,
            placementID: container.storageTopology.defaultPlacementID,
            initialGrants: grants,
            expectedRevision: 0
        )
    }
}
#endif
