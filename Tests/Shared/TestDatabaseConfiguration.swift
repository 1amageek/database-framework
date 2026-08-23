@_spi(Testing) @_spi(DatabaseExecution) import DatabaseEngine
import DatabaseKit
import DatabaseTypes
import StorageKit

// TEST_TIME_SEMANTICS: correctness - supplies monotonic time to test runtime
// dependencies.

/// Process-local monotonic time used by tests that do not exercise time.
public struct TestProcessMonotonicClock: StorageMonotonicClock {
    private static let clock = ContinuousClock()
    private static let origin = clock.now

    public init() {}

    public var now: StorageInstant {
        StorageInstant(
            durationSinceReference: Self.origin.duration(to: Self.clock.now)
        )
    }

    public func sleep(
        until deadline: StorageInstant
    ) async throws(StorageClockError) {
        let remaining = now.duration(to: deadline)
        guard remaining > .zero else { return }
        do {
            try await Self.clock.sleep(for: remaining)
        } catch {
            throw .cancelled
        }
    }
}

/// Stable absolute time used by tests that do not exercise wall-clock behavior.
public struct FixedTestWallClock: WallClock {
    public let now: Timestamp

    public init() {
        self.now = Timestamp(secondsSinceUnixEpoch: 0)
    }

    public init(now: Timestamp) {
        self.now = now
    }
}

extension DBConfiguration {
    /// Creates an explicitly clocked configuration for tests.
    public static func testing(
        name: String? = nil,
        databaseIdentifier: String? = nil,
        storageEngine: any StorageEngine,
        itemStorage: ItemStorageConfiguration = .v1,
        logging: DatabaseLoggingConfiguration = .disabled,
        metrics: DatabaseMetricsConfiguration = .disabled
    ) throws -> DBConfiguration {
        #if MultiBase
        let domainID = try DatabaseStorageDomain.ID("test-primary")
        let domain = try DatabaseStorageDomain(
            id: domainID,
            namespacePath: ["database", databaseIdentifier ?? "test"],
            storageEngine: storageEngine
        )
        let placementID = try Base.Placement.ID("test-default")
        let placement = try DatabaseStoragePlacement(
            id: placementID,
            domainID: domainID,
            path: ["bases"]
        )
        let topology = try DatabaseStorageTopology(
            controlDomainID: domainID,
            domains: [domain],
            placements: [placement],
            defaultPlacementID: placementID
        )
        let baseID = try Base.ID("test")
        let principal = Principal(
            identifier: "test-runner",
            roles: ["test-runner", "admin"]
        )
        return DBConfiguration(
            testingName: name,
            storageTopology: topology,
            monotonicClock: TestProcessMonotonicClock(),
            wallClock: FixedTestWallClock(),
            testingBaseID: baseID,
            testingPrincipal: principal,
            itemStorage: itemStorage,
            logging: logging,
            metrics: metrics
        )
        #else
        return DBConfiguration(
            name: name,
            storageEngine: storageEngine,
            databaseRoot: databaseIdentifier.map {
                Subspace("test-database", $0)
            } ?? Subspace(),
            monotonicClock: TestProcessMonotonicClock(),
            wallClock: FixedTestWallClock(),
            itemStorage: itemStorage,
            logging: logging,
            metrics: metrics
        )
        #endif
    }
}

#if MultiBase
extension DatabaseStorageTopology {
    /// Creates the canonical single-domain topology used by test hosts that
    /// own the topology directly instead of constructing a DBConfiguration.
    public static func testing(
        storageEngine: any StorageEngine
    ) throws -> DatabaseStorageTopology {
        let domainID = try DatabaseStorageDomain.ID("test-primary")
        let placementID = try Base.Placement.ID("test-default")
        return try DatabaseStorageTopology(
            controlDomainID: domainID,
            domains: [
                try DatabaseStorageDomain(
                    id: domainID,
                    namespacePath: ["database", "test"],
                    storageEngine: storageEngine
                )
            ],
            placements: [
                try DatabaseStoragePlacement(
                    id: placementID,
                    domainID: domainID,
                    path: ["bases"]
                )
            ],
            defaultPlacementID: placementID
        )
    }
}
#endif

/// Explicit test-only Base identity and authorization used by behavioral
/// fixtures that are not themselves testing authorization.
public enum TestBaseEnvironment {
    #if MultiBase
    public static func id() throws -> Base.ID {
        try Base.ID("test")
    }
    #endif

    public static var authorization: AuthorizationContext {
        .authenticated(
            Principal(
                identifier: "test-runner",
                roles: ["test-runner", "admin"]
            )
        )
    }
}

extension SecurityConfiguration {
    /// TestSupport-only spelling for the SPI policy bypass. Persisted Grants
    /// remain active and are installed by `testBaseContext`.
    public static var testingDisabled: SecurityConfiguration {
        .disabledForTesting
    }
}

extension DBContainer {
    /// Returns a context bound to the explicitly bootstrapped test Base.
    /// Production code cannot access this helper because TestSupport is never
    /// linked into production products.
    public func testBaseContext(
        authorization: AuthorizationContext = TestBaseEnvironment.authorization,
        autosaveEnabled: Bool = false
    ) -> DatabaseContext {
        #if MultiBase
        do {
            return session(authorization: authorization)
                .base(try TestBaseEnvironment.id())
                .newContext(autosaveEnabled: autosaveEnabled)
        } catch {
            preconditionFailure("The fixed test Base identity must be valid")
        }
        #else
        newContext(
            authorization: authorization,
            autosaveEnabled: autosaveEnabled
        )
        #endif
    }

    #if MultiBase
    /// Persists access for one test subject through the same Base-local Grant
    /// transaction used by production authorization.
    public func grantTestBaseAccess(
        to subject: Security.Subject,
        access: Security.Access
    ) async throws {
        let baseID = try TestBaseEnvironment.id()
        try await grantBaseAccessForTesting(
            Security.Grant(
                subject: subject,
                resource: .base(baseID),
                access: access
            ),
            authorization: TestBaseEnvironment.authorization
        )
    }

    /// Persists database access for one test subject through the production
    /// control-domain Grant transaction.
    public func grantTestDatabaseAccess(
        to subject: Security.Subject,
        access: Security.Access
    ) async throws {
        try await grantDatabaseAccessForTesting(
            Security.Grant(
                subject: subject,
                resource: .database,
                access: access
            ),
            authorization: TestBaseEnvironment.authorization
        )
    }
    #endif

    /// Returns administrative APIs bound to the explicitly bootstrapped test
    /// Base and test principal.
    public func testBaseAdmin() -> AdminContext {
        #if MultiBase
        do {
            return session(authorization: TestBaseEnvironment.authorization)
                .base(try TestBaseEnvironment.id())
                .admin()
        } catch {
            preconditionFailure("The fixed test Base identity must be valid")
        }
        #else
        return admin(authorization: TestBaseEnvironment.authorization)
        #endif
    }

    /// Executes a test-only operation while retaining the explicit test Base
    /// lease. This is for low-level behavioral fixtures that must inspect
    /// Base-local storage metadata without adding such access to production API.
    public func withTestBaseOperation<Result: Sendable>(
        _ operation: @Sendable () async throws -> Result
    ) async throws -> Result {
        try await testBaseContext().withDataOperation(operation)
    }

    /// Clears the complete Base-local data root while preserving the control
    /// catalog and persisted Grants, then rebuilds the fixture's index state.
    /// Test suites that reuse one backend call this between serialized cases.
    public func resetTestBaseData() async throws {
        try await testBaseContext().withTransaction { transaction in
            let storage = try self.executionStorage()
            let dataRoot = storage.root.subspace("data")
            try transaction.storageAccess.clearRange(
                beginKey: dataRoot.range().begin,
                endKey: dataRoot.range().end
            )
        }
        try await withTestBaseOperation {
            try await self.ensureIndexesReady()
        }
    }

    /// Initializes index lifecycle state under the explicit test Base lease.
    public func ensureTestBaseIndexesReady() async throws {
        try await withTestBaseOperation {
            try await self.ensureIndexesReady()
        }
    }

    /// Runs a low-level storage assertion in the explicit test Base and the
    /// same authorized transaction boundary used by production contexts.
    public func withTestBaseTransaction<Result: Sendable>(
        _ operation: @Sendable @escaping (
            any TransactionAccess
        ) async throws -> Result
    ) async throws -> Result {
        try await testBaseContext().withTransaction { transaction in
            try await operation(transaction.storageAccess)
        }
    }

    /// Runs a low-level storage assertion through the migration-maintenance
    /// admission path. Use this only to inspect durable state while ordinary
    /// data operations are intentionally blocked by an incomplete migration.
    public func withTestBaseMigrationMaintenanceTransaction<Result: Sendable>(
        _ operation: @Sendable @escaping (
            any TransactionAccess
        ) async throws -> Result
    ) async throws -> Result {
        try await withMigrationMaintenanceAccess {
            try await self.withTestBaseTransaction(operation)
        }
    }

    /// Returns the root retained by the explicit test Base lease without
    /// exposing production execution-storage SPI to individual test targets.
    public func testBaseDataRoot() async throws -> Subspace {
        try await withTestBaseOperation {
            try self.executionStorage().root
        }
    }

    /// Resolves a model directory inside the explicit test Base.
    public func testBaseDirectory<Model: Persistable>(
        for type: Model.Type,
        path: DirectoryPath<Model> = DirectoryPath()
    ) async throws -> Subspace {
        try await withTestBaseOperation {
            try await self.resolveDirectory(for: type, path: path)
        }
    }

    /// Resolves a runtime schema directory inside the explicit test Base.
    public func testBaseDirectory(
        for entity: Schema.Entity,
        path: AnyDirectoryPath? = nil
    ) async throws -> Subspace {
        try await withTestBaseOperation {
            try await self.resolveDirectory(for: entity, path: path)
        }
    }

    /// Builds a low-level store under the explicit test Base lease.
    package func testBaseStore<Model: Persistable>(
        for type: Model.Type,
        path: DirectoryPath<Model> = DirectoryPath()
    ) async throws -> DatabaseDataStore {
        try await withTestBaseOperation {
            try await self.store(for: type, path: path)
        }
    }

    /// Resolves a polymorphic projection inside the explicit test Base.
    public func testBasePolymorphicDirectory(
        for identifier: String
    ) async throws -> Subspace {
        try await withTestBaseOperation {
            try await self.resolvePolymorphicDirectory(for: identifier)
        }
    }

    /// Installs one historical schema snapshot in the explicit test Base.
    public func installTestBaseSchemaSnapshot(
        for version: Schema.Version
    ) async throws {
        let context = testBaseContext()
        try await withMigrationMaintenanceAccess {
            try await context.withDataOperation {
                try await self.installSchemaSnapshot(for: version)
            }
        }
    }

    #if MultiBase
    /// Installs one historical schema snapshot in an explicitly selected test
    /// Base without exposing Base-local schema mutation in production API.
    public func installTestBaseSchemaSnapshot(
        for version: Schema.Version,
        baseID: Base.ID
    ) async throws {
        let context = session(
            authorization: TestBaseEnvironment.authorization
        ).base(baseID).newContext()
        try await withMigrationMaintenanceAccess {
            try await context.withDataOperation {
                try await self.installSchemaSnapshot(for: version)
            }
        }
    }

    /// Replaces the schema fingerprint in an explicitly selected test Base so
    /// admission tests can exercise same-version schema divergence.
    public func overwriteTestBaseSchemaFingerprint(
        _ fingerprint: ByteString,
        baseID: Base.ID
    ) async throws {
        let lease = try acquireBaseLease(baseID)
        let context = session(
            authorization: TestBaseEnvironment.authorization
        ).base(baseID).newContext()
        let fingerprintKey = lease.root
            .subspace("metadata")
            .subspace("schema")
            .pack(Tuple("fingerprint"))
        try await withMigrationMaintenanceAccess {
            try await context.withDataOperation {
                try await self.withDatabaseTransaction(
                    requiredAccess: .administer,
                    configuration: .batch
                ) { transaction in
                    try transaction.setValue(
                        fingerprint,
                        for: fingerprintKey
                    )
                }
            }
        }
    }
    #endif

    /// Reads the schema version stored in the explicit test Base.
    public func testBaseCurrentSchemaVersion() async throws -> Schema.Version? {
        try await testBaseAdmin().migrationStatus().currentVersion
    }

    /// Resolves the compiled schema definition corresponding to the version
    /// checkpoint persisted in the explicit test Base.
    public func testBaseSchemaDefinition() async throws -> Schema? {
        guard let version = try await testBaseCurrentSchemaVersion() else {
            return nil
        }
        return try schemaDefinition(for: version)
    }

    /// Reads the durable database-wide entity catalog through the production
    /// control-domain namespace while keeping the SPI out of feature tests.
    public func testPersistedControlSchemaEntities() async throws -> [Schema.Entity] {
        try await persistedControlSchemaEntitiesForTesting()
    }
}
