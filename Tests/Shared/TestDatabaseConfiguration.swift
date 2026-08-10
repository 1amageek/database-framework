@_spi(Testing) import DatabaseEngine
import DatabaseKit
import DatabaseTypes
import StorageKit

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

public extension DBConfiguration {
    /// Creates an explicitly clocked configuration for tests.
    static func testing(
        name: String? = nil,
        databaseIdentifier: String = "test",
        storageEngine: any StorageEngine,
        indexConfigurations: [any IndexRuntimeConfiguration] = [],
        itemStorage: ItemStorageConfiguration = .v1,
        logging: DatabaseLoggingConfiguration = .disabled,
        metrics: DatabaseMetricsConfiguration = .disabled
    ) throws -> DBConfiguration {
        let domainID = try DatabaseStorageDomain.ID("test-primary")
        let placementID = try Base.Placement.ID("test-default")
        let domain = try DatabaseStorageDomain(
            id: domainID,
            namespacePath: ["database", databaseIdentifier],
            storageEngine: storageEngine
        )
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
            roles: ["test-runner"]
        )
        return DBConfiguration(
            testingName: name,
            storageTopology: topology,
            monotonicClock: TestProcessMonotonicClock(),
            wallClock: FixedTestWallClock(),
            testingBaseID: baseID,
            testingPrincipal: principal,
            indexConfigurations: indexConfigurations,
            itemStorage: itemStorage,
            logging: logging,
            metrics: metrics
        )
    }
}

public extension DatabaseStorageTopology {
    /// Creates the canonical single-domain topology used by test hosts that
    /// own the topology directly instead of constructing a DBConfiguration.
    static func testing(
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
                ),
            ],
            placements: [
                try DatabaseStoragePlacement(
                    id: placementID,
                    domainID: domainID,
                    path: ["bases"]
                ),
            ],
            defaultPlacementID: placementID
        )
    }
}

/// Explicit test-only Base identity and authorization used by behavioral
/// fixtures that are not themselves testing authorization.
public enum TestBaseEnvironment {
    public static func id() throws -> Base.ID {
        try Base.ID("test")
    }

    public static var authorization: AuthorizationContext {
        .authenticated(
            Principal(identifier: "test-runner", roles: ["test-runner"])
        )
    }
}

public extension SecurityConfiguration {
    /// TestSupport-only spelling for the SPI policy bypass. Persisted Grants
    /// remain active and are installed by `testBaseContext`.
    static var testingDisabled: SecurityConfiguration {
        .disabledForTesting
    }
}

public extension DBContainer {
    /// Returns a context bound to the explicitly bootstrapped test Base.
    /// Production code cannot access this helper because TestSupport is never
    /// linked into production products.
    func testBaseContext(
        authorization: AuthorizationContext = TestBaseEnvironment.authorization,
        autosaveEnabled: Bool = false
    ) -> DatabaseContext {
        do {
            return session(authorization: authorization)
                .base(try TestBaseEnvironment.id())
                .newContext(autosaveEnabled: autosaveEnabled)
        } catch {
            preconditionFailure("The fixed test Base identity must be valid")
        }
    }

    /// Persists access for one test subject through the same Base-local Grant
    /// transaction used by production authorization.
    func grantTestBaseAccess(
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
    func grantTestDatabaseAccess(
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

    /// Returns administrative APIs bound to the explicitly bootstrapped test
    /// Base and test principal.
    func testBaseAdmin() -> AdminContext {
        do {
            return session(authorization: TestBaseEnvironment.authorization)
                .base(try TestBaseEnvironment.id())
                .admin()
        } catch {
            preconditionFailure("The fixed test Base identity must be valid")
        }
    }

    /// Executes a test-only operation while retaining the explicit test Base
    /// lease. This is for low-level behavioral fixtures that must inspect
    /// Base-local storage metadata without adding such access to production API.
    func withTestBaseOperation<Result: Sendable>(
        _ operation: @Sendable () async throws -> Result
    ) async throws -> Result {
        try await testBaseContext().withBaseOperation(operation)
    }

    /// Clears the complete Base-local data root while preserving the control
    /// catalog and persisted Grants, then rebuilds the fixture's index state.
    /// Test suites that reuse one backend call this between serialized cases.
    func resetTestBaseData() async throws {
        try await withTestBaseOperation {
            let dataRoot = try self.activeDataSubspace(relativePath: [])
            try await self.engine.withTransaction { transaction in
                try transaction.clearRange(
                    beginKey: dataRoot.range().begin,
                    endKey: dataRoot.range().end
                )
            }
            try await self.ensureIndexesReady()
        }
    }

    /// Initializes index lifecycle state under the explicit test Base lease.
    func ensureTestBaseIndexesReady() async throws {
        try await withTestBaseOperation {
            try await self.ensureIndexesReady()
        }
    }

    /// Runs a low-level storage assertion in the explicit test Base and the
    /// same authorized transaction boundary used by production contexts.
    func withTestBaseTransaction<Result: Sendable>(
        _ operation: @Sendable @escaping (
            any TransactionAccess
        ) async throws -> Result
    ) async throws -> Result {
        try await testBaseContext().withTransaction { transaction in
            try await operation(transaction.storageAccess)
        }
    }

    /// Resolves a model directory inside the explicit test Base.
    func testBaseDirectory<Model: Persistable>(
        for type: Model.Type,
        path: DirectoryPath<Model> = DirectoryPath()
    ) async throws -> Subspace {
        try await withTestBaseOperation {
            try await self.resolveDirectory(for: type, path: path)
        }
    }

    /// Resolves a runtime schema directory inside the explicit test Base.
    func testBaseDirectory(
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
    func testBasePolymorphicDirectory(
        for identifier: String
    ) async throws -> Subspace {
        try await withTestBaseOperation {
            try await self.resolvePolymorphicDirectory(for: identifier)
        }
    }

    /// Installs one historical schema snapshot in the explicit test Base.
    func installTestBaseSchemaSnapshot(
        for version: Schema.Version
    ) async throws {
        let context = testBaseContext()
        try await context.withBaseOperation {
            try await self.installSchemaSnapshot(for: version)
        }
    }

    /// Reads the schema version stored in the explicit test Base.
    func testBaseCurrentSchemaVersion() async throws -> Schema.Version? {
        try await testBaseAdmin().migrationStatus().currentVersion
    }

    /// Resolves the compiled schema definition corresponding to the version
    /// checkpoint persisted in the explicit test Base.
    func testBaseSchemaDefinition() async throws -> Schema? {
        guard let version = try await testBaseCurrentSchemaVersion() else {
            return nil
        }
        return try schemaDefinition(for: version)
    }

    /// Reads the durable database-wide entity catalog through the production
    /// control-domain namespace while keeping the SPI out of feature tests.
    func testPersistedControlSchemaEntities() async throws -> [Schema.Entity] {
        try await persistedControlSchemaEntitiesForTesting()
    }
}
