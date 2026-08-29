import DatabaseKit
import StorageKit

/// Database configuration
///
/// Configures an injected storage engine and operational container services.
///
/// **Example usage**:
/// ```swift
/// let sqliteEngine = try SQLiteStorageEngine(configuration: .inMemory)
/// let config = DBConfiguration(storageEngine: sqliteEngine)
/// let container = try await DBContainer.open(for: schema, configuration: config)
/// ```
public struct DBConfiguration: Sendable {
    #if DATABASE_MULTI_BASE
    package struct TestingBootstrap: Sendable {
        package let baseID: Base.ID
        package let principal: Principal
    }
    #endif

    // MARK: - Properties

    /// Configuration name (optional, for debugging)
    public let name: String?

    #if DATABASE_MULTI_BASE
    /// Single-use lifecycle that owns every injected storage domain.
    private let storageTopologyLifecycle: DatabaseStorageTopologyLifecycle
    #else
    /// Single-use lifecycle that owns the injected storage engine.
    private let storageLifecycle: DatabaseStorageLifecycle

    /// Directory path of the database root inside the storage domain.
    ///
    /// The container opens or creates this path through StorageKit's Directory
    /// contract and owns the fixed layout below it. An empty path places the
    /// database root at the store root Directory, which a dedicated backend
    /// uses; a shared backend names the Directory that isolates this database.
    public let databaseRootPath: [String]
    #endif

    /// Canonical physical entity format for this database.
    public let itemStorage: ItemStorageConfiguration

    /// Container-scoped operational logging policy.
    public let logging: DatabaseLoggingConfiguration

    /// Container-scoped operational metrics policy.
    public let metrics: DatabaseMetricsConfiguration

    /// Monotonic time source for framework scheduling, deadlines, and retries.
    public let monotonicClock: any StorageMonotonicClock

    /// Absolute time source for persisted values and observable events.
    public let wallClock: any WallClock

    /// Explicit test-only Base bootstrap supplied by TestSupport.
    #if DATABASE_MULTI_BASE
    package let testingBootstrap: TestingBootstrap?
    #endif

    // MARK: - Initialization

    /// Create database configuration
    ///
    /// - Parameters:
    ///   - name: Configuration name for debugging (default: nil)
    #if DATABASE_MULTI_BASE
    ///   - storageTopology: Validated storage domains and placements.
    public init(
        name: String? = nil,
        storageTopology: DatabaseStorageTopology,
        monotonicClock: any StorageMonotonicClock,
        wallClock: any WallClock,
        itemStorage: ItemStorageConfiguration = .v1,
        logging: DatabaseLoggingConfiguration = .disabled,
        metrics: DatabaseMetricsConfiguration = .disabled
    ) {
        self.name = name
        self.storageTopologyLifecycle = DatabaseStorageTopologyLifecycle(
            topology: storageTopology
        )
        self.itemStorage = itemStorage
        self.logging = logging
        self.metrics = metrics
        self.monotonicClock = monotonicClock
        self.wallClock = wallClock
        self.testingBootstrap = nil
    }

    @_spi(Testing)
    public init(
        testingName name: String? = nil,
        storageTopology: DatabaseStorageTopology,
        monotonicClock: any StorageMonotonicClock,
        wallClock: any WallClock,
        testingBaseID: Base.ID,
        testingPrincipal: Principal,
        itemStorage: ItemStorageConfiguration = .v1,
        logging: DatabaseLoggingConfiguration = .disabled,
        metrics: DatabaseMetricsConfiguration = .disabled
    ) {
        self.name = name
        self.storageTopologyLifecycle = DatabaseStorageTopologyLifecycle(
            topology: storageTopology
        )
        self.itemStorage = itemStorage
        self.logging = logging
        self.metrics = metrics
        self.monotonicClock = monotonicClock
        self.wallClock = wallClock
        self.testingBootstrap = TestingBootstrap(
            baseID: testingBaseID,
            principal: testingPrincipal
        )
    }


    func claimStorageTopology() throws -> ClaimedDatabaseStorageTopology {
        try storageTopologyLifecycle.claim()
    }

    func finishOpeningStorageTopology() throws {
        try storageTopologyLifecycle.finishOpening()
    }

    func shutdownStorageTopology() async {
        await storageTopologyLifecycle.shutdown()
    }

    func shutdownStorageTopologyIfUnclaimed() async {
        await storageTopologyLifecycle.shutdownIfUnclaimed()
    }

    func requestStorageTopologyShutdown() {
        storageTopologyLifecycle.requestShutdown()
    }
    #else
    /// Creates a lightweight single-database configuration.
    ///
    /// The container exclusively owns `storageEngine` after opening starts.
    /// Base catalogs, placements, Compositions, and persisted Grants are not
    /// part of this configuration or its transaction path.
    public init(
        name: String? = nil,
        storageEngine: any StorageEngine,
        databaseRootPath: [String] = [],
        monotonicClock: any StorageMonotonicClock,
        wallClock: any WallClock,
        itemStorage: ItemStorageConfiguration = .v1,
        logging: DatabaseLoggingConfiguration = .disabled,
        metrics: DatabaseMetricsConfiguration = .disabled
    ) throws(DatabaseDirectoryLayoutError) {
        for (index, component) in databaseRootPath.enumerated()
        where component.isEmpty {
            throw .emptyRootPathComponent(index: index)
        }
        self.name = name
        self.storageLifecycle = DatabaseStorageLifecycle(
            storageEngine: storageEngine
        )
        self.databaseRootPath = databaseRootPath
        self.itemStorage = itemStorage
        self.logging = logging
        self.metrics = metrics
        self.monotonicClock = monotonicClock
        self.wallClock = wallClock
    }

    func claimStorageEngine() throws -> any StorageEngine {
        try storageLifecycle.claimStorageEngine()
    }

    func finishOpeningStorageEngine() throws {
        try storageLifecycle.finishOpening()
    }

    func shutdownStorageEngine() async {
        await storageLifecycle.shutdown()
    }

    func shutdownStorageEngineIfUnclaimed() async {
        await storageLifecycle.shutdownIfUnclaimed()
    }

    func requestStorageEngineShutdown() {
        storageLifecycle.requestShutdown()
    }
    #endif
}

// MARK: - CustomDebugStringConvertible

extension DBConfiguration: CustomDebugStringConvertible {
    public var debugDescription: String {
        let nameDesc = name ?? "unnamed"
        return "DBConfiguration(name: \(nameDesc), itemEncoding: \(itemStorage.encoding))"
    }
}
