import StorageKit
import DatabaseKit

/// Database configuration
///
/// Configures an injected storage engine and runtime index parameters.
///
/// **Example usage**:
/// ```swift
/// let sqliteEngine = try SQLiteStorageEngine(configuration: .inMemory)
/// let config = DBConfiguration(
///     storageEngine: sqliteEngine,
///     indexConfigurations: [
///         VectorIndexConfiguration<Document>(
///             field: Document.fields.embedding,
///             hnsw: .default
///         )
///     ]
/// )
/// let container = try await DBContainer.open(for: schema, configuration: config)
/// ```
public struct DBConfiguration: Sendable {
    // MARK: - Properties

    /// Configuration name (optional, for debugging)
    public let name: String?

    /// Single-use lifecycle that owns the injected storage engine.
    private let storageLifecycle: DatabaseStorageLifecycle

    /// Index configurations for runtime parameters
    ///
    /// Used for indexes that require heavy, environment-dependent parameters:
    /// - Vector indexes: dimensions, HNSW parameters
    /// - Full-text search: language settings, tokenizer configuration
    ///
    /// Multiple configurations for the same index are allowed (e.g., multi-language full-text).
    public let indexConfigurations: [any IndexRuntimeConfiguration]

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

    // MARK: - Initialization

    /// Create database configuration
    ///
    /// - Parameters:
    ///   - name: Configuration name for debugging (default: nil)
    ///   - storageEngine: Initialized storage engine.
    ///   - indexConfigurations: Runtime index configurations (default: [])
    public init(
        name: String? = nil,
        storageEngine: any StorageEngine,
        monotonicClock: any StorageMonotonicClock,
        wallClock: any WallClock,
        indexConfigurations: [any IndexRuntimeConfiguration] = [],
        itemStorage: ItemStorageConfiguration = .v1,
        logging: DatabaseLoggingConfiguration = .disabled,
        metrics: DatabaseMetricsConfiguration = .disabled
    ) {
        self.name = name
        self.storageLifecycle = DatabaseStorageLifecycle(
            storageEngine: storageEngine
        )
        self.indexConfigurations = indexConfigurations
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
}

// MARK: - CustomDebugStringConvertible

extension DBConfiguration: CustomDebugStringConvertible {
    public var debugDescription: String {
        let nameDesc = name ?? "unnamed"
        let indexConfigCount = indexConfigurations.count
        return "DBConfiguration(name: \(nameDesc), indexConfigs: \(indexConfigCount), itemEncoding: \(itemStorage.encoding))"
    }
}
