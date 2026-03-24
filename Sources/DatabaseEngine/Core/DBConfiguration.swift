import Foundation
import StorageKit
#if FOUNDATION_DB
import FDBStorage
#endif
import Core

/// Database configuration
///
/// Configures the storage backend and runtime index parameters.
/// Defaults to FoundationDB when no backend is specified.
///
/// **Example usage**:
/// ```swift
/// // Default (FDB)
/// let container = try await DBContainer(for: schema)
///
/// // FDB with specific database instance
/// let config = DBConfiguration(
///     backend: .fdb(.init(database: db))
/// )
/// let container = try await DBContainer(for: schema, configuration: config)
///
/// // Custom backend (e.g., SQLite)
/// let sqliteEngine = try SQLiteStorageEngine(configuration: .inMemory)
/// let config = DBConfiguration(backend: .custom(sqliteEngine))
/// let container = try await DBContainer(for: schema, configuration: config)
///
/// // With index configurations
/// let config = DBConfiguration(
///     indexConfigurations: [
///         VectorIndexConfiguration<Document>(
///             keyPath: \.embedding,
///             dimensions: 1536,
///             hnswParameters: .default
///         )
///     ]
/// )
/// let container = try await DBContainer(for: schema, configuration: config)
/// ```
public struct DBConfiguration: DataStoreConfiguration, Sendable {

    // MARK: - DataStoreConfiguration

    /// Schema (always nil — schema is owned by DBContainer, not configuration)
    public let schema: Schema? = nil

    /// Storage backend specification
    public enum StorageBackend: Sendable {
        #if FOUNDATION_DB
        /// FoundationDB
        ///
        /// FDB client initialization is handled automatically.
        /// If no configuration is provided, connects to the default cluster.
        case fdb(FDBStorageEngine.Configuration = .init())
        #endif

        /// Custom StorageEngine (e.g., SQLite, InMemory)
        ///
        /// Use this for non-FDB backends. The engine must already be created.
        case custom(any StorageEngine)
    }

    // MARK: - Properties

    /// Configuration name (optional, for debugging)
    public let name: String?

    /// Storage backend (default: .fdb())
    public let backend: StorageBackend

    /// Index configurations for runtime parameters
    ///
    /// Used for indexes that require heavy, environment-dependent parameters:
    /// - Vector indexes: dimensions, HNSW parameters
    /// - Full-text search: language settings, tokenizer configuration
    ///
    /// Multiple configurations for the same index are allowed (e.g., multi-language full-text).
    public let indexConfigurations: [any IndexConfiguration]

    // MARK: - Initialization

    /// Create database configuration
    ///
    /// All parameters have defaults, enabling zero-configuration usage:
    /// ```swift
    /// let container = try await DBContainer(for: schema)
    /// ```
    ///
    /// - Parameters:
    ///   - name: Configuration name for debugging (default: nil)
    ///   - backend: Storage backend (default: .fdb())
    ///   - indexConfigurations: Runtime index configurations (default: [])
    #if FOUNDATION_DB
    public init(
        name: String? = nil,
        backend: StorageBackend = .fdb(),
        indexConfigurations: [any IndexConfiguration] = []
    ) {
        self.name = name
        self.backend = backend
        self.indexConfigurations = indexConfigurations
    }
    #else
    public init(
        name: String? = nil,
        backend: StorageBackend,
        indexConfigurations: [any IndexConfiguration] = []
    ) {
        self.name = name
        self.backend = backend
        self.indexConfigurations = indexConfigurations
    }
    #endif
}

// MARK: - CustomDebugStringConvertible

extension DBConfiguration: CustomDebugStringConvertible {
    public var debugDescription: String {
        let nameDesc = name ?? "unnamed"
        let backendDesc: String
        switch backend {
        #if FOUNDATION_DB
        case .fdb:
            backendDesc = "fdb"
        #endif
        case .custom(let engine):
            backendDesc = "custom(\(type(of: engine)))"
        }
        let indexConfigCount = indexConfigurations.count
        return "DBConfiguration(name: \(nameDesc), backend: \(backendDesc), indexConfigs: \(indexConfigCount))"
    }
}
