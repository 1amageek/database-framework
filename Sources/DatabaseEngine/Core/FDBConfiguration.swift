import Foundation
import FoundationDB
import Core

/// FDB configuration
///
/// Configuration for FoundationDB connection:
/// - Schema definition
/// - FoundationDB cluster file URL
/// - Index configurations for runtime parameters
///
/// **Example usage**:
/// ```swift
/// let schema = Schema([User.self, Order.self, Document.self])
/// let config = FDBConfiguration(
///     url: URL(filePath: "/etc/foundationdb/fdb.cluster"),
///     indexConfigurations: [
///         VectorIndexConfiguration<Document>(
///             keyPath: \.embedding,
///             dimensions: 1536,
///             hnswParameters: .default
///         )
///     ]
/// )
/// let container = try FDBContainer(for: schema, configuration: config)
/// ```
public struct FDBConfiguration: DataStoreConfiguration, Sendable {

    // MARK: - Properties

    /// Configuration name (optional, for debugging)
    public let name: String?

    /// Schema for this configuration (optional)
    ///
    /// If specified, used for validation that configuration's schema is a subset of the container's schema.
    /// If nil, the container's schema is used directly.
    public let schema: Schema?

    /// FoundationDB API version (optional)
    ///
    /// If nil, assumes API version has already been selected globally.
    /// If specified, will attempt to select this version during initialization.
    /// Note: API version can only be selected once per process.
    public let apiVersion: Int32?

    /// FoundationDB cluster file URL (optional)
    ///
    /// URL pointing to the FoundationDB cluster file (fdb.cluster).
    ///
    /// If nil, uses the default cluster file location.
    /// - macOS/Linux: `/etc/foundationdb/fdb.cluster`
    ///
    /// **Example**:
    /// ```swift
    /// let config = FDBConfiguration(
    ///     schema: schema,
    ///     url: URL(filePath: "/custom/path/fdb.cluster")
    /// )
    /// ```
    public let url: URL?

    /// Index configurations for runtime parameters
    ///
    /// Used for indexes that require heavy, environment-dependent parameters:
    /// - Vector indexes: dimensions, HNSW parameters
    /// - Full-text search: language settings, tokenizer configuration
    ///
    /// Multiple configurations for the same index are allowed (e.g., multi-language full-text).
    public let indexConfigurations: [any IndexConfiguration]

    // MARK: - Initialization

    /// Create FDB configuration
    ///
    /// - Parameters:
    ///   - name: Configuration name for debugging (default: nil)
    ///   - schema: Schema for this configuration (default: nil = all models)
    ///   - apiVersion: FoundationDB API version (default: nil = use already selected version)
    ///   - url: FoundationDB cluster file URL (default: nil = use default location)
    ///   - indexConfigurations: Runtime index configurations (default: [])
    ///
    /// **Example - Basic**:
    /// ```swift
    /// let config = FDBConfiguration(schema: Schema([User.self]))
    /// ```
    ///
    /// **Example - With vector index**:
    /// ```swift
    /// let config = FDBConfiguration(
    ///     schema: Schema([Document.self]),
    ///     indexConfigurations: [
    ///         VectorIndexConfiguration<Document>(
    ///             keyPath: \.embedding,
    ///             dimensions: 1536,
    ///             hnswParameters: .init(M: 16, efConstruction: 200, efSearch: 50)
    ///         )
    ///     ]
    /// )
    /// ```
    ///
    /// **Example - Multi-language full-text**:
    /// ```swift
    /// let config = FDBConfiguration(
    ///     schema: Schema([Article.self]),
    ///     indexConfigurations: [
    ///         FullTextIndexConfiguration<Article>(keyPath: \.content, language: "ja", tokenizer: .morphological),
    ///         FullTextIndexConfiguration<Article>(keyPath: \.content, language: "en", tokenizer: .standard)
    ///     ]
    /// )
    /// ```
    public init(
        name: String? = nil,
        schema: Schema? = nil,
        apiVersion: Int32? = nil,
        url: URL? = nil,
        indexConfigurations: [any IndexConfiguration] = []
    ) {
        self.name = name
        self.schema = schema
        self.apiVersion = apiVersion
        self.url = url
        self.indexConfigurations = indexConfigurations
    }

    /// Convenience initializer with required schema (backward compatible)
    ///
    /// - Parameters:
    ///   - schema: Schema (required)
    ///   - apiVersion: FoundationDB API version
    ///   - url: FoundationDB cluster file URL
    public init(
        schema: Schema,
        apiVersion: Int32? = nil,
        url: URL? = nil
    ) {
        self.name = nil
        self.schema = schema
        self.apiVersion = apiVersion
        self.url = url
        self.indexConfigurations = []
    }
}

// MARK: - CustomDebugStringConvertible

extension FDBConfiguration: CustomDebugStringConvertible {
    public var debugDescription: String {
        let schemaDesc = schema.map { String(describing: $0) } ?? "nil"
        let nameDesc = name ?? "unnamed"
        let urlDesc = url?.path ?? "default"
        let indexConfigCount = indexConfigurations.count
        return "FDBConfiguration(name: \(nameDesc), schema: \(schemaDesc), url: \(urlDesc), apiVersion: \(apiVersion?.description ?? "nil"), indexConfigs: \(indexConfigCount))"
    }
}
