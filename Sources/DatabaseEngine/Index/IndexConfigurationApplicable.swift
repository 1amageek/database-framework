// IndexConfigurationApplicable.swift
// FDBIndexing - Protocols for applying IndexConfiguration to IndexMaintainer
//
// These protocols bridge IndexConfiguration (defined in FDBModel) with
// IndexMaintainer implementations (defined in upper layers).

import Core

/// Protocol for IndexMaintainer that accepts a single IndexConfiguration
///
/// **Responsibility**: IndexMaintainer implementors conform to this protocol
/// to receive runtime configuration after creation.
///
/// **Design Flow**:
/// ```
/// IndexConfiguration (FDBModel)
///       ↓
/// IndexConfigurationApplicable (FDBIndexing)
///       ↓
/// HNSWIndexMaintainer (upper layer: fdb-indexes)
/// ```
///
/// **Usage Example**:
/// ```swift
/// struct HNSWIndexMaintainer<Item: Persistable>: IndexMaintainer, IndexConfigurationApplicable {
///     typealias Configuration = VectorIndexConfiguration<Item>
///
///     private var dimensions: Int = 0
///     private var hnswParameters: HNSWParameters = .default
///     private var loadIntoMemory: Bool = false
///
///     mutating func apply(configuration: Configuration) {
///         self.dimensions = configuration.dimensions
///         self.hnswParameters = configuration.hnswParameters
///         self.loadIntoMemory = configuration.loadIntoMemory
///     }
///
///     func updateIndex(oldItem: Item?, newItem: Item?, transaction: any TransactionProtocol) async throws {
///         // Use dimensions, hnswParameters in index maintenance
///     }
/// }
/// ```
///
/// **When to Use**:
/// - Vector indexes (HNSW parameters)
/// - Custom indexes with environment-dependent settings
/// - Any index that needs exactly one configuration
public protocol IndexConfigurationApplicable {
    /// The IndexConfiguration type this maintainer accepts
    associatedtype Configuration: IndexConfiguration

    /// Apply the configuration to this maintainer
    ///
    /// Called after IndexMaintainer creation, before first use.
    ///
    /// - Parameter configuration: The runtime configuration to apply
    mutating func apply(configuration: Configuration)
}

/// Protocol for IndexMaintainer that accepts multiple IndexConfigurations
///
/// **Purpose**: Support indexes that need multiple configurations for the same field.
/// The primary use case is multi-language full-text search.
///
/// **Usage Example**:
/// ```swift
/// struct FullTextIndexMaintainer<Item: Persistable>: IndexMaintainer, MultiIndexConfigurationApplicable {
///     typealias Configuration = FullTextIndexConfiguration<Item>
///
///     private var languageConfigs: [String: Configuration] = [:]
///
///     mutating func apply(configurations: [Configuration]) {
///         for config in configurations {
///             languageConfigs[config.language] = config
///         }
///     }
///
///     func updateIndex(oldItem: Item?, newItem: Item?, transaction: any TransactionProtocol) async throws {
///         // Index content for each configured language
///         for (language, config) in languageConfigs {
///             let tokenizer = makeTokenizer(for: config)
///             // ... tokenize and index
///         }
///     }
/// }
/// ```
///
/// **When to Use**:
/// - Full-text search with multiple languages
/// - Indexes that need variant configurations
/// - Any scenario where one index field needs multiple processing pipelines
public protocol MultiIndexConfigurationApplicable {
    /// The IndexConfiguration type this maintainer accepts
    associatedtype Configuration: IndexConfiguration

    /// Apply multiple configurations to this maintainer
    ///
    /// Called after IndexMaintainer creation, before first use.
    /// All configurations for this index are passed at once.
    ///
    /// - Parameter configurations: Array of runtime configurations to apply
    mutating func apply(configurations: [Configuration])
}

// MARK: - Helper Extensions

extension IndexConfigurationApplicable {
    /// The kind identifier this maintainer expects
    public static var expectedKindIdentifier: String {
        Configuration.kindIdentifier
    }
}

extension MultiIndexConfigurationApplicable {
    /// The kind identifier this maintainer expects
    public static var expectedKindIdentifier: String {
        Configuration.kindIdentifier
    }
}
