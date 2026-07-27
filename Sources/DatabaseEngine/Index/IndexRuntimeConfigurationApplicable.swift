// IndexRuntimeConfigurationApplicable.swift
// DatabaseEngine - Protocols for applying IndexRuntimeConfiguration to IndexMaintainer
//
// These protocols apply IndexRuntimeConfiguration values to concrete maintainers while
// preserving static type checking.

import DatabaseKit

/// Protocol for IndexMaintainer that accepts a single IndexRuntimeConfiguration
///
/// **Responsibility**: IndexMaintainer implementors conform to this protocol
/// to receive runtime configuration after creation.
///
/// **Design Flow**:
/// ```
/// IndexRuntimeConfiguration (Core)
///       ↓
/// IndexRuntimeConfigurationApplicable (DatabaseEngine)
///       ↓
/// HNSWIndexMaintainer (specialized index module)
/// ```
///
/// **Usage Example**:
/// ```swift
/// struct HNSWIndexMaintainer<Item: Persistable>: IndexMaintainer, IndexRuntimeConfigurationApplicable {
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
///     func updateIndex(oldItem: Item?, newItem: Item?, transaction: any TransactionAccess) async throws {
///         // Use dimensions, hnswParameters in index maintenance
///     }
/// }
/// ```
///
/// **When to Use**:
/// - Vector indexes (HNSW parameters)
/// - Custom indexes with environment-dependent settings
/// - Any index that needs exactly one configuration
public protocol IndexRuntimeConfigurationApplicable {
    /// The IndexRuntimeConfiguration type this maintainer accepts
    associatedtype Configuration: IndexRuntimeConfiguration

    /// Apply the configuration to this maintainer
    ///
    /// Called after IndexMaintainer creation, before first use.
    ///
    /// - Parameter configuration: The runtime configuration to apply
    mutating func apply(configuration: Configuration)
}

/// Protocol for an index maintainer that accepts multiple runtime configurations.
///
/// **Purpose**: Support indexes that need multiple configurations for the same field.
/// The primary use case is multi-language full-text search.
///
/// **Usage Example**:
/// ```swift
/// struct FullTextIndexMaintainer<Item: Persistable>: IndexMaintainer, MultipleIndexRuntimeConfigurationApplicable {
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
///     func updateIndex(oldItem: Item?, newItem: Item?, transaction: any TransactionAccess) async throws {
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
public protocol MultipleIndexRuntimeConfigurationApplicable {
    /// The IndexRuntimeConfiguration type this maintainer accepts
    associatedtype Configuration: IndexRuntimeConfiguration

    /// Apply multiple configurations to this maintainer
    ///
    /// Called after IndexMaintainer creation, before first use.
    /// All configurations for this index are passed at once.
    ///
    /// - Parameter configurations: Array of runtime configurations to apply
    mutating func apply(configurations: [Configuration])
}

// MARK: - Helper Extensions

extension IndexRuntimeConfigurationApplicable {
    /// The kind identifier this maintainer expects
    public static var expectedKindIdentifier: String {
        Configuration.kindIdentifier
    }
}

extension MultipleIndexRuntimeConfigurationApplicable {
    /// The kind identifier this maintainer expects
    public static var expectedKindIdentifier: String {
        Configuration.kindIdentifier
    }
}
