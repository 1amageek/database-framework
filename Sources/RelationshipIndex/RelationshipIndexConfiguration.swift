// RelationshipIndexConfiguration.swift
// RelationshipIndex - Configuration for relationship indexes
//
// Provides related item loading capability for relationship index maintenance.

import Foundation
import Core
import Relationship
import DatabaseEngine
import FoundationDB

/// Type-erased loader for related items
///
/// Used by RelationshipIndexMaintainer to load related items for relationship index building.
///
/// - Parameters:
///   - typeName: Name of the related type (e.g., "Customer")
///   - foreignKey: Foreign key value pointing to the related item
///   - transaction: FDB transaction
/// - Returns: The related item as type-erased Persistable, or nil if not found
public typealias RelatedItemLoader = @Sendable (
    _ typeName: String,
    _ foreignKey: any Sendable,
    _ transaction: any TransactionProtocol
) async throws -> (any Persistable)?

/// Configuration for relationship indexes
///
/// Provides the related item loader needed for relationship index maintenance.
/// This configuration is passed through `FDBConfiguration.indexConfigurations`.
///
/// **Usage**:
/// ```swift
/// let config = FDBConfiguration(
///     indexConfigurations: [
///         RelationshipIndexConfiguration(
///             indexName: "Order_customer_name_total",
///             relatedItemLoader: { typeName, foreignKey, transaction in
///                 // Load related item using container/store
///                 return try await container.loadItem(typeName: typeName, id: foreignKey, transaction: transaction)
///             }
///         )
///     ]
/// )
/// ```
///
/// **Automatic Setup**:
/// FDBContainer automatically creates RelationshipIndexConfiguration for all relationship indexes
/// when schema contains relationships. Users typically don't need to create this manually.
public struct RelationshipIndexConfiguration: IndexConfiguration, @unchecked Sendable {
    // MARK: - IndexConfiguration Protocol

    /// Kind identifier for relationship indexes
    ///
    /// Matches `RelationshipIndexKind.identifier` without needing type parameters.
    public static var kindIdentifier: String { "relationship" }

    public let keyPath: AnyKeyPath

    public let modelTypeName: String

    public var indexName: String { _indexName }

    public var subspaceKey: String? { nil }

    // MARK: - Relationship Specific

    /// Index name for matching
    private let _indexName: String

    /// Loader for related items
    public let relatedItemLoader: RelatedItemLoader

    // MARK: - Initialization

    /// Initialize relationship index configuration
    ///
    /// - Parameters:
    ///   - indexName: Name of the relationship index
    ///   - modelTypeName: Name of the owning model type
    ///   - relatedItemLoader: Closure to load related items
    public init(
        indexName: String,
        modelTypeName: String,
        relatedItemLoader: @escaping RelatedItemLoader
    ) {
        self._indexName = indexName
        self.modelTypeName = modelTypeName
        // Use a placeholder keyPath - relationship indexes span multiple types
        self.keyPath = \RelationshipIndexPlaceholder.placeholder
        self.relatedItemLoader = relatedItemLoader
    }
}

// MARK: - Placeholder Type for AnyKeyPath

/// Internal placeholder type for relationship index configuration
///
/// Since relationship indexes don't have a single source keyPath (they combine
/// fields from multiple types), we use this placeholder to satisfy the
/// IndexConfiguration protocol requirement.
internal struct RelationshipIndexPlaceholder {
    var placeholder: Int = 0
}
