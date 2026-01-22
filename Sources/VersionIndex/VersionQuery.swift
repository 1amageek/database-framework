// VersionQuery.swift
// VersionIndex - Query extension for version history indexes
//
// Provides FDBContext extension and query builder for temporal versioning.

import Foundation
import Core
import DatabaseEngine
import FoundationDB

// MARK: - Version Entry Point

/// Entry point for version history queries
///
/// **Usage**:
/// ```swift
/// import VersionIndex
///
/// // Get version history for an item
/// let history = try await context.versions(Document.self)
///     .forItem(documentId)
///     .limit(10)
///     .execute()
///
/// // Get latest version
/// let latest = try await context.versions(Document.self)
///     .forItem(documentId)
///     .latest()
///
/// // Get version at specific point
/// let atVersion = try await context.versions(Document.self)
///     .forItem(documentId)
///     .at(version)
/// ```
public struct VersionEntryPoint<T: Persistable>: Sendable {
    private let queryContext: IndexQueryContext

    internal init(queryContext: IndexQueryContext) {
        self.queryContext = queryContext
    }

    /// Query version history for a specific item
    ///
    /// - Parameter id: The item's ID
    /// - Returns: Version query builder
    public func forItem<ID: TupleElement & Sendable>(_ id: ID) -> VersionQueryBuilder<T> {
        VersionQueryBuilder(
            queryContext: queryContext,
            primaryKey: [id]
        )
    }

    /// Query version history for a specific item with compound ID
    ///
    /// - Parameter ids: The item's compound ID components
    /// - Returns: Version query builder
    public func forItem(_ ids: [any TupleElement & Sendable]) -> VersionQueryBuilder<T> {
        VersionQueryBuilder(
            queryContext: queryContext,
            primaryKey: ids
        )
    }
}

// MARK: - Version Query Builder

/// Builder for version history queries
///
/// Supports retrieving historical versions of items.
public struct VersionQueryBuilder<T: Persistable>: Sendable {
    // MARK: - Properties

    private let queryContext: IndexQueryContext
    internal let primaryKey: [any TupleElement & Sendable]
    private var limitCount: Int?
    private var indexName: String?

    // MARK: - Initialization

    internal init(
        queryContext: IndexQueryContext,
        primaryKey: [any TupleElement & Sendable]
    ) {
        self.queryContext = queryContext
        self.primaryKey = primaryKey
    }

    // MARK: - Configuration Methods

    /// Limit the number of versions to return
    ///
    /// - Parameter count: Maximum number of versions
    /// - Returns: Updated query builder
    public func limit(_ count: Int) -> Self {
        var copy = self
        copy.limitCount = count
        return copy
    }

    /// Specify a custom version index name
    ///
    /// - Parameter name: Index name
    /// - Returns: Updated query builder
    public func index(_ name: String) -> Self {
        var copy = self
        copy.indexName = name
        return copy
    }

    // MARK: - Execution

    /// Get version history (newest first)
    ///
    /// - Returns: Array of (version, item) tuples
    public func execute() async throws -> [(version: Version, item: T)] {
        let indexName = self.indexName ?? buildDefaultIndexName()
        let typeSubspace = try await queryContext.indexSubspace(for: T.self)
        let indexSubspace = typeSubspace.subspace(indexName)

        let rawResults: [(version: Version, data: [UInt8])] = try await queryContext.withTransaction { transaction in
            let maintainer = self.createMaintainer(indexSubspace: indexSubspace, indexName: indexName)
            let pk = self.primaryKey.map { $0 as any TupleElement }
            return try await maintainer.getVersionHistory(
                primaryKey: pk,
                limit: self.limitCount,
                transaction: transaction
            )
        }

        // Deserialize items
        var results: [(version: Version, item: T)] = []
        for (version, data) in rawResults {
            if !data.isEmpty {
                do {
                    let item: T = try DataAccess.deserialize(data)
                    results.append((version: version, item: item))
                } catch {
                    // Skip items that can't be deserialized (deletion markers, etc.)
                    continue
                }
            }
        }

        return results
    }

    /// Get the latest version of the item
    ///
    /// - Returns: The latest version of the item, or nil if not found
    public func latest() async throws -> T? {
        let indexName = self.indexName ?? buildDefaultIndexName()
        let typeSubspace = try await queryContext.indexSubspace(for: T.self)
        let indexSubspace = typeSubspace.subspace(indexName)

        let data: [UInt8]? = try await queryContext.withTransaction { transaction in
            let maintainer = self.createMaintainer(indexSubspace: indexSubspace, indexName: indexName)
            let pk = self.primaryKey.map { $0 as any TupleElement }
            return try await maintainer.getLatestVersion(
                primaryKey: pk,
                transaction: transaction
            )
        }

        guard let itemData = data, !itemData.isEmpty else {
            return nil
        }

        return try DataAccess.deserialize(itemData)
    }

    /// Get version at a specific version marker
    ///
    /// - Parameter version: The version to retrieve
    /// - Returns: The item at that version, or nil if not found
    public func at(_ version: Version) async throws -> T? {
        let history = try await limit(Int.max).execute()

        // Find the version that matches or is immediately before the requested version
        for (v, item) in history {
            if v <= version {
                return item
            }
        }

        return nil
    }

    // MARK: - Private Methods

    private func buildDefaultIndexName() -> String {
        // Find the first VersionIndexKind for this type
        for descriptor in T.indexDescriptors {
            if descriptor.kindIdentifier == VersionIndexKind<T>.identifier {
                return descriptor.name
            }
        }
        return "\(T.persistableType)_version_id"
    }

    private func createMaintainer(indexSubspace: Subspace, indexName: String) -> VersionIndexMaintainer<T> {
        // Default strategy
        let strategy: VersionHistoryStrategy = .keepAll

        return VersionIndexMaintainer<T>(
            index: Index(
                name: indexName,
                kind: VersionIndexKind<T>(fieldNames: ["id"], strategy: strategy),
                rootExpression: FieldKeyExpression(fieldName: "id"),
                keyPaths: []
            ),
            strategy: strategy,
            subspace: indexSubspace,
            idExpression: FieldKeyExpression(fieldName: "id")
        )
    }
}

// MARK: - FDBContext Extension

extension FDBContext {
    /// Start a version history query
    ///
    /// This method is available when you import `VersionIndex`.
    ///
    /// **Usage**:
    /// ```swift
    /// import VersionIndex
    ///
    /// // Get version history
    /// let history = try await context.versions(Document.self)
    ///     .forItem(documentId)
    ///     .limit(10)
    ///     .execute()
    ///
    /// // Get latest version
    /// let latest = try await context.versions(Document.self)
    ///     .forItem(documentId)
    ///     .latest()
    /// ```
    ///
    /// - Parameter type: The Persistable type to query
    /// - Returns: Entry point for configuring the version query
    public func versions<T: Persistable>(_ type: T.Type) -> VersionEntryPoint<T> {
        VersionEntryPoint(queryContext: indexQueryContext)
    }
}

// MARK: - Version Query Error

/// Errors for version query operations
public enum VersionQueryError: Error, CustomStringConvertible {
    /// Index not found
    case indexNotFound(String)

    /// Item not found
    case itemNotFound

    /// Deserialization failed
    case deserializationFailed

    public var description: String {
        switch self {
        case .indexNotFound(let name):
            return "Version index not found: \(name)"
        case .itemNotFound:
            return "Item not found in version history"
        case .deserializationFailed:
            return "Failed to deserialize version data"
        }
    }
}
