// VersionQuery.swift
// VersionIndex - Query extension for version history indexes
//
// Provides DatabaseContext extension and query builder for temporal versioning.

import DatabaseTypes
import DatabaseKit
import DatabaseEngine
import StorageKit

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

    internal var currentTimestamp: Timestamp {
        queryContext.context.container.wallClock.now
    }

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
        let response = try await queryContext.context.query(
            try toSelectQuery(),
            as: T.self,
            options: .default
        )

        return try response.rows.map { row in
            let item = try QueryRowCodec.decode(row, as: T.self)
            guard let versionData = row.annotations["version"]?.bytesValue else {
                throw VersionQueryError.invalidResponse("Missing version annotation")
            }
            return (
                version: Version(bytes: versionData),
                item: item
            )
        }
    }

    internal func executeDirect(
        configuration: TransactionConfiguration = .default
    ) async throws -> [(version: Version, item: T)] {
        let indexName = try self.indexName ?? resolveIndexName()
        let typeSubspace = try await queryContext.indexSubspace(for: T.self)
        let indexSubspace = typeSubspace.subspace(indexName)

        let rawResults: [(version: Version, data: ByteString)] = try await queryContext.withTransaction(configuration: configuration) { transaction in
            let maintainer = try self.createMaintainer(
                indexSubspace: indexSubspace,
                indexName: indexName
            )
            let pk = self.primaryKey.map { $0 as any TupleElement }
            return try await maintainer.getVersionHistory(
                primaryKey: pk,
                limit: self.limitCount,
                transaction: transaction
            )
        }

        // Deserialize items (empty data marks deletion and is skipped by design)
        var results: [(version: Version, item: T)] = []
        for (version, data) in rawResults {
            guard !data.isEmpty else { continue }
            let item: T = try DataAccess.deserialize(data)
            results.append((version: version, item: item))
        }

        return results
    }

    /// Get the latest version of the item
    ///
    /// - Returns: The latest version of the item, or nil if not found
    public func latest() async throws -> T? {
        try await limit(1).execute().first?.item
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

    internal func toSelectQuery() throws -> SelectQuery {
        let queryLimit: UInt64?
        if let limitCount {
            guard let converted = UInt64(exactly: limitCount) else {
                throw VersionQueryError.invalidLimit(limitCount)
            }
            queryLimit = converted
        } else {
            queryLimit = nil
        }

        var parameters: [String: FieldValue] = [
            VersionReadParameter.primaryKey: .array(
                try primaryKey.map { try DatabaseEngine.CanonicalTupleElementCodec.encode($0) }
            )
        ]
        if let queryLimit {
            parameters[VersionReadParameter.limit] = .uint64(queryLimit)
        }
        if let indexName {
            parameters[VersionReadParameter.indexName] = .string(indexName)
        }

        return SelectQuery(
            projection: .all,
            source: .table(TableRef(table: T.persistableType)),
            accessPath: .index(
                IndexScanSource(
                    indexName: try self.indexName ?? resolveIndexName(),
                    kindIdentifier: "version",
                    parameters: parameters
                )
            ),
            limit: queryLimit
        )
    }

    // MARK: - Private Methods

    private func resolveIndexName() throws -> String {
        guard let descriptor = try T.indexDescriptors.first(where: {
            $0.kindIdentifier == "version"
        }) else {
            throw VersionQueryError.indexNotFound(
                "No version index is declared for \(T.persistableType)"
            )
        }
        return descriptor.name
    }

    private func createMaintainer(
        indexSubspace: Subspace,
        indexName: String
    ) throws -> VersionIndexMaintainer<T> {
        guard let descriptor = try T.indexDescriptors.first(where: {
            $0.name == indexName && $0.kindIdentifier == "version"
        }) else {
            throw VersionQueryError.indexNotFound(indexName)
        }
        guard let strategyName = descriptor.kind.metadata["strategy"]?.stringValue else {
            throw VersionQueryError.invalidResponse(
                "Version index '\(indexName)' is missing strategy metadata"
            )
        }
        let strategy: VersionHistoryStrategy
        switch strategyName {
        case "keepAll":
            strategy = .keepAll
        case "keepLast":
            let count = try descriptor.kind.requireInt("strategyCount")
            strategy = .keepLast(count)
        case "keepForDuration":
            let duration = try descriptor.kind.requireTimeSpan(
                "strategyDuration"
            )
            strategy = .keepForDuration(duration)
        default:
            throw VersionQueryError.invalidResponse(
                "Version index '\(indexName)' has unknown strategy '\(strategyName)'"
            )
        }
        let fieldNames = descriptor.fieldNames

        let rootFieldName = fieldNames.first ?? "id"

        return VersionIndexMaintainer<T>(
            index: Index(
                name: indexName,
                kind: descriptor.kind,
                rootExpression: FieldKeyExpression(fieldName: rootFieldName)
            ),
            strategy: strategy,
            subspace: indexSubspace,
            idExpression: FieldKeyExpression(fieldName: "id"),
            wallClock: queryContext.context.container.wallClock
        )
    }
}

// MARK: - DatabaseContext Extension

extension DatabaseContext {
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

    /// Canonical query response is missing required metadata
    case invalidResponse(String)
    case invalidLimit(Int)

    public var description: String {
        switch self {
        case .indexNotFound(let name):
            return "Version index not found: \(name)"
        case .itemNotFound:
            return "Item not found in version history"
        case .deserializationFailed:
            return "Failed to deserialize version data"
        case .invalidResponse(let reason):
            return "Invalid version query response: \(reason)"
        case .invalidLimit(let limit):
            return "Version query limit must be a nonnegative Int64 value: \(limit)"
        }
    }
}
