/// SchemaRegistry - Persists and loads TypeCatalog entries in FoundationDB
///
/// Analogous to PostgreSQL's `pg_catalog` system tables.
/// Stores type metadata under `/_catalog/[typeName]` in FDB as JSON.
/// Enables CLI and dynamic tools to discover and decode data without compiled types.

import Foundation
import FoundationDB
import Core

/// Manages persistence and retrieval of TypeCatalog entries in FDB
public struct SchemaRegistry: Sendable {
    nonisolated(unsafe) private let database: any DatabaseProtocol

    /// Catalog key prefix
    private static let catalogPrefix = "_catalog"

    /// In-memory cache for catalogs (reduces CLI latency by 10-100x)
    private let cache: SchemaCatalogCache

    public init(database: any DatabaseProtocol, cacheTTLSeconds: Int = 300) {
        self.database = database
        self.cache = SchemaCatalogCache(ttlSeconds: cacheTTLSeconds)
    }

    // MARK: - Write

    /// Persist all entities from a Schema as TypeCatalog entries
    ///
    /// Called during `FDBContainer.init` after `ensureIndexesReady()`.
    /// Overwrites existing catalog entries.
    public func persist(_ schema: Schema) async throws {
        let catalogs = try schema.entities.map { try TypeCatalog(from: $0) }
        let encoder = JSONEncoder()
        encoder.outputFormatting = [.sortedKeys]

        try await database.withTransaction { transaction in
            for catalog in catalogs {
                let key = Self.key(for: catalog.typeName)
                let data = try encoder.encode(catalog)
                let value = Array(data)
                transaction.setValue(value, for: key)
            }
        }

        // Invalidate cache after schema changes
        cache.clear()
    }

    // MARK: - Read

    /// Load all TypeCatalog entries from FDB
    ///
    /// **Cache Strategy**: Returns cached catalogs if TTL not expired, otherwise fetches from FDB.
    public func loadAll() async throws -> [TypeCatalog] {
        // Check cache first
        if let cached = cache.get() {
            return cached
        }

        // Cache miss - fetch from FDB
        let prefix = Tuple([Self.catalogPrefix]).pack()
        let subspace = Subspace(prefix: prefix)
        let (begin, end) = subspace.range()

        let catalogs = try await database.withTransaction { transaction in
            var catalogs: [TypeCatalog] = []
            let decoder = JSONDecoder()

            let sequence = transaction.getRange(begin: begin, end: end, snapshot: true)
            for try await (_, value) in sequence {
                let data = Data(value)
                let catalog = try decoder.decode(TypeCatalog.self, from: data)
                catalogs.append(catalog)
            }
            return catalogs
        }

        // Populate cache
        cache.set(catalogs)

        return catalogs
    }

    /// Load a single TypeCatalog by type name
    public func load(typeName: String) async throws -> TypeCatalog? {
        let key = Self.key(for: typeName)

        return try await database.withTransaction { transaction in
            guard let value = try await transaction.getValue(for: key, snapshot: true) else {
                return nil
            }
            let data = Data(value)
            return try JSONDecoder().decode(TypeCatalog.self, from: data)
        }
    }

    // MARK: - Single TypeCatalog Operations

    /// Persist a single TypeCatalog entry
    ///
    /// Used by CLI to apply individual schema definitions.
    /// Overwrites existing catalog entry if present.
    public func persist(_ catalog: TypeCatalog) async throws {
        let encoder = JSONEncoder()
        encoder.outputFormatting = [.sortedKeys]

        try await database.withTransaction { transaction in
            let key = Self.key(for: catalog.typeName)
            let data = try encoder.encode(catalog)
            let value = Array(data)
            transaction.setValue(value, for: key)
        }

        // Invalidate cache after schema change
        cache.clear()
    }

    /// Delete a single TypeCatalog entry
    ///
    /// Used by CLI to drop schema definitions.
    /// No-op if entry doesn't exist.
    public func delete(typeName: String) async throws {
        let key = Self.key(for: typeName)
        try await database.withTransaction { transaction in
            transaction.clear(key: key)
        }

        // Invalidate cache after schema deletion
        cache.clear()
    }

    // MARK: - Key Construction

    /// Build FDB key for a catalog entry: (_catalog, typeName) as Tuple
    private static func key(for typeName: String) -> FDB.Bytes {
        Tuple([catalogPrefix, typeName]).pack()
    }
}
