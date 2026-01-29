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

    public init(database: any DatabaseProtocol) {
        self.database = database
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
    }

    // MARK: - Read

    /// Load all TypeCatalog entries from FDB
    public func loadAll() async throws -> [TypeCatalog] {
        let prefix = Tuple([Self.catalogPrefix]).pack()
        let subspace = Subspace(prefix: prefix)
        let (begin, end) = subspace.range()

        return try await database.withTransaction { transaction in
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

    // MARK: - Key Construction

    /// Build FDB key for a catalog entry: (_catalog, typeName) as Tuple
    private static func key(for typeName: String) -> FDB.Bytes {
        Tuple([catalogPrefix, typeName]).pack()
    }
}
