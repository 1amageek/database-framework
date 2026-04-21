/// SchemaRegistry - Persists and loads Schema.Entity in FoundationDB
///
/// Analogous to PostgreSQL's `pg_catalog` system tables.
/// Stores entity metadata under `(_schema, entityName)`.
/// Enables CLI and dynamic tools to discover and decode data without compiled types.

import Foundation
import StorageKit
import Core

/// Manages persistence and retrieval of Schema.Entity entries in FDB
public struct SchemaRegistry: Sendable {
    nonisolated(unsafe) private let database: any StorageEngine

    /// Schema key prefix
    private static let catalogPrefix = "_schema"

    /// In-memory cache for entities (reduces CLI latency by 10-100x)
    private let cache: SchemaCatalogCache

    public init(database: any StorageEngine, cacheTTLSeconds: Int = 300) {
        self.database = database
        self.cache = SchemaCatalogCache(ttlSeconds: cacheTTLSeconds)
    }

    // MARK: - Write

    /// Persist all entities and ontology from a Schema
    ///
    /// Called during `DBContainer.init` after `ensureIndexesReady()`.
    /// Overwrites existing entries. Entity and ontology are written atomically.
    public func persist(
        _ schema: Schema,
        mode: SchemaRegistryPersistMode = .strict
    ) async throws {
        let entities = schema.entities
        try await validateCompatibility(of: entities, mode: mode)
        let encoder = JSONEncoder()
        encoder.outputFormatting = [.sortedKeys]

        try await database.withTransaction { transaction in
            // Persist entity metadata
            for entity in entities {
                let key = Self.key(for: entity.name)
                let data = try encoder.encode(entity)
                let value = Array(data)
                transaction.setValue(value, for: key)
            }

        }

        // Invalidate cache after schema changes
        cache.clear()
    }

    // MARK: - Read

    /// Load all Schema.Entity entries from FDB
    ///
    /// **Cache Strategy**: Returns cached entities if TTL not expired, otherwise fetches from FDB.
    public func loadAll() async throws -> [Schema.Entity] {
        // Check cache first
        if let cached = cache.get() {
            return cached
        }

        // Cache miss - fetch from FDB
        let entities = try await loadAllFromStorage()

        // Populate cache
        cache.set(entities)

        return entities
    }

    /// Load a single Schema.Entity by name
    public func load(typeName: String) async throws -> Schema.Entity? {
        let key = Self.key(for: typeName)

        return try await database.withTransaction { transaction in
            guard let value = try await transaction.getValue(for: key, snapshot: true) else {
                return nil
            }
            let data = Data(value)
            return try JSONDecoder().decode(Schema.Entity.self, from: data)
        }
    }

    // MARK: - Single Entity Operations

    /// Persist a single Schema.Entity entry
    ///
    /// Used by CLI to apply individual schema definitions.
    /// Overwrites existing entry if present.
    public func persist(
        _ entity: Schema.Entity,
        mode: SchemaRegistryPersistMode = .strict
    ) async throws {
        try await validateCompatibility(of: [entity], mode: mode)
        let encoder = JSONEncoder()
        encoder.outputFormatting = [.sortedKeys]

        try await database.withTransaction { transaction in
            let key = Self.key(for: entity.name)
            let data = try encoder.encode(entity)
            let value = Array(data)
            transaction.setValue(value, for: key)
        }

        // Invalidate cache after schema change
        cache.clear()
    }

    /// Delete a single Schema.Entity entry
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

    /// Build FDB key for a schema entry: (_schema, typeName) as Tuple
    private static func key(for typeName: String) -> Bytes {
        Tuple([catalogPrefix, typeName]).pack()
    }

    private func loadAllFromStorage() async throws -> [Schema.Entity] {
        let prefix = Tuple([Self.catalogPrefix]).pack()
        let subspace = Subspace(prefix: prefix)
        let (begin, end) = subspace.range()

        return try await database.withTransaction { transaction in
            var entities: [Schema.Entity] = []
            let decoder = JSONDecoder()

            let sequence = try await transaction.collectRange(
                from: .firstGreaterOrEqual(begin),
                to: .firstGreaterOrEqual(end),
                snapshot: true
            )
            for (_, value) in sequence {
                let data = Data(value)
                let entity = try decoder.decode(Schema.Entity.self, from: data)
                entities.append(entity)
            }
            return entities
        }
    }

    private func validateCompatibility(
        of entities: [Schema.Entity],
        mode: SchemaRegistryPersistMode
    ) async throws {
        let names = Array(Set(entities.map(\.name)))
        let existingByName: [String: Schema.Entity] = try await database.withTransaction { transaction in
            var existing: [String: Schema.Entity] = [:]
            let decoder = JSONDecoder()

            for name in names {
                let key = Self.key(for: name)
                guard let value = try await transaction.getValue(for: key, snapshot: true) else {
                    continue
                }
                let entity = try decoder.decode(Schema.Entity.self, from: Data(value))
                existing[name] = entity
            }

            return existing
        }

        for entity in entities {
            guard let existing = existingByName[entity.name] else {
                continue
            }

            let report = entity.compatibilityReport(from: existing)
            guard !report.isCompatible else {
                continue
            }

            guard mode.allowsBreakingChanges(for: entity.name) else {
                throw SchemaRegistryError.incompatibleEntityEvolution(
                    entityName: entity.name,
                    issues: report.issues
                )
            }
        }
    }
}

public enum SchemaRegistryPersistMode: Sendable {
    case strict
    case allowBreakingChanges(entityNames: Set<String>)

    fileprivate func allowsBreakingChanges(for entityName: String) -> Bool {
        switch self {
        case .strict:
            return false
        case .allowBreakingChanges(let entityNames):
            return entityNames.contains(entityName)
        }
    }
}

public enum SchemaRegistryError: Error, CustomStringConvertible, Sendable {
    case incompatibleEntityEvolution(entityName: String, issues: [SchemaCompatibilityIssue])

    public var description: String {
        switch self {
        case .incompatibleEntityEvolution(let entityName, let issues):
            let summary = issues.map { $0.description }.joined(separator: " | ")
            return "Schema entity '\(entityName)' is not append-only compatible with the persisted registry entry: \(summary)"
        }
    }
}
