/// SchemaRegistry - Persists and loads Schema.Entity in FoundationDB
///
/// Analogous to PostgreSQL's `pg_catalog` system tables.
/// Stores entity metadata under `(_schema, entityName)`.
/// Enables CLI and dynamic tools to discover and decode data without compiled types.

import DatabaseTypes
import StorageKit
import DatabaseKit

/// Manages persistence and retrieval of Schema.Entity entries in FDB
public struct SchemaRegistry: Sendable {
    private let transactionExecutor: StorageTransactionExecutor
    private let clock: any StorageMonotonicClock

    /// Schema catalog inside the control-domain root.
    private let catalog: Subspace

    /// In-memory cache for entities (reduces CLI latency by 10-100x)
    private let cache: SchemaCatalogCache

    public init(
        database: any StorageEngine,
        root: Subspace,
        clock: any StorageMonotonicClock,
        cacheTTLSeconds: Int = 300
    ) {
        self.transactionExecutor = StorageTransactionExecutor(engine: database)
        self.catalog = root.subspace("_schema")
        self.clock = clock
        self.cache = SchemaCatalogCache(
            timeToLive: .seconds(Int64(cacheTTLSeconds)),
            monotonicClock: clock
        )
    }

    // MARK: - Write

    /// Persist all entities and ontology from a Schema
    ///
    /// Called during `DBContainer.init` after `ensureIndexesReady()`.
    /// Replaces changed entries. Entity and ontology are updated atomically.
    public func persist(
        _ schema: Schema,
        mode: SchemaRegistryPersistMode = .strict
    ) async throws {
        try await transactionExecutor.withTransaction(
            configuration: .default,
            clock: clock
        ) { transaction in
            try await persist(schema, mode: mode, transaction: transaction)
        }

        // Invalidate cache after schema changes
        cache.clear()
    }

    /// Atomically replaces the active schema catalog in a caller-owned transaction.
    ///
    /// Removed entities are cleared in the same transaction as updated entities so
    /// the persisted catalog is an exact projection of the compiled schema.
    func persist(
        _ schema: Schema,
        mode: SchemaRegistryPersistMode = .strict,
        transaction: any TransactionAccess
    ) async throws {
        let entities = schema.entities
        try await validateCompatibility(
            of: entities,
            mode: mode,
            transaction: transaction
        )

        let targetNames = Set(entities.map { $0.name })
        let catalogRange = catalog.range()
        let existingRows = try await TransactionRangeCollection.collect(using: transaction,
            from: .firstGreaterOrEqual(catalogRange.begin),
            to: .firstGreaterOrEqual(catalogRange.end),
            limit: 0,
            reverse: false,
            snapshot: false,
            streamingMode: .wantAll
        )
        var existingValuesByName: [String: ByteString] = [:]
        existingValuesByName.reserveCapacity(existingRows.count)
        for (storedKey, value) in existingRows {
            let existing = try SchemaEntityEntryCodec.decode(value)
            let expectedKey = key(for: existing.name)
            if !targetNames.contains(existing.name) || storedKey != expectedKey {
                try transaction.clear(key: storedKey)
                continue
            }
            existingValuesByName[existing.name] = value
        }
        for entity in entities {
            let encodedEntity = try SchemaEntityEntryCodec.encode(entity)
            if existingValuesByName[entity.name] != encodedEntity {
                try transaction.setValue(
                    encodedEntity,
                    for: key(for: entity.name)
                )
            }
        }
        cache.clear()
    }

    /// Writes the initial compiled schema into an existing bootstrap transaction.
    ///
    /// Compatibility validation is intentionally owned by the versioned bootstrap
    /// caller, which holds the schema-version conflict key in the same transaction.
    func persistInitialSchema(
        _ schema: Schema,
        transaction: any TransactionAccess
    ) async throws {
        let targetNames = Set(schema.entities.map { $0.name })
        let catalogRange = catalog.range()
        let existingRows = try await TransactionRangeCollection.collect(using: transaction,
            from: .firstGreaterOrEqual(catalogRange.begin),
            to: .firstGreaterOrEqual(catalogRange.end),
            limit: 0,
            reverse: false,
            snapshot: false,
            streamingMode: .wantAll
        )
        for (key, value) in existingRows {
            let existing = try SchemaEntityEntryCodec.decode(value)
            if !targetNames.contains(existing.name) {
                try transaction.clear(key: key)
            }
        }
        for entity in schema.entities {
            try transaction.setValue(
                try SchemaEntityEntryCodec.encode(entity),
                for: key(for: entity.name)
            )
        }
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

    /// Loads the complete catalog from a caller-owned transaction so schema,
    /// version, fingerprint, and generation can share one storage snapshot.
    package func loadAll(
        transaction: any TransactionAccess
    ) async throws -> [Schema.Entity] {
        let range = catalog.range()
        let rows = try await TransactionRangeCollection.collect(
            using: transaction,
            from: .firstGreaterOrEqual(range.begin),
            to: .firstGreaterOrEqual(range.end),
            limit: 0,
            reverse: false,
            snapshot: false,
            streamingMode: .wantAll
        )
        var entities: [Schema.Entity] = []
        entities.reserveCapacity(rows.count)
        for (_, value) in rows {
            entities.append(try SchemaEntityEntryCodec.decode(value))
        }
        return entities.sorted { $0.name < $1.name }
    }

    /// Load a single Schema.Entity by name
    public func load(typeName: String) async throws -> Schema.Entity? {
        let key = key(for: typeName)

        return try await transactionExecutor.withTransaction(configuration: .default, clock: clock) { transaction in
            guard let value = try await transaction.getValue(for: key, snapshot: true) else {
                return nil
            }
            return try SchemaEntityEntryCodec.decode(value)
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

        try await transactionExecutor.withTransaction(configuration: .default, clock: clock) { transaction in
            let key = key(for: entity.name)
            let value = try SchemaEntityEntryCodec.encode(entity)
            try transaction.setValue(value, for: key)
        }

        // Invalidate cache after schema change
        cache.clear()
    }

    /// Delete a single Schema.Entity entry
    ///
    /// Used by CLI to drop schema definitions.
    /// No-op if entry doesn't exist.
    public func delete(typeName: String) async throws {
        let key = key(for: typeName)
        try await transactionExecutor.withTransaction(configuration: .default, clock: clock) { transaction in
            try transaction.clear(key: key)
        }

        // Invalidate cache after schema deletion
        cache.clear()
    }

    // MARK: - Key Construction

    /// Build FDB key for a schema entry: (_schema, typeName) as Tuple
    private func key(for typeName: String) -> ByteString {
        catalog.pack(Tuple(typeName))
    }

    private func loadAllFromStorage() async throws -> [Schema.Entity] {
        let (begin, end) = catalog.range()

        return try await transactionExecutor.withTransaction(configuration: .default, clock: clock) { transaction in
            var entities: [Schema.Entity] = []

            let sequence = try await TransactionRangeCollection.collect(using: transaction,
                from: .firstGreaterOrEqual(begin),
                to: .firstGreaterOrEqual(end),
                limit: 0,
                reverse: false,
                snapshot: true,
                streamingMode: .wantAll
            )
            for (_, value) in sequence {
                let entity = try SchemaEntityEntryCodec.decode(value)
                entities.append(entity)
            }
            return entities
        }
    }

    private func validateCompatibility(
        of entities: [Schema.Entity],
        mode: SchemaRegistryPersistMode
    ) async throws {
        try await transactionExecutor.withTransaction(configuration: .default, clock: clock) { transaction in
            try await validateCompatibility(
                of: entities,
                mode: mode,
                transaction: transaction
            )
        }
    }

    private func validateCompatibility(
        of entities: [Schema.Entity],
        mode: SchemaRegistryPersistMode,
        transaction: any TransactionAccess
    ) async throws {
        let names = Set(entities.map { $0.name })
        var existingByName: [String: Schema.Entity] = [:]
        existingByName.reserveCapacity(names.count)
        for name in names {
            let key = key(for: name)
            guard let value = try await transaction.getValue(
                for: key,
                snapshot: false
            ) else {
                continue
            }
            existingByName[name] = try SchemaEntityEntryCodec.decode(value)
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
