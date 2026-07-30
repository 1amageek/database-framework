/// Validated context for read-only catalog and raw-storage inspection.

import StorageKit
import DatabaseEngine
import DatabaseKit

/// Opens only databases with the canonical persisted format descriptor.
public struct CatalogDataAccess: Sendable {
    private let transactionExecutor: StorageTransactionExecutor
    private let clock: any StorageMonotonicClock
    private let entities: [String: Schema.Entity]

    public static func open(
        database: any StorageEngine,
        clock: any StorageMonotonicClock
    ) async throws -> CatalogDataAccess {
        _ = try await DatabaseFormatCatalog(
            database: database,
            clock: clock
        ).loadRequired()
        let entities = try await SchemaRegistry(
            database: database,
            clock: clock
        ).loadAll()
        return try CatalogDataAccess(
            database: database,
            clock: clock,
            entities: entities
        )
    }

    package init(
        database: any StorageEngine,
        clock: any StorageMonotonicClock,
        entities: [Schema.Entity]
    ) throws {
        self.transactionExecutor = StorageTransactionExecutor(engine: database)
        self.clock = clock
        var map: [String: Schema.Entity] = [:]
        for entity in entities {
            guard map[entity.name] == nil else {
                throw CLIError.invalidArguments(
                    "Duplicate catalog entity '\(entity.name)'"
                )
            }
            map[entity.name] = entity
        }
        self.entities = map
    }

    package var allEntities: [Schema.Entity] {
        entities.values.sorted { $0.name < $1.name }
    }

    package func withTransaction<Value: Sendable>(
        _ operation: @escaping @Sendable (
            any TransactionAccess
        ) async throws -> Value
    ) async throws -> Value {
        try await transactionExecutor.withTransaction(
            configuration: .default,
            clock: clock,
            operation
        )
    }
}
