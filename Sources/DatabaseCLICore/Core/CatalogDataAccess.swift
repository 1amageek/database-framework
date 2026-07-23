/// Validated context for read-only catalog and raw-storage inspection.

import StorageKit
import DatabaseEngine
import Core

/// Opens only databases with the canonical persisted format descriptor.
public struct CatalogDataAccess: Sendable {
    public let database: any StorageEngine
    private let entities: [String: Schema.Entity]

    public static func open(
        database: any StorageEngine
    ) async throws -> CatalogDataAccess {
        _ = try await DatabaseFormatCatalog(
            database: database
        ).loadRequired()
        let entities = try await SchemaRegistry(database: database).loadAll()
        return try CatalogDataAccess(
            database: database,
            entities: entities
        )
    }

    package init(
        database: any StorageEngine,
        entities: [Schema.Entity]
    ) throws {
        self.database = database
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
}
