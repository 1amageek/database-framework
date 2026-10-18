import DatabaseKit
import DatabaseTypes
import StorageKit

/// Binds one DDL command and stages its complete effects in the admitted transaction.
struct SQLDDLExecutor: Sendable {
    let context: DatabaseContext
    let transaction: DatabaseTransaction
    let options: SQLBlockExecutionOptions
    let workMeter: DatabaseWorkMeter
    let parameters: [QueryParameter]
    let request: SQLDDLRequest

    func execute(_ command: SQLDDLStatement, step: Int) async throws -> SQLCommandKind {
        try workMeter.consume(at: .mutationPlanning)
        let metadataClaim = try await reserveMetadata(for: command)
        defer { withExtendedLifetime(metadataClaim) {} }
        switch command {
        case .createSchema(let name, let ifNotExists):
            let lease = await transaction.currentSQLSchemaLease()
            let catalog = lease.sqlCatalog.catalog
            if catalog.namespaces.contains(where: { same($0.name, name) }) {
                guard ifNotExists else { throw SQLDDLExecutionError.duplicateObject(name) }
                return .createSchema
            }
            try validateNamespace(name)
            let namespace = try SQLNamespace(id: request.objectID(step: step, role: "namespace"), name: name)
            try await stage(catalog: rebuilding(catalog, namespaces: catalog.namespaces + [namespace]), step: step)
            return .createSchema
        case .renameSchema(let name, let newName):
            let lease = await transaction.currentSQLSchemaLease()
            let catalog = lease.sqlCatalog.catalog
            guard let old = catalog.namespaces.first(where: { same($0.name, name) }) else { throw SQLDDLExecutionError.missingObject(name) }
            try validateNamespace(newName)
            guard !catalog.namespaces.contains(where: { same($0.name, newName) }) else { throw SQLDDLExecutionError.duplicateObject(newName) }
            let namespaces = try catalog.namespaces.map { same($0.id, old.id) ? try SQLNamespace(id: $0.id, name: newName) : $0 }
            try await stage(catalog: rebuilding(catalog, namespaces: namespaces), step: step)
            return .alterSchema
        case .dropSchema(let names, let ifExists, let behavior):
            try await dropSchemas(names, ifExists: ifExists, behavior: behavior, step: step)
            return .dropSchema
        case .createTable(let declaration):
            try await createTable(declaration, step: step)
            return .createTable
        case .alterTable(let declaration):
            try await alterTable(declaration, step: step)
            return .alterTable
        case .dropTable(let names, let ifExists, let behavior):
            let lease = await transaction.currentSQLSchemaLease()
            var selected: [String] = []
            for name in names {
                if let relation = try resolveIfPresent(name, snapshot: lease.sqlCatalog) {
                    guard !lease.sqlCatalog.catalog.materializedViews.contains(where: { same($0.backingRelationID, relation.declaration.id) }) else {
                        throw SQLDDLExecutionError.invalidDefinition("DROP TABLE requires an ordinary table")
                    }
                    selected.append(relation.declaration.id)
                }
                else if !ifExists { throw SQLDDLExecutionError.missingObject(name.table) }
            }
            try await dropRelations(selected, behavior: behavior, step: step)
            return .dropTable
        case .createSequence, .alterSequence, .dropSequence:
            return try await executeSequence(command, step: step)
        case .createIndex, .dropIndex:
            return try await SQLIndexesExecutor.execute(command: command, context: context,
                transaction: transaction, options: options, workMeter: workMeter, parameters: parameters, request: request, step: step)
        case .createView, .dropView, .createMaterializedView, .refreshMaterializedView, .dropMaterializedView:
            return try await SQLViewsExecutor.execute(command: command, context: context,
                transaction: transaction, options: options, workMeter: workMeter, parameters: parameters, request: request, step: step)
        }
    }

    func cleanupTemporaryObjects(step: Int) async throws {
        let lease = await transaction.currentSQLSchemaLease()
        guard lease.sqlCatalog.catalog.namespaces.contains(where: { same($0.id, request.temporaryNamespace) }) else { return }
        let metadataClaim = try await reserveMetadata(for: .dropSchema(names: [request.temporaryNamespace], ifExists: true, behavior: .cascade))
        defer { withExtendedLifetime(metadataClaim) {} }
        try await dropSchemas([request.temporaryNamespace], ifExists: true, behavior: .cascade, step: step)
    }

    func stage(catalog: SQLCatalog, schema: Schema? = nil, rewrites: [SQLSchemaRewrite] = [], step: Int) async throws {
        let lease = await transaction.currentSQLSchemaLease()
        let physicalSchema = schema ?? lease.schema
        if physicalSchema != lease.schema || !rewrites.isEmpty || catalog.indexes != lease.sqlCatalog.catalog.indexes {
            var completeRewrites = rewrites
            for entity in lease.schema.entities where physicalSchema.entity(named: entity.name) == nil
                && !completeRewrites.contains(where: { same($0.entityName, entity.name) }) {
                completeRewrites.append(SQLSchemaRewrite(entityName: entity.name) { _ in nil })
            }
            let candidate = try context.container.prepareSQLSchemaCandidate(schema: physicalSchema, catalog: catalog,
                sourceLease: lease, workMeter: workMeter)
            _ = try await transaction.stageSQLSchemaCandidate(candidate, expectedGeneration: lease.generation,
                idempotencyKey: request.objectID(step: step, role: "publication:\(lease.generation)"), rewrites: completeRewrites)
        } else {
            _ = try await transaction.stageSQLCatalog(catalog, expectedGeneration: lease.generation,
                idempotencyKey: request.objectID(step: step, role: "publication:\(lease.generation)"))
        }
    }

    func rebuilding(_ catalog: SQLCatalog, namespaces: [SQLNamespace]? = nil,
        relations: [SQLRelation]? = nil, sequences: [SQLSequence]? = nil) throws -> SQLCatalog {
        try SQLCatalog(namespaces: namespaces ?? catalog.namespaces, relations: relations ?? catalog.relations,
            sequences: sequences ?? catalog.sequences, views: catalog.views, materializedViews: catalog.materializedViews, indexes: catalog.indexes)
    }

    func same(_ lhs: String, _ rhs: String) -> Bool { lhs.utf8.elementsEqual(rhs.utf8) }

    func resolveIfPresent(_ name: TableRef, snapshot: SQLCatalogSnapshot) throws -> SQLCatalogResolvedRelation? {
        let user = try context.readPolicy().sqlCurrentUser
        let names: [String]
        if let schema = name.schema { names = [schema] }
        else { names = options.searchPath.entries.compactMap { entry in
            switch entry { case .schema(let value): return value; case .currentUser: return user }
        } }
        for namespaceName in names {
            guard let namespace = snapshot.catalog.namespaces.first(where: { same($0.name, namespaceName) }) else { continue }
            if let relation = snapshot.catalog.relations.first(where: { same($0.namespaceID, namespace.id) && same($0.name, name.table) }),
                !snapshot.catalog.materializedViews.contains(where: { same($0.backingRelationID, relation.id) }) {
                return try snapshot.resolveRelation(name: name.table, namespace: namespace.name,
                    searchPath: options.searchPath, currentUser: user)
            }
            if nameExists(name.table, namespaceID: namespace.id, catalog: snapshot.catalog) {
                throw SQLDDLExecutionError.invalidDefinition("operation requires an ordinary table")
            }
        }
        return nil
    }

    func namespace(for name: TableRef, catalog: SQLCatalog, temporary: Bool = false) throws -> SQLNamespace {
        if temporary {
            guard name.schema == nil else { throw SQLDDLExecutionError.invalidDefinition("temporary table cannot select an explicit schema") }
            return try SQLNamespace(id: request.temporaryNamespace, name: request.temporaryNamespace)
        }
        if let explicit = name.schema {
            guard let namespace = catalog.namespaces.first(where: { same($0.name, explicit) }) else { throw SQLDDLExecutionError.missingObject(explicit) }
            return namespace
        }
        let user = try context.readPolicy().sqlCurrentUser
        for entry in options.searchPath.entries {
            let selected: String?
            switch entry { case .schema(let name): selected = name; case .currentUser: selected = user }
            if let selected, let namespace = catalog.namespaces.first(where: { same($0.name, selected) }),
                !same(namespace.id, request.temporaryNamespace) { return namespace }
        }
        throw SQLDDLExecutionError.invalidDefinition("search path has no existing creation schema")
    }

    func nameExists(_ name: String, namespaceID: String, catalog: SQLCatalog) -> Bool {
        catalog.relations.contains { same($0.namespaceID, namespaceID) && same($0.name, name) }
            || catalog.sequences.contains { same($0.namespaceID, namespaceID) && same($0.name, name) }
            || catalog.indexes.contains { same($0.namespaceID, namespaceID) && same($0.name, name) }
            || catalog.views.contains { same($0.namespaceID, namespaceID) && same($0.name, name) }
            || catalog.materializedViews.contains { same($0.namespaceID, namespaceID) && same($0.name, name) }
    }

    private func validateNamespace(_ name: String) throws {
        guard !name.hasPrefix("pg_"), !name.hasPrefix("sql_temp_") else { throw SQLDDLExecutionError.invalidDefinition("reserved schema name") }
    }
}
