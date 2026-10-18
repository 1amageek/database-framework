import DatabaseKit
import DatabaseTypes

enum SQLIndexesExecutor {
    static func execute(command: SQLDDLStatement, context: DatabaseContext,
        transaction: DatabaseTransaction, options: SQLBlockExecutionOptions,
        workMeter: DatabaseWorkMeter, parameters: [QueryParameter], request: SQLDDLRequest,
        step: Int) async throws -> SQLCommandKind {
        let lease = await transaction.currentSQLSchemaLease()
        let catalog = lease.sqlCatalog.catalog
        let currentUser = try context.readPolicy().sqlCurrentUser
        var indexes = catalog.indexes
        var relations = catalog.relations
        switch command {
        case .createIndex(let declaration):
            guard declaration.name.schema == nil else {
                throw SQLDDLExecutionError.invalidDefinition("CREATE INDEX name cannot select a schema")
            }
            let relation: SQLRelation
            let namespaceID: String
            if let view = try SQLViews.materializedView(named: declaration.table, in: lease.sqlCatalog,
                searchPath: options.searchPath, currentUser: currentUser) {
                guard let backing = catalog.relations.first(where: { $0.id == view.backingRelationID }) else {
                    throw SQLDDLExecutionError.missingObject(declaration.table.table)
                }
                relation = backing
                namespaceID = view.namespaceID
            } else {
                relation = try lease.sqlCatalog.resolveRelation(name: declaration.table.table,
                    namespace: declaration.table.schema, searchPath: options.searchPath, currentUser: currentUser).declaration
                namespaceID = relation.namespaceID
            }
            let duplicate = catalog.relations.contains { $0.namespaceID == namespaceID && $0.name == declaration.name.table }
                || catalog.sequences.contains { $0.namespaceID == namespaceID && $0.name == declaration.name.table }
                || catalog.views.contains { $0.namespaceID == namespaceID && $0.name == declaration.name.table }
                || catalog.materializedViews.contains { $0.namespaceID == namespaceID && $0.name == declaration.name.table }
                || catalog.indexes.contains { $0.namespaceID == namespaceID && $0.name == declaration.name.table }
            if duplicate {
                guard declaration.ifNotExists else { throw SQLDDLExecutionError.duplicateObject(declaration.name.table) }
                return .createIndex
            }
            let references = SelectQuery(projection: declaration.keys.map { .expression($0.expression, alias: nil) },
                source: .table(declaration.table), filter: declaration.predicate).referencedColumns
            for reference in references {
                guard reference.table == nil || reference.table == declaration.table.table,
                    relation.columns.contains(where: { $0.name == reference.column }) else {
                    throw SQLDDLExecutionError.invalidDefinition("index column does not belong to its relation")
                }
            }
            let bindings = try relation.columns.filter { column in references.contains { $0.column == column.name } }.map {
                try SQLViewColumnBinding(id: $0.id, name: $0.name,
                    type: SQLConstraintCompiler.columnType($0, relation: relation, physicalSchema: lease.schema))
            }
            let index = try SQLIndex(id: request.objectID(step: step, role: "index"), namespaceID: namespaceID,
                name: declaration.name.table, relationID: relation.id, keys: declaration.keys,
                predicate: declaration.predicate, unique: declaration.unique, nulls: declaration.nulls, bindings: bindings)
            _ = try SQLIndexCompiler.compile(index, relation: relation, physicalSchema: lease.schema, workMeter: workMeter)
            indexes.append(index)
        case .dropIndex(let names, let ifExists, let behavior):
            for name in names {
                var namespaces: [String] = []
                if let schema = name.schema {
                    if let namespace = catalog.namespaces.first(where: { $0.name == schema }) { namespaces.append(namespace.id) }
                } else {
                    for entry in options.searchPath.entries {
                        let selected: String?
                        switch entry { case .schema(let name): selected = name; case .currentUser: selected = currentUser }
                        if let selected, let namespace = catalog.namespaces.first(where: { $0.name == selected }) { namespaces.append(namespace.id) }
                    }
                }
                guard let index = namespaces.lazy.compactMap({ namespace in
                    indexes.first { $0.namespaceID == namespace && $0.name == name.table }
                }).first else {
                    if ifExists { continue }
                    throw SQLDDLExecutionError.missingObject(name.table)
                }
                for (slot, relation) in relations.enumerated() {
                    try workMeter.consume(at: .mutationPlanning)
                    let dependents = relation.constraints.filter {
                        guard case .foreignKey(let key) = $0.kind else { return false }
                        return key.referencedUniqueIndexID == index.id
                    }
                    guard !dependents.isEmpty else { continue }
                    guard behavior == .cascade else {
                        throw SQLDDLExecutionError.dependentObject(dependents[0].name)
                    }
                    relations[slot] = try SQLRelation(id: relation.id, namespaceID: relation.namespaceID,
                        name: relation.name, entityName: relation.entityName, columns: relation.columns,
                        primaryKeyColumnIDs: relation.primaryKeyColumnIDs, primaryKeyConstraint: relation.primaryKeyConstraint,
                        constraints: relation.constraints.filter { candidate in !dependents.contains { $0.id == candidate.id } },
                        fieldNumberHighWaterMark: relation.fieldNumberHighWaterMark)
                }
                indexes.removeAll { $0.id == index.id }
            }
        default: throw SQLDDLExecutionError.invalidDefinition("index executor received another DDL command")
        }
        let candidateCatalog = try SQLCatalog(namespaces: catalog.namespaces, relations: relations,
            sequences: catalog.sequences, views: catalog.views, materializedViews: catalog.materializedViews, indexes: indexes)
        let candidate = try context.container.prepareSQLSchemaCandidate(schema: lease.schema,
            catalog: candidateCatalog, sourceLease: lease, workMeter: workMeter)
        _ = try await transaction.stageSQLSchemaCandidate(candidate, expectedGeneration: lease.generation,
            idempotencyKey: request.objectID(step: step, role: "index-publication:\(lease.generation)"), rewrites: [])
        if case .createIndex = command { return .createIndex }
        return .dropIndex
    }
}
