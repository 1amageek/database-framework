import DatabaseKit

/// Pure catalog validation and stable dependency closure for views.
enum SQLViews {
    static func validate(catalog: SQLCatalog, physicalSchema: Schema) throws {
        let definitions = catalog.views.map { ($0.id, $0.definition) }
            + catalog.materializedViews.map { ($0.id, $0.definition) }
        let byID = Dictionary(uniqueKeysWithValues: definitions.map { (SQLCatalogName($0.0), $0.1) })
        var completed = Set<SQLCatalogName>()
        var visiting = Set<SQLCatalogName>()
        func visit(_ id: String) throws {
            let key = SQLCatalogName(id)
            if completed.contains(key) { return }
            guard visiting.insert(key).inserted else { throw SQLViewExecutionError.cyclicDependency(id) }
            guard let definition = byID[key] else { throw SQLViewExecutionError.missingDependency(id) }
            for binding in definition.bindings {
                let columns: [(id: String, type: QueryColumnType)]
                if let nested = byID[SQLCatalogName(binding.relationID)] {
                    try visit(binding.relationID)
                    columns = nested.columns.map { ($0.id, $0.type) }
                } else if let relation = catalog.relations.first(where: {
                    SQLCatalogName($0.id) == SQLCatalogName(binding.relationID)
                }), let entity = physicalSchema.entity(named: relation.entityName) {
                    columns = try relation.columns.map { column in
                        switch column.backing {
                        case .field(let number):
                            guard let field = entity.fieldMapByNumber[number] else {
                                throw SQLViewExecutionError.missingColumn(relation: binding.relationID, column: column.id)
                            }
                            return (column.id, field.queryType ?? .primitive(field.type, isArray: field.isArray))
                        case .virtual(let type): return (column.id, type)
                        }
                    }
                } else { throw SQLViewExecutionError.missingDependency(binding.relationID) }
                for column in binding.columns {
                    guard let current = columns.first(where: { SQLCatalogName($0.id) == SQLCatalogName(column.id) }) else {
                        throw SQLViewExecutionError.missingColumn(relation: binding.relationID, column: column.id)
                    }
                    guard current.type == column.type else {
                        throw SQLViewExecutionError.incompatibleColumn(relation: binding.relationID, column: column.id)
                    }
                }
            }
            visiting.remove(key); completed.insert(key)
        }
        for (id, _) in definitions { try visit(id) }
        for view in catalog.materializedViews {
            guard let backing = catalog.relations.first(where: {
                SQLCatalogName($0.id) == SQLCatalogName(view.backingRelationID)
            }), let entity = physicalSchema.entity(named: backing.entityName),
                backing.columns.count == view.definition.columns.count else {
                throw SQLViewExecutionError.missingDependency(view.backingRelationID)
            }
            for (column, output) in zip(backing.columns, view.definition.columns) {
                guard SQLCatalogName(column.id) == SQLCatalogName(output.id),
                      case .field(let number) = column.backing,
                      let field = entity.fieldMapByNumber[number],
                      field.queryType == output.type else {
                    throw SQLViewExecutionError.incompatibleColumn(relation: view.id, column: output.id)
                }
            }
        }
    }

    static func dependentIDs(catalog: SQLCatalog, relationIDs: [String],
        removedColumns: [String: [String]] = [:]) -> [String] {
        var affected = Set(relationIDs.map(SQLCatalogName.init))
        var result: [String] = []
        let definitions = catalog.views.map { ($0.id, $0.definition) }
            + catalog.materializedViews.map { ($0.id, $0.definition) }
        var changed = true
        while changed {
            changed = false
            for (id, definition) in definitions where !affected.contains(SQLCatalogName(id)) {
                let depends = definition.bindings.contains { binding in
                    if affected.contains(SQLCatalogName(binding.relationID)) { return true }
                    return removedColumns.first(where: {
                        SQLCatalogName($0.key) == SQLCatalogName(binding.relationID)
                    }).map { removed in
                        binding.columns.contains { column in removed.value.contains {
                            SQLCatalogName($0) == SQLCatalogName(column.id)
                        } }
                    } ?? false
                }
                if depends { affected.insert(SQLCatalogName(id)); result.append(id); changed = true }
            }
        }
        return result
    }

    static func removing(ids: [String], from catalog: SQLCatalog) throws -> SQLCatalog {
        let removed = Set(ids.map(SQLCatalogName.init))
        let backings = Set(catalog.materializedViews.filter { removed.contains(SQLCatalogName($0.id)) }
            .map { SQLCatalogName($0.backingRelationID) })
        return try SQLCatalog(namespaces: catalog.namespaces,
            relations: catalog.relations.filter { !backings.contains(SQLCatalogName($0.id)) },
            sequences: catalog.sequences,
            views: catalog.views.filter { !removed.contains(SQLCatalogName($0.id)) },
            materializedViews: catalog.materializedViews.filter { !removed.contains(SQLCatalogName($0.id)) },
            indexes: catalog.indexes.filter { !backings.contains(SQLCatalogName($0.relationID)) })
    }

    static func validateReplacement(_ old: SQLViewDefinition, with new: SQLViewDefinition,
        name: String) throws {
        guard new.columns.count >= old.columns.count else { throw SQLViewExecutionError.incompatibleReplacement(name) }
        for (old, new) in zip(old.columns, new.columns) {
            guard SQLCatalogName(old.name) == SQLCatalogName(new.name), old.type == new.type else {
                throw SQLViewExecutionError.incompatibleReplacement(name)
            }
        }
    }
}

extension SQLViews {
    static func materializedView(named table: TableRef, in snapshot: SQLCatalogSnapshot,
        searchPath: SQLSearchPath, currentUser: String?) throws -> SQLMaterializedView? {
        let paths: [String]
        if let schema = table.schema { paths = [schema] }
        else { paths = searchPath.entries.compactMap {
            switch $0 { case .schema(let name): return name; case .currentUser: return currentUser }
        } }
        for path in paths {
            guard let namespace = snapshot.catalog.namespaces.first(where: { CanonicalSourceName.equal($0.name, path) }) else { continue }
            if let view = snapshot.catalog.materializedViews.first(where: {
                SQLCatalogName($0.namespaceID) == SQLCatalogName(namespace.id) && CanonicalSourceName.equal($0.name, table.table)
            }) { return view }
            if snapshot.catalog.relations.contains(where: {
                SQLCatalogName($0.namespaceID) == SQLCatalogName(namespace.id) && CanonicalSourceName.equal($0.name, table.table)
            }) || snapshot.catalog.views.contains(where: {
                SQLCatalogName($0.namespaceID) == SQLCatalogName(namespace.id) && CanonicalSourceName.equal($0.name, table.table)
            }) { return nil }
        }
        return nil
    }
}
