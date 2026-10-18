import DatabaseKit

extension SQLReadResolution {
    private func sourceBinding(_ table: TableRef) -> SQLViewSourceBinding? {
        viewBindings?.first { CanonicalSourceName.equal($0.schema, table.schema)
            && CanonicalSourceName.equal($0.table, table.table) }
    }

    /// Resolves the shared namespace before classifying a view or physical source.
    func sqlObjectID(_ table: TableRef) throws -> String {
        if let binding = sourceBinding(table) { return binding.relationID }
        if viewBindings != nil { throw SQLViewExecutionError.missingDependency(table.table) }
        let namespaces: [String]
        if let schema = table.schema {
            guard snapshot.catalog.namespaces.contains(where: { CanonicalSourceName.equal($0.name, schema) }) else {
                throw SQLCatalogBindingError.missingNamespace(schema)
            }
            namespaces = [schema]
        }
        else { namespaces = searchPath.entries.compactMap {
            switch $0 { case .schema(let name): return name; case .currentUser: return currentUser }
        } }
        for name in namespaces {
            guard let namespace = snapshot.catalog.namespaces.first(where: {
                CanonicalSourceName.equal($0.name, name)
            }) else { continue }
            let namespaceID = SQLCatalogName(namespace.id)
            if let view = snapshot.catalog.views.first(where: {
                SQLCatalogName($0.namespaceID) == namespaceID && CanonicalSourceName.equal($0.name, table.table)
            }) { return view.id }
            if let view = snapshot.catalog.materializedViews.first(where: {
                SQLCatalogName($0.namespaceID) == namespaceID && CanonicalSourceName.equal($0.name, table.table)
            }) { return view.id }
            if let relation = snapshot.catalog.relations.first(where: { relation in
                SQLCatalogName(relation.namespaceID) == namespaceID && CanonicalSourceName.equal(relation.name, table.table)
                    && !snapshot.catalog.materializedViews.contains(where: {
                        SQLCatalogName($0.backingRelationID) == SQLCatalogName(relation.id)
                    })
            }) { return relation.id }
        }
        throw SQLCatalogBindingError.missingRelation(namespace: table.schema, name: table.table)
    }

    func ordinaryView(_ table: TableRef) throws -> SQLView? {
        let id = try sqlObjectID(table)
        return snapshot.catalog.views.first { SQLCatalogName($0.id) == SQLCatalogName(id) }
    }

    func materializedView(_ table: TableRef) throws -> SQLMaterializedView? {
        let id = try sqlObjectID(table)
        return snapshot.catalog.materializedViews.first { SQLCatalogName($0.id) == SQLCatalogName(id) }
    }

    func rejectReadOnlyMutation(_ table: TableRef) throws {
        let id = try sqlObjectID(table)
        if snapshot.catalog.views.contains(where: { SQLCatalogName($0.id) == SQLCatalogName(id) })
            || snapshot.catalog.materializedViews.contains(where: { SQLCatalogName($0.id) == SQLCatalogName(id)
                || SQLCatalogName($0.backingRelationID) == SQLCatalogName(id) }) {
            throw SQLViewExecutionError.readOnly(table.table)
        }
    }

    func forView(_ definition: SQLViewDefinition, id: String,
        workMeter: DatabaseWorkMeter) throws -> SQLReadResolution {
        guard !expandingViewIDs.contains(where: { SQLCatalogName($0) == SQLCatalogName(id) }) else {
            throw SQLViewExecutionError.cyclicDependency(id)
        }
        let claim = try workMeter.reserveIntermediate(bytes: UInt64(definition.bindings.count) * 128 + 512,
            at: .bindingCandidate)
        return SQLReadResolution(policy: policy, snapshot: snapshot, searchPath: searchPath,
            currentUser: currentUser, reservation: claim, viewBindings: definition.bindings,
            expandingViewIDs: expandingViewIDs + [id])
    }

    func viewColumns(_ table: TableRef, definition: SQLViewDefinition) throws -> [SQLViewColumn] {
        guard table.partitions.isEmpty else {
            throw SQLViewExecutionError.invalidDefinition("Views do not accept storage partitions")
        }
        guard let binding = sourceBinding(table) else { return definition.columns }
        return try binding.columns.map { column in
            guard let current = definition.columns.first(where: {
                SQLCatalogName($0.id) == SQLCatalogName(column.id)
            }) else { throw SQLViewExecutionError.missingColumn(relation: binding.relationID, column: column.id) }
            return try SQLViewColumn(id: current.id, name: column.name,
                type: current.type, isNullable: current.isNullable)
        }
    }

    func resolveSQLViewPhysicalSource(_ table: TableRef) throws -> SQLCatalogResolvedRelation {
        let id = try sqlObjectID(table)
        let materialized = snapshot.catalog.materializedViews.first { SQLCatalogName($0.id) == SQLCatalogName(id) }
        if let materialized, !materialized.isPopulated { throw SQLViewExecutionError.notPopulated(table.table) }
        let relationID = materialized?.backingRelationID ?? id
        guard let resolved = snapshot.resolvedRelation(id: relationID) else {
            throw SQLViewExecutionError.missingDependency(relationID)
        }
        let bindings: [SQLViewColumnBinding]?
        if let materialized {
            bindings = try viewColumns(table, definition: materialized.definition).map {
                try SQLViewColumnBinding(id: $0.id, name: $0.name, type: $0.type)
            }
        } else { bindings = sourceBinding(table)?.columns }
        guard let bindings else { return resolved }
        let columns = try bindings.map { binding -> SQLCatalogResolvedColumn in
            guard let current = resolved.columns.first(where: {
                SQLCatalogName($0.declaration.id) == SQLCatalogName(binding.id)
            }) else { throw SQLViewExecutionError.missingColumn(relation: relationID, column: binding.id) }
            guard current.type == binding.type else {
                throw SQLViewExecutionError.incompatibleColumn(relation: relationID, column: binding.id)
            }
            let column = try SQLColumn(id: current.declaration.id, name: binding.name,
                backing: current.declaration.backing, policy: current.declaration.policy,
                sequenceBindings: current.declaration.sequenceBindings)
            return SQLCatalogResolvedColumn(declaration: column, field: current.field,
                type: current.type, isNullable: current.isNullable)
        }
        let declaration = try SQLRelation(id: resolved.declaration.id,
            namespaceID: resolved.declaration.namespaceID, name: table.table,
            entityName: resolved.entity.name, columns: columns.map { $0.declaration },
            primaryKeyColumnIDs: nil)
        return SQLCatalogResolvedRelation(declaration: declaration, entity: resolved.entity, columns: columns)
    }
}
