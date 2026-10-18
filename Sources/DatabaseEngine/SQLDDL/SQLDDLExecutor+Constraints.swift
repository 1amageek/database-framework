import DatabaseKit

extension SQLDDLExecutor {
    static func nextFieldNumber(after highWaterMark: Int) throws -> Int {
        let next = highWaterMark.addingReportingOverflow(1)
        guard !next.overflow else { throw SQLDDLExecutionError.invalidDefinition("field number exhausted") }
        _ = try encodedFieldNumber(next.partialValue)
        return next.partialValue
    }

    static func encodedFieldNumber(_ number: Int) throws -> UInt32 {
        guard number > 0, let encoded = UInt32(exactly: number) else {
            throw SQLDDLExecutionError.invalidDefinition("field number exhausted")
        }
        return encoded
    }

    func replacing(_ relation: SQLRelation, name: String? = nil, columns: [SQLColumn]? = nil,
        primary: [String]?? = nil, primaryAttributes: SQLConstraintAttributes?? = nil,
        constraints: [SQLConstraint]? = nil, highWaterMark: Int? = nil) throws -> SQLRelation {
        try SQLRelation(id: relation.id, namespaceID: relation.namespaceID, name: name ?? relation.name,
            entityName: relation.entityName, columns: columns ?? relation.columns,
            primaryKeyColumnIDs: primary ?? relation.primaryKeyColumnIDs,
            primaryKeyConstraint: primaryAttributes ?? relation.primaryKeyConstraint,
            constraints: constraints ?? relation.constraints,
            fieldNumberHighWaterMark: highWaterMark ?? relation.fieldNumberHighWaterMark)
    }

    func applying(_ declarations: [SQLTableConstraint], to relation: SQLRelation, catalog: SQLCatalog) throws -> SQLRelation {
        var result = relation
        let ordered = declarations.filter { if case .primaryKey = $0.kind { return true }; return false }
            + declarations.filter { if case .primaryKey = $0.kind { return false }; return true }
        for (offset, declaration) in ordered.enumerated() {
            try workMeter.consume(at: .bindingCandidate)
            let id = SQLCatalogSnapshot.availableID(relation.id + ":constraint:\(relation.constraints.count + offset)",
                used: result.constraints.map { $0.id } + (result.primaryKeyConstraint.map { [$0.id] } ?? []))
            let name = declaration.name ?? SQLCatalogSnapshot.availableID(relation.name + "_constraint_\(relation.constraints.count + offset)",
                used: result.constraints.map { $0.name } + (result.primaryKeyConstraint.map { [$0.name] } ?? []))
            let attributes = try SQLConstraintAttributes(id: id, name: name, timing: declaration.timing)
            let kind: SQLConstraintKind
            switch declaration.kind {
            case .primaryKey(let names):
                guard result.primaryKeyColumnIDs == nil else { throw SQLDDLExecutionError.invalidDefinition("multiple primary keys") }
                result = try replacing(result, primary: .some(columnIDs(names, relation: result)), primaryAttributes: .some(attributes))
                continue
            case .unique(let names, let nulls): kind = .unique(columnIDs: try columnIDs(names, relation: result), nulls: nulls)
            case .notNull(let name): kind = .notNull(columnID: try columnIDs([name], relation: result)[0])
            case .check(let expression): kind = .check(expression)
            case .foreignKey(let key):
                let target: SQLRelation
                if same(key.referencedTable.table, result.name), key.referencedTable.schema == nil || catalog.namespaces.contains(where: { same($0.id, result.namespaceID) && same($0.name, key.referencedTable.schema!) }) {
                    target = result
                } else {
                    target = try resolveDeclaration(key.referencedTable, catalog: catalog)
                }
                let explicitTargets = try key.referencedColumns.map { try columnIDs($0, relation: target) }
                guard let targetIDs = explicitTargets ?? target.primaryKeyColumnIDs else { throw SQLDDLExecutionError.invalidDefinition("REFERENCES without columns requires a primary key") }
                let targetSet = Set(targetIDs.map(SQLCatalogName.init))
                let primaryMatches = target.primaryKeyConstraint?.timing == .immediate
                    && target.primaryKeyColumnIDs.map { Set($0.map(SQLCatalogName.init)) == targetSet && $0.count == targetIDs.count } == true
                let uniqueMatches = target.constraints.contains { constraint in
                    guard constraint.timing == .immediate, case .unique(let ids, _) = constraint.kind else { return false }
                    return ids.count == targetIDs.count && Set(ids.map(SQLCatalogName.init)) == targetSet
                }
                let indexID: String?
                if primaryMatches || uniqueMatches { indexID = nil }
                else {
                    guard let index = catalog.indexes.first(where: { index in
                        guard index.unique, index.predicate == nil, same(index.relationID, target.id), let ids = index.directColumnIDs else { return false }
                        return ids.count == targetIDs.count && Set(ids.map(SQLCatalogName.init)) == targetSet
                    }) else { throw SQLDDLExecutionError.invalidDefinition("REFERENCES requires a nondeferrable unique key") }
                    indexID = index.id
                }
                kind = .foreignKey(try SQLForeignKey(columnIDs: columnIDs(key.columns, relation: result),
                    referencedRelationID: target.id,
                    referencedColumnIDs: explicitTargets,
                    match: key.match, onUpdate: key.onUpdate, onDelete: key.onDelete,
                    onDeleteColumnIDs: key.onDeleteColumns.map { try columnIDs($0, relation: result) },
                    referencedUniqueIndexID: indexID))
            }
            let constraint = try SQLConstraint(attributes: attributes, kind: kind)
            result = try replacing(result, constraints: result.constraints + [constraint])
        }
        return result
    }

    func columnIDs(_ names: [String], relation: SQLRelation) throws -> [String] {
        try names.map { name in
            guard let column = relation.columns.first(where: { same($0.name, name) }) else { throw SQLDDLExecutionError.missingObject(name) }
            return column.id
        }
    }

    func resolveDeclaration(_ name: TableRef, catalog: SQLCatalog) throws -> SQLRelation {
        let user = try context.readPolicy().sqlCurrentUser
        let namespaceNames: [String]
        if let schema = name.schema { namespaceNames = [schema] }
        else { namespaceNames = options.searchPath.entries.compactMap { entry in
            switch entry { case .schema(let schema): return schema; case .currentUser: return user }
        } }
        for namespaceName in namespaceNames {
            guard let namespace = catalog.namespaces.first(where: { same($0.name, namespaceName) }) else { continue }
            if catalog.views.contains(where: { same($0.namespaceID, namespace.id) && same($0.name, name.table) })
                || catalog.materializedViews.contains(where: { same($0.namespaceID, namespace.id) && same($0.name, name.table) }) {
                throw SQLDDLExecutionError.invalidDefinition("foreign key target must be an ordinary table")
            }
            if let relation = catalog.relations.first(where: { same($0.namespaceID, namespace.id) && same($0.name, name.table) }) { return relation }
        }
        throw SQLDDLExecutionError.missingObject(name.table)
    }

    func replacingEntity(_ entity: Schema.Entity, fields: [FieldSchema]) throws -> Schema.Entity {
        try Schema.Entity(name: entity.name, identifierType: entity.identifierType, fields: fields,
            directoryComponents: entity.directoryComponents, directoryLayer: entity.directoryLayer,
            indexes: entity.indexes, relationships: entity.relationships, fieldAccessRules: entity.fieldAccessRules,
            enumMetadata: entity.enumMetadata, ontology: entity.ontology, polymorphicMembership: entity.polymorphicMembership)
    }
}
