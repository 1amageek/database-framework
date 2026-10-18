import DatabaseKit

extension SQLDDLExecutor {
    func dropSchemas(_ names: [String], ifExists: Bool, behavior: SQLDropBehavior, step: Int) async throws {
        let lease = await transaction.currentSQLSchemaLease()
        let catalog = lease.sqlCatalog.catalog
        var namespaces: [String] = []
        for name in names {
            if let namespace = catalog.namespaces.first(where: { same($0.name, name) }) { namespaces.append(namespace.id) }
            else if !ifExists { throw SQLDDLExecutionError.missingObject(name) }
        }
        let relations = catalog.relations.filter { relation in namespaces.contains { same($0, relation.namespaceID) } }.map { $0.id }
        let containedViews = catalog.views.filter { view in namespaces.contains { same($0, view.namespaceID) } }.map { $0.id }
            + catalog.materializedViews.filter { view in namespaces.contains { same($0, view.namespaceID) } }.map { $0.id }
        let containedSequences = catalog.sequences.filter { sequence in namespaces.contains { same($0, sequence.namespaceID) } }
        let containedIndexes = catalog.indexes.filter { index in namespaces.contains { same($0, index.namespaceID) } }
        if behavior == .restrict, !relations.isEmpty || !containedViews.isEmpty || !containedSequences.isEmpty || !containedIndexes.isEmpty {
            throw SQLDDLExecutionError.dependentObject(names.first ?? "schema")
        }
        try await dropRelations(relations, behavior: behavior, explicitViews: containedViews,
            removedNamespaceIDs: namespaces, step: step)
    }

    func dropRelations(_ ids: [String], behavior: SQLDropBehavior, explicitViews: [String] = [],
        removedNamespaceIDs: [String] = [], step: Int) async throws {
        let lease = await transaction.currentSQLSchemaLease()
        let catalog = lease.sqlCatalog.catalog
        var dependentViews = SQLViews.dependentIDs(catalog: catalog, relationIDs: ids + explicitViews)
        for id in explicitViews where !dependentViews.contains(where: { same($0, id) }) { dependentViews.append(id) }
        if behavior == .restrict, dependentViews.contains(where: { id in !explicitViews.contains(where: { same($0, id) }) }) {
            throw SQLDDLExecutionError.dependentObject(dependentViews[0])
        }
        var filtered = try SQLViews.removing(ids: dependentViews, from: catalog)
        var relations: [SQLRelation] = []
        for relation in filtered.relations {
            if ids.contains(where: { same($0, relation.id) }) { continue }
            var constraints: [SQLConstraint] = []
            for constraint in relation.constraints {
                if case .foreignKey(let key) = constraint.kind, ids.contains(where: { same($0, key.referencedRelationID) }) {
                    guard behavior == .cascade else { throw SQLDDLExecutionError.dependentObject(constraint.name) }
                    continue
                }
                constraints.append(constraint)
            }
            relations.append(try replacing(relation, constraints: constraints))
        }
        let survivingIDs = Set(relations.map { SQLCatalogName($0.id) })
        let sequences = filtered.sequences.filter { sequence in
            if removedNamespaceIDs.contains(where: { same($0, sequence.namespaceID) }) { return false }
            guard let owner = sequence.ownedBy else { return true }
            return survivingIDs.contains(SQLCatalogName(owner.relationID))
        }
        let removedSequenceIDs = filtered.sequences.filter { old in !sequences.contains(where: { same($0.id, old.id) }) }.map { $0.id }
        if !removedSequenceIDs.isEmpty {
            relations = try relations.map { relation in
                let columns = try relation.columns.map { column in
                    let referenced = column.sequenceBindings.contains { binding in removedSequenceIDs.contains { same($0, binding.sequenceID) } }
                    guard referenced else { return column }
                    guard behavior == .cascade else { throw SQLDDLExecutionError.dependentObject(column.name) }
                    return try SQLColumn(id: column.id, name: column.name, backing: column.backing)
                }
                return try replacing(relation, columns: columns)
            }
        }
        filtered = try SQLCatalog(namespaces: filtered.namespaces.filter { namespace in !removedNamespaceIDs.contains { same($0, namespace.id) } },
            relations: relations, sequences: sequences,
            views: filtered.views, materializedViews: filtered.materializedViews,
            indexes: filtered.indexes.filter { survivingIDs.contains(SQLCatalogName($0.relationID)) })
        let survivingEntities = Set(filtered.relations.map { SQLCatalogName($0.entityName) })
        let removedEntities = lease.schema.entities.filter { !survivingEntities.contains(SQLCatalogName($0.name)) }
        let schema = try Schema(entities: lease.schema.entities.filter { survivingEntities.contains(SQLCatalogName($0.name)) }, version: lease.schema.version)
        let rewrites = removedEntities.map { entity in SQLSchemaRewrite(entityName: entity.name) { _ in nil } }
        try await stage(catalog: filtered, schema: schema, rewrites: rewrites, step: step)
    }

    func removingReferencedConstraints(_ catalog: SQLCatalog, relationID: String,
        columnIDs: [String], behavior: SQLDropBehavior, includingIndexReferences: Bool = true) throws -> SQLCatalog {
        let relations = try catalog.relations.map { relation in
            let constraints = try relation.constraints.filter { constraint in
                guard case .foreignKey(let key) = constraint.kind, same(key.referencedRelationID, relationID) else { return true }
                if !includingIndexReferences, key.referencedUniqueIndexID != nil { return true }
                let target = key.referencedColumnIDs ?? catalog.relations.first(where: { same($0.id, relationID) })?.primaryKeyColumnIDs ?? []
                guard target.contains(where: { targetID in columnIDs.contains { same($0, targetID) } }) else { return true }
                guard behavior == .cascade else { throw SQLDDLExecutionError.dependentObject(constraint.name) }
                return false
            }
            return try replacing(relation, constraints: constraints)
        }
        return try rebuilding(catalog, relations: relations)
    }

    func removingColumnDependencies(_ catalog: SQLCatalog, relation: SQLRelation,
        column: SQLColumn, physicalSchema: Schema, behavior: SQLDropBehavior) throws -> SQLCatalog {
        var dropping = [column.id]
        var changed = true
        while changed {
            changed = false
            for candidate in relation.columns where !dropping.contains(where: { same($0, candidate.id) }) {
                guard case .generated(let expression) = candidate.policy else { continue }
                if try expressionReferences(expression, relation: relation, schema: physicalSchema, columnIDs: dropping) {
                    guard behavior == .cascade else { throw SQLDDLExecutionError.dependentObject(candidate.name) }
                    dropping.append(candidate.id); changed = true
                }
            }
        }
        let dependentViews = SQLViews.dependentIDs(catalog: catalog, relationIDs: [], removedColumns: [relation.id: dropping])
        guard behavior == .cascade || dependentViews.isEmpty else { throw SQLDDLExecutionError.dependentObject(column.name) }
        let withoutViews = try SQLViews.removing(ids: dependentViews, from: catalog)
        let withoutReferences = try removingReferencedConstraints(withoutViews, relationID: relation.id, columnIDs: dropping, behavior: behavior)
        let currentRelation = withoutReferences.relations.first { same($0.id, relation.id) } ?? relation
        let constraints = try currentRelation.constraints.filter { constraint in
            switch constraint.kind {
            case .notNull(let id): return !dropping.contains { same($0, id) }
            case .unique(let ids, _): return !ids.contains { id in dropping.contains { same($0, id) } }
            case .foreignKey(let key): return !key.columnIDs.contains { id in dropping.contains { same($0, id) } }
            case .check(let expression): return try !expressionReferences(expression, relation: relation, schema: physicalSchema, columnIDs: dropping)
            }
        }
        let primaryRemoved = relation.primaryKeyColumnIDs?.contains { id in dropping.contains { same($0, id) } } == true
        let replacement = try replacing(relation, columns: relation.columns.filter { candidate in !dropping.contains { same($0, candidate.id) } },
            primary: primaryRemoved ? .some(nil) : nil, primaryAttributes: primaryRemoved ? .some(nil) : nil, constraints: constraints)
        let indexes = withoutReferences.indexes.filter { index in
            !same(index.relationID, relation.id) || !index.bindings.contains { binding in dropping.contains { same($0, binding.id) } }
        }
        let removedSequenceIDs = withoutReferences.sequences.compactMap { sequence -> String? in
            guard let owner = sequence.ownedBy, same(owner.relationID, relation.id),
                  dropping.contains(where: { same($0, owner.columnID) }) else { return nil }
            return sequence.id
        }
        let relations = try withoutReferences.relations.map { source in
            let retained = same(source.id, relation.id) ? replacement : source
            let columns = try retained.columns.map { column in
                guard case .input = column.policy, column.sequenceBindings.contains(where: { binding in
                    removedSequenceIDs.contains { same($0, binding.sequenceID) }
                }) else { return column }
                guard behavior == .cascade else { throw SQLDDLExecutionError.dependentObject(column.name) }
                return try SQLColumn(id: column.id, name: column.name, backing: column.backing,
                    policy: .input(defaultExpression: nil))
            }
            return try replacing(retained, columns: columns)
        }
        return try SQLCatalog(namespaces: withoutReferences.namespaces, relations: relations,
            sequences: withoutReferences.sequences.filter { sequence in
                !removedSequenceIDs.contains { same($0, sequence.id) }
            }, views: withoutReferences.views, materializedViews: withoutReferences.materializedViews, indexes: indexes)
    }

    private func expressionReferences(_ expression: Expression, relation: SQLRelation,
        schema: Schema, columnIDs: [String]) throws -> Bool {
        let bound = try SQLConstraintCompiler.binder(relation: relation, physicalSchema: schema, workMeter: workMeter).bind(expression)
        var pending = [bound]
        while let current = pending.popLast() {
            try workMeter.consume(at: .bindingCandidate)
            if let slot = current.columnSlot, columnIDs.contains(where: { same($0, relation.columns[slot].id) }) { return true }
            pending.append(contentsOf: current.children)
        }
        return false
    }
}
