import DatabaseKit

/// Dependency visibility over the same immutable catalog and principal as binding.
struct SQLInformationSchemaVisibility {
    let resolution: SQLReadResolution

    func relation(_ relation: SQLCatalogResolvedRelation) throws -> Bool {
        try resolution.policy.permitsSQLMetadata(entityName: relation.entity.name)
    }

    func column(_ column: SQLCatalogResolvedColumn, in relation: SQLCatalogResolvedRelation) throws -> Bool {
        if let field = column.field,
           try !resolution.policy.permitsSQLMetadata(entityName: relation.entity.name, fields: [field.name]) { return false }
        if case .generated(let expression) = column.declaration.policy {
            let bound = try SQLConstraintCompiler.bindGenerated(expression, column: column.declaration,
                relation: relation.declaration, physicalSchema: resolution.policy.schema,
                workMeter: resolution.workMeter)
            return try expressionFields(bound, relation: relation)
        }
        return column.field != nil
    }

    func columns(_ ids: [String], in relation: SQLCatalogResolvedRelation) throws -> Bool {
        for id in ids {
            guard let column = relation.columns.first(where: { SQLCatalogName($0.declaration.id) == SQLCatalogName(id) }) else {
                throw SQLViewExecutionError.missingColumn(relation: relation.declaration.id, column: id)
            }
            if try !self.column(column, in: relation) { return false }
        }
        return true
    }

    func expression(_ expression: Expression, in relation: SQLCatalogResolvedRelation) throws -> Bool {
        let bound = try SQLConstraintCompiler.binder(relation: relation.declaration,
            physicalSchema: resolution.policy.schema, workMeter: resolution.workMeter).bindPredicate(expression)
        return try expressionFields(bound, relation: relation)
    }

    private func expressionFields(_ bound: CanonicalBoundExpression, relation: SQLCatalogResolvedRelation) throws -> Bool {
        if let slot = bound.columnSlot {
            guard relation.columns.indices.contains(slot) else {
                throw SQLViewExecutionError.missingDependency(relation.declaration.id)
            }
            if try !column(relation.columns[slot], in: relation) { return false }
        }
        for child in bound.children where try !expressionFields(child, relation: relation) { return false }
        return true
    }

    func view(_ definition: SQLViewDefinition) throws -> Bool {
        // Catalog admission already proves dependency acyclicity. An iterative
        // walk keeps deep admitted chains off the process stack.
        var pending = try SQLArrayBuffer<SQLViewDefinition>(meter: resolution.workMeter)
        try pending.append(definition)
        let visitedClaim = try resolution.workMeter.reserveIntermediate(bytes: 128, at: .bindingCandidate)
        defer { visitedClaim.release() }
        var visited = Set<SQLCatalogName>()
        var index = 0
        while index < pending.values.count {
            let current = pending.values[index]
            index += 1
            for binding in current.bindings {
                try resolution.workMeter.consume(at: .bindingCandidate)
                if binding.kind == .informationSchema {
                    _ = try SQLInformationSchemaRelation.validating(binding)
                    continue
                }
                let id = SQLCatalogName(binding.relationID)
                if let source = resolution.snapshot.resolvedRelation(id: binding.relationID) {
                    if try !relation(source) || !columns(binding.columns.map { $0.id }, in: source) { return false }
                    continue
                }
                let nested: SQLViewDefinition
                if let source = resolution.snapshot.catalog.views.first(where: { SQLCatalogName($0.id) == id }) {
                    nested = source.definition
                } else if let source = resolution.snapshot.catalog.materializedViews.first(where: { SQLCatalogName($0.id) == id }) {
                    guard let backing = resolution.snapshot.resolvedRelation(id: source.backingRelationID) else {
                        throw SQLViewExecutionError.missingDependency(source.backingRelationID)
                    }
                    if try !relation(backing) || !columns(binding.columns.map { $0.id }, in: backing) { return false }
                    nested = source.definition
                } else { throw SQLViewExecutionError.missingDependency(binding.relationID) }
                if visited.contains(id) { continue }
                try visitedClaim.reserveAdditional(bytes: 128, at: .bindingCandidate)
                visited.insert(id)
                try pending.append(nested)
            }
        }
        return true
    }

    func sequence(_ sequence: SQLSequence) throws -> Bool {
        guard let owner = sequence.ownedBy else { return true }
        guard let source = resolution.snapshot.resolvedRelation(id: owner.relationID) else {
            throw SQLViewExecutionError.missingDependency(owner.relationID)
        }
        return try relation(source) && columns([owner.columnID], in: source)
    }
}
