import DatabaseKit
import DatabaseTypes

extension SQLDDLExecutor {
    func alterTable(_ declaration: SQLAlterTable, step: Int) async throws {
        let initial = await transaction.currentSQLSchemaLease()
        guard try resolveIfPresent(declaration.name, snapshot: initial.sqlCatalog) != nil else {
            guard declaration.ifExists else { throw SQLDDLExecutionError.missingObject(declaration.name.table) }
            return
        }
        let sourceRelation = try resolveIfPresent(declaration.name, snapshot: initial.sqlCatalog)!.declaration
        let relationID = sourceRelation.id
        var workingCatalog = initial.sqlCatalog.catalog
        var workingSchema = initial.schema
        var transformations: [SQLDDLRowTransformation] = []
        for action in declaration.actions {
            let catalog = workingCatalog
            guard let relation = catalog.relations.first(where: { same($0.id, relationID) }),
                  let entity = workingSchema.entity(named: relation.entityName) else { throw SQLDDLExecutionError.missingObject(relationID) }
            var replacement = relation
            var physical = entity
            var sequences = catalog.sequences
            var rowTransforms: [SQLDDLRowTransformation] = []
            var candidateCatalog = catalog
            switch action {
            case .renameTable(let name):
                guard !nameExists(name, namespaceID: relation.namespaceID, catalog: catalog) else { throw SQLDDLExecutionError.duplicateObject(name) }
                replacement = try replacing(relation, name: name)
                replacement = try renamePolicyTable(replacement, from: relation.name, to: name)
            case .renameColumn(let name, let newName):
                guard !relation.columns.contains(where: { same($0.name, newName) }) else { throw SQLDDLExecutionError.duplicateObject(newName) }
                let selected = try requireColumn(name, relation: relation)
                let columns = try relation.columns.map { column in
                    same(column.id, selected.id) ? try SQLColumn(id: column.id, name: newName, backing: column.backing,
                        policy: column.policy, sequenceBindings: column.sequenceBindings) : column
                }
                replacement = try replacing(relation, columns: columns)
                // Policy expressions bind logical names; preserve their meaning when a column is renamed.
                replacement = try renamePolicyColumns(replacement, from: name, to: newName)
            case .setDefault(let name, let expression):
                let column = try requireColumn(name, relation: relation)
                guard case .input = column.policy else { throw SQLDDLExecutionError.invalidDefinition("generated or identity column cannot acquire a default") }
                replacement = try replaceColumn(column, in: relation, policy: .input(defaultExpression: expression), bindings: [])
            case .dropDefault(let name):
                let column = try requireColumn(name, relation: relation)
                guard case .input = column.policy else { throw SQLDDLExecutionError.invalidDefinition("generated or identity column has no input default") }
                replacement = try replaceColumn(column, in: relation, policy: .input(defaultExpression: nil), bindings: [])
            case .setNotNull(let name):
                let column = try requireColumn(name, relation: relation)
                if !relation.constraints.contains(where: { if case .notNull(let id) = $0.kind { return same(id, column.id) }; return false }) {
                    let attributes = try SQLConstraintAttributes(id: request.objectID(step: step, role: "not-null:\(column.id)"), name: relation.name + "_" + name + "_not_null")
                    replacement = try replacing(relation, constraints: relation.constraints + [SQLConstraint(attributes: attributes, kind: .notNull(columnID: column.id))])
                }
            case .dropNotNull(let name):
                let column = try requireColumn(name, relation: relation)
                guard relation.primaryKeyColumnIDs?.contains(where: { same($0, column.id) }) != true else { throw SQLDDLExecutionError.dependentObject(name) }
                replacement = try replacing(relation, constraints: relation.constraints.filter { if case .notNull(let id) = $0.kind { return !same(id, column.id) }; return true })
            case .addConstraint(let declaration):
                replacement = try applying([declaration], to: relation, catalog: catalog)
            case .dropConstraint(let name, let ifExists, let behavior):
                let isPrimary = relation.primaryKeyConstraint.map { same($0.name, name) } == true
                guard isPrimary || relation.constraints.contains(where: { same($0.name, name) }) else {
                    if ifExists { continue }
                    throw SQLDDLExecutionError.missingObject(name)
                }
                let keyIDs: [String]?
                if isPrimary { keyIDs = relation.primaryKeyColumnIDs }
                else if let constraint = relation.constraints.first(where: { same($0.name, name) }), case .unique(let ids, _) = constraint.kind { keyIDs = ids }
                else { keyIDs = nil }
                if let keyIDs { candidateCatalog = try removingReferencedConstraints(catalog, relationID: relation.id, columnIDs: keyIDs, behavior: behavior, includingIndexReferences: false) }
                let currentRelation = candidateCatalog.relations.first { same($0.id, relation.id) } ?? relation
                replacement = try replacing(currentRelation, primary: isPrimary ? .some(nil) : nil,
                    primaryAttributes: isPrimary ? .some(nil) : nil,
                    constraints: currentRelation.constraints.filter { !same($0.name, name) })
            case .addColumn(let definition, let ifNotExists):
                if relation.columns.contains(where: { same($0.name, definition.name) }) {
                    if ifNotExists { continue }
                    throw SQLDDLExecutionError.duplicateObject(definition.name)
                }
                let number = try Self.nextFieldNumber(after: relation.fieldNumberHighWaterMark)
                let encodedNumber = try Self.encodedFieldNumber(number)
                let column = try makeColumn(definition, number: number, relationID: relation.id,
                    namespaceID: relation.namespaceID, tableName: relation.name, sequences: &sequences)
                replacement = try replacing(relation, columns: relation.columns + [column], highWaterMark: number)
                if case .field = column.backing {
                    physical = try replacingEntity(entity, fields: entity.fields + [physicalField(number: number, type: .sql(definition.type))])
                }
                let tempCatalog = try rebuilding(catalog, relations: catalog.relations.map { same($0.id, relation.id) ? replacement : $0 }, sequences: sequences)
                replacement = try applying(implicitConstraints(definition), to: replacement, catalog: tempCatalog)
                let targetSchema = try Schema(entities: workingSchema.entities.map { same($0.name, entity.name) ? physical : $0 }, version: workingSchema.version)
                let targetRelation = replacement
                let constant: FieldValue?
                if case .input(let expression?) = column.policy, try !isVolatile(expression, relation: replacement, schema: targetSchema) {
                    constant = try await evaluate(expression, type: .sql(definition.type), row: CanonicalSourceRow(values: [], layout: CanonicalSourceLayout(slots: [])), relation: replacement, schema: targetSchema, column: column)
                } else { constant = nil }
                if case .field = column.backing {
                    rowTransforms = [SQLDDLRowTransformation { _, model in
                        let value: FieldValue
                        switch column.policy {
                        case .input(let expression):
                            if let constant { value = constant }
                            else if let expression { value = try await evaluate(expression, type: .sql(definition.type), row: CanonicalSourceRow(values: [], layout: CanonicalSourceLayout(slots: [])), relation: targetRelation, schema: targetSchema, column: column) }
                            else { value = .null }
                        case .identity(let identity):
                            let next = try await SQLSequenceRuntime.nextValue(sequenceID: identity.sequenceID,
                                occurrence: relation.id + ":" + column.id)
                            value = try await evaluate(.literal(.int(next)), type: .sql(definition.type),
                                row: CanonicalSourceRow(values: [], layout: CanonicalSourceLayout(slots: [])),
                                relation: targetRelation, schema: targetSchema, column: column)
                        case .generated:
                            value = .null
                        }
                        var fields = model.fields
                        fields.append(try PersistableField(number: encodedNumber, name: "field_\(number)", value: value))
                        return try PersistedModel(entity: model.entity, fields: fields)
                    }]
                }
            case .alterColumnType(let name, let type, let using):
                let column = try requireColumn(name, relation: relation)
                guard let number = column.fieldNumber, let oldField = entity.fieldMapByNumber[number] else { throw SQLDDLExecutionError.invalidDefinition("virtual column type cannot be altered") }
                let encodedNumber = try Self.encodedFieldNumber(number)
                let descriptor = try physicalField(number: number, type: .sql(type))
                let field = FieldSchema(name: oldField.name, fieldNumber: number, type: descriptor.type, isOptional: descriptor.isOptional, isArray: descriptor.isArray, queryType: descriptor.queryType)
                physical = try replacingEntity(entity, fields: entity.fields.map { $0.fieldNumber == number ? field : $0 })
                let expression = using.map { Expression.cast($0, targetType: type) } ?? .col(name)
                let bound = try SQLConstraintCompiler.binder(relation: sourceRelation, physicalSchema: initial.schema, workMeter: workMeter)
                    .bindScalarAssignment(expression, to: .sql(type))
                let sourceSchema = initial.schema
                rowTransforms = [SQLDDLRowTransformation { original, model in
                    let row = try await SQLConstraintRows.row(fields: try DatabaseEntityProjection.fieldObject(for: original), relation: sourceRelation, physicalSchema: sourceSchema,
                        environment: temporalEnvironment, workMeter: workMeter)
                    let evaluated = try await DatabaseContext.evaluateCanonicalScalar(bound, on: row, temporalEnvironment: temporalEnvironment,
                        session: ActiveSQLSession.binding, workMeter: workMeter)
                    let value = try encode(evaluated.value, type: .sql(type))
                    let fields = try model.fields.map { $0.number == encodedNumber ? try PersistableField(number: $0.number, name: oldField.name, value: value) : $0 }
                    return try PersistedModel(entity: model.entity, fields: fields)
                }]
            case .dropColumn(let name, let ifExists, let behavior):
                guard let column = relation.columns.first(where: { same($0.name, name) }) else {
                    if ifExists { continue }; throw SQLDDLExecutionError.missingObject(name)
                }
                candidateCatalog = try removingColumnDependencies(catalog, relation: relation, column: column, physicalSchema: workingSchema, behavior: behavior)
                guard let updated = candidateCatalog.relations.first(where: { same($0.id, relation.id) }) else { throw SQLDDLExecutionError.missingObject(relation.id) }
                replacement = updated
                if column.fieldNumber != nil {
                    let survivingNumbers = Set(replacement.columns.compactMap { $0.fieldNumber } + entity.fields.filter { field in !relation.columns.contains { $0.fieldNumber == field.fieldNumber } }.map { $0.fieldNumber })
                    let survivingEncodedNumbers = Set(try survivingNumbers.map(Self.encodedFieldNumber))
                    physical = try replacingEntity(entity, fields: entity.fields.filter { survivingNumbers.contains($0.fieldNumber) })
                    rowTransforms = [SQLDDLRowTransformation { _, model in
                        try PersistedModel(entity: model.entity, fields: model.fields.filter { survivingEncodedNumbers.contains($0.number) })
                    }]
                }
            }
            let relations = candidateCatalog.relations.map { same($0.id, relationID) ? replacement : $0 }
            var finalCatalog = try rebuilding(candidateCatalog, relations: relations, sequences: sequences.filter { sequence in
                guard let owner = sequence.ownedBy, same(owner.relationID, relationID) else { return true }
                return replacement.columns.contains { same($0.id, owner.columnID) }
            })
            let activeEntities = Set(finalCatalog.relations.map { SQLCatalogName($0.entityName) })
            let schema = try Schema(entities: workingSchema.entities.filter { activeEntities.contains(SQLCatalogName($0.name)) }.map { same($0.name, entity.name) ? physical : $0 }, version: workingSchema.version)
            if case .alterColumnType = action {
                let indexes = try finalCatalog.indexes.map { index in
                    guard same(index.relationID, relationID) else { return index }
                    let bindings = try index.bindings.map { binding in
                        guard let column = replacement.columns.first(where: { same($0.id, binding.id) }) else { throw SQLDDLExecutionError.missingObject(binding.id) }
                        return try SQLViewColumnBinding(id: binding.id, name: binding.name,
                            type: SQLConstraintCompiler.columnType(column, relation: replacement, physicalSchema: schema))
                    }
                    return try SQLIndex(id: index.id, namespaceID: index.namespaceID, name: index.name,
                        relationID: index.relationID, keys: index.keys, predicate: index.predicate,
                        unique: index.unique, nulls: index.nulls, bindings: bindings)
                }
                finalCatalog = try SQLCatalog(namespaces: finalCatalog.namespaces, relations: finalCatalog.relations,
                    sequences: finalCatalog.sequences, views: finalCatalog.views,
                    materializedViews: finalCatalog.materializedViews, indexes: indexes)
            }
            workingCatalog = finalCatalog
            workingSchema = schema
            transformations.append(contentsOf: rowTransforms)
        }
        guard let targetRelation = workingCatalog.relations.first(where: { same($0.id, relationID) }) else { throw SQLDDLExecutionError.missingObject(relationID) }
        let finalSchema = workingSchema
        let finalTransforms = transformations
        let rewrites: [SQLSchemaRewrite]
        if !finalTransforms.isEmpty {
            rewrites = [SQLSchemaRewrite(entityName: sourceRelation.entityName) { original in
                var candidate = original
                for transform in finalTransforms { candidate = try await transform.apply(original, candidate) }
                return try await regenerate(candidate, relation: targetRelation, schema: finalSchema)
            }]
        } else { rewrites = [] }
        try await stage(catalog: workingCatalog, schema: finalSchema, rewrites: rewrites, step: step)
    }

    var temporalEnvironment: SQLTemporalEnvironment {
        context.inheritedSQLTemporalEnvironment ?? context.makeSQLTemporalEnvironment()
    }

    func requireColumn(_ name: String, relation: SQLRelation) throws -> SQLColumn {
        guard let column = relation.columns.first(where: { same($0.name, name) }) else { throw SQLDDLExecutionError.missingObject(name) }
        return column
    }

    func replaceColumn(_ column: SQLColumn, in relation: SQLRelation, policy: SQLColumnPolicy,
        bindings: [SQLSequenceBinding]) throws -> SQLRelation {
        try replacing(relation, columns: relation.columns.map { same($0.id, column.id)
            ? try SQLColumn(id: $0.id, name: $0.name, backing: $0.backing, policy: policy, sequenceBindings: bindings) : $0 })
    }

    func evaluate(_ expression: Expression, type: QueryColumnType, row: CanonicalSourceRow,
        relation: SQLRelation, schema: Schema, column: SQLColumn) async throws -> FieldValue {
        let bound = try SQLConstraintCompiler.binder(relation: relation, physicalSchema: schema, workMeter: workMeter)
            .bindScalarAssignment(expression, to: type)
        let catalog = try context.readPolicy().sqlCatalog.catalog
        let bindings = try SQLSequenceDefaultBindings.compile(expression, column: column, relation: relation, catalog: catalog, physicalSchema: schema)
        return try await SQLSequenceRuntime.$defaultBindings.withValue(bindings) {
            let evaluated = try await DatabaseContext.evaluateCanonicalScalar(bound, on: row,
                temporalEnvironment: temporalEnvironment, session: ActiveSQLSession.binding, workMeter: workMeter)
            return try encode(evaluated.value, type: type)
        }
    }

    func regenerate(_ model: PersistedModel, relation: SQLRelation, schema: Schema) async throws -> PersistedModel {
        let row = try await SQLConstraintRows.row(fields: try DatabaseEntityProjection.fieldObject(for: model), relation: relation, physicalSchema: schema,
            environment: temporalEnvironment, workMeter: workMeter, recomputingStored: true)
        let prepared = try SQLConstraintRows.fields(row, relation: relation, physicalSchema: schema,
            preserving: try DatabaseEntityProjection.fieldObject(for: model), workMeter: workMeter)
        guard let entity = schema.entity(named: model.entity) else { throw SQLDDLExecutionError.missingObject(model.entity) }
        return try PersistedModel(entity: model.entity, fields: entity.fields.map { field in
            try PersistableField(number: Self.encodedFieldNumber(field.fieldNumber), name: field.name, value: prepared.fields[field.name] ?? .null)
        })
    }

    func isVolatile(_ expression: Expression, relation: SQLRelation, schema: Schema) throws -> Bool {
        let bound = try SQLConstraintCompiler.binder(relation: relation, physicalSchema: schema, workMeter: workMeter).bind(expression)
        var pending = [bound]
        while let current = pending.popLast() {
            try workMeter.consume(at: .bindingCandidate)
            if case .function(let function) = current.expression,
                ["random", "clock_timestamp", "timeofday", "nextval", "setval", "currval", "lastval", "setseed"].contains(function.name) { return true }
            pending.append(contentsOf: current.children)
        }
        return false
    }
}
