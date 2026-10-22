import DatabaseKit
import DatabaseTypes
import StorageKit

extension SQLDDLExecutor {
    func createTable(_ declaration: SQLCreateTable, step: Int) async throws {
        let lease = await transaction.currentSQLSchemaLease()
        let catalog = lease.sqlCatalog.catalog
        let namespace = try namespace(for: declaration.name, catalog: catalog, temporary: declaration.temporary)
        if nameExists(declaration.name.table, namespaceID: namespace.id, catalog: catalog) {
            guard declaration.ifNotExists else { throw SQLDDLExecutionError.duplicateObject(declaration.name.table) }
            if case .query(let query, let names, _) = declaration.source {
                _ = try await captureTableQuery(query, names: names, withData: false)
            }
            return
        }
        let relationID = request.objectID(step: step, role: "table")
        let entityName = relationID
        var fields = [FieldSchema(name: "id", fieldNumber: 1, type: .string)]
        var columns: [SQLColumn] = []
        var sequences = catalog.sequences
        var constraints: [SQLTableConstraint] = []
        var retainedRows: SQLBlockCollectedRows?
        switch declaration.source {
        case .columns(let declarations, let tableConstraints):
            constraints = tableConstraints
            for (offset, definition) in declarations.enumerated() {
                let number = offset + 2
                let column = try makeColumn(definition, number: number, relationID: relationID,
                    namespaceID: namespace.id, tableName: declaration.name.table, sequences: &sequences)
                columns.append(column)
                if case .field = column.backing { fields.append(try physicalField(number: number, type: .sql(definition.type))) }
                constraints.append(contentsOf: implicitConstraints(definition))
            }
        case .query(let query, let names, let withData):
            let captured = try await captureTableQuery(query, names: names, withData: withData)
            retainedRows = captured.rows
            for (offset, metadata) in captured.columns.enumerated() {
                let number = offset + 2
                columns.append(try SQLColumn(id: "column:\(number)", name: metadata.name, fieldNumber: number))
                fields.append(try physicalField(number: number, type: metadata.type))
            }
        }
        let entity = try Schema.Entity(name: entityName, identifierType: .string, fields: fields,
            directoryComponents: [.staticPath("sql-tables"), .staticPath(relationID)])
        let baseRelation = try SQLRelation(id: relationID, namespaceID: namespace.id,
            name: declaration.name.table, entityName: entityName, columns: columns,
            fieldNumberHighWaterMark: max(1, declarationsHighWater(columns)))
        let namespaces = catalog.namespaces.contains(where: { same($0.id, namespace.id) }) ? catalog.namespaces : catalog.namespaces + [namespace]
        let temporaryCatalog = try rebuilding(catalog, namespaces: namespaces,
            relations: catalog.relations + [baseRelation], sequences: sequences)
        let relation = try applying(constraints, to: baseRelation, catalog: temporaryCatalog)
        let candidate = try rebuilding(temporaryCatalog, relations: catalog.relations + [relation])
        let schema = try Schema(entities: lease.schema.entities + [entity], version: lease.schema.version)
        try await stage(catalog: candidate, schema: schema, step: step)
        if let retainedRows {
            for rowIndex in retainedRows.visibleRange {
                try workMeter.consume(at: .mutationPlanning)
                let row = retainedRows.rows.withElement(at: rowIndex) { copy $0 }
                var persisted = [try PersistableField(number: 1, name: "id",
                    value: .string(request.objectID(step: step, role: "row:\(rowIndex)")))]
                for (slot, column) in columns.enumerated() {
                    guard let number = column.fieldNumber,
                          let field = entity.fieldMapByNumber[number] else { throw SQLDDLExecutionError.invalidDefinition("CTAS backing field is missing") }
                    persisted.append(try PersistableField(number: Self.encodedFieldNumber(number), name: field.name,
                        value: encode(row.values[slot], type: field.queryType!)))
                }
                try await transaction.savePersistedModel(PersistedModel(entity: entityName, fields: persisted), precondition: .notExists)
            }
        }
    }

    func implicitConstraints(_ definition: SQLColumnDefinition) -> [SQLTableConstraint] {
        var constraints = definition.constraints
        switch definition.policy {
        case .identity, .serial:
            constraints.append(SQLTableConstraint(kind: .notNull(column: definition.name)))
        default: break
        }
        return constraints
    }

    private func declarationsHighWater(_ columns: [SQLColumn]) -> Int { columns.count + 1 }

    func makeColumn(_ definition: SQLColumnDefinition, number: Int, relationID: String,
        namespaceID: String, tableName: String, sequences: inout [SQLSequence]) throws -> SQLColumn {
        let id = "column:\(number)"
        let backing: SQLColumnBacking
        let policy: SQLColumnPolicy
        switch definition.policy {
        case .input(let expression): backing = .field(number: number); policy = .input(defaultExpression: expression)
        case .generated(let expression, let stored):
            backing = stored ? .field(number: number) : .virtual(type: .sql(definition.type))
            policy = .generated(expression)
        case .identity(let generation, let options):
            backing = .field(number: number)
            let sequenceID = relationID + ":sequence:\(number)"
            let owner = try SQLSequenceOwner(relationID: relationID, columnID: id)
            let typedOptions = SQLSequenceOptions(type: options.type ?? definition.type, start: options.start,
                increment: options.increment, minimum: options.minimum, maximum: options.maximum, cycle: options.cycle)
            sequences.append(try sequenceDeclaration(id: sequenceID, namespaceID: namespaceID,
                name: tableName + "_" + definition.name + "_seq", options: typedOptions, owner: owner, previous: nil))
            policy = .identity(try SQLIdentityColumn(sequenceID: sequenceID, generation: generation))
        case .serial:
            guard [.smallint, .integer, .bigint].contains(definition.type) else { throw SQLDDLExecutionError.invalidDefinition("SERIAL requires integer type") }
            backing = .field(number: number)
            let sequenceID = relationID + ":sequence:\(number)"
            let sequenceName = tableName + "_" + definition.name + "_seq"
            sequences.append(try sequenceDeclaration(id: sequenceID, namespaceID: namespaceID,
                name: sequenceName, options: SQLSequenceOptions(type: definition.type),
                owner: SQLSequenceOwner(relationID: relationID, columnID: id), previous: nil))
            policy = .input(defaultExpression: .function(FunctionCall(name: "nextval", arguments: [.literal(.sqlUnknownString(sequenceName))])))
        }
        let sequenceBindings: [SQLSequenceBinding]
        if case .serial = definition.policy {
            sequenceBindings = [try SQLSequenceBinding(originalName: tableName + "_" + definition.name + "_seq", sequenceID: relationID + ":sequence:\(number)")]
        } else { sequenceBindings = [] }
        return try SQLColumn(id: id, name: definition.name, backing: backing, policy: policy, sequenceBindings: sequenceBindings)
    }

    func physicalField(number: Int, type: QueryColumnType) throws -> FieldSchema {
        let kind: FieldSchemaType
        let isArray: Bool
        switch type {
        case .primitive(let primitive, let array): kind = primitive; isArray = array
        case .sql(let sql):
            isArray = false
            switch sql {
            case .boolean: kind = .bool
            case .smallint: kind = .int16
            case .integer: kind = .int32
            case .bigint: kind = .int64
            case .real: kind = .float32
            case .doublePrecision: kind = .float64
            case .decimal: kind = .object
            case .text, .char, .varchar: kind = .string
            case .blob, .binary, .varbinary: kind = .bytes
            case .date: kind = .date
            case .time(false, _): kind = .time
            case .timestamp(false, _): kind = .dateTime
            case .timestamp(true, _): kind = .timestamp
            case .uuid: kind = .uuid
            case .custom: throw SQLDDLExecutionError.invalidDefinition("unknown column type")
            default: kind = .object
            }
        }
        try QueryFieldStorageAdapter.validateDescriptor(type, fieldType: kind, isArray: isArray)
        return FieldSchema(name: "field_\(number)", fieldNumber: number, type: kind,
            isOptional: true, isArray: isArray, queryType: type)
    }

    func encode(_ value: QueryValue, type: QueryColumnType) throws -> FieldValue {
        try QueryFieldStorageAdapter.encode(value, type: type, limits: SQLConstraintRows.limits(workMeter))
    }

    private func captureTableQuery(_ query: SelectQuery, names: [String]?, withData: Bool) async throws
        -> (columns: [QueryColumn], rows: SQLBlockCollectedRows?) {
        try await DatabaseReadSession.withSQLMutationSession(context: context, transaction: transaction,
            workMeter: workMeter) { session in
            let resolution = try session.makeSQLReadResolution(searchPath: options.searchPath, workMeter: workMeter)
            let execution = ReadExecutionContext(options: ReadExecutionOptions(budget: options.budget),
                monotonicClock: context.container.monotonicClock, workMeter: workMeter,
                queryStructuralLimits: options.structuralLimits,
                temporalEnvironment: context.inheritedSQLTemporalEnvironment ?? context.makeSQLTemporalEnvironment())
                .withoutExternalPageWindow().withSQLResolution(resolution)
            let scope = try SQLStatementReadScope(context: context, session: session, execution: execution, parameters: parameters)
            return try await ActiveSQLStatementReads.$scope.withValue(scope) {
                do {
                    let plan = try ActiveSQLStatementReads.$metadataOnly.withValue(true) {
                        try context.bindStatementReadQuery(query, scope: scope)
                    }
                    if let names, names.count > plan.columns.count { throw SQLDDLExecutionError.invalidDefinition("CTAS column list has wrong arity") }
                    let columns = try plan.columns.enumerated().map { index, column in
                        try QueryColumn(number: Self.encodedFieldNumber(index + 1), name: names.flatMap { index < $0.count ? $0[index] : nil } ?? column.name,
                            type: column.type, isNullable: column.isNullable)
                    }
                    let rows: SQLBlockCollectedRows?
                    if withData { try scope.prepare(query); try await scope.capture(); try scope.seal(); rows = try await scope.select(query) }
                    else { rows = nil }
                    try await scope.close()
                    return (columns, rows)
                } catch {
                    let primary = error
                    do { try await scope.close() }
                    catch { throw SQLRelationCleanupError(operationError: primary, cleanupError: error) }
                    throw primary
                }
            }
        }
    }
}
