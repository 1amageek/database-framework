import DatabaseKit
import DatabaseTypes
import StorageKit

/// Transaction-local lifecycle of ordinary and materialized SQL views.
struct SQLViewsExecutor: Sendable {
    let ddl: SQLDDLExecutor

    static func execute(command: SQLDDLStatement, context: DatabaseContext,
        transaction: DatabaseTransaction, options: SQLBlockExecutionOptions,
        workMeter: DatabaseWorkMeter, parameters: [QueryParameter], request: SQLDDLRequest,
        step: Int) async throws -> SQLCommandKind {
        let executor = Self(ddl: SQLDDLExecutor(context: context, transaction: transaction,
            options: options, workMeter: workMeter, parameters: parameters, request: request))
        switch command {
        case .createView(let value): try await executor.create(value, step: step)
        case .createMaterializedView(let value): try await executor.createMaterialized(value, step: step)
        case .refreshMaterializedView(let name, let withData): try await executor.refresh(name, withData: withData, step: step)
        case .dropView(let names, let ifExists, let behavior):
            try await executor.drop(names, materialized: false, ifExists: ifExists, behavior: behavior, step: step)
        case .dropMaterializedView(let names, let ifExists, let behavior):
            try await executor.drop(names, materialized: true, ifExists: ifExists, behavior: behavior, step: step)
        default: throw SQLBlockExecutionError.unsupportedStatement
        }
        return command.command
    }

    private func catalog(_ source: SQLCatalog, views: [SQLView]? = nil,
        materialized: [SQLMaterializedView]? = nil, relations: [SQLRelation]? = nil) throws -> SQLCatalog {
        try SQLCatalog(namespaces: source.namespaces, relations: relations ?? source.relations,
            sequences: source.sequences, views: views ?? source.views,
            materializedViews: materialized ?? source.materializedViews, indexes: source.indexes)
    }

    private func create(_ value: SQLCreateView, step: Int) async throws {
        let lease = await ddl.transaction.currentSQLSchemaLease()
        let source = lease.sqlCatalog.catalog
        let namespace = try ddl.namespace(for: value.name, catalog: source)
        let existing = source.views.first { ddl.same($0.namespaceID, namespace.id) && ddl.same($0.name, value.name.table) }
        if existing != nil {
            guard value.orReplace else { throw SQLDDLExecutionError.duplicateObject(value.name.table) }
        } else if ddl.nameExists(value.name.table, namespaceID: namespace.id, catalog: source) {
            throw SQLDDLExecutionError.duplicateObject(value.name.table)
        }
        let captured = try await capture(query: value.query, columns: value.columns, withData: false)
        let definition = captured.definition
        if let existing { try SQLViews.validateReplacement(existing.definition, with: definition, name: value.name.table) }
        let view = try SQLView(id: existing?.id ?? ddl.request.objectID(step: step, role: "view"),
            namespaceID: namespace.id, name: value.name.table, definition: definition)
        var views = source.views.filter { !ddl.same($0.id, view.id) }; views.append(view)
        try await ddl.stage(catalog: catalog(source, views: views), step: step)
    }

    private func createMaterialized(_ value: SQLCreateMaterializedView, step: Int) async throws {
        let lease = await ddl.transaction.currentSQLSchemaLease()
        let source = lease.sqlCatalog.catalog
        let namespace = try ddl.namespace(for: value.name, catalog: source)
        if ddl.nameExists(value.name.table, namespaceID: namespace.id, catalog: source) {
            guard value.ifNotExists else { throw SQLDDLExecutionError.duplicateObject(value.name.table) }
            return
        }
        let captured = try await capture(query: value.query, columns: value.columns, withData: value.withData)
        let definition = captured.definition
        let id = ddl.request.objectID(step: step, role: "materialized-view")
        let backingID = ddl.request.objectID(step: step, role: "materialized-backing")
        let entity = try backingEntity(definition, name: backingID)
        let columns = try definition.columns.enumerated().map { ordinal, column in
            try SQLColumn(id: column.id, name: column.name, fieldNumber: ordinal + 2)
        }
        let backing = try SQLRelation(id: backingID, namespaceID: namespace.id,
            name: backingID, entityName: entity.name, columns: columns,
            fieldNumberHighWaterMark: entity.fields.count)
        let view = try SQLMaterializedView(id: id, namespaceID: namespace.id, name: value.name.table,
            definition: definition, backingRelationID: backingID, isPopulated: value.withData)
        let candidate = try catalog(source, materialized: source.materializedViews + [view],
            relations: source.relations + [backing])
        try await ddl.stage(catalog: candidate, schema: Schema(entities: lease.schema.entities + [entity],
            version: lease.schema.version), step: step)
        if let rows = captured.rows { try await populate(rows, definition: definition, entity: entity, step: step) }
    }

    private func refresh(_ name: TableRef, withData: Bool, step: Int) async throws {
        let lease = await ddl.transaction.currentSQLSchemaLease()
        let resolution = try ddl.context.makeSQLReadResolution(searchPath: ddl.options.searchPath, workMeter: ddl.workMeter)
        guard let view = try resolution.materializedView(name),
              let backing = lease.sqlCatalog.resolvedRelation(id: view.backingRelationID) else {
            throw SQLDDLExecutionError.missingObject(name.table)
        }
        let rows = withData ? try await captureStored(view.definition, id: view.id) : nil
        let replacement = try SQLMaterializedView(id: view.id, namespaceID: view.namespaceID,
            name: view.name, definition: view.definition, backingRelationID: view.backingRelationID, isPopulated: withData)
        let source = lease.sqlCatalog.catalog
        let materialized = source.materializedViews.map { ddl.same($0.id, view.id) ? replacement : $0 }
        try await ddl.stage(catalog: catalog(source, materialized: materialized),
            rewrites: [SQLSchemaRewrite(entityName: backing.entity.name, transform: { _ in nil })], step: step)
        if let rows { try await populate(rows, definition: view.definition, entity: backing.entity, step: step) }
    }

    private func drop(_ names: [TableRef], materialized: Bool, ifExists: Bool,
        behavior: SQLDropBehavior, step: Int) async throws {
        let lease = await ddl.transaction.currentSQLSchemaLease()
        let source = lease.sqlCatalog.catalog
        let resolution = try ddl.context.makeSQLReadResolution(searchPath: ddl.options.searchPath, workMeter: ddl.workMeter)
        var ids: [String] = []
        for name in names {
            try ddl.workMeter.consume(at: .mutationPlanning)
            do {
                let id: String?
                if materialized { id = try resolution.materializedView(name)?.id }
                else { id = try resolution.ordinaryView(name)?.id }
                guard let id else { throw SQLDDLExecutionError.missingObject(name.table) }
                ids.append(id)
            } catch SQLCatalogBindingError.missingRelation {
                if !ifExists { throw SQLDDLExecutionError.missingObject(name.table) }
            } catch SQLDDLExecutionError.missingObject {
                if !ifExists { throw SQLDDLExecutionError.missingObject(name.table) }
            }
        }
        let dependent = SQLViews.dependentIDs(catalog: source, relationIDs: ids)
            .filter { id in !ids.contains(where: { ddl.same($0, id) }) }
        if !dependent.isEmpty, behavior == .restrict { throw SQLDDLExecutionError.dependentObject(dependent[0]) }
        let candidate = try SQLViews.removing(ids: ids + dependent, from: source)
        let retained = Set(candidate.relations.map { SQLCatalogName($0.id) })
        let removedEntities = Set(source.relations.filter { !retained.contains(SQLCatalogName($0.id)) }
            .map { SQLCatalogName($0.entityName) })
        let schema = try Schema(entities: lease.schema.entities.filter { !removedEntities.contains(SQLCatalogName($0.name)) },
            version: lease.schema.version)
        try await ddl.stage(catalog: candidate, schema: schema, step: step)
    }

    private func backingEntity(_ definition: SQLViewDefinition, name: String) throws -> Schema.Entity {
        var fields = [FieldSchema(name: "id", fieldNumber: 1, type: .string)]
        for (ordinal, column) in definition.columns.enumerated() {
            fields.append(try ddl.physicalField(number: ordinal + 2, type: column.type))
        }
        return try Schema.Entity(name: name, identifierType: .string, fields: fields,
            directoryComponents: [.staticPath("sql-materialized-views"), .staticPath(name)])
    }

    private func populate(_ rows: SQLBlockCollectedRows, definition: SQLViewDefinition,
        entity: Schema.Entity, step: Int) async throws {
        for index in rows.visibleRange {
            try ddl.workMeter.consume(at: .mutationPlanning)
            let row = rows.rows.withElement(at: index) { copy $0 }
            guard row.values.count == definition.columns.count else {
                throw SQLViewExecutionError.invalidDefinition("Materialized output arity changed")
            }
            var fields = [try PersistableField(number: 1, name: "id",
                value: .string(ddl.request.objectID(step: step, role: "materialized-row:\(index)")))]
            for (ordinal, column) in definition.columns.enumerated() {
                let number = ordinal + 2
                guard let field = entity.fieldMapByNumber[number] else {
                    throw SQLViewExecutionError.missingColumn(relation: entity.name, column: column.id)
                }
                fields.append(try PersistableField(number: UInt32(number), name: field.name,
                    value: ddl.encode(row.values[ordinal], type: column.type)))
            }
            try await ddl.transaction.savePersistedModel(PersistedModel(entity: entity.name, fields: fields), precondition: .notExists)
        }
    }

    private func capture(query: SelectQuery, columns: [String]?, withData: Bool) async throws
        -> (definition: SQLViewDefinition, rows: SQLBlockCollectedRows?) {
        try await withScope { scope in
            let definition = try ddl.context.admitSQLViewDefinition(query: query, columnNames: columns, scope: scope)
            if withData {
                try scope.prepare(definition.query); try await scope.capture(); try scope.seal()
                let rows = try await scope.select(definition.query)
                try admitPopulationRows(rows)
                return (definition, rows)
            }
            return (definition, nil)
        }
    }

    private func captureStored(_ definition: SQLViewDefinition, id: String) async throws -> SQLBlockCollectedRows {
        try await withScope(definition: definition, id: id) { scope in
            try scope.prepare(definition.query); try await scope.capture(); try scope.seal()
            let rows = try await scope.select(definition.query)
            try admitPopulationRows(rows)
            return rows
        }
    }

    private func admitPopulationRows(_ rows: SQLBlockCollectedRows) throws {
        let requested = UInt32(exactly: rows.visibleRange.count)
        guard let requested, requested <= ddl.workMeter.budget.maximumRows else {
            throw DatabaseWorkLimitError.maximumRows(stage: .mutationPlanning, consumed: 0,
                requested: requested ?? .max, maximum: ddl.workMeter.budget.maximumRows)
        }
        try ddl.workMeter.checkpoint(at: .mutationPlanning)
    }

    private func withScope<Result: Sendable>(definition: SQLViewDefinition? = nil, id: String? = nil,
        _ operation: @escaping @Sendable (SQLStatementReadScope) async throws -> Result) async throws -> Result {
        try await DatabaseReadSession.withSQLMutationSession(context: ddl.context, transaction: ddl.transaction,
            workMeter: ddl.workMeter) { session in
            let base = try session.makeSQLReadResolution(searchPath: ddl.options.searchPath, workMeter: ddl.workMeter)
            let resolution: SQLReadResolution
            if let definition, let id { resolution = try base.forView(definition, id: id, workMeter: ddl.workMeter) }
            else { resolution = base }
            let execution = ReadExecutionContext(options: ReadExecutionOptions(budget: ddl.options.budget),
                monotonicClock: ddl.context.container.monotonicClock, workMeter: ddl.workMeter,
                queryStructuralLimits: ddl.options.structuralLimits,
                temporalEnvironment: ddl.context.inheritedSQLTemporalEnvironment ?? ddl.context.makeSQLTemporalEnvironment())
                .withoutExternalPageWindow().withSQLResolution(resolution)
            let scope = try SQLStatementReadScope(context: ddl.context, session: session, execution: execution)
            return try await ActiveSQLStatementReads.$scope.withValue(scope) {
                do {
                    let result = try await operation(scope)
                    try await scope.close(); return result
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
