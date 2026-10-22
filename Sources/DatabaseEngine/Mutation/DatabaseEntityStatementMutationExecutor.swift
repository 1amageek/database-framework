import DatabaseKit
import DatabaseTypes

@_spi(DatabaseExecution)
public struct DatabaseEntityStatementMutationExecutor: Sendable {
    private let container: DBContainer
    private let limits: DatabaseEntityMutationLimits
    private var temporalEnvironment: SQLTemporalEnvironment?
    private var sqlCatalog: SQLCatalog?
    private var sqlSchema: Schema?
    private var sqlSession: SQLSessionBinding?

    public init(
        container: DBContainer,
        limits: DatabaseEntityMutationLimits
    ) {
        self.container = container
        self.limits = limits
        self.temporalEnvironment = nil
    }

    public func execute(
        _ statementInput: QueryStatement,
        preconditions: [EntityMutationPrecondition] = [],
        transaction: DatabaseTransaction,
        workMeter: DatabaseWorkMeter,
        temporalEnvironment: SQLTemporalEnvironment? = nil
    ) async throws -> [EntityMutationEffect] {
        var statement = self
        statement.temporalEnvironment = temporalEnvironment ?? transaction.makeSQLTemporalEnvironment()
        let (catalog, schema) = try await transaction.sqlPolicyCatalog()
        statement.sqlCatalog = catalog
        statement.sqlSchema = schema
        let temporal = SQLTemporalStatementBinding(container: container, environment: statement.temporalEnvironment!)
        return try await ActiveSQLTemporalStatement.$binding.withValue(temporal) { [statement] in
            try await statement.executeStatement(statement: statementInput, preconditions: preconditions,
                transaction: transaction, workMeter: workMeter)
        }
    }

    func executeSQL(
        _ input: QueryStatement, searchPath: SQLSearchPath,
        preconditions: [EntityMutationPrecondition] = [], transaction: DatabaseTransaction,
        workMeter: DatabaseWorkMeter, temporalEnvironment: SQLTemporalEnvironment? = nil
    ) async throws -> SQLMutationOutcome {
        guard let binding = ActiveDatabaseTransactionContext.binding else { throw DatabaseTransactionError.invalidOperationContext }
        #if DATABASE_MULTI_BASE
        let context = DatabaseContext(container: container, resource: binding.resource, authorization: binding.authorization)
        #else
        let context = DatabaseContext(container: container, authorization: binding.authorization)
        #endif
        return try await DatabaseReadSession.withSQLMutationSession(context: context, transaction: transaction, workMeter: workMeter) { session in
            let resolution = try session.makeSQLReadResolution(searchPath: searchPath, workMeter: workMeter)
            let execution = ReadExecutionContext(options: ReadExecutionOptions(budget: workMeter.budget),
                monotonicClock: container.monotonicClock, workMeter: workMeter,
                temporalEnvironment: temporalEnvironment ?? transaction.makeSQLTemporalEnvironment())
                .withoutExternalPageWindow().withSQLResolution(resolution)
            let scope = try SQLStatementReadScope(context: context, session: session, execution: execution)
            do {
                let result = try await ActiveSQLStatementReads.$scope.withValue(scope) {
                    let prepared = try await prepareSQLReads(input, scope: scope, transaction: transaction, workMeter: workMeter)
                    try scope.seal()
                    return try await executeSQL(input, searchPath: searchPath, preparation: prepared, readScope: scope,
                        preconditions: preconditions, transaction: transaction, workMeter: workMeter,
                        temporalEnvironment: temporalEnvironment)
                }
                try await scope.close()
                return result
            } catch {
                let primary = error
                do { try await scope.close() }
                catch { throw SQLRelationCleanupError(operationError: primary, cleanupError: error) }
                throw primary
            }
        }
    }

    func executeSQL(
        _ input: QueryStatement, searchPath: SQLSearchPath,
        preparation: SQLMutationPreparation, readScope: SQLStatementReadScope,
        preconditions: [EntityMutationPrecondition] = [], transaction: DatabaseTransaction,
        workMeter: DatabaseWorkMeter, temporalEnvironment: SQLTemporalEnvironment? = nil
    ) async throws -> SQLMutationOutcome {
        guard preparation.input == input, preparation.scopeIdentity == ObjectIdentifier(readScope),
              preparation.transactionIdentity == ObjectIdentifier(transaction),
              preparation.readClaims.workMeter === workMeter,
              preparation.executor.container === container else { throw DatabaseTransactionError.invalidOperationContext }
        try readScope.validate()
        let statement = preparation.executor
        let temporal = SQLTemporalStatementBinding(container: container, environment: statement.temporalEnvironment!)
        return try await ActiveSQLTemporalStatement.$binding.withValue(temporal) {
            try await ActiveSQLStatementReads.$declarations.withValue(preparation.declarations) {
              try await ActiveSQLSession.$binding.withValue(statement.sqlSession) {
                try await DatabaseEntityMutationExecutor(container: container, limits: limits)
                    .validate(preconditions, transaction: transaction, workMeter: workMeter)
                return try await statement.executePreparedSQL(input, preparation: preparation, scope: readScope,
                    transaction: transaction, workMeter: workMeter)
              }
            }
        }
    }

    private func executeStatement(
        statement: QueryStatement, preconditions: [EntityMutationPrecondition],
        transaction: DatabaseTransaction, workMeter: DatabaseWorkMeter
    ) async throws -> [EntityMutationEffect] {
        let entities = DatabaseEntityMutationExecutor(
            container: container,
            limits: limits
        )
        switch statement {
        case .insert(let query):
            return try await executeInsert(
                query,
                transaction: transaction,
                entities: entities,
                preconditions: preconditions,
                workMeter: workMeter
            )
        case .update(let query):
            return try await executeUpdate(
                query,
                transaction: transaction,
                entities: entities,
                preconditions: preconditions,
                workMeter: workMeter
            )
        case .delete(let query):
            return try await executeDelete(
                query,
                transaction: transaction,
                entities: entities,
                preconditions: preconditions,
                workMeter: workMeter
            )
        case .sparqlUpdate:
            throw DatabaseEntityStatementMutationError.unsupportedStatement(
                "SPARQL updates require a graph mutation executor"
            )
        case .createGraph, .dropGraph:
            throw DatabaseEntityStatementMutationError.unsupportedStatement(
                "graph definitions are managed by the application runtime"
            )
        case .with, .merge, .truncate, .ddl:
            throw DatabaseEntityStatementMutationError.unsupportedStatement(
                "SQL-only statements require the SQL statement executor"
            )
        case .select, .construct, .ask, .describe:
            throw DatabaseEntityStatementMutationError.unsupportedStatement(
                "read-only statements require a query executor"
            )
        }
    }

    private func executeInsert(
        _ query: InsertQuery,
        transaction: DatabaseTransaction,
        entities: DatabaseEntityMutationExecutor,
        preconditions: [EntityMutationPrecondition],
        workMeter: DatabaseWorkMeter
    ) async throws -> [EntityMutationEffect] {
        guard query.returning == nil else {
            throw DatabaseEntityStatementMutationError.unsupportedStatement(
                "INSERT RETURNING is not representable by mutation effects"
            )
        }
        let entity = try resolve(query.target)
        let columns = try insertColumns(query.columns, entity: entity)
        let rows: [[Expression]]
        switch query.source {
        case .values(let values):
            rows = values
        case .defaultValues:
            guard query.columns == nil else {
                throw DatabaseEntityStatementMutationError.unsupportedStatement(
                    "DEFAULT VALUES cannot specify an explicit column list"
                )
            }
            rows = [[]]
        case .select:
            throw DatabaseEntityStatementMutationError.unsupportedStatement(
                "INSERT SELECT requires a transaction-scoped select executor"
            )
        }

        guard !rows.isEmpty else { return [] }
        guard let requestedRows = UInt32(exactly: rows.count),
              requestedRows <= workMeter.budget.maximumRows else {
            throw DatabaseWorkLimitError.maximumRows(
                stage: .mutationPlanning,
                consumed: 0,
                requested: UInt32(exactly: rows.count) ?? UInt32.max,
                maximum: workMeter.budget.maximumRows
            )
        }
        guard rows.count <= limits.maximumChanges else {
            throw DatabaseEntityMutationError.changeLimitExceeded(
                actual: rows.count,
                maximum: limits.maximumChanges
            )
        }

        let insertBinder = try assignmentBinder(entity: entity, table: query.target, includingColumns: false, workMeter: workMeter)
        let preparedRows = try rows.map { row -> [PreparedAssignment] in
            if row.isEmpty, case .defaultValues = query.source { return [] }
            guard row.count == columns.count || (query.columns == nil && sqlSession != nil && row.count < columns.count) else {
                throw DatabaseEntityStatementMutationError.unsupportedStatement(
                    "INSERT row has \(row.count) values for \(columns.count) columns"
                )
            }
            return try zip(columns, row).compactMap { target, expression in
                if case .defaultValue = expression { return nil }
                guard let schema = target.field else {
                    throw SQLConstraintError.generatedAssignment(relation: entity.relation!.id, column: target.column!.name)
                }
                if let column = target.column {
                    if case .generated = column.policy { throw SQLConstraintError.generatedAssignment(relation: entity.relation!.id, column: column.name) }
                    if case .identity(let identity) = column.policy {
                        if query.overriding == .userValue { return nil }
                        if identity.generation == .always && query.overriding != .systemValue {
                            throw SQLConstraintError.identityAssignment(relation: entity.relation!.id, column: column.name)
                        }
                    }
                }
                return try prepareAssignment(schema: schema, expression: expression, entity: entity, binder: insertBinder)
            }
        }
        let emptyAssignmentRow = CanonicalSourceRow(values: [], layout: CanonicalSourceLayout(slots: []))
        let conflictAssignments: [PreparedAssignment]
        let conflictFilter: CanonicalBoundExpression?
        if case .some(.doUpdate(let assignments, let filter)) = query.onConflict {
            conflictAssignments = try prepareAssignments(assignments, entity: entity, table: query.target, workMeter: workMeter)
            conflictFilter = try prepareFilter(filter, entity: entity, table: query.target, workMeter: workMeter)
        } else {
            conflictAssignments = []
            conflictFilter = nil
        }

        var changes = try DatabaseRetainedArrayBuilder<EntityMutationChange>(
            workMeter: workMeter,
            stage: .mutationPlanning,
            layout: try DatabaseRetainedArrayLayout.forElement(EntityMutationChange.self),
            expectedCount: rows.count
        )
        for (rowIndex, row) in rows.enumerated() {
            try workMeter.consume(at: .mutationPlanning)
            var payloads = try payloadBuilder(workMeter: workMeter)
            let suppliedFields: FieldObject
            if row.isEmpty, case .defaultValues = query.source {
                suppliedFields = FieldObject()
            } else {
                guard row.count == columns.count || (query.columns == nil && sqlSession != nil && row.count < columns.count) else {
                    throw DatabaseEntityStatementMutationError.unsupportedStatement(
                        "INSERT row has \(row.count) values for \(columns.count) columns"
                    )
                }
                let evaluator = DatabaseExpressionEvaluator(
                    fields: [:],
                    workMeter: workMeter
                )
                var suppliedEntries: [(key: String, value: FieldValue)] = []
                for assignment in preparedRows[rowIndex] {
                    suppliedEntries.append((
                        key: assignment.schema.name,
                        value: try await evaluateAssignment(assignment, evaluator: evaluator, row: emptyAssignmentRow, payloads: &payloads, workMeter: workMeter)
                    ))
                }
                suppliedFields = try FieldObject(consume suppliedEntries)
            }

            let policyFields = try await applyingInsertPolicies(to: suppliedFields, entity: entity,
                payloads: &payloads, workMeter: workMeter)
            let candidate = try entity.runtime.persistedModel(from: policyFields)
            let candidateFields = try DatabaseEntityProjection.fieldObject(
                for: candidate
            )
            let candidateIdentity = try entity.runtime.identity(for: candidate)
            let targetIdentity = try EntityReference(
                entity: candidateIdentity.entity,
                id: candidateIdentity.id,
                partitions: query.target.partitions
            )
            let resolved = try entities.resolveReference(
                targetIdentity,
                model: candidate
            )
            try workMeter.consume(at: .storageRow)
            let existing = try await transaction.loadPersistedModel(
                entity: entity.name,
                id: resolved.id,
                partition: resolved.partition
            )

            switch (query.onConflict, existing) {
            case (.none, _), (.some(.doNothing), .none),
                 (.some(.doUpdate), .none):
                let change = EntityMutationChange(
                    kind: .insert,
                    identity: targetIdentity,
                    fields: candidateFields
                )
                let payloadOwner = try payloads.finish().moveToSharedOwnership(at: .mutationPlanning)
                try withExtendedLifetime(payloadOwner) { try changes.append(
                    footprint: try DatabaseEntityMutationFootprintMeter
                        .footprint(of: change, workMeter: workMeter),
                    make: { change }
                ) }
            case (.some(.doNothing), .some):
                continue
            case (.some(.doUpdate), .some(let model)):
                let originalFields = try DatabaseEntityProjection.fieldObject(
                    for: model
                )
                let evaluation = evaluationFields(
                    originalFields,
                    table: query.target
                )
                if let conflictFilter,
                   try await !matches(conflictFilter, fields: originalFields, entity: entity,
                                      table: query.target, workMeter: workMeter) {
                    continue
                }
                let updatedFields = try await applying(
                    conflictAssignments,
                    to: originalFields,
                    evaluationFields: evaluation,
                    table: query.target,
                    entity: entity,
                    payloads: &payloads,
                    workMeter: workMeter
                )
                let updated = try entity.runtime.persistedModel(
                    from: updatedFields
                )
                let change = EntityMutationChange(
                    kind: .update,
                    identity: try entity.runtime.identity(for: model),
                    fields: try DatabaseEntityProjection.fieldObject(
                        for: updated
                    )
                )
                let payloadOwner = try payloads.finish().moveToSharedOwnership(at: .mutationPlanning)
                try withExtendedLifetime(payloadOwner) { try changes.append(
                    footprint: try DatabaseEntityMutationFootprintMeter
                        .footprint(of: change, workMeter: workMeter),
                    make: { change }
                ) }
            }
        }
        return try await executeRetainedChanges(
            changes.finish(),
            entities: entities,
            preconditions: preconditions,
            transaction: transaction,
            workMeter: workMeter
        )
    }

    private func executeUpdate(
        _ query: UpdateQuery,
        transaction: DatabaseTransaction,
        entities: DatabaseEntityMutationExecutor,
        preconditions: [EntityMutationPrecondition],
        workMeter: DatabaseWorkMeter
    ) async throws -> [EntityMutationEffect] {
        guard query.from == nil else {
            throw DatabaseEntityStatementMutationError.unsupportedStatement(
                "UPDATE FROM requires a transaction-scoped join executor"
            )
        }
        guard query.returning == nil else {
            throw DatabaseEntityStatementMutationError.unsupportedStatement(
                "UPDATE RETURNING is not representable by mutation effects"
            )
        }
        guard !query.assignments.isEmpty else {
            throw DatabaseEntityStatementMutationError.unsupportedStatement(
                "UPDATE has no assignments"
            )
        }
        let entity = try resolve(query.target)
        let assignments = try prepareAssignments(query.assignments, entity: entity, table: query.target, workMeter: workMeter)
        let filter = try prepareFilter(query.filter, entity: entity, table: query.target, workMeter: workMeter)
        let scan = try await scanChanges(
            entity,
            transaction: transaction,
            workMeter: workMeter
        ) { model in
            try workMeter.consume(at: .mutationPlanning)
            let originalFields = try DatabaseEntityProjection.fieldObject(
                for: model
            )
            let evaluation = evaluationFields(
                originalFields,
                table: query.target
            )
            if let filter,
               try await !matches(filter, fields: originalFields, entity: entity,
                                  table: query.target, workMeter: workMeter) {
                return nil
            }
            var payloads = try payloadBuilder(workMeter: workMeter)
            let updatedFields = try await applying(
                assignments,
                to: originalFields,
                evaluationFields: evaluation,
                table: query.target,
                entity: entity,
                payloads: &payloads,
                workMeter: workMeter
            )
            let updated = try entity.runtime.persistedModel(
                from: updatedFields
            )
            return try DatabasePreparedStatementMutationChange(
                change: EntityMutationChange(
                    kind: .update,
                    identity: try entity.runtime.identity(for: model),
                    fields: try DatabaseEntityProjection.fieldObject(
                        for: updated
                    )
                ),
                payloads: try payloads.finish().moveToSharedOwnership(at: .mutationPlanning),
                workMeter: workMeter
            )
        }
        return try await executeRetainedChanges(
            scan.takeChanges(),
            entities: entities,
            preconditions: preconditions,
            transaction: transaction,
            workMeter: workMeter
        )
    }

    private func executeDelete(
        _ query: DeleteQuery,
        transaction: DatabaseTransaction,
        entities: DatabaseEntityMutationExecutor,
        preconditions: [EntityMutationPrecondition],
        workMeter: DatabaseWorkMeter
    ) async throws -> [EntityMutationEffect] {
        guard query.using == nil else {
            throw DatabaseEntityStatementMutationError.unsupportedStatement(
                "DELETE USING requires a transaction-scoped join executor"
            )
        }
        guard query.returning == nil else {
            throw DatabaseEntityStatementMutationError.unsupportedStatement(
                "DELETE RETURNING is not representable by mutation effects"
            )
        }
        let entity = try resolve(query.target)
        let filter = try prepareFilter(query.filter, entity: entity, table: query.target, workMeter: workMeter)
        let scan = try await scanChanges(
            entity,
            transaction: transaction,
            workMeter: workMeter
        ) { model in
            try workMeter.consume(at: .mutationPlanning)
            let fields = try DatabaseEntityProjection.fieldObject(
                for: model
            )
            if let filter,
               try await !matches(filter, fields: fields, entity: entity,
                                  table: query.target, workMeter: workMeter) {
                return nil
            }
            return try DatabasePreparedStatementMutationChange(
                change: EntityMutationChange(
                    kind: .delete,
                    identity: try entity.runtime.identity(for: model)
                ),
                workMeter: workMeter
            )
        }
        return try await executeRetainedChanges(
            scan.takeChanges(),
            entities: entities,
            preconditions: preconditions,
            transaction: transaction,
            workMeter: workMeter
        )
    }

    private func scanChanges(
        _ entity: ResolvedEntity,
        transaction: DatabaseTransaction,
        workMeter: DatabaseWorkMeter,
        transform: @Sendable (
            borrowing PersistedModel
        ) async throws -> DatabasePreparedStatementMutationChange?
    ) async throws -> sending DatabaseRetainedEntityMutationScan {
        let configuredMaximum = workMeter.budget.maximumRows
        guard let maximumRows = Int(exactly: configuredMaximum),
              maximumRows < Int.max else {
            throw DatabaseEntityStatementMutationError
                .scanLimitUnsupportedOnCurrentPlatform(
                    maximum: configuredMaximum
                )
        }
        let scan = try await transaction.scanPersistedModelChangesForMutation(
            entity: entity.name,
            partition: entity.partition,
            maximumRows: maximumRows,
            maximumChanges: limits.maximumChanges,
            workMeter: workMeter,
            transform: transform
        )
        guard !scan.hasMoreSourceRows else {
            throw DatabaseEntityStatementMutationError.scanLimitExceeded(
                actual: maximumRows + 1,
                maximum: configuredMaximum
            )
        }
        return scan
    }

    private func executeRetainedChanges(
        _ changes: consuming DatabaseRetainedBuffer<EntityMutationChange>,
        entities: DatabaseEntityMutationExecutor,
        preconditions: [EntityMutationPrecondition],
        transaction: DatabaseTransaction,
        workMeter: DatabaseWorkMeter
    ) async throws -> [EntityMutationEffect] {
        guard !changes.isEmpty else {
            try await entities.validate(
                preconditions,
                transaction: transaction,
                workMeter: workMeter
            )
            return []
        }
        let prepared: DatabasePreparedEntityMutation
        do {
            let retained = changes.moveRetainingReservation()
            defer { retained.reservation.release() }
            prepared = try entities.prepare(
                retained.elements,
                preconditions: preconditions,
                workMeter: workMeter
            )
        }
        return try await entities.executeStatementEffects(
            prepared,
            workMeter: workMeter,
            transaction: transaction
        )
    }

    private func resolve(_ table: TableRef) throws -> ResolvedEntity {
        try sqlSession?.resolution.rejectReadOnlyMutation(table)
        let selected = try sqlSession?.resolution.resolve(table)
        guard selected != nil || table.schema == nil else {
            throw DatabaseEntityStatementMutationError.unsupportedStatement(
                "compiled entities do not use SQL schema qualifiers"
            )
        }
        let physicalName = selected?.entity.name ?? table.table
        guard let entity = (sqlSchema ?? container.schema).entities.first(where: {
            SQLConstraintCompiler.same($0.name, physicalName)
        }) else {
            throw DatabaseEntityMutationError.unknownEntity(table.table)
        }
        guard let runtime = container.runtimeConfiguration.entityRuntimes
            .registration(named: entity.name) else {
            throw DatabaseEntityMutationError.entityHasNoPersistableType(
                table.table
            )
        }
        let partition: AnyDirectoryPath?
        do {
            if !table.partitions.isEmpty || (sqlSession == nil && entity.hasDynamicDirectory) {
                partition = try AnyDirectoryPath(
                    entity: entity,
                    partitions: table.partitions
                )
            } else {
                partition = nil
            }
        } catch let error as DirectoryPathError {
            throw DatabaseEntityMutationError.invalidPartition(
                entity: entity.name,
                reason: error.description
            )
        } catch {
            throw DatabaseEntityMutationError.invalidPartition(
                entity: entity.name,
                reason: "Partition does not match the compiled entity schema"
            )
        }
        return ResolvedEntity(
            name: entity.name,
            runtime: runtime,
            fields: entity.fields,
            partition: partition,
            relation: selected?.declaration ?? sqlCatalog?.relations.first(where: { SQLConstraintCompiler.same($0.entityName, entity.name) })
        )
    }

    private struct InsertColumn: Sendable {
        let field: FieldSchema?
        let column: SQLColumn?
    }

    private func insertColumns(_ names: [String]?, entity: ResolvedEntity) throws -> [InsertColumn] {
        let ordered: [InsertColumn]
        if let relation = entity.relation, sqlSession != nil {
            ordered = relation.columns.map { column in
                InsertColumn(field: column.fieldNumber.flatMap { number in entity.fields.first { $0.fieldNumber == number } }, column: column)
            }
        } else {
            ordered = entity.fields.sorted { $0.fieldNumber < $1.fieldNumber }.map { InsertColumn(field: $0, column: policyColumn($0, entity: entity)) }
        }
        guard let names else { return ordered }
        var seen = Set<SQLCatalogName>()
        return try names.map { name in
            guard seen.insert(SQLCatalogName(name)).inserted else {
                throw DatabaseEntityMutationError.invalidCompiledSchema(entity: entity.name, reason: "INSERT column is duplicated")
            }
            guard let target = ordered.first(where: {
                let targetName = sqlSession == nil ? $0.field?.name : $0.column?.name
                return targetName.map { SQLConstraintCompiler.same($0, name) } ?? false
            }) else {
                throw DatabaseEntityMutationError.invalidCompiledSchema(entity: entity.name, reason: "INSERT column is not declared")
            }
            return target
        }
    }

    private func applying(
        _ assignments: [PreparedAssignment],
        to fields: FieldObject,
        evaluationFields: [String: FieldValue],
        table: TableRef,
        entity: ResolvedEntity,
        payloads: inout DatabaseRetainedArrayBuilder<DatabaseQueryPayloadLease>,
        workMeter: DatabaseWorkMeter
    ) async throws -> FieldObject {
        var byName = Dictionary(
            uniqueKeysWithValues: fields.fields.map { ($0.key, $0.value) }
        )
        let evaluator = DatabaseExpressionEvaluator(
            fields: evaluationFields,
            workMeter: workMeter
        )
        let row = try await assignmentRow(fields: fields, entity: entity, table: table, workMeter: workMeter)
        // A selected column may borrow its source claim; keep that owner through destination admission.
        for lease in row.payloadLeases {
            try payloads.append(footprint: DatabaseIntermediateFootprint(), make: { lease })
        }
        for assignment in assignments {
            if case .defaultValue = assignment.expression {
                if let column = policyColumn(assignment.schema, entity: entity), case .generated = column.policy { continue }
                byName[assignment.schema.name] = try await policyDefault(field: assignment.schema, entity: entity,
                    payloads: &payloads, workMeter: workMeter)
                continue
            }
            byName[assignment.schema.name] = try await evaluateAssignment(
                assignment, evaluator: evaluator, row: row, payloads: &payloads, workMeter: workMeter
            )
        }
        return try await applyingGenerated(to: FieldObject(byName.map { (key: $0.key, value: $0.value) }),
            entity: entity, payloads: &payloads, workMeter: workMeter)
    }

    private func evaluationFields(
        _ fields: FieldObject,
        table: TableRef
    ) -> [String: FieldValue] {
        var values: [String: FieldValue] = [:]
        for field in fields.fields {
            values[field.key] = field.value
            values["\(table.table).\(field.key)"] = field.value
            if let alias = table.alias {
                values["\(alias).\(field.key)"] = field.value
            }
        }
        return values
    }

    private struct PreparedAssignment: Sendable {
        let schema: FieldSchema
        let expression: Expression
        let bound: CanonicalBoundExpression?
    }

    private func assignmentBinder(
        entity: ResolvedEntity, table: TableRef, includingColumns: Bool,
        workMeter: DatabaseWorkMeter
    ) throws -> CanonicalQueryTypeBinder {
        if includingColumns, sqlSession != nil, let relation = entity.relation, let schema = sqlSchema {
            return try SQLConstraintCompiler.binder(relation: relation, physicalSchema: schema, workMeter: workMeter, tableAlias: table.alias)
        }
        let fields = includingColumns ? entity.fields : []
        let columns = try fields.map { field in
            try QueryColumn(number: fieldNumber(field, entity: entity.name), name: logicalName(field, entity: entity),
                type: field.queryType ?? .primitive(field.type, isArray: field.isArray), isNullable: field.isOptional)
        }
        return CanonicalQueryTypeBinder(columns: columns, resolveColumn: { reference in
            guard reference.table == nil || reference.table == table.table || reference.table == table.alias,
                  let slot = fields.firstIndex(where: { SQLConstraintCompiler.same(logicalName($0, entity: entity), reference.column) }) else {
                throw DatabaseExpressionEvaluationError.typeMismatch(operation: "mutation source column")
            }
            return slot
        }, wildcardSlots: { _ in Array(fields.indices) }, workMeter: workMeter)
    }

    private func prepareAssignment(
        schema: FieldSchema, expression: Expression, entity: ResolvedEntity,
        binder: CanonicalQueryTypeBinder
    ) throws -> PreparedAssignment {
        _ = try fieldNumber(schema, entity: entity.name)
        if case .defaultValue = expression { return PreparedAssignment(schema: schema, expression: expression, bound: nil) }
        try binder.requireScalarPhase(expression)
        let type = schema.queryType ?? .primitive(schema.type, isArray: schema.isArray)
        let selected: Bool
        switch type {
        case .sql: selected = true
        case .primitive(let kind, false):
            switch kind {
            case .string, .bytes, .uuid, .bool, .int8, .int16, .int32, .int64, .uint8, .uint16,
                 .uint32, .uint64, .float32, .float64, .decimal, .date, .time, .dateTime, .timestamp: selected = true
            default: selected = false
            }
        default: selected = false
        }
        return PreparedAssignment(schema: schema, expression: expression,
            bound: selected ? try binder.bindScalarAssignment(expression, to: type) : nil)
    }

    private func prepareAssignments(
        _ assignments: [Assignment], entity: ResolvedEntity, table: TableRef,
        workMeter: DatabaseWorkMeter
    ) throws -> [PreparedAssignment] {
        let binder = try assignmentBinder(entity: entity, table: table, includingColumns: true, workMeter: workMeter)
        var seen = Set<String>()
        return try assignments.compactMap { assignment in
            guard assignment.columns.count == 1 else { throw SQLMutationError.invalidStatement("tuple assignment requires SQL execution") }
            guard seen.insert(assignment.columns[0]).inserted else {
                throw DatabaseEntityStatementMutationError.unsupportedStatement(
                    "column '\(assignment.columns[0])' is assigned more than once")
            }
            let target = entity.relation?.columns.first(where: { SQLConstraintCompiler.same($0.name, assignment.columns[0]) })
            if sqlSession != nil, let target, case .virtual = target.backing {
                guard case .defaultValue = assignment.value else { throw SQLConstraintError.generatedAssignment(relation: entity.relation!.id, column: target.name) }
                return nil
            }
            guard let schema = entity.fields.first(where: {
                if sqlSession != nil && policyColumn($0, entity: entity) == nil { return false }
                return SQLConstraintCompiler.same(logicalName($0, entity: entity), assignment.columns[0])
            }) else {
                throw DatabaseEntityMutationError.invalidCompiledSchema(entity: entity.name,
                    reason: "assignment column '\(assignment.columns[0])' is not compiled")
            }
            if let column = policyColumn(schema, entity: entity) {
                switch column.policy {
                case .generated:
                    guard case .defaultValue = assignment.value else { throw SQLConstraintError.generatedAssignment(relation: entity.relation!.id, column: column.name) }
                case .identity(let identity) where identity.generation == .always:
                    guard case .defaultValue = assignment.value else { throw SQLConstraintError.identityAssignment(relation: entity.relation!.id, column: column.name) }
                default: break
                }
            }
            return try prepareAssignment(schema: schema, expression: assignment.value, entity: entity, binder: binder)
        }
    }

    private func prepareFilter(
        _ expression: Expression?, entity: ResolvedEntity, table: TableRef, workMeter: DatabaseWorkMeter
    ) throws -> CanonicalBoundExpression? {
        guard let expression else { return nil }
        return try assignmentBinder(entity: entity, table: table, includingColumns: true, workMeter: workMeter)
            .bindPredicate(expression)
    }

    private func matches(
        _ filter: CanonicalBoundExpression, fields: FieldObject, entity: ResolvedEntity,
        table: TableRef, workMeter: DatabaseWorkMeter
    ) async throws -> Bool {
        let row = try await assignmentRow(fields: fields, entity: entity, table: table, workMeter: workMeter)
        let result = try await DatabaseContext.evaluateCanonicalScalar(filter, on: row, temporalEnvironment: temporalEnvironment, session: sqlSession, workMeter: workMeter)
        return try withExtendedLifetime(result.leases) {
            switch result.value {
            case .primitive(.bool(let value)): return value
            case .primitive(.null): return false
            default: throw CanonicalReadError.expressionEvaluation(.typeMismatch(operation: "mutation predicate"))
            }
        }
    }

    private func payloadBuilder(workMeter: DatabaseWorkMeter) throws -> DatabaseRetainedArrayBuilder<DatabaseQueryPayloadLease> {
        try DatabaseRetainedArrayBuilder(workMeter: workMeter, stage: .mutationPlanning,
            layout: DatabaseRetainedArrayLayout.forElement(DatabaseQueryPayloadLease.self))
    }

    private func assignmentRow(
        fields: FieldObject, entity: ResolvedEntity, table: TableRef, workMeter: DatabaseWorkMeter
    ) async throws -> CanonicalSourceRow {
        if sqlSession != nil, let relation = entity.relation, let schema = sqlSchema {
            return try await SQLConstraintRows.row(fields: fields, relation: relation, physicalSchema: schema,
                environment: temporalEnvironment!, workMeter: workMeter)
        }
        let layoutFootprint = try CanonicalRelationalFootprintMeter.sourceLayoutFootprint(
            fieldNames: entity.fields.lazy.map { $0.name }, sourceName: table.table,
            workMeter: workMeter, stage: .mutationPlanning)
        let layoutReservation = try workMeter.reserveIntermediate(bytes: layoutFootprint.bytes, at: .mutationPlanning)
        let layout = CanonicalSourceLayout(slots: entity.fields.map { .init(name: $0.name, scope: table.table) },
            scopes: [table.table], reservation: layoutReservation,
            types: entity.fields.map { $0.queryType ?? .primitive($0.type, isArray: $0.isArray) },
            nullability: entity.fields.map { $0.isOptional })
        let limits = assignmentValueLimits(workMeter)
        func value(at slot: Int) throws -> QueryValue {
            let schema = entity.fields[slot]
            let payload = fields[schema.name] ?? .null
            if let type = schema.queryType {
                return try QueryFieldStorageAdapter.decode(payload, type: type, limits: limits)
            }
            return .primitive(payload)
        }
        let footprint = try CanonicalRelationalFootprintMeter.positionalValueArrayFootprint(
            count: entity.fields.count, valueAt: value, annotations: [:], version: nil,
            workMeter: workMeter, stage: .mutationPlanning)
        let reservation = try workMeter.reserveIntermediate(rows: footprint.rows, bytes: footprint.bytes, at: .mutationPlanning)
        return try CanonicalSourceRow(values: entity.fields.indices.map(value), layout: layout,
            payloadLeases: [DatabaseQueryPayloadLease(retaining: reservation)])
    }

    private func assignmentValueLimits(_ workMeter: DatabaseWorkMeter) -> QueryValueLimits {
        QueryValueLimits(maximumRetainedBytes: workMeter.budget.maximumIntermediateBytes,
            maximumTotalNodes: workMeter.budget.maximumWorkUnits, maximumNestingDepth: workMeter.budget.maximumWorkUnits,
            maximumCollectionElements: workMeter.budget.maximumWorkUnits)
    }

    private func evaluateAssignment(
        _ assignment: PreparedAssignment, evaluator: DatabaseExpressionEvaluator, row: CanonicalSourceRow,
        payloads: inout DatabaseRetainedArrayBuilder<DatabaseQueryPayloadLease>, workMeter: DatabaseWorkMeter
    ) async throws -> FieldValue {
        guard let bound = assignment.bound else { return try evaluator.evaluate(assignment.expression) }
        let result = try await DatabaseContext.evaluateCanonicalScalar(bound, on: row, temporalEnvironment: temporalEnvironment, session: sqlSession, workMeter: workMeter)
        for lease in result.leases {
            guard lease.workMeter === workMeter else { throw DatabaseIntermediateReservationError.workMeterMismatch }
            try payloads.append(footprint: DatabaseIntermediateFootprint(), make: { lease })
        }
        guard let descriptor = assignment.schema.queryType else {
            guard case .primitive(let value) = result.value else { throw QueryValueError.incompatibleType }
            return value
        }
        let limits = assignmentValueLimits(workMeter)
        if case .primitive = result.value {
            return try QueryFieldStorageAdapter.encode(result.value, type: descriptor, limits: limits)
        }
        let maximum = try DatabaseEntityMutationFootprintMeter.encodedValueFootprint(
            result.value, type: descriptor, workMeter: workMeter)
        let encoded = try DatabaseQueryScopedFieldValue.producing(maximumFootprint: maximum,
            workMeter: workMeter, stage: .mutationPlanning) {
                try QueryFieldStorageAdapter.encode(result.value, type: descriptor, limits: limits)
            }
        if let reservation = encoded.retainedReservation {
            try payloads.append(footprint: DatabaseIntermediateFootprint(), make: { DatabaseQueryPayloadLease(retaining: reservation) })
        }
        return encoded.withValue { copy $0 }
    }

    private func fieldNumber(
        _ schema: FieldSchema,
        entity: String
    ) throws -> UInt32 {
        guard schema.fieldNumber > 0,
              let number = UInt32(exactly: schema.fieldNumber) else {
            throw DatabaseEntityMutationError.invalidCompiledSchema(
                entity: entity,
                reason: "field '\(schema.name)' has invalid number \(schema.fieldNumber)"
            )
        }
        return number
    }

    private struct ResolvedEntity: Sendable {
        let name: String
        let runtime: EntityRuntimeRegistration
        let fields: [FieldSchema]
        let partition: AnyDirectoryPath?
        let relation: SQLRelation?
    }
}

extension DatabaseEntityStatementMutationExecutor {
    private func policyColumn(_ field: FieldSchema, entity: ResolvedEntity) -> SQLColumn? {
        entity.relation?.columns.first { $0.fieldNumber == field.fieldNumber }
    }

    private func logicalName(_ field: FieldSchema, entity: ResolvedEntity) -> String {
        sqlSession == nil ? field.name : (policyColumn(field, entity: entity)?.name ?? field.name)
    }

    private func policyDefault(field: FieldSchema, entity: ResolvedEntity,
        payloads: inout DatabaseRetainedArrayBuilder<DatabaseQueryPayloadLease>, workMeter: DatabaseWorkMeter) async throws -> FieldValue {
        guard let column = policyColumn(field, entity: entity) else { return sqlSession == nil ? (field.defaultValue ?? .null) : .null }
        let expression: Expression
        switch column.policy {
        case .input(let declared):
            guard let declared else { return sqlSession == nil ? (field.defaultValue ?? .null) : .null }
            expression = declared
        case .identity(let identity):
            let relation = entity.relation!
            let occurrence = "\(relation.id.utf8.count):\(relation.id)\(column.id.utf8.count):\(column.id)"
            expression = .literal(.int(try await SQLSequenceRuntime.nextValue(sequenceID: identity.sequenceID, occurrence: occurrence)))
        case .generated: return .null
        }
        let binder = try assignmentBinder(entity: entity, table: TableRef(table: entity.name), includingColumns: false, workMeter: workMeter)
        let assignment = try prepareAssignment(schema: field, expression: expression, entity: entity, binder: binder)
        let row = CanonicalSourceRow(values: [], layout: CanonicalSourceLayout(slots: []))
        let snapshot = try sqlSession?.resolution.snapshot ?? SQLSequenceRuntime.request?.context.readPolicy().sqlCatalog
        let bindings = entity.relation.flatMap { relation in
            snapshot?.sequenceDefaultBindings(relationID: relation.id, columnID: column.id)
        }
        return try await SQLSequenceRuntime.$defaultBindings.withValue(bindings) {
            try await evaluateAssignment(assignment, evaluator: DatabaseExpressionEvaluator(fields: [:], workMeter: workMeter),
                row: row, payloads: &payloads, workMeter: workMeter)
        }
    }

    private func applyingInsertPolicies(to supplied: FieldObject, entity: ResolvedEntity,
        payloads: inout DatabaseRetainedArrayBuilder<DatabaseQueryPayloadLease>, workMeter: DatabaseWorkMeter) async throws -> FieldObject {
        guard entity.relation != nil else { return supplied }
        var fields = supplied.fields
        for field in entity.fields where supplied[field.name] == nil {
            if let column = policyColumn(field, entity: entity), case .generated = column.policy { continue }
            let value = try await policyDefault(field: field, entity: entity, payloads: &payloads, workMeter: workMeter)
            fields.append((key: field.name, value: value))
        }
        return try await applyingGenerated(to: FieldObject(consume fields), entity: entity,
            payloads: &payloads, workMeter: workMeter)
    }

    private func applyingGenerated(to fields: FieldObject, entity: ResolvedEntity,
        payloads: inout DatabaseRetainedArrayBuilder<DatabaseQueryPayloadLease>, workMeter: DatabaseWorkMeter) async throws -> FieldObject {
        guard let relation = entity.relation, let schema = sqlSchema,
              relation.columns.contains(where: { if case .generated = $0.policy { return true }; return false }) else { return fields }
        let row = try await SQLConstraintRows.row(fields: fields, relation: relation, physicalSchema: schema,
            environment: temporalEnvironment!, workMeter: workMeter, recomputingStored: true)
        for lease in row.payloadLeases {
            try payloads.append(footprint: DatabaseIntermediateFootprint(), make: { lease })
        }
        let encoded = try SQLConstraintRows.fields(row, relation: relation, physicalSchema: schema,
            preserving: fields, workMeter: workMeter)
        for lease in encoded.leases { try payloads.append(footprint: DatabaseIntermediateFootprint(), make: { lease }) }
        return encoded.fields
    }
}

extension DatabaseEntityStatementMutationExecutor {
    func prepareSQLReads(_ input: QueryStatement, scope: SQLStatementReadScope,
        transaction: DatabaseTransaction, workMeter: DatabaseWorkMeter) async throws -> SQLMutationPreparation {
        try QueryStructuralValidator.validate(input, limits: scope.execution.queryStructuralLimits)
        var executor = self
        executor.temporalEnvironment = scope.temporalEnvironment ?? transaction.makeSQLTemporalEnvironment()
        let (catalog, schema) = try await transaction.sqlPolicyCatalog()
        executor.sqlCatalog = catalog
        executor.sqlSchema = schema
        executor.sqlSession = try await transaction.makeSQLSessionBinding(searchPath: scope.searchPath, workMeter: workMeter)
        let declarations = try scope.declarationSnapshot()
        return try await ActiveSQLStatementReads.$declarations.withValue(declarations) {
            try await executor.prepareSQL(input, scope: scope, transaction: transaction, workMeter: workMeter)
        }
    }

    private struct SQLStatementBinding {
        let table: TableRef
        let source: DataSource?
        let returning: [ProjectionItem]?
        let aliases: SQLReturningAliases?
        let expressions: [Expression]
        let insertSourceQuery: SelectQuery?
        let entity: ResolvedEntity
        let relation: SQLRelation
        let layout: CanonicalSourceLayout
        let sourceLayout: CanonicalSourceLayout?
        let joined: CanonicalSourceLayout
        let layoutClaim: DatabaseIntermediateReservation
    }

    /// Resolves and types the ordinary DML path without capturing or evaluating rows.
    func analyzeSQL(_ input: QueryStatement, scope: SQLStatementReadScope,
        transaction: DatabaseTransaction, workMeter: DatabaseWorkMeter) async throws -> [QueryColumn] {
        try QueryStructuralValidator.validate(input, limits: scope.execution.queryStructuralLimits)
        var executor = self
        executor.temporalEnvironment = scope.temporalEnvironment ?? transaction.makeSQLTemporalEnvironment()
        let (catalog, schema) = try await transaction.sqlPolicyCatalog()
        executor.sqlCatalog = catalog
        executor.sqlSchema = schema
        executor.sqlSession = try await transaction.makeSQLSessionBinding(searchPath: scope.searchPath, workMeter: workMeter)
        let binding = try executor.bindSQL(input, scope: scope, workMeter: workMeter)
        _ = try executor.sqlPreparationBindings(input, entity: binding.entity,
            targetLayout: binding.layout, joinedLayout: binding.joined, scope: scope)
        let returningLayout = try executor.sqlReturningRow(current: executor.sqlNullRow(binding.layout), old: nil, new: nil,
            source: binding.sourceLayout.map(executor.sqlNullRow), aliases: binding.aliases,
            merge: { if case .merge = input { return true }; return false }(), action: nil, workMeter: workMeter).layout
        return try binding.returning.map {
            try scope.bindProjection($0, layout: returningLayout,
                allowingMergeAction: { if case .merge = input { return true }; return false }()).columns
        } ?? []
    }

    private func bindSQL(_ input: QueryStatement, scope: SQLStatementReadScope,
        workMeter: DatabaseWorkMeter) throws -> SQLStatementBinding {
        let table: TableRef
        let source: DataSource?
        let returning: [ProjectionItem]?
        let aliases: SQLReturningAliases?
        var expressions: [Expression] = []
        var insertSourceQuery: SelectQuery?
        switch input {
        case .insert(let query):
            table = query.target; source = nil; returning = query.returning; aliases = query.returningAliases
            if case .values(let rows) = query.source { expressions += rows.flatMap { $0 } }
            if case .select(let query) = query.source { insertSourceQuery = SelectQuery(projection: [.wildcard()], source: .subquery(query)) }
            if case .doUpdate(let assignments, let filter) = query.onConflict {
                expressions += assignments.map { $0.value }; if let filter { expressions.append(filter) }
            }
        case .update(let query):
            table = query.target; source = query.from; returning = query.returning; aliases = query.returningAliases
            expressions += query.assignments.map { $0.value }; if let filter = query.filter { expressions.append(filter) }
        case .delete(let query):
            table = query.target; source = query.using; returning = query.returning; aliases = query.returningAliases
            if let filter = query.filter { expressions.append(filter) }
        case .merge(let query):
            table = query.target; source = query.source; returning = query.returning; aliases = query.returningAliases
            expressions.append(query.condition)
            var unconditional = Set<SQLMergeMatch>()
            for clause in query.clauses {
                guard !unconditional.contains(clause.match) else { throw SQLMutationError.invalidStatement("unreachable MERGE clause") }
                if clause.condition == nil { unconditional.insert(clause.match) }
                if let condition = clause.condition { expressions.append(condition) }
                switch clause.action {
                case .update(let assignments):
                    guard clause.match != .notMatchedByTarget else { throw SQLMutationError.invalidStatement("source-only UPDATE") }
                    expressions += assignments.map { $0.value }
                case .delete:
                    guard clause.match != .notMatchedByTarget else { throw SQLMutationError.invalidStatement("source-only DELETE") }
                case .insert(_, let values, _):
                    guard clause.match == .notMatchedByTarget else { throw SQLMutationError.invalidStatement("matched INSERT") }
                    expressions += values ?? []
                case .doNothing: break
                }
            }
        default: throw SQLMutationError.invalidStatement("expected SQL mutation")
        }
        let entity = try resolve(table)
        guard let relation = entity.relation, let schema = sqlSchema else { throw SQLMutationError.invalidStatement("missing SQL relation") }
        let columns = try relation.columns.enumerated().map { index, column in
            try QueryColumn(number: UInt32(index + 1), name: column.name,
                type: SQLConstraintCompiler.columnType(column, relation: relation, physicalSchema: schema), isNullable: true)
        }
        let metadataBytes = try DatabaseIntermediateFootprint(bytes: UInt64(MemoryLayout<CanonicalSourceLayout.Slot>.stride + 64))
            .multiplied(by: UInt64(columns.count))
        let layoutClaim = try workMeter.reserveIntermediate(bytes: metadataBytes.bytes, at: .mutationPlanning)
        let layout = CanonicalSourceLayout(slots: columns.map { .init(name: $0.name, scope: table.effectiveName) },
            scopes: [table.effectiveName], reservation: layoutClaim, types: columns.map { Optional($0.type) },
            nullability: columns.map { $0.isNullable })
        let sourceLayout = try source.map { try scope.relationLayout($0, outerLayout: layout) }
        let joined = try sourceLayout.map { try sqlNullRow(layout).merged(with: sqlNullRow($0)).layout } ?? layout
        return SQLStatementBinding(table: table, source: source, returning: returning, aliases: aliases,
            expressions: expressions, insertSourceQuery: insertSourceQuery, entity: entity, relation: relation,
            layout: layout, sourceLayout: sourceLayout, joined: joined, layoutClaim: layoutClaim)
    }

    private func prepareSQL(_ input: QueryStatement, scope: SQLStatementReadScope,
        transaction: DatabaseTransaction, workMeter: DatabaseWorkMeter) async throws -> SQLMutationPreparation {
        let binding = try bindSQL(input, scope: scope, workMeter: workMeter)
        let table = binding.table, source = binding.source, returning = binding.returning, aliases = binding.aliases
        let expressions = binding.expressions, insertSourceQuery = binding.insertSourceQuery
        let entity = binding.entity, relation = binding.relation, layout = binding.layout
        let sourceLayout = binding.sourceLayout, joined = binding.joined
        var readClaims = try payloadBuilder(workMeter: workMeter)
        try readClaims.append(footprint: DatabaseIntermediateFootprint(), make: { DatabaseQueryPayloadLease(retaining: binding.layoutClaim) })
        let demandProjection = expressions.map { ProjectionItem.expression($0) } + (returning ?? [])
        let referenceQuery = SelectQuery(projection: demandProjection, source: .table(table))
        var fields = Set<String>()
        func physicalFields(_ column: SQLColumn) -> Set<String> {
            var selected = Set<String>()
            if let number = column.fieldNumber, let field = entity.fields.first(where: { $0.fieldNumber == number }) {
                selected.insert(field.name)
            } else if case .generated(let expression) = column.policy {
                let dependencies = SelectQuery(projection: [.expression(expression)], source: .values([])).referencedColumns
                for dependency in dependencies {
                    if let column = relation.columns.first(where: { SQLConstraintCompiler.same($0.name, dependency.column) }),
                       let number = column.fieldNumber, let field = entity.fields.first(where: { $0.fieldNumber == number }) { selected.insert(field.name) }
                }
            }
            return selected
        }
        for reference in referenceQuery.referencedColumns {
            let transitionNames = [aliases?.old ?? "old", aliases?.new ?? "new"]
            if reference.table == nil || reference.table == table.effectiveName || transitionNames.contains(reference.table!) || reference.table == "excluded" {
                if let column = relation.columns.first(where: { SQLConstraintCompiler.same($0.name, reference.column) }) { fields.formUnion(physicalFields(column)) }
            }
        }
        if returning?.contains(where: { if case .wildcard(let qualifier) = $0 { return qualifier == nil || qualifier == table.effectiveName || qualifier == aliases?.old || qualifier == aliases?.new || qualifier == "old" || qualifier == "new" }; return false }) == true {
            for column in relation.columns { fields.formUnion(physicalFields(column)) }
        }
        guard let maximumRows = Int(exactly: workMeter.budget.maximumRows) else { throw SQLMutationError.invalidStatement("row budget out of range") }
        let sourceProjection: [ProjectionItem]
        if let sourceLayout {
            var references = referenceQuery.referencedColumns.filter { sourceLayout.matching($0).count == 1 }
            var wildcard = false
            for item in returning ?? [] {
                if case .wildcard(let qualifier) = item {
                    if let qualifier, sourceLayout.scopes.contains(qualifier) { wildcard = true }
                    if qualifier == nil, case .merge = input { wildcard = true }
                }
            }
            if wildcard {
                // Binds every visible source slot positionally; source output names may repeat.
                sourceProjection = [.wildcard()]
            } else {
                references = references.filter { $0.table == nil || $0.table != table.effectiveName }
                sourceProjection = references.sorted { $0.displayName < $1.displayName }.map { .expression(.column($0)) }
            }
        } else { sourceProjection = [] }
        let readQuery = source.map { SelectQuery(projection: sourceProjection.isEmpty ? [.expression(.literal(.int(1)))] : sourceProjection, source: $0) }
        let bindings = try sqlPreparationBindings(input, entity: entity, targetLayout: layout, joinedLayout: joined, scope: scope)
        for bound in bindings { try scope.prepare(bound, outerLayout: joined) }
        if let insertSourceQuery { try scope.prepare(insertSourceQuery) }
        if let readQuery { try scope.prepare(readQuery, outerLayout: layout) }
        let returnLayout = try sqlReturningRow(current: sqlNullRow(layout), old: nil, new: nil,
            source: sourceLayout.map(sqlNullRow), aliases: aliases, merge: { if case .merge = input { return true }; return false }(),
            action: nil, workMeter: workMeter).layout
        let returningPlan = try returning.map { try scope.bindProjection($0, layout: returnLayout, allowingMergeAction: { if case .merge = input { return true }; return false }()) }
        if let returningPlan { for expression in returningPlan.projection { try scope.prepare(expression, outerLayout: returnLayout) } }
        var returningFields: (current: Set<String>, old: Set<String>, new: Set<String>) = ([], [], [])
        let oldName = aliases?.old ?? "old"
        let newName = aliases?.new ?? "new"
        func returningFieldsForColumn(_ name: String) -> Set<String> {
            guard let column = relation.columns.first(where: { SQLConstraintCompiler.same($0.name, name) }) else { return [] }
            return physicalFields(column)
        }
        for reference in SelectQuery(projection: returning ?? [], source: .values([])).referencedColumns {
            let selected = returningFieldsForColumn(reference.column)
            if reference.table == oldName { returningFields.old.formUnion(selected) }
            else if reference.table == newName { returningFields.new.formUnion(selected) }
            else if reference.table == nil || reference.table == table.effectiveName { returningFields.current.formUnion(selected) }
        }
        for item in returning ?? [] {
            guard case .wildcard(let qualifier) = item else { continue }
            let selected = relation.columns.reduce(into: Set<String>()) { $0.formUnion(returningFieldsForColumn($1.name)) }
            if qualifier == oldName { returningFields.old.formUnion(selected) }
            else if qualifier == newName { returningFields.new.formUnion(selected) }
            else if qualifier == nil || qualifier == table.effectiveName { returningFields.current.formUnion(selected) }
        }
        try await scope.capture()
        let models: DatabaseRetainedPersistedModels
        if case .insert(let query) = input, query.onConflict == nil {
            let empty = try DatabaseRetainedArrayBuilder<DatabaseRetainedPersistedModels.Entry?>(workMeter: workMeter,
                stage: .mutationPlanning, layout: DatabaseRetainedArrayLayout.forElement(DatabaseRetainedPersistedModels.Entry?.self))
            models = try DatabaseRetainedPersistedModels(buffer: empty.finish())
        } else {
            models = try await transaction.scanSQLMutationModels(relation: relation, partition: entity.partition, fields: fields,
                maximumRows: maximumRows, workMeter: workMeter)
        }
        return try SQLMutationPreparation(executor: self, input: input, scopeIdentity: ObjectIdentifier(scope),
            transactionIdentity: ObjectIdentifier(transaction), mutationOwner: scope.allocateMutationOwner(),
            declarations: try scope.declarationSnapshot(), target: table, relation: relation, models: models,
            targetLayout: layout, source: source, sourceReadQuery: readQuery, sourceLayout: sourceLayout,
            returningPlan: returningPlan, returningReadFields: returningFields, readClaims: readClaims.finish().moveToSharedOwnership(at: .mutationPlanning))
    }

    private func sqlNullRow(_ layout: CanonicalSourceLayout) -> CanonicalSourceRow {
        CanonicalSourceRow(values: Array(repeating: .primitive(.null), count: layout.slots.count), layout: layout,
            sourcePresence: Array(repeating: false, count: layout.frameScopes.reduce(0) { $0 + $1.count }))
    }

    private func sqlRow(_ model: borrowing PersistedModel, entity: ResolvedEntity, table: TableRef,
        workMeter: DatabaseWorkMeter) async throws -> CanonicalSourceRow {
        // The caller's async model borrow retains its owner across this same-task row boundary.
        await Task.yield()
        try workMeter.checkpoint(at: .mutationPlanning)
        do {
            let row = try await assignmentRow(fields: DatabaseEntityProjection.fieldObject(for: model), entity: entity,
                table: table, workMeter: workMeter).applyingAlias(table.effectiveName)
            await Task.yield()
            try workMeter.checkpoint(at: .mutationPlanning)
            return row
        } catch {
            await Task.yield()
            throw error
        }
    }

    private func sqlReturningRow(current: CanonicalSourceRow, old: CanonicalSourceRow?, new: CanonicalSourceRow?,
        source: CanonicalSourceRow?, aliases: SQLReturningAliases?, merge: Bool, action: String?,
        workMeter: DatabaseWorkMeter) throws -> CanonicalSourceRow {
        var visible = current
        if let source { visible = try visible.merged(with: source) }
        let visibleSlots = merge ? visible.layout.visibleSlots : current.layout.visibleSlots.compactMap { index in
            visible.layout.matching(ColumnRef(table: current.layout.slots[index].scope, column: current.layout.slots[index].name)).first
        }
        let old = (old ?? sqlNullRow(current.layout)).applyingAlias(aliases?.old ?? "old")
        let new = (new ?? sqlNullRow(current.layout)).applyingAlias(aliases?.new ?? "new")
        let complete = try visible.merged(with: old).merged(with: new)
        let footprint = try CanonicalRelationalFootprintMeter.footprint(of: complete, workMeter: workMeter)
        let claim = try workMeter.reserveIntermediate(bytes: footprint.bytes, at: .mutationPlanning)
        return CanonicalSourceRow(values: complete.values,
            layout: CanonicalSourceLayout(slots: complete.layout.slots, scopes: complete.layout.scopes,
                reservation: claim, types: complete.layout.types,
                nullability: merge ? Array(repeating: true, count: complete.layout.slots.count) : complete.layout.nullability,
                visibleSlots: visibleSlots, frameSizes: complete.layout.frameSizes, frameScopes: complete.layout.frameScopes,
                bindingSlots: Array(complete.layout.bindingSlots)),
            annotations: action.map { ["sql.merge.action": .string($0)] } ?? [:], payloadLeases: complete.payloadLeases,
            sourcePresence: complete.sourcePresence)
    }
}

extension DatabaseEntityStatementMutationExecutor {
    private struct SQLAction: Sendable {
        let change: DatabaseSQLMutationChange
        let source: CanonicalSourceRow?
        let action: String
        let row: CanonicalSourceRow?
        let identity: EntityReference
        let leases: DatabaseSharedRetainedArray<DatabaseQueryPayloadLease>
    }
    private struct SQLTarget: Sendable {
        let model: PersistedModel
        let row: CanonicalSourceRow
        let identity: EntityReference
    }
    private struct SQLAssignment: Sendable {
        let fields: [FieldSchema]
        let bound: CanonicalBoundExpression?
    }
    private struct SQLArbiter: Sendable {
        let ids: [String]
        let nulls: SQLNullUniqueness
        let attributes: SQLConstraintAttributes?
        let index: SQLCompiledIndex?

        init(ids: [String], nulls: SQLNullUniqueness,
            attributes: SQLConstraintAttributes? = nil, index: SQLCompiledIndex? = nil) {
            self.ids = ids
            self.nulls = nulls
            self.attributes = attributes
            self.index = index
        }
    }

    private func executePreparedSQL(_ input: QueryStatement, preparation: SQLMutationPreparation,
        scope: SQLStatementReadScope, transaction: DatabaseTransaction,
        workMeter: DatabaseWorkMeter) async throws -> SQLMutationOutcome {
        let table = preparation.target
        let entity = try resolve(table)
        var targets = try SQLMutationArray<SQLTarget>(workMeter: workMeter)
        for entry in preparation.models {
            guard let entry else { continue }
            try await entry.withModel { model in
                let row = try await sqlRow(model, entity: entity, table: table, workMeter: workMeter)
                try targets.append(SQLTarget(model: copy model, row: row, identity: entity.runtime.identity(for: model)))
            }
        }
        let source: CanonicalRelation?
        if let from = preparation.source, let readQuery = preparation.sourceReadQuery {
            source = try await scope.relation(from, readQuery: readQuery)
        } else { source = nil }
        var actions = try SQLMutationArray<SQLAction>(workMeter: workMeter)
        let sourceLayout = preparation.sourceLayout
        let targetNull = sqlNullRow(preparation.targetLayout)
        let joinedLayout = try sourceLayout.map { try targetNull.merged(with: sqlNullRow($0)).layout } ?? preparation.targetLayout
        switch input {
        case .insert(let query):
            try await planSQLInsert(query, entity: entity, targets: targets.elements, inputRow: nil,
                scope: scope, actions: &actions, workMeter: workMeter)
        case .update(let query):
            let assignments = try sqlAssignments(query.assignments, entity: entity, layout: joinedLayout, scope: scope)
            let filter = try query.filter.map { try scope.bindPredicate($0, layout: joinedLayout) }
            for target in targets.elements {
                if let source {
                    for row in source.rows {
                        let joined = try target.row.merged(with: row)
                        guard try await sqlMatches(filter, row: joined, scope: scope) else { continue }
                        try await appendSQLUpdate(target, assignments: assignments, evaluation: joined, source: row,
                            entity: entity, table: table, scope: scope, actions: &actions, workMeter: workMeter)
                        break
                    }
                } else if try await sqlMatches(filter, row: target.row, scope: scope) {
                    try await appendSQLUpdate(target, assignments: assignments, evaluation: target.row, source: nil,
                        entity: entity, table: table, scope: scope, actions: &actions, workMeter: workMeter)
                }
            }
        case .delete(let query):
            let filter = try query.filter.map { try scope.bindPredicate($0, layout: joinedLayout) }
            for target in targets.elements {
                var selected: CanonicalSourceRow?
                var found = false
                if let source {
                    for row in source.rows {
                        if try await sqlMatches(filter, row: target.row.merged(with: row), scope: scope) { selected = row; found = true; break }
                    }
                } else { found = try await sqlMatches(filter, row: target.row, scope: scope) }
                if found { try appendSQLDelete(target, source: selected, actions: &actions, workMeter: workMeter) }
            }
        case .merge(let query):
            guard let source else { throw SQLMutationError.invalidStatement("MERGE source is absent") }
            let join = try scope.bindPredicate(query.condition, layout: joinedLayout)
            var matchedTargets = try SQLMutationArray<EntityReference>(workMeter: workMeter)
            for row in source.rows {
                var matched = false
                for target in targets.elements {
                    let joined = try target.row.merged(with: row)
                    guard try await sqlMatches(join, row: joined, scope: scope) else { continue }
                    matched = true
                    if !matchedTargets.elements.contains(target.identity) { try matchedTargets.append(target.identity) }
                    try await planSQLMerge(query, match: .matched, target: target, source: row, evaluation: joined,
                        entity: entity, scope: scope, actions: &actions, targets: targets.elements, workMeter: workMeter)
                }
                if !matched {
                    try await planSQLMerge(query, match: .notMatchedByTarget, target: nil, source: row,
                        evaluation: targetNull.merged(with: row), entity: entity, scope: scope,
                        actions: &actions, targets: targets.elements, workMeter: workMeter)
                }
            }
            let sourceNull = sourceLayout.map(sqlNullRow)
            for target in targets.elements where !matchedTargets.elements.contains(target.identity) {
                let row = try sourceNull.map { try target.row.merged(with: $0) } ?? target.row
                try await planSQLMerge(query, match: .notMatchedBySource, target: target, source: sourceNull,
                    evaluation: row, entity: entity, scope: scope, actions: &actions, targets: targets.elements, workMeter: workMeter)
            }
        default: throw SQLMutationError.invalidStatement("expected SQL mutation")
        }
        // Statement-local cardinality wins before cross-definition write ownership is claimed.
        for action in actions.elements {
            try scope.claimMutationTarget(action.identity, owner: preparation.mutationOwner)
            if let newModel = action.change.newModel {
                let identity = try entity.runtime.identity(for: newModel)
                if identity != action.identity { try scope.claimMutationTarget(identity, owner: preparation.mutationOwner) }
            }
        }
        var changes = try DatabaseRetainedArrayBuilder<DatabaseSQLMutationChange>(workMeter: workMeter,
            stage: .mutationPlanning, layout: DatabaseRetainedArrayLayout.forElement(DatabaseSQLMutationChange.self),
            expectedCount: actions.elements.count)
        for action in actions.elements {
            try changes.append(footprint: DatabaseIntermediateFootprint(), make: { action.change })
        }
        let outcomes = try await DatabaseEntityMutationExecutor(container: container, limits: limits)
            .executeSQLChanges(changes.finish().moveToSharedOwnership(at: .mutationPlanning), workMeter: workMeter, transaction: transaction)
        var output = try DatabaseRetainedArrayBuilder<CanonicalSourceRow>(workMeter: workMeter,
            stage: .projection, layout: DatabaseRetainedArrayLayout.forElement(CanonicalSourceRow.self), expectedCount: outcomes.count)
        let aliases: SQLReturningAliases?
        let merge: Bool
        switch input {
        case .insert(let query): aliases = query.returningAliases; merge = false
        case .update(let query): aliases = query.returningAliases; merge = false
        case .delete(let query): aliases = query.returningAliases; merge = false
        case .merge(let query): aliases = query.returningAliases; merge = true
        default: aliases = nil; merge = false
        }
        if preparation.returningPlan != nil {
            let readPolicy = try scope.context.readPolicy()
            for (index, outcome) in outcomes.enumerated() {
                var oldFields = preparation.returningReadFields.old
                var newFields = preparation.returningReadFields.new
                if outcome.newModel != nil { newFields.formUnion(preparation.returningReadFields.current) }
                else { oldFields.formUnion(preparation.returningReadFields.current) }
                if let model = outcome.oldModel, !oldFields.isEmpty { try readPolicy.authorizeGet(model, fields: oldFields) }
                if let model = outcome.newModel, !newFields.isEmpty { try readPolicy.authorizeGet(model, fields: newFields) }
                let old: CanonicalSourceRow?
                if let model = outcome.oldModel { old = try await sqlRow(model, entity: entity, table: table, workMeter: workMeter) } else { old = nil }
                let new: CanonicalSourceRow?
                if let model = outcome.newModel { new = try await sqlRow(model, entity: entity, table: table, workMeter: workMeter) } else { new = nil }
                let current = new ?? old ?? targetNull
                let row = try sqlReturningRow(current: current, old: old, new: new, source: actions.elements[index].source,
                    aliases: aliases, merge: merge, action: merge ? actions.elements[index].action : nil, workMeter: workMeter)
                try output.append(footprint: CanonicalRelationalFootprintMeter.footprint(of: row, workMeter: workMeter), make: { row })
            }
        }
        let returning: SQLBlockCollectedRows?
        if let plan = preparation.returningPlan {
            returning = try await scope.project(plan, rows: output.finish().moveToSharedOwnership(at: .projection), affectedRows: outcomes.count)
        } else { returning = nil }
        guard let outputCount = UInt32(exactly: outcomes.count) else { throw SQLMutationError.invalidStatement("affected count out of range") }
        try workMeter.recordOutputRows(outputCount)
        return SQLMutationOutcome(effects: outcomes, affectedRows: UInt64(outcomes.count), returning: returning)
    }

    private func sqlMatches(_ bound: CanonicalBoundExpression?, row: CanonicalSourceRow,
        scope: SQLStatementReadScope) async throws -> Bool {
        // Every candidate advances cooperatively, including an absent predicate's TRUE result.
        let workMeter = scope.execution.workMeter
        await Task.yield()
        try workMeter.checkpoint(at: .mutationPlanning)
        do {
            let result: Bool
            if let bound {
                let value = try await scope.evaluate(bound, on: row)
                result = try withExtendedLifetime(value.leases) {
                    switch value.value {
                    case .primitive(.bool(let flag)): return flag
                    case .primitive(.null): return false
                    default: throw DatabaseExpressionEvaluationError.typeMismatch(operation: "mutation predicate")
                    }
                }
            } else { result = true }
            await Task.yield()
            try workMeter.checkpoint(at: .mutationPlanning)
            return result
        } catch {
            await Task.yield()
            throw error
        }
    }

    private func sqlAssignments(_ assignments: [Assignment], entity: ResolvedEntity,
        layout: CanonicalSourceLayout, scope: SQLStatementReadScope) throws -> [SQLAssignment] {
        var seen = Set<SQLCatalogName>()
        let expanded = try assignments.flatMap { assignment -> [Assignment] in
            if assignment.columns.count > 1, case .row(let cells) = assignment.value, cells.contains(.defaultValue) {
                guard cells.count == assignment.columns.count else { throw SQLMutationError.cardinalityViolation }
                return zip(assignment.columns, cells).map { Assignment(column: $0.0, value: $0.1) }
            }
            return [assignment]
        }
        return try expanded.compactMap { assignment in
            guard !assignment.columns.isEmpty else { throw SQLMutationError.invalidStatement("empty assignment target") }
            let fields = try assignment.columns.compactMap { name -> FieldSchema? in
                guard seen.insert(SQLCatalogName(name)).inserted,
                      let column = entity.relation?.columns.first(where: { SQLConstraintCompiler.same($0.name, name) }) else {
                    throw SQLMutationError.invalidStatement("unknown or duplicate assignment target")
                }
                if case .virtual = column.backing {
                    guard assignment.value == .defaultValue else { throw SQLConstraintError.generatedAssignment(relation: entity.relation!.id, column: name) }
                    return nil
                }
                guard let number = column.fieldNumber, let field = entity.fields.first(where: { $0.fieldNumber == number }) else {
                    throw SQLMutationError.invalidStatement("missing physical assignment field")
                }
                switch column.policy {
                case .generated:
                    guard assignment.value == .defaultValue else { throw SQLConstraintError.generatedAssignment(relation: entity.relation!.id, column: name) }
                case .identity(let identity) where identity.generation == .always:
                    guard assignment.value == .defaultValue else { throw SQLConstraintError.identityAssignment(relation: entity.relation!.id, column: name) }
                default: break
                }
                return field
            }
            if fields.isEmpty { return nil }
            if assignment.value == .defaultValue {
                guard assignment.columns.count == 1 else { throw SQLMutationError.invalidStatement("tuple DEFAULT requires a row constructor") }
                return SQLAssignment(fields: fields, bound: nil)
            }
            let type: QueryColumnType
            if fields.count == 1 { type = fields[0].queryType ?? .primitive(fields[0].type, isArray: fields[0].isArray) }
            else {
                let columns = try fields.enumerated().map { index, field in
                    try QueryColumn(number: UInt32(index + 1), name: field.name,
                        type: field.queryType ?? .primitive(field.type, isArray: field.isArray), isNullable: field.isOptional)
                }
                type = .sql(.composite(try QueryCompositeType(columns: columns, limits: SQLConstraintRows.limits(scope.execution.workMeter))))
            }
            return SQLAssignment(fields: fields, bound: try scope.bindAssignment(assignment.value, to: type, layout: layout))
        }
    }

    private func sqlApply(_ assignments: [SQLAssignment], fields: FieldObject, evaluation: CanonicalSourceRow,
        entity: ResolvedEntity, scope: SQLStatementReadScope,
        payloads: inout DatabaseRetainedArrayBuilder<DatabaseQueryPayloadLease>, workMeter: DatabaseWorkMeter) async throws -> FieldObject {
        var result = fields.fields
        for assignment in assignments {
            let values: [QueryValue]?
            if let bound = assignment.bound {
                let evaluated = try await scope.evaluate(bound, on: evaluation)
                for lease in evaluated.leases { try payloads.append(footprint: DatabaseIntermediateFootprint(), make: { lease }) }
                if assignment.fields.count == 1 { values = [evaluated.value] }
                else if case .composite(let tuple) = evaluated.value { values = Array(tuple.fields) }
                else if evaluated.value == .primitive(.null) { values = Array(repeating: .primitive(.null), count: assignment.fields.count) }
                else { throw SQLMutationError.cardinalityViolation }
                guard values!.count == assignment.fields.count else { throw SQLMutationError.cardinalityViolation }
            } else { values = nil }
            for (index, field) in assignment.fields.enumerated() {
                if let column = policyColumn(field, entity: entity), case .generated = column.policy { continue }
                let value: FieldValue
                if let values {
                    if let type = field.queryType {
                        let maximum = try DatabaseEntityMutationFootprintMeter.encodedValueFootprint(values[index], type: type, workMeter: workMeter)
                        let encoded = try DatabaseQueryScopedFieldValue.producing(maximumFootprint: maximum,
                            workMeter: workMeter, stage: .mutationPlanning) {
                                try QueryFieldStorageAdapter.encode(values[index], type: type, limits: assignmentValueLimits(workMeter))
                            }
                        if let reservation = encoded.retainedReservation {
                            try payloads.append(footprint: DatabaseIntermediateFootprint(), make: { DatabaseQueryPayloadLease(retaining: reservation) })
                        }
                        value = encoded.withValue { copy $0 }
                    } else if case .primitive(let scalar) = values[index] { value = scalar }
                    else { throw QueryValueError.incompatibleType }
                } else { value = try await policyDefault(field: field, entity: entity, payloads: &payloads, workMeter: workMeter) }
                result.removeAll { SQLConstraintCompiler.same($0.key, field.name) }; result.append((key: field.name, value: value))
            }
        }
        return try await applyingGenerated(to: FieldObject(result), entity: entity, payloads: &payloads, workMeter: workMeter)
    }

    private func appendSQLUpdate(_ target: SQLTarget, assignments: [SQLAssignment], evaluation: CanonicalSourceRow,
        source: CanonicalSourceRow?, entity: ResolvedEntity, table: TableRef, scope: SQLStatementReadScope,
        actions: inout SQLMutationArray<SQLAction>, workMeter: DatabaseWorkMeter) async throws {
        var payloads = try payloadBuilder(workMeter: workMeter)
        let fields = try await sqlApply(assignments, fields: DatabaseEntityProjection.fieldObject(for: target.model),
            evaluation: evaluation, entity: entity, scope: scope, payloads: &payloads, workMeter: workMeter)
        let model = try entity.runtime.persistedModel(from: fields)
        let row = try await sqlRow(model, entity: entity, table: table, workMeter: workMeter)
        let footprint = try DatabaseEntityMutationFootprintMeter.footprint(identity: target.identity, model: model, workMeter: workMeter)
        try actions.append(SQLAction(change: DatabaseSQLMutationChange(kind: .update, oldModel: target.model, newModel: model),
            source: source, action: "UPDATE", row: row, identity: target.identity,
            leases: payloads.finish().moveToSharedOwnership(at: .mutationPlanning)), footprint: footprint)
    }

    private func appendSQLDelete(_ target: SQLTarget, source: CanonicalSourceRow?,
        actions: inout SQLMutationArray<SQLAction>, workMeter: DatabaseWorkMeter) throws {
        let payloads = try payloadBuilder(workMeter: workMeter)
        try actions.append(SQLAction(change: DatabaseSQLMutationChange(kind: .delete, oldModel: target.model, newModel: nil),
            source: source, action: "DELETE", row: nil, identity: target.identity,
            leases: payloads.finish().moveToSharedOwnership(at: .mutationPlanning)))
    }
}

extension DatabaseEntityStatementMutationExecutor {
    private func sqlArbiters(_ query: InsertQuery, relation: SQLRelation,
        workMeter: DatabaseWorkMeter) throws -> [SQLArbiter] {
        var keys: [SQLArbiter] = []
        if let ids = relation.primaryKeyColumnIDs, let attributes = relation.primaryKeyConstraint {
            keys.append(SQLArbiter(ids: ids, nulls: .notDistinct, attributes: attributes))
        }
        for constraint in relation.constraints {
            if case .unique(let ids, let nulls) = constraint.kind {
                keys.append(SQLArbiter(ids: ids, nulls: nulls, attributes: constraint.attributes))
            }
        }
        if let catalog = sqlCatalog, let schema = sqlSchema {
            for index in catalog.indexes where index.unique
                && SQLConstraintCompiler.same(index.relationID, relation.id) {
                let compiled = try SQLIndexCompiler.compile(index, relation: relation,
                    physicalSchema: schema, workMeter: workMeter)
                keys.append(SQLArbiter(ids: SQLIndexCompiler.conflictColumns(index, relation: relation) ?? [],
                    nulls: index.nulls, index: compiled))
            }
        }
        let selected: [SQLArbiter]
        switch query.conflictTarget {
        case .columns(let names, let predicate):
            let ids = try names.map { name -> SQLCatalogName in
                guard let column = relation.columns.first(where: { SQLConstraintCompiler.same($0.name, name) }) else { throw SQLMutationError.invalidConflictTarget }
                return SQLCatalogName(column.id)
            }
            guard Set(ids).count == ids.count else { throw SQLMutationError.invalidConflictTarget }
            selected = try keys.filter { arbiter in
                guard Set(arbiter.ids.map(SQLCatalogName.init)) == Set(ids) else { return false }
                guard let index = arbiter.index else { return true }
                return try SQLIndexCompiler.predicateImplies(query: predicate, index: index.declaration, relation: relation)
            }
        case .constraint(let name):
            selected = keys.filter { arbiter in
                arbiter.attributes.map { SQLConstraintCompiler.same($0.name, name) } ?? false
            }
        case nil:
            if case .doUpdate = query.onConflict { throw SQLMutationError.invalidConflictTarget }
            selected = keys
        }
        if query.conflictTarget != nil && selected.isEmpty { throw SQLMutationError.invalidConflictTarget }
        if query.conflictTarget != nil && selected.contains(where: { if case .deferrable? = $0.attributes?.timing { return true }; return false }) {
            throw SQLMutationError.deferrableConflictTarget
        }
        return selected.filter { if case .deferrable? = $0.attributes?.timing { return false }; return true }
    }

    private func sqlKeyMatches(_ left: CanonicalSourceRow, _ right: CanonicalSourceRow,
        arbiter: SQLArbiter, relation: SQLRelation, workMeter: DatabaseWorkMeter) async throws -> Bool {
        if let index = arbiter.index {
            guard let a = try await index.key(row: left, environment: temporalEnvironment!, workMeter: workMeter),
                  let b = try await index.key(row: right, environment: temporalEnvironment!, workMeter: workMeter) else {
                return false
            }
            if arbiter.nulls == .distinct && (a.hasNull || b.hasNull) { return false }
            return try a.equal(to: b, workMeter: workMeter)
        }
        if arbiter.nulls == .distinct && (SQLConstraintRows.nullCount(left, ids: arbiter.ids, relation: relation) > 0
            || SQLConstraintRows.nullCount(right, ids: arbiter.ids, relation: relation) > 0) { return false }
        let a = try SQLConstraintRows.key(left, ids: arbiter.ids, relation: relation,
            environment: temporalEnvironment!, workMeter: workMeter)
        let b = try SQLConstraintRows.key(right, ids: arbiter.ids, relation: relation,
            environment: temporalEnvironment!, workMeter: workMeter)
        return try SQLConstraintRows.equal(a, b, workMeter: workMeter)
    }

    private func planSQLInsert(_ query: InsertQuery, entity: ResolvedEntity, targets: [SQLTarget],
        inputRow: CanonicalSourceRow?, scope: SQLStatementReadScope,
        actions: inout SQLMutationArray<SQLAction>, workMeter: DatabaseWorkMeter) async throws {
        guard let relation = entity.relation else { throw SQLMutationError.invalidConflictTarget }
        let columns = try insertColumns(query.columns, entity: entity)
        let arbiters = try query.onConflict.map { _ in try sqlArbiters(query, relation: relation, workMeter: workMeter) } ?? []
        let row: CanonicalSourceRow
        if let inputRow { row = inputRow }
        else { row = CanonicalSourceRow(values: [], layout: CanonicalSourceLayout(slots: [])) }
        let rawRows: [[Expression]]
        var selected: CanonicalRelation?
        switch query.source {
        case .values(let values):
            guard !values.isEmpty, let width = values.first?.count, width > 0, values.allSatisfy({ $0.count == width }) else {
                throw SQLMutationError.invalidStatement("invalid INSERT VALUES shape")
            }
            rawRows = values
        case .defaultValues:
            guard query.columns == nil else { throw SQLMutationError.invalidStatement("DEFAULT VALUES column list") }
            rawRows = [[]]
        case .select(let select):
            // Retained canonical SELECT values are consumed positionally, without a primitive roundtrip.
            selected = try await scope.relation(.subquery(select), readQuery: SelectQuery(projection: [.wildcard()], source: .subquery(select)))
            rawRows = []
        }
        let nullTarget = try await assignmentRow(fields: FieldObject(), entity: entity, table: query.target, workMeter: workMeter)
            .applyingAlias(query.target.effectiveName)
        let conflictLayout = try nullTarget.merged(with: nullTarget.applyingAlias("excluded")).layout
        let conflictAssignments: [SQLAssignment]
        let conflictFilter: CanonicalBoundExpression?
        if case .doUpdate(let assignments, let filter) = query.onConflict {
            conflictAssignments = try sqlAssignments(assignments, entity: entity, layout: conflictLayout, scope: scope)
            conflictFilter = try filter.map { try scope.bindPredicate($0, layout: conflictLayout) }
        } else { conflictAssignments = []; conflictFilter = nil }
        if let selected {
            for selectedRow in selected.rows {
                let values = selectedRow.layout.visibleSlots.map { selectedRow.values[$0] }
                let expressions = values.enumerated().map { Expression.column(ColumnRef(column: "c\($0.offset)")) }
                let layout = CanonicalSourceLayout(slots: values.indices.map { .init(name: "c\($0)", scope: nil) },
                    types: selectedRow.layout.visibleSlots.map { selectedRow.layout.types[$0] })
                let evaluation = CanonicalSourceRow(values: values, layout: layout, payloadLeases: selectedRow.payloadLeases)
                try await planSQLInsertRow(expressions, defaults: false, query: query, columns: columns, entity: entity,
                    evaluation: evaluation, targets: targets, arbiters: arbiters, conflictAssignments: conflictAssignments,
                    conflictFilter: conflictFilter, returningSource: nil, scope: scope, actions: &actions, workMeter: workMeter)
            }
        } else {
            for expressions in rawRows {
                try await planSQLInsertRow(expressions, defaults: { if case .defaultValues = query.source { return true }; return false }(),
                    query: query, columns: columns, entity: entity, evaluation: row, targets: targets,
                    arbiters: arbiters, conflictAssignments: conflictAssignments, conflictFilter: conflictFilter,
                    returningSource: inputRow, scope: scope, actions: &actions, workMeter: workMeter)
            }
        }
    }

    private func planSQLInsertRow(_ expressions: [Expression], defaults: Bool, query: InsertQuery,
        columns: [InsertColumn], entity: ResolvedEntity, evaluation: CanonicalSourceRow,
        targets: [SQLTarget], arbiters: [SQLArbiter], conflictAssignments: [SQLAssignment],
        conflictFilter: CanonicalBoundExpression?, returningSource: CanonicalSourceRow?, scope: SQLStatementReadScope,
        actions: inout SQLMutationArray<SQLAction>, workMeter: DatabaseWorkMeter) async throws {
        try workMeter.consume(at: .mutationPlanning)
        guard defaults || expressions.count == columns.count || (query.columns == nil && expressions.count < columns.count) else {
            throw SQLMutationError.invalidStatement("INSERT source/target arity")
        }
        var assignments: [SQLAssignment] = []
        for (target, expression) in zip(columns, expressions) {
            if expression == .defaultValue { continue }
            guard let field = target.field else { throw SQLConstraintError.generatedAssignment(relation: entity.relation!.id, column: target.column!.name) }
            if let column = target.column {
                if case .generated = column.policy { throw SQLConstraintError.generatedAssignment(relation: entity.relation!.id, column: column.name) }
                if case .identity(let identity) = column.policy {
                    if query.overriding == .userValue { continue }
                    if identity.generation == .always && query.overriding != .systemValue { throw SQLConstraintError.identityAssignment(relation: entity.relation!.id, column: column.name) }
                }
            }
            assignments.append(SQLAssignment(fields: [field], bound: try scope.bindAssignment(expression,
                to: field.queryType ?? .primitive(field.type, isArray: field.isArray), layout: evaluation.layout)))
        }
        var payloads = try payloadBuilder(workMeter: workMeter)
        var supplied = try await sqlApply(assignments, fields: FieldObject(), evaluation: evaluation, entity: entity,
            scope: scope, payloads: &payloads, workMeter: workMeter)
        if entity.runtime.compiledModelContract == nil, let id = entity.fields.first(where: { $0.name == "id" }),
           entity.relation?.columns.contains(where: { $0.fieldNumber == id.fieldNumber }) == false {
            guard let request = SQLMutationRuntime.request else { throw SQLMutationError.identityRequestRequired }
            let value = try await request.allocateHiddenIdentity(relationID: entity.relation!.id,
                column: id, statement: SQLMutationRuntime.statementIndex)
            supplied = try FieldObject(supplied.fields.filter { $0.key != id.name } + [(key: id.name, value: value)])
        }
        let fields = try await applyingInsertPolicies(to: supplied, entity: entity, payloads: &payloads, workMeter: workMeter)
        let candidate = try entity.runtime.persistedModel(from: fields)
        let identity = try entity.runtime.identity(for: candidate)
        if !query.target.partitions.isEmpty {
            let requested = try EntityReference(entity: identity.entity, id: identity.id,
                partitions: query.target.partitions)
            _ = try DatabaseEntityMutationExecutor(container: container, limits: limits)
                .resolveReference(requested, model: candidate)
        }
        let candidateRow = try await sqlRow(candidate, entity: entity, table: query.target, workMeter: workMeter)
        var conflict: SQLTarget?
        var conflictIsModified = false
        if query.onConflict != nil {
            for arbiter in arbiters {
                for target in targets {
                    let action = actions.elements.last(where: { $0.identity == target.identity })
                    if action?.change.kind == .delete { continue }
                    let current = action?.row ?? target.row
                    if try await sqlKeyMatches(current, candidateRow, arbiter: arbiter, relation: entity.relation!, workMeter: workMeter) {
                        conflict = SQLTarget(model: action?.change.newModel ?? target.model, row: current, identity: target.identity)
                        conflictIsModified = action != nil; break
                    }
                }
                if conflict != nil { break }
                for action in actions.elements where action.change.kind == .insert {
                    guard let row = action.row, let model = action.change.newModel else { continue }
                    if try await sqlKeyMatches(row, candidateRow, arbiter: arbiter, relation: entity.relation!, workMeter: workMeter) {
                        conflict = SQLTarget(model: model, row: row, identity: action.identity); conflictIsModified = true; break
                    }
                }
                if conflict != nil { break }
            }
        }
        if let conflict {
            switch query.onConflict {
            case .doNothing: return
            case .doUpdate:
                // PostgreSQL checks repeated modification before the later action predicate.
                guard !conflictIsModified else { throw SQLMutationError.cardinalityViolation }
                let joined = try conflict.row.merged(with: candidateRow.applyingAlias("excluded"))
                guard try await sqlMatches(conflictFilter, row: joined, scope: scope) else { return }
                try await appendSQLUpdate(conflict, assignments: conflictAssignments, evaluation: joined, source: nil,
                    entity: entity, table: query.target, scope: scope, actions: &actions, workMeter: workMeter)
                return
            case nil: break
            }
        }
        let footprint = try DatabaseEntityMutationFootprintMeter.footprint(identity: identity, model: candidate, workMeter: workMeter)
        guard actions.elements.count < limits.maximumChanges else { throw DatabaseEntityMutationError.changeLimitExceeded(actual: actions.elements.count + 1, maximum: limits.maximumChanges) }
        try actions.append(SQLAction(change: DatabaseSQLMutationChange(kind: .insert, oldModel: nil, newModel: candidate),
            source: returningSource, action: "INSERT", row: candidateRow, identity: identity,
            leases: payloads.finish().moveToSharedOwnership(at: .mutationPlanning)), footprint: footprint)
    }

    private func planSQLMerge(_ query: MergeQuery, match: SQLMergeMatch, target: SQLTarget?, source: CanonicalSourceRow?,
        evaluation: CanonicalSourceRow, entity: ResolvedEntity, scope: SQLStatementReadScope,
        actions: inout SQLMutationArray<SQLAction>, targets: [SQLTarget], workMeter: DatabaseWorkMeter) async throws {
        for clause in query.clauses where clause.match == match {
            let filter = try clause.condition.map { try scope.bindPredicate($0, layout: evaluation.layout) }
            guard try await sqlMatches(filter, row: evaluation, scope: scope) else { continue }
            switch clause.action {
            case .doNothing: return
            case .insert(let columns, let values, let overriding):
                let insert = InsertQuery(target: query.target, columns: columns,
                    source: values.map { .values([$0]) } ?? .defaultValues, overriding: overriding)
                try await planSQLInsert(insert, entity: entity, targets: targets, inputRow: source, scope: scope,
                    actions: &actions, workMeter: workMeter)
            case .update(let assignments):
                guard let target else { throw SQLMutationError.invalidStatement("MERGE UPDATE target") }
                guard !actions.elements.contains(where: { $0.identity == target.identity }) else { throw SQLMutationError.cardinalityViolation }
                try await appendSQLUpdate(target, assignments: sqlAssignments(assignments, entity: entity, layout: evaluation.layout, scope: scope),
                    evaluation: evaluation, source: source, entity: entity, table: query.target, scope: scope,
                    actions: &actions, workMeter: workMeter)
            case .delete:
                guard let target else { throw SQLMutationError.invalidStatement("MERGE DELETE target") }
                guard !actions.elements.contains(where: { $0.identity == target.identity }) else { throw SQLMutationError.cardinalityViolation }
                try appendSQLDelete(target, source: source, actions: &actions, workMeter: workMeter)
            }
            return
        }
    }
}

extension DatabaseEntityStatementMutationExecutor {
    private func sqlPreparationBindings(_ input: QueryStatement, entity: ResolvedEntity,
        targetLayout: CanonicalSourceLayout, joinedLayout: CanonicalSourceLayout,
        scope: SQLStatementReadScope) throws -> [CanonicalBoundExpression] {
        var bindings: [CanonicalBoundExpression] = []
        func assignments(_ values: [Assignment], _ layout: CanonicalSourceLayout) throws {
            bindings += try sqlAssignments(values, entity: entity, layout: layout, scope: scope).compactMap { $0.bound }
        }
        func predicate(_ value: Expression?, _ layout: CanonicalSourceLayout) throws {
            if let value { bindings.append(try scope.bindPredicate(value, layout: layout)) }
        }
        func insert(_ query: InsertQuery, _ layout: CanonicalSourceLayout) throws {
            let columns = try insertColumns(query.columns, entity: entity)
            let rows: [[Expression]]
            switch query.source {
            case .values(let values):
                guard let count = values.first?.count, count > 0, values.allSatisfy({ $0.count == count }) else { throw SQLMutationError.invalidStatement("invalid INSERT VALUES shape") }
                rows = values
            case .defaultValues:
                guard query.columns == nil else { throw SQLMutationError.invalidStatement("DEFAULT VALUES columns") }; rows = []
            case .select(let selectedQuery):
                let source = try scope.relationLayout(.subquery(selectedQuery))
                // Selected values are consumed positionally, as execution does; output
                // names may repeat (for example implicit `?column?` names).
                let slots = source.visibleSlots
                let positional = CanonicalSourceLayout(slots: slots.indices.map { .init(name: "c\($0)", scope: nil) },
                    types: slots.map { source.types[$0] }, nullability: slots.map { source.nullability[$0] })
                let expressions = slots.indices.map { Expression.column(ColumnRef(column: "c\($0)")) }
                try cells(expressions, query: query, layout: positional, columns: columns)
                rows = []
            }
            for row in rows { try cells(row, query: query, layout: layout, columns: columns) }
        }
        func cells(_ expressions: [Expression], query: InsertQuery?, layout: CanonicalSourceLayout, columns: [InsertColumn]) throws {
            guard expressions.count == columns.count || (query?.columns == nil && expressions.count < columns.count) else { throw SQLMutationError.invalidStatement("INSERT source/target arity") }
            for (column, expression) in zip(columns, expressions) {
                if expression == .defaultValue { continue }
                guard let field = column.field else { throw SQLConstraintError.generatedAssignment(relation: entity.relation!.id, column: column.column!.name) }
                if let policy = column.column {
                    if case .generated = policy.policy { throw SQLConstraintError.generatedAssignment(relation: entity.relation!.id, column: policy.name) }
                    if case .identity(let identity) = policy.policy {
                        if query?.overriding == .userValue { continue }
                        if identity.generation == .always && query?.overriding != .systemValue { throw SQLConstraintError.identityAssignment(relation: entity.relation!.id, column: policy.name) }
                    }
                }
                bindings.append(try scope.bindAssignment(expression, to: field.queryType ?? .primitive(field.type, isArray: field.isArray), layout: layout))
            }
        }
        switch input {
        case .insert(let query):
            try insert(query, CanonicalSourceLayout(slots: []))
            if query.onConflict != nil {
                _ = try sqlArbiters(query, relation: entity.relation!, workMeter: scope.execution.workMeter)
            }
            if case .doUpdate(let values, let filter) = query.onConflict {
                let layout = try sqlNullRow(targetLayout).merged(with: sqlNullRow(targetLayout).applyingAlias("excluded")).layout
                try assignments(values, layout); try predicate(filter, layout)
            }
        case .update(let query):
            guard !query.assignments.isEmpty else { throw SQLMutationError.invalidStatement("empty UPDATE assignments") }
            try assignments(query.assignments, joinedLayout); try predicate(query.filter, joinedLayout)
        case .delete(let query): try predicate(query.filter, joinedLayout)
        case .merge(let query):
            try predicate(query.condition, joinedLayout)
            for clause in query.clauses {
                // The NULL-extended frame preserves position, while action scope is restricted by the match class.
                let layout: CanonicalSourceLayout
                switch clause.match {
                case .matched: layout = joinedLayout
                case .notMatchedByTarget:
                    let sourceSlots = joinedLayout.slots.indices.filter { joinedLayout.slots[$0].scope != entity.relation?.name && joinedLayout.slots[$0].scope != targetLayout.scopes.first }
                    layout = CanonicalSourceLayout(slots: sourceSlots.map { joinedLayout.slots[$0] },
                        scopes: joinedLayout.scopes.filter { $0 != targetLayout.scopes.first },
                        types: sourceSlots.map { joinedLayout.types[$0] }, nullability: sourceSlots.map { joinedLayout.nullability[$0] })
                case .notMatchedBySource: layout = targetLayout
                }
                try predicate(clause.condition, layout)
                switch clause.action {
                case .update(let values): try assignments(values, layout)
                case .insert(let columns, let values, let overriding):
                    try insert(InsertQuery(target: query.target, columns: columns,
                        source: values.map { .values([$0]) } ?? .defaultValues, overriding: overriding), layout)
                case .delete, .doNothing: break
                }
            }
        default: throw SQLMutationError.invalidStatement("expected mutation")
        }
        return bindings
    }
}
