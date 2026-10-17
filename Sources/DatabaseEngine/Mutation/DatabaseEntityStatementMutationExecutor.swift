import DatabaseKit
import DatabaseTypes

@_spi(DatabaseExecution)
public struct DatabaseEntityStatementMutationExecutor: Sendable {
    private let container: DBContainer
    private let limits: DatabaseEntityMutationLimits
    private var temporalEnvironment: SQLTemporalEnvironment?

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
        return try await statement.executeStatement(statement: statementInput, preconditions: preconditions,
            transaction: transaction, workMeter: workMeter)
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
            guard row.count == columns.count else {
                throw DatabaseEntityStatementMutationError.unsupportedStatement(
                    "INSERT row has \(row.count) values for \(columns.count) columns"
                )
            }
            return try zip(columns, row).map { schema, expression in
                try prepareAssignment(schema: schema, expression: expression, entity: entity, binder: insertBinder)
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
                guard row.count == columns.count else {
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

            let candidate = try entity.runtime.persistedModel(
                from: suppliedFields
            )
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
        guard table.schema == nil else {
            throw DatabaseEntityStatementMutationError.unsupportedStatement(
                "compiled entities do not use SQL schema qualifiers"
            )
        }
        guard let entity = container.schema.entities.first(where: {
            $0.name == table.table
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
            if entity.hasDynamicDirectory || !table.partitions.isEmpty {
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
            partition: partition
        )
    }

    private func insertColumns(
        _ names: [String]?,
        entity: ResolvedEntity
    ) throws -> [FieldSchema] {
        let ordered = entity.fields.sorted { $0.fieldNumber < $1.fieldNumber }
        guard let names else { return ordered }
        var seen = Set<String>()
        return try names.map { name in
            guard seen.insert(name).inserted else {
                throw DatabaseEntityMutationError.invalidCompiledSchema(
                    entity: entity.name,
                    reason: "INSERT column '\(name)' is duplicated"
                )
            }
            guard let field = entity.fields.first(where: { $0.name == name }) else {
                throw DatabaseEntityMutationError.invalidCompiledSchema(
                    entity: entity.name,
                    reason: "INSERT column '\(name)' is not compiled"
                )
            }
            return field
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
        let row = try assignmentRow(fields: fields, entity: entity, table: table, workMeter: workMeter)
        // A selected column may borrow its source claim; keep that owner through destination admission.
        for lease in row.payloadLeases {
            try payloads.append(footprint: DatabaseIntermediateFootprint(), make: { lease })
        }
        for assignment in assignments {
            byName[assignment.schema.name] = try await evaluateAssignment(
                assignment, evaluator: evaluator, row: row, payloads: &payloads, workMeter: workMeter
            )
        }
        return try FieldObject(
            byName.map { (key: $0.key, value: $0.value) }
        )
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
        let fields = includingColumns ? entity.fields : []
        let columns = try fields.map { field in
            try QueryColumn(number: fieldNumber(field, entity: entity.name), name: field.name,
                type: field.queryType ?? .primitive(field.type, isArray: field.isArray), isNullable: field.isOptional)
        }
        return CanonicalQueryTypeBinder(columns: columns, resolveColumn: { reference in
            guard reference.table == nil || reference.table == table.table || reference.table == table.alias,
                  let slot = fields.firstIndex(where: { $0.name == reference.column }) else {
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
        try binder.requireScalarPhase(expression)
        let type = schema.queryType ?? .primitive(schema.type, isArray: schema.isArray)
        let selected: Bool
        switch type {
        case .sql(.text), .sql(.char), .sql(.varchar), .sql(.blob), .sql(.binary), .sql(.varbinary),
             .sql(.uuid), .sql(.boolean), .sql(.smallint), .sql(.integer), .sql(.bigint),
             .sql(.real), .sql(.doublePrecision), .sql(.decimal),
             .sql(.date), .sql(.time), .sql(.timestamp), .sql(.interval): selected = true
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
        return try assignments.map { assignment in
            guard seen.insert(assignment.column).inserted else {
                throw DatabaseEntityStatementMutationError.unsupportedStatement(
                    "column '\(assignment.column)' is assigned more than once")
            }
            guard let schema = entity.fields.first(where: { $0.name == assignment.column }) else {
                throw DatabaseEntityMutationError.invalidCompiledSchema(entity: entity.name,
                    reason: "assignment column '\(assignment.column)' is not compiled")
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
        let row = try assignmentRow(fields: fields, entity: entity, table: table, workMeter: workMeter)
        let result = try await DatabaseContext.evaluateCanonicalScalar(filter, on: row, temporalEnvironment: temporalEnvironment, workMeter: workMeter)
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
    ) throws -> CanonicalSourceRow {
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
        let result = try await DatabaseContext.evaluateCanonicalScalar(bound, on: row, temporalEnvironment: temporalEnvironment, workMeter: workMeter)
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
    }
}
