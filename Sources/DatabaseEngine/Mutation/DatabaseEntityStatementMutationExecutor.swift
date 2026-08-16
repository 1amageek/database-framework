import DatabaseKit
import DatabaseTypes

@_spi(DatabaseExecution)
public struct DatabaseEntityStatementMutationExecutor: Sendable {
    private let container: DBContainer
    private let limits: DatabaseEntityMutationLimits

    public init(
        container: DBContainer,
        limits: DatabaseEntityMutationLimits
    ) {
        self.container = container
        self.limits = limits
    }

    public func execute(
        _ statement: QueryStatement,
        preconditions: [EntityMutationPrecondition] = [],
        transaction: DatabaseTransaction,
        workMeter: DatabaseWorkMeter
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
        guard rows.count <= limits.maximumChanges else {
            throw DatabaseEntityMutationError.changeLimitExceeded(
                actual: rows.count,
                maximum: limits.maximumChanges
            )
        }

        var changes: [EntityMutationChange] = []
        changes.reserveCapacity(rows.count)
        for row in rows {
            try workMeter.consume(at: .mutationPlanning)
            let suppliedFields: FieldObject
            if row.isEmpty, case .defaultValues = query.source {
                suppliedFields = FieldObject()
            } else {
                guard row.count == columns.count else {
                    throw DatabaseEntityStatementMutationError.unsupportedStatement(
                        "INSERT row has \(row.count) values for \(columns.count) columns"
                    )
                }
                let evaluator = DatabaseExpressionEvaluator(fields: [:])
                let suppliedEntries = try zip(columns, row).map {
                    schema,
                    expression in
                    _ = try fieldNumber(schema, entity: entity.name)
                    return (
                        key: schema.name,
                        value: try evaluator.evaluate(expression)
                    )
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
            let existing = try await transaction.loadPersistedModel(
                entity: entity.name,
                id: resolved.id,
                partition: resolved.partition
            )

            switch (query.onConflict, existing) {
            case (.none, _), (.some(.doNothing), .none),
                 (.some(.doUpdate), .none):
                changes.append(
                    EntityMutationChange(
                        kind: .insert,
                        identity: targetIdentity,
                        fields: candidateFields
                    )
                )
            case (.some(.doNothing), .some):
                continue
            case (.some(.doUpdate(let assignments, let filter)), .some(let model)):
                let originalFields = try DatabaseEntityProjection.fieldObject(
                    for: model
                )
                let evaluation = evaluationFields(
                    originalFields,
                    table: query.target
                )
                if let filter,
                   try !DatabaseExpressionEvaluator(fields: evaluation)
                    .predicate(filter) {
                    continue
                }
                let updatedFields = try applying(
                    assignments,
                    to: originalFields,
                    evaluationFields: evaluation,
                    entity: entity
                )
                let updated = try entity.runtime.persistedModel(
                    from: updatedFields
                )
                changes.append(
                    EntityMutationChange(
                        kind: .update,
                        identity: try entity.runtime.identity(for: model),
                        fields: try DatabaseEntityProjection.fieldObject(
                            for: updated
                        )
                    )
                )
            }
        }

        guard !changes.isEmpty else {
            try await entities.validate(
                preconditions,
                transaction: transaction,
                workMeter: workMeter
            )
            return []
        }
        return try await entities.execute(
            changes,
            preconditions: preconditions,
            workMeter: workMeter,
            transaction: transaction
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
        let models = try await scan(
            entity,
            transaction: transaction,
            workMeter: workMeter
        )
        var changes: [EntityMutationChange] = []
        for model in models {
            try workMeter.consume(at: .mutationPlanning)
            let originalFields = try DatabaseEntityProjection.fieldObject(
                for: model
            )
            let evaluation = evaluationFields(originalFields, table: query.target)
            if let filter = query.filter,
               try !DatabaseExpressionEvaluator(fields: evaluation)
                .predicate(filter) {
                continue
            }
            let updatedFields = try applying(
                query.assignments,
                to: originalFields,
                evaluationFields: evaluation,
                entity: entity
            )
            let updated = try entity.runtime.persistedModel(from: updatedFields)
            changes.append(
                EntityMutationChange(
                    kind: .update,
                    identity: try entity.runtime.identity(for: model),
                    fields: try DatabaseEntityProjection.fieldObject(for: updated)
                )
            )
            guard changes.count <= limits.maximumChanges else {
                throw DatabaseEntityMutationError.changeLimitExceeded(
                    actual: changes.count,
                    maximum: limits.maximumChanges
                )
            }
        }
        guard !changes.isEmpty else {
            try await entities.validate(
                preconditions,
                transaction: transaction,
                workMeter: workMeter
            )
            return []
        }
        return try await entities.execute(
            changes,
            preconditions: preconditions,
            workMeter: workMeter,
            transaction: transaction
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
        let models = try await scan(
            entity,
            transaction: transaction,
            workMeter: workMeter
        )
        var changes: [EntityMutationChange] = []
        for model in models {
            try workMeter.consume(at: .mutationPlanning)
            let fields = try DatabaseEntityProjection.fieldObject(for: model)
            if let filter = query.filter,
               try !DatabaseExpressionEvaluator(
                    fields: evaluationFields(fields, table: query.target)
               ).predicate(filter) {
                continue
            }
            changes.append(
                EntityMutationChange(
                    kind: .delete,
                    identity: try entity.runtime.identity(for: model)
                )
            )
            guard changes.count <= limits.maximumChanges else {
                throw DatabaseEntityMutationError.changeLimitExceeded(
                    actual: changes.count,
                    maximum: limits.maximumChanges
                )
            }
        }
        guard !changes.isEmpty else {
            try await entities.validate(
                preconditions,
                transaction: transaction,
                workMeter: workMeter
            )
            return []
        }
        return try await entities.execute(
            changes,
            preconditions: preconditions,
            workMeter: workMeter,
            transaction: transaction
        )
    }

    private func scan(
        _ entity: ResolvedEntity,
        transaction: DatabaseTransaction,
        workMeter: DatabaseWorkMeter
    ) async throws -> [PersistedModel] {
        let configuredMaximum = workMeter.budget.maximumRows
        guard let maximumRows = Int(exactly: configuredMaximum),
              maximumRows < Int.max else {
            throw DatabaseEntityStatementMutationError
                .scanLimitUnsupportedOnCurrentPlatform(
                    maximum: configuredMaximum
                )
        }
        let models = try await transaction.scanPersistedModelsForExecution(
            entity: entity.name,
            partition: entity.partition,
            limit: maximumRows + 1
        )
        guard models.count <= maximumRows else {
            throw DatabaseEntityStatementMutationError.scanLimitExceeded(
                actual: models.count,
                maximum: configuredMaximum
            )
        }
        try workMeter.consume(UInt64(models.count), at: .storageRow)
        return models
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
        _ assignments: [Assignment],
        to fields: FieldObject,
        evaluationFields: [String: FieldValue],
        entity: ResolvedEntity
    ) throws -> FieldObject {
        var byName = Dictionary(
            uniqueKeysWithValues: fields.fields.map { ($0.key, $0.value) }
        )
        var seen = Set<String>()
        let evaluator = DatabaseExpressionEvaluator(fields: evaluationFields)
        for assignment in assignments {
            guard seen.insert(assignment.column).inserted else {
                throw DatabaseEntityStatementMutationError.unsupportedStatement(
                    "column '\(assignment.column)' is assigned more than once"
                )
            }
            guard let schema = entity.fields.first(where: {
                $0.name == assignment.column
            }) else {
                throw DatabaseEntityMutationError.invalidCompiledSchema(
                    entity: entity.name,
                    reason: "assignment column '\(assignment.column)' is not compiled"
                )
            }
            _ = try fieldNumber(schema, entity: entity.name)
            byName[assignment.column] = try evaluator.evaluate(assignment.value)
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
