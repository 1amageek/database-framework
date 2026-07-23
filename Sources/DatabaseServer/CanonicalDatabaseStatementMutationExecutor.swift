import Core
import DatabaseEngine
import DatabaseValue
import DatabaseWire
import Graph
import GraphIndex
import QueryIR
import StorageKit

public struct CanonicalDatabaseStatementMutationExecutor: DatabaseStatementMutationExecutor {
    private let runtimeLimits: DatabaseRuntimeLimits
    private let graphStore: any RDFGraphMutationStore
    private let loadSource: AnySPARQLLoadSource
    private let functionRegistry: SPARQLFunctionRegistry

    public init(
        runtimeLimits: DatabaseRuntimeLimits = .default,
        graphStore: any RDFGraphMutationStore = CanonicalRDFGraphStore(),
        loadSource: AnySPARQLLoadSource = .unconfigured,
        functionRegistry: SPARQLFunctionRegistry = .empty
    ) {
        self.runtimeLimits = runtimeLimits
        self.graphStore = graphStore
        self.loadSource = loadSource
        self.functionRegistry = functionRegistry
    }

    public func prepare(
        _ validatedStatement: ValidatedDatabaseStatement,
        budget: DatabaseExecutionBudget = DatabaseExecutionBudget(),
        context: DatabaseOperationContext
    ) async throws -> CanonicalPreparedStatementMutation {
        let statement = validatedStatement.statement
        let workMeter = DatabaseWorkMeter(budget: budget)
        guard case .sparqlUpdate(let request) = statement else {
            return CanonicalPreparedStatementMutation(
                payload: .statement(statement),
                workMeter: workMeter,
                structuralLimits: validatedStatement.structuralLimits
            )
        }
        return CanonicalPreparedStatementMutation(
            payload: .sparql(
                try await prepareSPARQLUpdate(
                    request,
                    context: context,
                    workMeter: workMeter
                )
            ),
            workMeter: workMeter,
            structuralLimits: validatedStatement.structuralLimits
        )
    }

    private func prepareSPARQLUpdate(
        _ request: SPARQLUpdateRequest,
        context: DatabaseOperationContext,
        workMeter: DatabaseWorkMeter
    ) async throws -> PreparedSPARQLUpdateRequest {
        let first = try await prepareSPARQLOperation(
            request.firstOperation,
            ordinal: 0,
            context: context,
            workMeter: workMeter
        )
        var additional: [PreparedSPARQLUpdateOperation] = []
        additional.reserveCapacity(request.additionalOperations.count)
        for (index, operation) in request.additionalOperations.enumerated() {
            additional.append(
                try await prepareSPARQLOperation(
                    operation,
                    ordinal: UInt64(index + 1),
                    context: context,
                    workMeter: workMeter
                )
            )
        }
        return PreparedSPARQLUpdateRequest(
            firstOperation: first,
            additionalOperations: consume additional
        )
    }

    private func prepareSPARQLOperation(
        _ operation: SPARQLUpdateOperation,
        ordinal: UInt64,
        context: DatabaseOperationContext,
        workMeter: DatabaseWorkMeter
    ) async throws -> PreparedSPARQLUpdateOperation {
        switch operation {
        case .insertData(let query):
            return .insertData(query)
        case .deleteData(let query):
            return .deleteData(query)
        case .modify(let query):
            return .modify(query)
        case .deleteWhere(let query):
            return .deleteWhere(query)
        case .load(let query):
            return try await prepareLoad(
                query,
                operationOrdinal: ordinal,
                context: context,
                workMeter: workMeter
            )
        case .clear(let query):
            return .clear(query)
        case .createGraph(let query):
            return .createGraph(query)
        case .drop(let query):
            return .drop(query)
        case .graphTransfer(let query):
            return .graphTransfer(query)
        }
    }

    public func execute(
        _ prepared: CanonicalPreparedStatementMutation,
        preconditions: [MutationExecuteOperation.Precondition] = [],
        graphPartitions: [DatabaseObjectField] = [],
        context: DatabaseOperationContext,
        transaction: any Transaction
    ) async throws -> MutationExecuteOperation.Result {
        let persistence = context.container.newContext().makePersistenceHandler()
        let records = DatabaseRecordMutationExecutor(
            container: context.container,
            runtimeLimits: runtimeLimits
        )
        let workMeter = prepared.workMeter

        switch prepared.payload {
        case .sparql(let request):
            try requireNoRDFGraphPartitions(graphPartitions)
            return .rdf(
                try await SPARQLUpdateExecutor(
                    graphStore: graphStore,
                    runtimeLimits: runtimeLimits,
                    structuralLimits: prepared.structuralLimits,
                    functionRegistry: functionRegistry
                ).execute(
                    request,
                    preconditions: preconditions,
                    context: context,
                    transaction: transaction,
                    persistence: persistence,
                    records: records,
                    workMeter: workMeter
                )
            )
        case .statement(let statement):
            return try await execute(
                statement,
                preconditions: preconditions,
                graphPartitions: graphPartitions,
                context: context,
                transaction: transaction,
                persistence: persistence,
                records: records,
                workMeter: workMeter
            )
        }
    }

    private func execute(
        _ statement: QueryStatement,
        preconditions: [MutationExecuteOperation.Precondition],
        graphPartitions: [DatabaseObjectField],
        context: DatabaseOperationContext,
        transaction: any Transaction,
        persistence: any ModelPersistenceHandler,
        records: DatabaseRecordMutationExecutor,
        workMeter: DatabaseWorkMeter
    ) async throws -> MutationExecuteOperation.Result {
        switch statement {
        case .insert(let query):
            try requireNoGraphPartitions(graphPartitions)
            return .records(
                try await executeInsert(
                    query,
                    context: context,
                    transaction: transaction,
                    persistence: persistence,
                    records: records,
                    preconditions: preconditions,
                    workMeter: workMeter
                )
            )
        case .update(let query):
            try requireNoGraphPartitions(graphPartitions)
            return .records(
                try await executeUpdate(
                    query,
                    context: context,
                    transaction: transaction,
                    persistence: persistence,
                    records: records,
                    preconditions: preconditions,
                    workMeter: workMeter
                )
            )
        case .delete(let query):
            try requireNoGraphPartitions(graphPartitions)
            return .records(
                try await executeDelete(
                    query,
                    context: context,
                    transaction: transaction,
                    persistence: persistence,
                    records: records,
                    preconditions: preconditions,
                    workMeter: workMeter
                )
            )
        case .sparqlUpdate:
            throw DatabaseMutationError.unsupportedStatement(
                "SPARQL update request reached execution without preparation"
            )
        case .createGraph, .dropGraph:
            throw DatabaseMutationError.unsupportedStatement(
                "graph definitions are managed by the compiled application runtime"
            )
        case .select, .construct, .ask, .describe:
            throw DatabaseMutationError.unsupportedStatement(
                "read-only statements must use query.execute"
            )
        }
    }

    private func prepareLoad(
        _ query: LoadQuery,
        operationOrdinal: UInt64,
        context: DatabaseOperationContext,
        workMeter: DatabaseWorkMeter
    ) async throws -> PreparedSPARQLUpdateOperation {
        guard let idempotencyKey = context.metadata.idempotencyKey,
              !idempotencyKey.isEmpty else {
            throw DatabaseMutationError.idempotencyKeyRequired
        }
        do {
            let document = try await loadSource.load(
                SPARQLLoadRequest(
                    sourceIRI: query.source,
                    maximumDocumentBytes: runtimeLimits.maximumLoadDocumentBytes,
                    maximumTriples: runtimeLimits.maximumMutations,
                    workMeter: workMeter
                )
            )
            guard document.byteCount
                    <= UInt64(runtimeLimits.maximumLoadDocumentBytes) else {
                throw SPARQLLoadSourceError.documentTooLarge(
                    actual: document.byteCount,
                    maximum: runtimeLimits.maximumLoadDocumentBytes
                )
            }
            guard document.tripleCount <= runtimeLimits.maximumMutations else {
                throw SPARQLLoadSourceError.tripleLimitExceeded(
                    actual: document.tripleCount,
                    maximum: runtimeLimits.maximumMutations
                )
            }

            var triples = document.takeTriples()
            let scope = SPARQLBlankNodeScope(
                idempotencyKey: idempotencyKey,
                operationOrdinal: operationOrdinal,
                solutionOrdinal: 0
            )
            for index in triples.indices {
                try workMeter.consume(at: .validation)
                let triple = triples[index]
                let scoped = Graph.RDFTriple(
                    subject: scopeBlankNodes(triple.subject, scope: scope),
                    predicate: scopeBlankNodes(triple.predicate, scope: scope),
                    object: scopeBlankNodes(triple.object, scope: scope)
                )
                do {
                    try scoped.quad.validate()
                } catch {
                    throw SPARQLLoadSourceError.invalidDocument(
                        String(describing: error)
                    )
                }
                triples[index] = scoped
            }
            return .load(
                PreparedSPARQLLoad(
                    destination: query.destination,
                    triples: consume triples
                )
            )
        } catch let error as SPARQLLoadSourceError {
            if query.silent, error.isSilentSuppressible {
                return .silentLoadNoOp
            }
            throw error
        }
    }

    private func scopeBlankNodes(
        _ term: DatabaseRDFTerm,
        scope: SPARQLBlankNodeScope
    ) -> DatabaseRDFTerm {
        switch term {
        case .blankNode(let label):
            return .blankNode(scope.identifier(for: label))
        case .tripleTerm(let subject, let predicate, let object):
            return .tripleTerm(
                subject: scopeBlankNodes(subject, scope: scope),
                predicate: scopeBlankNodes(predicate, scope: scope),
                object: scopeBlankNodes(object, scope: scope)
            )
        case .iri, .literal:
            return term
        }
    }

    private func requireNoGraphPartitions(
        _ graphPartitions: [DatabaseObjectField]
    ) throws {
        guard graphPartitions.isEmpty else {
            throw DatabaseMutationError.invalidGraphPartitions(
                "SQL statements do not consume graph partitions"
            )
        }
    }

    private func requireNoRDFGraphPartitions(
        _ graphPartitions: [DatabaseObjectField]
    ) throws {
        guard graphPartitions.isEmpty else {
            throw DatabaseMutationError.invalidGraphPartitions(
                "authoritative RDF graph mutations do not consume record partitions"
            )
        }
    }

    private func executeInsert(
        _ query: InsertQuery,
        context: DatabaseOperationContext,
        transaction: any Transaction,
        persistence: any ModelPersistenceHandler,
        records: DatabaseRecordMutationExecutor,
        preconditions: [MutationExecuteOperation.Precondition],
        workMeter: DatabaseWorkMeter
    ) async throws -> [MutationExecuteOperation.RecordEffect] {
        guard query.returning == nil else {
            throw DatabaseMutationError.unsupportedStatement(
                "INSERT RETURNING is not representable by mutation effects"
            )
        }
        let entity = try resolve(query.target, container: context.container)
        let columns = try insertColumns(query.columns, entity: entity)
        let rows: [[QueryIR.Expression]]
        switch query.source {
        case .values(let values):
            rows = values
        case .defaultValues:
            guard query.columns == nil else {
                throw DatabaseMutationError.unsupportedStatement(
                    "DEFAULT VALUES cannot specify an explicit column list"
                )
            }
            rows = [[]]
        case .select:
            throw DatabaseMutationError.unsupportedStatement(
                "INSERT SELECT requires a transaction-scoped select executor"
            )
        }

        guard !rows.isEmpty else { return [] }
        guard rows.count <= runtimeLimits.maximumMutations else {
            throw DatabaseMutationError.mutationLimitExceeded(
                actual: rows.count,
                maximum: runtimeLimits.maximumMutations
            )
        }

        var changes: [MutationExecuteOperation.Change] = []
        changes.reserveCapacity(rows.count)
        for row in rows {
            try workMeter.consume(at: .mutationPlanning)
            let suppliedFields: [DatabaseObjectField]
            if row.isEmpty, case .defaultValues = query.source {
                suppliedFields = []
            } else {
                guard row.count == columns.count else {
                    throw DatabaseMutationError.unsupportedStatement(
                        "INSERT row has \(row.count) values for \(columns.count) columns"
                    )
                }
                let evaluator = DatabaseExpressionEvaluator(fields: [:])
                suppliedFields = try zip(columns, row).map { schema, expression in
                    DatabaseObjectField(
                        number: try fieldNumber(schema, entity: entity.name),
                        name: schema.name,
                        value: try evaluator.evaluate(expression)
                    )
                }
            }

            let candidate = try entity.type.decodeDatabaseRecord(suppliedFields)
            let candidateFields = try DatabaseRecordProjection.fields(for: candidate)
            let candidateIdentity = try DatabaseRecordProjection.identity(for: candidate)
            let targetIdentity = RecordIdentity(
                entity: candidateIdentity.entity,
                id: candidateIdentity.id,
                partitions: query.target.partitions
            )
            let resolved = try DatabaseResolvedRecordIdentity.resolve(
                targetIdentity,
                container: context.container,
                model: candidate
            )
            let existing = try await persistence.load(
                entity.name,
                id: resolved.id,
                partition: resolved.partition,
                transaction: transaction
            )

            switch (query.onConflict, existing) {
            case (.none, _), (.some(.doNothing), .none), (.some(.doUpdate), .none):
                changes.append(
                    MutationExecuteOperation.Change(
                        kind: .insert,
                        identity: targetIdentity,
                        fields: candidateFields
                    )
                )
            case (.some(.doNothing), .some):
                continue
            case (.some(.doUpdate(let assignments, let filter)), .some(let model)):
                let originalFields = try DatabaseRecordProjection.fields(for: model)
                let evaluation = evaluationFields(
                    originalFields,
                    table: query.target
                )
                if let filter,
                   try !DatabaseExpressionEvaluator(fields: evaluation).predicate(filter) {
                    continue
                }
                let updatedFields = try applying(
                    assignments,
                    to: originalFields,
                    evaluationFields: evaluation,
                    entity: entity
                )
                let updated = try entity.type.decodeDatabaseRecord(updatedFields)
                changes.append(
                    MutationExecuteOperation.Change(
                        kind: .update,
                        identity: try DatabaseRecordProjection.identity(for: model),
                        fields: try DatabaseRecordProjection.fields(for: updated)
                    )
                )
            }
        }

        guard !changes.isEmpty else {
            try await records.validate(
                preconditions,
                transaction: transaction,
                persistence: persistence,
                workMeter: workMeter
            )
            return []
        }
        return try await records.execute(
            changes,
            preconditions: preconditions,
            workMeter: workMeter,
            transaction: transaction,
            persistence: persistence
        )
    }

    private func executeUpdate(
        _ query: UpdateQuery,
        context: DatabaseOperationContext,
        transaction: any Transaction,
        persistence: any ModelPersistenceHandler,
        records: DatabaseRecordMutationExecutor,
        preconditions: [MutationExecuteOperation.Precondition],
        workMeter: DatabaseWorkMeter
    ) async throws -> [MutationExecuteOperation.RecordEffect] {
        guard query.from == nil else {
            throw DatabaseMutationError.unsupportedStatement(
                "UPDATE FROM requires a transaction-scoped join executor"
            )
        }
        guard query.returning == nil else {
            throw DatabaseMutationError.unsupportedStatement(
                "UPDATE RETURNING is not representable by mutation effects"
            )
        }
        guard !query.assignments.isEmpty else {
            throw DatabaseMutationError.unsupportedStatement("UPDATE has no assignments")
        }
        let entity = try resolve(query.target, container: context.container)
        let models = try await scan(
            entity,
            transaction: transaction,
            persistence: persistence,
            workMeter: workMeter
        )
        var changes: [MutationExecuteOperation.Change] = []
        for model in models {
            try workMeter.consume(at: .mutationPlanning)
            let originalFields = try DatabaseRecordProjection.fields(for: model)
            let evaluation = evaluationFields(originalFields, table: query.target)
            if let filter = query.filter,
               try !DatabaseExpressionEvaluator(fields: evaluation).predicate(filter) {
                continue
            }
            let updatedFields = try applying(
                query.assignments,
                to: originalFields,
                evaluationFields: evaluation,
                entity: entity
            )
            let updated = try entity.type.decodeDatabaseRecord(updatedFields)
            changes.append(
                MutationExecuteOperation.Change(
                    kind: .update,
                    identity: try DatabaseRecordProjection.identity(for: model),
                    fields: try DatabaseRecordProjection.fields(for: updated)
                )
            )
            guard changes.count <= runtimeLimits.maximumMutations else {
                throw DatabaseMutationError.mutationLimitExceeded(
                    actual: changes.count,
                    maximum: runtimeLimits.maximumMutations
                )
            }
        }
        guard !changes.isEmpty else {
            try await records.validate(
                preconditions,
                transaction: transaction,
                persistence: persistence,
                workMeter: workMeter
            )
            return []
        }
        return try await records.execute(
            changes,
            preconditions: preconditions,
            workMeter: workMeter,
            transaction: transaction,
            persistence: persistence
        )
    }

    private func executeDelete(
        _ query: DeleteQuery,
        context: DatabaseOperationContext,
        transaction: any Transaction,
        persistence: any ModelPersistenceHandler,
        records: DatabaseRecordMutationExecutor,
        preconditions: [MutationExecuteOperation.Precondition],
        workMeter: DatabaseWorkMeter
    ) async throws -> [MutationExecuteOperation.RecordEffect] {
        guard query.using == nil else {
            throw DatabaseMutationError.unsupportedStatement(
                "DELETE USING requires a transaction-scoped join executor"
            )
        }
        guard query.returning == nil else {
            throw DatabaseMutationError.unsupportedStatement(
                "DELETE RETURNING is not representable by mutation effects"
            )
        }
        let entity = try resolve(query.target, container: context.container)
        let models = try await scan(
            entity,
            transaction: transaction,
            persistence: persistence,
            workMeter: workMeter
        )
        var changes: [MutationExecuteOperation.Change] = []
        for model in models {
            try workMeter.consume(at: .mutationPlanning)
            let fields = try DatabaseRecordProjection.fields(for: model)
            if let filter = query.filter,
               try !DatabaseExpressionEvaluator(
                    fields: evaluationFields(fields, table: query.target)
               ).predicate(filter) {
                continue
            }
            changes.append(
                MutationExecuteOperation.Change(
                    kind: .delete,
                    identity: try DatabaseRecordProjection.identity(for: model)
                )
            )
            guard changes.count <= runtimeLimits.maximumMutations else {
                throw DatabaseMutationError.mutationLimitExceeded(
                    actual: changes.count,
                    maximum: runtimeLimits.maximumMutations
                )
            }
        }
        guard !changes.isEmpty else {
            try await records.validate(
                preconditions,
                transaction: transaction,
                persistence: persistence,
                workMeter: workMeter
            )
            return []
        }
        return try await records.execute(
            changes,
            preconditions: preconditions,
            workMeter: workMeter,
            transaction: transaction,
            persistence: persistence
        )
    }

    private func scan(
        _ entity: ResolvedEntity,
        transaction: any Transaction,
        persistence: any ModelPersistenceHandler,
        workMeter: DatabaseWorkMeter
    ) async throws -> [any Persistable] {
        guard let maximumRows = Int(exactly: runtimeLimits.maximumRows),
              maximumRows < Int.max else {
            throw DatabaseRuntimeConfigurationError
                .unsupportedOnCurrentPlatform(
                    limit: .maximumRows,
                    actual: UInt64(runtimeLimits.maximumRows),
                    maximum: UInt64(Int.max - 1)
                )
        }
        let limit = maximumRows + 1
        let models = try await persistence.scan(
            entity.name,
            partition: entity.partition,
            limit: limit,
            transaction: transaction
        )
        guard models.count <= maximumRows else {
            throw DatabaseRuntimeLimitError.resultLimitExceeded(
                actual: models.count,
                maximum: runtimeLimits.maximumRows
            )
        }
        try workMeter.consume(UInt64(models.count), at: .storageRow)
        return models
    }

    private func resolve(
        _ table: TableRef,
        container: DBContainer
    ) throws -> ResolvedEntity {
        guard table.schema == nil else {
            throw DatabaseMutationError.unsupportedStatement(
                "compiled entities do not use SQL schema qualifiers"
            )
        }
        guard let entity = container.schema.entities.first(where: { $0.name == table.table }) else {
            throw DatabaseMutationError.unknownEntity(table.table)
        }
        guard let type = entity.persistableType else {
            throw DatabaseMutationError.entityHasNoPersistableType(table.table)
        }
        let partition: AnyDirectoryPath?
        do {
            partition = try CanonicalPartitionBinding.makeAnyBinding(
                for: type,
                partitions: table.partitions
            )
        } catch CanonicalReadError.invalidPartition(_, let reason) {
            throw DatabaseMutationError.invalidPartition(
                entity: entity.name,
                reason: reason
            )
        } catch {
            throw DatabaseMutationError.invalidPartition(
                entity: entity.name,
                reason: String(describing: error)
            )
        }
        return ResolvedEntity(
            name: entity.name,
            type: type,
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
                throw DatabaseMutationError.invalidCompiledSchema(
                    entity: entity.name,
                    reason: "INSERT column '\(name)' is duplicated"
                )
            }
            guard let field = entity.fields.first(where: { $0.name == name }) else {
                throw DatabaseMutationError.invalidCompiledSchema(
                    entity: entity.name,
                    reason: "INSERT column '\(name)' is not compiled"
                )
            }
            return field
        }
    }

    private func applying(
        _ assignments: [Assignment],
        to fields: [DatabaseObjectField],
        evaluationFields: [String: DatabaseValue],
        entity: ResolvedEntity
    ) throws -> [DatabaseObjectField] {
        var byName = Dictionary(uniqueKeysWithValues: fields.map { ($0.name, $0) })
        var seen = Set<String>()
        let evaluator = DatabaseExpressionEvaluator(fields: evaluationFields)
        for assignment in assignments {
            guard seen.insert(assignment.column).inserted else {
                throw DatabaseMutationError.unsupportedStatement(
                    "column '\(assignment.column)' is assigned more than once"
                )
            }
            guard let schema = entity.fields.first(where: { $0.name == assignment.column }) else {
                throw DatabaseMutationError.invalidCompiledSchema(
                    entity: entity.name,
                    reason: "assignment column '\(assignment.column)' is not compiled"
                )
            }
            byName[assignment.column] = DatabaseObjectField(
                number: try fieldNumber(schema, entity: entity.name),
                name: schema.name,
                value: try evaluator.evaluate(assignment.value)
            )
        }
        return byName.values.sorted { $0.number < $1.number }
    }

    private func evaluationFields(
        _ fields: [DatabaseObjectField],
        table: TableRef
    ) -> [String: DatabaseValue] {
        var values: [String: DatabaseValue] = [:]
        for field in fields {
            values[field.name] = field.value
            values["\(table.table).\(field.name)"] = field.value
            if let alias = table.alias {
                values["\(alias).\(field.name)"] = field.value
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
            throw DatabaseMutationError.invalidCompiledSchema(
                entity: entity,
                reason: "field '\(schema.name)' has invalid number \(schema.fieldNumber)"
            )
        }
        return number
    }

    private struct ResolvedEntity: Sendable {
        let name: String
        let type: any Persistable.Type
        let fields: [FieldSchema]
        let partition: AnyDirectoryPath?
    }
}
