import DatabaseMaintenanceOperations
import DatabaseSchemaOperations
import DatabaseJobRuntime
import DatabaseGraphOperations
import DatabaseMutationOperations
import DatabaseQueryOperations
import DatabaseCommandOperations
import DatabaseOperationCore
import DatabaseKit
import DatabaseEngine
import DatabaseTypes
@_spi(DatabaseOperations) import DatabaseWire
#if DATABASE_OPERATIONS_GRAPH_INDEXES
import GraphIndex
#endif
import StorageKit

public struct CanonicalDatabaseStatementMutationExecutor: DatabaseStatementMutationExecutor {
    private let runtimeLimits: DatabaseOperationLimits
    #if DATABASE_OPERATIONS_GRAPH_INDEXES
    private let graphStoreOverride: (any RDFGraphMutationStore)?
    private let loadSource: AnySPARQLLoadSource
    private let functionRegistry: SPARQLFunctionRegistry
    private let graphOperationLimits: GraphOperationLimits
    #endif

    public init(runtimeLimits: DatabaseOperationLimits = .default) {
        self.runtimeLimits = runtimeLimits
        #if DATABASE_OPERATIONS_GRAPH_INDEXES
        self.graphStoreOverride = nil
        self.loadSource = .unconfigured
        self.functionRegistry = .empty
        self.graphOperationLimits = .default
        #endif
    }

    #if DATABASE_OPERATIONS_GRAPH_INDEXES
    public init(
        runtimeLimits: DatabaseOperationLimits = .default,
        loadSource: AnySPARQLLoadSource,
        functionRegistry: SPARQLFunctionRegistry = .empty,
        graphOperationLimits: GraphOperationLimits = .default
    ) {
        self.runtimeLimits = runtimeLimits
        self.graphStoreOverride = nil
        self.loadSource = loadSource
        self.functionRegistry = functionRegistry
        self.graphOperationLimits = graphOperationLimits
    }

    public init(
        runtimeLimits: DatabaseOperationLimits = .default,
        graphStore: any RDFGraphMutationStore,
        loadSource: AnySPARQLLoadSource = .unconfigured,
        functionRegistry: SPARQLFunctionRegistry = .empty,
        graphOperationLimits: GraphOperationLimits = .default
    ) {
        self.runtimeLimits = runtimeLimits
        self.graphStoreOverride = graphStore
        self.loadSource = loadSource
        self.functionRegistry = functionRegistry
        self.graphOperationLimits = graphOperationLimits
    }
    #endif

    public func prepare(
        _ validatedStatement: ValidatedDatabaseStatement,
        budget: ExecutionBudget = ExecutionBudget(),
        context: DatabaseOperationContext
    ) async throws -> CanonicalPreparedStatementMutation {
        let statement = validatedStatement.statement
        let workMeter = DatabaseWorkMeter(
            budget: budget,
            monotonicClock: context.executor.monotonicClock
        )
        guard case .sparqlUpdate(let request) = statement else {
            return CanonicalPreparedStatementMutation(
                payload: .statement(statement),
                workMeter: workMeter,
                structuralLimits: validatedStatement.structuralLimits
            )
        }
        #if DATABASE_OPERATIONS_GRAPH_INDEXES
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
        #else
        _ = request
        throw DatabaseMutationError.featureUnavailable(
            "SPARQL updates require the GraphIndexes package trait"
        )
        #endif
    }

    #if DATABASE_OPERATIONS_GRAPH_INDEXES
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
    #endif

    public func execute(
        _ prepared: CanonicalPreparedStatementMutation,
        preconditions: [MutationExecuteOperation.Precondition] = [],
        graphPartitions: FieldObject = FieldObject(),
        context: DatabaseOperationContext,
        transaction: DatabaseTransaction
    ) async throws -> MutationExecuteOperation.Result {
        let entities = try context.requireDataExecutor()
            .makeEntityMutationExecutor(
            runtimeLimits: runtimeLimits
        )
        let workMeter = prepared.workMeter

        switch prepared.payload {
        #if DATABASE_OPERATIONS_GRAPH_INDEXES
        case .sparql(let request):
            try requireNoRDFGraphPartitions(graphPartitions)
            let graphStore = try graphStore(for: context)
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
                    entities: entities,
                    workMeter: workMeter
                )
            )
        #endif
        case .statement(let statement):
            return try await execute(
                statement,
                preconditions: preconditions,
                graphPartitions: graphPartitions,
                context: context,
                transaction: transaction,
                entities: entities,
                workMeter: workMeter
            )
        }
    }

    #if DATABASE_OPERATIONS_GRAPH_INDEXES
    private func graphStore(
        for context: DatabaseOperationContext
    ) throws -> any RDFGraphMutationStore {
        if let graphStoreOverride {
            return graphStoreOverride
        }
        return CanonicalRDFGraphStore(
            rootSubspace: CanonicalRDFGraphStore.rootSubspace(
                forBaseRoot: try context.requireDataContext()
                    .requireOperationDataRoot().root
            )
        )
    }
    #endif

    private func execute(
        _ statement: QueryStatement,
        preconditions: [MutationExecuteOperation.Precondition],
        graphPartitions: FieldObject,
        context: DatabaseOperationContext,
        transaction: DatabaseTransaction,
        entities: DatabaseEntityMutationExecutor,
        workMeter: DatabaseWorkMeter
    ) async throws -> MutationExecuteOperation.Result {
        switch statement {
        case .insert(let query):
            try requireNoGraphPartitions(graphPartitions)
            return .entities(
                try await executeInsert(
                    query,
                    context: context,
                    transaction: transaction,
                    entities: entities,
                    preconditions: preconditions,
                    workMeter: workMeter
                )
            )
        case .update(let query):
            try requireNoGraphPartitions(graphPartitions)
            return .entities(
                try await executeUpdate(
                    query,
                    context: context,
                    transaction: transaction,
                    entities: entities,
                    preconditions: preconditions,
                    workMeter: workMeter
                )
            )
        case .delete(let query):
            try requireNoGraphPartitions(graphPartitions)
            return .entities(
                try await executeDelete(
                    query,
                    context: context,
                    transaction: transaction,
                    entities: entities,
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

    #if DATABASE_OPERATIONS_GRAPH_INDEXES
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
                    maximumDocumentBytes:
                        graphOperationLimits.maximumLoadDocumentBytes,
                    maximumTriples: runtimeLimits.maximumMutations,
                    workMeter: workMeter
                )
            )
            guard document.byteCount
                    <= UInt64(
                        graphOperationLimits.maximumLoadDocumentBytes
                    ) else {
                throw SPARQLLoadSourceError.documentTooLarge(
                    actual: document.byteCount,
                    maximum: graphOperationLimits.maximumLoadDocumentBytes
                )
            }
            guard document.tripleCount <= runtimeLimits.maximumMutations else {
                throw SPARQLLoadSourceError.tripleLimitExceeded(
                    actual: document.tripleCount,
                    maximum: runtimeLimits.maximumMutations
                )
            }

            var triples = document.takeTriples()
            let blankNodeResolver = SPARQLUpdateBlankNodeResolver(
                idempotencyKey: idempotencyKey,
                operationOrdinal: operationOrdinal,
                solutionOrdinal: 0
            )
            for index in triples.indices {
                try workMeter.consume(at: .validation)
                let triple = triples[index]
                let scoped = RDFTriple(
                    subject: try resolveBlankNodes(
                        triple.subject,
                        blankNodeResolver: blankNodeResolver
                    ),
                    predicate: triple.predicate,
                    object: try resolveBlankNodes(
                        triple.object,
                        blankNodeResolver: blankNodeResolver
                    )
                )
                do {
                    try scoped.quad.validate()
                } catch {
                    throw SPARQLLoadSourceError.invalidDocument(
                        "Loaded RDF document is invalid"
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

    private func resolveBlankNodes(
        _ subject: RDFSubject,
        blankNodeResolver: SPARQLUpdateBlankNodeResolver
    ) throws -> RDFSubject {
        switch subject {
        case .blankNode(let identifier):
            return .blankNode(
                try RDFBlankNodeIdentifier(
                    blankNodeResolver.identifier(for: identifier.rawValue)
                )
            )
        case .iri:
            return subject
        }
    }

    private func resolveBlankNodes(
        _ term: RDFTerm,
        blankNodeResolver: SPARQLUpdateBlankNodeResolver
    ) throws -> RDFTerm {
        switch term {
        case .blankNode(let identifier):
            return .blankNode(
                try RDFBlankNodeIdentifier(
                    blankNodeResolver.identifier(for: identifier.rawValue)
                )
            )
        case .tripleTerm(let subject, let predicate, let object):
            return .tripleTerm(
                subject: try resolveBlankNodes(subject, blankNodeResolver: blankNodeResolver),
                predicate: predicate,
                object: try resolveBlankNodes(object, blankNodeResolver: blankNodeResolver)
            )
        case .iri, .literal:
            return term
        }
    }
    #endif

    private func requireNoGraphPartitions(
        _ graphPartitions: FieldObject
    ) throws {
        guard graphPartitions.isEmpty else {
            throw DatabaseMutationError.invalidGraphPartitions(
                "SQL statements do not consume graph partitions"
            )
        }
    }

    #if DATABASE_OPERATIONS_GRAPH_INDEXES
    private func requireNoRDFGraphPartitions(
        _ graphPartitions: FieldObject
    ) throws {
        guard graphPartitions.isEmpty else {
            throw DatabaseMutationError.invalidGraphPartitions(
                "authoritative RDF graph mutations do not consume entity partitions"
            )
        }
    }
    #endif

    private func executeInsert(
        _ query: InsertQuery,
        context: DatabaseOperationContext,
        transaction: DatabaseTransaction,
        entities: DatabaseEntityMutationExecutor,
        preconditions: [MutationExecuteOperation.Precondition],
        workMeter: DatabaseWorkMeter
    ) async throws -> [MutationExecuteOperation.EntityEffect] {
        guard query.returning == nil else {
            throw DatabaseMutationError.unsupportedStatement(
                "INSERT RETURNING is not representable by mutation effects"
            )
        }
        let entity = try resolve(
            query.target,
            schema: context.executor.schema,
            runtimeConfiguration: context.executor.runtimeConfiguration
        )
        let columns = try insertColumns(query.columns, entity: entity)
        let rows: [[Expression]]
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
            let suppliedFields: FieldObject
            if row.isEmpty, case .defaultValues = query.source {
                suppliedFields = FieldObject()
            } else {
                guard row.count == columns.count else {
                    throw DatabaseMutationError.unsupportedStatement(
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
                let originalFields = try DatabaseEntityProjection.fieldObject(
                    for: model
                )
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
                let updated = try entity.runtime.persistedModel(
                    from: updatedFields
                )
                changes.append(
                    MutationExecuteOperation.Change(
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
        context: DatabaseOperationContext,
        transaction: DatabaseTransaction,
        entities: DatabaseEntityMutationExecutor,
        preconditions: [MutationExecuteOperation.Precondition],
        workMeter: DatabaseWorkMeter
    ) async throws -> [MutationExecuteOperation.EntityEffect] {
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
        let entity = try resolve(
            query.target,
            schema: context.executor.schema,
            runtimeConfiguration: context.executor.runtimeConfiguration
        )
        let models = try await scan(
            entity,
            transaction: transaction,
            workMeter: workMeter
        )
        var changes: [MutationExecuteOperation.Change] = []
        for model in models {
            try workMeter.consume(at: .mutationPlanning)
            let originalFields = try DatabaseEntityProjection.fieldObject(
                for: model
            )
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
            let updated = try entity.runtime.persistedModel(
                from: updatedFields
            )
            changes.append(
                MutationExecuteOperation.Change(
                    kind: .update,
                    identity: try entity.runtime.identity(for: model),
                    fields: try DatabaseEntityProjection.fieldObject(
                        for: updated
                    )
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
        context: DatabaseOperationContext,
        transaction: DatabaseTransaction,
        entities: DatabaseEntityMutationExecutor,
        preconditions: [MutationExecuteOperation.Precondition],
        workMeter: DatabaseWorkMeter
    ) async throws -> [MutationExecuteOperation.EntityEffect] {
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
        let entity = try resolve(
            query.target,
            schema: context.executor.schema,
            runtimeConfiguration: context.executor.runtimeConfiguration
        )
        let models = try await scan(
            entity,
            transaction: transaction,
            workMeter: workMeter
        )
        var changes: [MutationExecuteOperation.Change] = []
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
                MutationExecuteOperation.Change(
                    kind: .delete,
                    identity: try entity.runtime.identity(for: model)
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
        guard let maximumRows = Int(exactly: runtimeLimits.maximumRows),
              maximumRows < Int.max else {
            throw DatabaseOperationConfigurationError
                .unsupportedOnCurrentPlatform(
                    limit: .maximumRows,
                    actual: UInt64(runtimeLimits.maximumRows),
                    maximum: UInt64(Int.max - 1)
                )
        }
        let limit = maximumRows + 1
        let models = try await transaction.scanPersistedModels(
            entity: entity.name,
            partition: entity.partition,
            limit: limit
        )
        guard models.count <= maximumRows else {
            throw DatabaseOperationLimitError.resultLimitExceeded(
                actual: models.count,
                maximum: runtimeLimits.maximumRows
            )
        }
        try workMeter.consume(UInt64(models.count), at: .storageRow)
        return models
    }

    private func resolve(
        _ table: TableRef,
        schema: Schema,
        runtimeConfiguration: DatabaseRuntimeConfiguration
    ) throws -> ResolvedEntity {
        guard table.schema == nil else {
            throw DatabaseMutationError.unsupportedStatement(
                "compiled entities do not use SQL schema qualifiers"
            )
        }
        guard let entity = schema.entities.first(where: {
            $0.name == table.table
        }) else {
            throw DatabaseMutationError.unknownEntity(table.table)
        }
        guard let runtime = runtimeConfiguration.entityRuntimes.registration(
            named: entity.name
        ) else {
            throw DatabaseMutationError.entityHasNoPersistableType(table.table)
        }
        let partition: AnyDirectoryPath?
        do {
            partition = try CanonicalPartitionBinding.makeAnyBinding(
                for: entity,
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
        to fields: FieldObject,
        evaluationFields: [String: FieldValue],
        entity: ResolvedEntity
    ) throws -> FieldObject {
        var byName = Dictionary(
            uniqueKeysWithValues: fields.fields.map {
                ($0.key, $0.value)
            }
        )
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
            _ = try fieldNumber(schema, entity: entity.name)
            byName[assignment.column] = try evaluator.evaluate(
                assignment.value
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
        let runtime: EntityRuntimeRegistration
        let fields: [FieldSchema]
        let partition: AnyDirectoryPath?
    }
}

#if DATABASE_OPERATIONS_GRAPH_INDEXES
extension CanonicalDatabaseStatementMutationExecutor:
    GraphStatementMutationExecutor {}
#endif
