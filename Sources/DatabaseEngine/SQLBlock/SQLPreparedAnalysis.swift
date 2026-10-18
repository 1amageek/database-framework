import DatabaseKit

/// One inference owner for every binding operation in a prepared definition.
enum SQLPreparedAnalysisRuntime {
    @TaskLocal static var parameters: CanonicalParameterBindingState?
}

/// Effect-free analysis shared by PREPARE and EXPLAIN.
enum SQLPreparedAnalysis {
    static func analyze(_ statement: QueryStatement, parameters: [QueryParameter],
        context: DatabaseContext, transaction: DatabaseTransaction,
        options: SQLBlockExecutionOptions, temporal: SQLTemporalEnvironment,
        workMeter: DatabaseWorkMeter, occurrences: QueryOccurrenceArena? = nil,
        occurrence: QueryOccurrenceID? = nil) async throws -> SQLExplainPlanNode {
        let operation: @Sendable (DatabaseReadSession) async throws -> sending SQLExplainPlanNode = { session in
            let resolution = try session.makeSQLReadResolution(searchPath: options.searchPath, workMeter: workMeter)
            let execution = ReadExecutionContext(options: ReadExecutionOptions(budget: options.budget),
                monotonicClock: context.container.monotonicClock, workMeter: workMeter,
                queryStructuralLimits: options.structuralLimits, temporalEnvironment: temporal)
                .withoutExternalPageWindow().withSQLResolution(resolution)
            let scope = try SQLStatementReadScope(context: context, session: session,
                execution: execution, parameters: parameters)
            do {
                let result = try await ActiveSQLStatementReads.$scope.withValue(scope) {
                    try await ActiveSQLStatementReads.$metadataOnly.withValue(true) {
                        try await bind(statement, context: context, transaction: transaction,
                            options: options, scope: scope, workMeter: workMeter,
                            occurrences: occurrences, occurrence: occurrence)
                    }
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
        var main = statement
        while case .with(let composed) = main { main = composed.body }
        if case .select = main {
            return try await DatabaseReadSession.withSession(context: context, workMeter: workMeter, operation)
        }
        return try await DatabaseReadSession.withSQLMutationSession(context: context,
            transaction: transaction, workMeter: workMeter, operation)
    }

    private static func bind(_ statement: QueryStatement, context: DatabaseContext,
        transaction: DatabaseTransaction, options: SQLBlockExecutionOptions,
        scope: SQLStatementReadScope, workMeter: DatabaseWorkMeter,
        occurrences: QueryOccurrenceArena? = nil, occurrence: QueryOccurrenceID? = nil) async throws -> SQLExplainPlanNode {
        try workMeter.consume(at: .bindingCandidate)
        let claim = try workMeter.reserveIntermediate(bytes: 512, at: .bindingCandidate)
        switch statement {
        case .select(let query):
            return try context.boundSQLExplainPlan(query, parameters: scope.parameters, options: scope.execution,
                occurrences: occurrences, occurrence: occurrence)
        case .insert, .update, .delete, .merge:
            guard let count = Int(exactly: options.budget.maximumRows), count > 0 else {
                throw SQLBlockExecutionError.mutationLimitOutOfRange(options.budget.maximumRows)
            }
            let executor = DatabaseEntityStatementMutationExecutor(container: context.container,
                limits: try DatabaseEntityMutationLimits(maximumChanges: count, maximumPreconditions: 1))
            let columns = try await executor.analyzeSQL(statement, scope: scope,
                transaction: transaction, workMeter: workMeter)
            let operation: String, target: TableRef
            switch statement {
            case .insert(let query): operation = "Insert"; target = query.target
            case .update(let query): operation = "Update"; target = query.target
            case .delete(let query): operation = "Delete"; target = query.target
            case .merge(let query): operation = "Merge"; target = query.target
            default: throw SQLBlockExecutionError.unsupportedStatement
            }
            guard let resolution = scope.execution.sqlResolution else { throw DatabaseTransactionError.invalidOperationContext }
            let relation = try resolution.resolve(target)
            try claim.reserveAdditional(bytes: DatabaseIntermediateCollectionMeter.arrayFootprint(
                count: columns.count, element: QueryColumn.self).bytes, at: .bindingCandidate)
            return SQLExplainPlanNode(operation: operation,
                details: [.init(name: "Relation", value: relation.declaration.name),
                    .init(name: "Relation identity", value: relation.declaration.id, verboseOnly: true)],
                outputs: columns, storageReservation: claim)
        case .truncate(let query):
            guard let resolution = scope.execution.sqlResolution else { throw DatabaseTransactionError.invalidOperationContext }
            for table in query.tables { _ = try resolution.resolve(table) }
            try claim.reserveAdditional(bytes: UInt64(query.tables.reduce(0) { $0 + $1.table.utf8.count + 2 }),
                at: .bindingCandidate)
            return SQLExplainPlanNode(operation: "Truncate",
                details: [.init(name: "Relations", value: query.tables.map { $0.table }.joined(separator: ", "))],
                storageReservation: claim)
        case .with(let composed):
            if case .select(let body) = composed.body,
               composed.statements.allSatisfy({ if case .select = $0.statement { return true }; return false }) {
                let definitions = composed.statements.compactMap { definition -> NamedSubquery? in
                    guard case .select(let query) = definition.statement else { return nil }
                    return NamedSubquery(name: definition.name, columns: definition.columns, query: query,
                        materialized: definition.materialized, search: definition.search, cycle: definition.cycle)
                }
                return try await bind(.select(body.replacing(subqueries: definitions + (body.subqueries ?? []))
                    .replacing(withRecursive: composed.recursive || body.withRecursive)), context: context,
                    transaction: transaction, options: options, scope: scope, workMeter: workMeter)
            }
            try claim.reserveAdditional(bytes: DatabaseIntermediateCollectionMeter.arrayFootprint(
                count: composed.statements.count + 1, element: SQLExplainPlanNode.self).bytes, at: .bindingCandidate)
            var children: [SQLExplainPlanNode] = []
            children.reserveCapacity(composed.statements.count + 1)
            let ordered = try SQLBlockExecutor(context: context).orderedDefinitions(composed, scope: scope, workMeter: workMeter)
            for definition in ordered {
                let input: QueryStatement
                let aliases: [String]?
                if case .select(let body) = definition.statement, composed.recursive {
                    let declaration = NamedSubquery(name: definition.name, columns: definition.columns,
                        query: body, materialized: definition.materialized, search: definition.search, cycle: definition.cycle)
                    input = .select(SelectQuery(projection: [.wildcard()], source: .table(TableRef(definition.name)),
                        subqueries: [declaration], withRecursive: true))
                    aliases = nil
                } else {
                    if case .select = definition.statement {} else {
                        guard definition.search == nil, definition.cycle == nil else {
                            throw SQLStatementReadError.invalidDeclaration(definition.name)
                        }
                    }
                    input = definition.statement
                    aliases = definition.columns
                }
                let node = try await bind(input, context: context, transaction: transaction,
                    options: options, scope: scope, workMeter: workMeter)
                let columns: [QueryColumn]?
                switch definition.statement {
                case .insert(let query): columns = query.returning == nil ? nil : node.outputs
                case .update(let query): columns = query.returning == nil ? nil : node.outputs
                case .delete(let query): columns = query.returning == nil ? nil : node.outputs
                case .merge(let query): columns = query.returning == nil ? nil : node.outputs
                default: columns = node.outputs
                }
                _ = try scope.register(name: definition.name, columns: columns, aliases: aliases)
                children.append(node)
            }
            let body = try await bind(composed.body, context: context, transaction: transaction,
                options: options, scope: scope, workMeter: workMeter)
            children.append(body)
            return SQLExplainPlanNode(operation: "With", outputs: body.outputs, children: children, storageReservation: claim)
        default: throw SQLBlockExecutionError.unsupportedStatement
        }
    }
}
