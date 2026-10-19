import DatabaseKit
import DatabaseTypes
import StorageKit

/// Executes a single-request SQL block without publishing intermediate results.
@_spi(DatabaseExecution)
public struct SQLBlockExecutor: Sendable {
    private let context: DatabaseContext

    public init(context: DatabaseContext) { self.context = context }

    public func execute(
        _ block: SQLBlock,
        parameters: [QueryParameter] = [],
        options: SQLBlockExecutionOptions
    ) async throws -> SQLBlockResult {
        let clock = context.container.monotonicClock
        let deadline = TransactionExecutionDeadline(
            instant: clock.now.advanced(by: .milliseconds(Int64(options.budget.timeoutMilliseconds))),
            timeoutMilliseconds: options.budget.timeoutMilliseconds)
        let meter = DatabaseWorkMeter(budget: options.budget, monotonicClock: clock)
        return try await execute(block, parameters: parameters, options: options,
            workMeter: meter, executionDeadline: deadline)
    }

    public func executeEncoded(_ block: SQLBlock, parameters: [QueryParameter] = [],
        options: SQLBlockExecutionOptions, encoder: any SQLBlockResponseEncoder) async throws -> SQLEncodedResponse {
        let clock = context.container.monotonicClock
        let deadline = TransactionExecutionDeadline(
            instant: clock.now.advanced(by: .milliseconds(Int64(options.budget.timeoutMilliseconds))),
            timeoutMilliseconds: options.budget.timeoutMilliseconds)
        let meter = DatabaseWorkMeter(budget: options.budget, monotonicClock: clock)
        return try await executeEncoded(block, parameters: parameters, options: options,
            workMeter: meter, executionDeadline: deadline, encoder: encoder)
    }

    package func execute(
        _ block: SQLBlock, parameters: [QueryParameter] = [], options: SQLBlockExecutionOptions,
        workMeter meter: DatabaseWorkMeter, executionDeadline deadline: TransactionExecutionDeadline,
        occurrences: QueryOccurrenceArena? = nil, occurrence: QueryOccurrenceID? = nil,
        occurrenceRoots: [QueryOccurrenceID?]? = nil, failurePosition: SQLBlockFailurePosition? = nil
    ) async throws -> SQLBlockResult {
        let boxed = try await executeCollecting(block, parameters: parameters, options: options,
            workMeter: meter, executionDeadline: deadline, occurrences: occurrences, occurrence: occurrence,
            occurrenceRoots: occurrenceRoots, failurePosition: failurePosition, encoder: nil)
        return try Ready(boxed.take()).promoteToOutput()
    }

    package func executeEncoded(
        _ block: SQLBlock, parameters: [QueryParameter] = [], options: SQLBlockExecutionOptions,
        workMeter meter: DatabaseWorkMeter, executionDeadline deadline: TransactionExecutionDeadline,
        occurrences: QueryOccurrenceArena? = nil, occurrence: QueryOccurrenceID? = nil,
        occurrenceRoots: [QueryOccurrenceID?]? = nil, failurePosition: SQLBlockFailurePosition? = nil,
        encoder: any SQLBlockResponseEncoder
    ) async throws -> SQLEncodedResponse {
        let boxed = try await executeCollecting(block, parameters: parameters, options: options,
            workMeter: meter, executionDeadline: deadline, occurrences: occurrences, occurrence: occurrence,
            occurrenceRoots: occurrenceRoots, failurePosition: failurePosition, encoder: encoder)
        return try Ready(boxed.take()).promoteToEncodedOutput()
    }

    private func executeCollecting(
        _ block: SQLBlock, parameters: [QueryParameter] = [], options: SQLBlockExecutionOptions,
        workMeter meter: DatabaseWorkMeter, executionDeadline deadline: TransactionExecutionDeadline,
        occurrences: QueryOccurrenceArena? = nil, occurrence: QueryOccurrenceID? = nil,
        occurrenceRoots: [QueryOccurrenceID?]? = nil, failurePosition: SQLBlockFailurePosition? = nil,
        encoder: (any SQLBlockResponseEncoder)?
    ) async throws -> DatabaseReadResultBox<Collecting> {
        if let roots = occurrenceRoots {
            guard occurrences != nil, occurrence == nil, roots.count == block.steps.count else {
                throw SQLBlockExecutionError.unsupportedStatement
            }
        } else {
            guard (occurrences == nil) == (occurrence == nil),
                  occurrences == nil || block.steps.count == 1 else { throw SQLBlockExecutionError.unsupportedStatement }
        }
        let temporal = context.inheritedSQLTemporalEnvironment ?? context.makeSQLTemporalEnvironment()
        let boxed = try await context.withDataOperation {
            try await context.withSQLTemporalStatement(environment: temporal) {
                try await executeAdmitted(block, parameters: parameters, options: options,
                    workMeter: meter, deadline: deadline, occurrences: occurrences, occurrence: occurrence, occurrenceRoots: occurrenceRoots, failurePosition: failurePosition, encoder: encoder)
            }
        }
        try meter.checkpoint(at: .resultMaterialization)
        return boxed
    }

    private func executeAdmitted(
        _ block: SQLBlock, parameters: [QueryParameter], options: SQLBlockExecutionOptions,
        workMeter meter: DatabaseWorkMeter, deadline: TransactionExecutionDeadline,
        occurrences: QueryOccurrenceArena?, occurrence: QueryOccurrenceID?, occurrenceRoots: [QueryOccurrenceID?]?, failurePosition: SQLBlockFailurePosition?,
        encoder: (any SQLBlockResponseEncoder)?
    ) async throws -> DatabaseReadResultBox<Collecting> {
        let clock = context.container.monotonicClock
        try QueryStructuralValidator.validate(block, parameters: parameters, limits: options.structuralLimits)
        let binder = try QueryParameterBinder(parameters: parameters, structuralLimits: options.structuralLimits)
        try binder.validate(block)
        try validateTransactionControls(block)
        let timeline = try SQLBlockStatementTimeline(count: block.steps.count, workMeter: meter)
        let requiredAccess: Security.Access = try requiredAccess(for: block, workMeter: meter)
        let ddlRequest = SQLDDLRequest()
        let mutations = SQLMutationRequest(workMeter: meter)
        let request = SQLSequenceRequest(context: context, searchPath: options.searchPath,
            meter: meter, configuration: options.configuration, deadline: deadline)
        try await context.container.sqlSequenceCoordinator.enter(clock: clock, deadline: deadline)
        let boxed: DatabaseReadResultBox<Collecting>
        do {
            boxed = try await SQLMutationRuntime.$request.withValue(mutations) {
              try await SQLSequenceRuntime.$request.withValue(request) {
                try await SQLSequenceRuntime.$workMeter.withValue(meter) {
                    while true {
                        do {
                            return try await context.withCheckpointTransaction(requiredAccess: requiredAccess,
                                configuration: options.configuration, checkpointLimits: options.checkpointLimits,
                                workMeter: meter, executionDeadline: deadline) { transaction in
                                failurePosition?.reset()
                                await request.beginAttempt()
                                await mutations.beginAttempt()
                                let collecting = try await collect(block, parameters: parameters, options: options,
                                    binder: binder, transaction: transaction, timeline: timeline, workMeter: meter, ddlRequest: ddlRequest,
                                    occurrences: occurrences, occurrence: occurrence, occurrenceRoots: occurrenceRoots, failurePosition: failurePosition, encoder: encoder)
                                let boxed = DatabaseReadResultBox(consume collecting)
                                if case .transaction(.rollback) = block.steps.last {
                                    throw RollbackDemand(result: boxed)
                                }
                                return boxed
                            }
                        } catch let demand as RollbackDemand {
                            try ensureDatabaseTaskIsActive()
                            return demand.result
                        } catch let demand as SQLSequenceAllocationDemand {
                            failurePosition?.reset()
                            // Only the unwrapped private demand proves rollback and close succeeded.
                            // Composite cleanup failures never enter this independent allocator path.
                            try await request.allocate(demand)
                        }
                    }
                }
              }
            }
        } catch {
            do { try await request.close() }
            catch let cleanupError {
                await context.container.sqlSequenceCoordinator.leave()
                throw SQLSequenceRequestCleanupError(operationError: error, cleanupError: cleanupError)
            }
            await context.container.sqlSequenceCoordinator.leave()
            throw error
        }
        do { try await request.close() }
        catch {
            await context.container.sqlSequenceCoordinator.leave()
            throw error
        }
        await context.container.sqlSequenceCoordinator.leave()
        return boxed
    }

    private func collect(
        _ block: SQLBlock, parameters: [QueryParameter], options: SQLBlockExecutionOptions,
        binder: QueryParameterBinder, transaction: DatabaseTransaction,
        timeline: SQLBlockStatementTimeline, workMeter: DatabaseWorkMeter, ddlRequest: SQLDDLRequest,
        occurrences: QueryOccurrenceArena?, occurrence: QueryOccurrenceID?, occurrenceRoots: [QueryOccurrenceID?]?, failurePosition: SQLBlockFailurePosition?,
        encoder: (any SQLBlockResponseEncoder)?
    ) async throws -> Collecting {
        var results = try DatabaseRetainedArrayBuilder<SQLBlockStatementOutcome>(workMeter: workMeter,
            stage: .resultMaterialization,
            layout: try DatabaseRetainedArrayLayout.forElement(SQLBlockStatementOutcome.self),
            expectedCount: block.steps.count)
        let outputClaim: DatabaseIntermediateReservation?
        if encoder == nil {
            let outputFootprint = try DatabaseIntermediateCollectionMeter.arrayFootprint(
                count: block.steps.count, element: SQLStatementResult.self)
            outputClaim = try workMeter.reserveIntermediate(bytes: outputFootprint.bytes, at: .resultMaterialization)
        } else { outputClaim = nil }
        let checkpointFootprint = try DatabaseIntermediateCollectionMeter.arrayFootprint(
            count: block.steps.count, element: NamedCheckpoint.self)
        let checkpointClaim = try workMeter.reserveIntermediate(bytes: checkpointFootprint.bytes, at: .mutationPlanning)
        let inherited = context.makeSQLTemporalEnvironment()
        let defaults = SQLBlockSettings(searchPath: options.searchPath, temporal: inherited.configuration)
        var settings = defaults
        let prepared = SQLPreparedRequest()
        var checkpoints: [NamedCheckpoint] = []
        checkpoints.reserveCapacity(block.steps.count)
        defer { withExtendedLifetime(checkpointClaim) {} }

        for (index, step) in block.steps.enumerated() {
            let stepOccurrence = occurrenceRoots.map { $0[index] } ?? occurrence
            do {
            try workMeter.consume(at: .mutationPlanning)
            let admission = try results.prepareAppend(footprint: DatabaseIntermediateFootprint(), at: .resultMaterialization)
            let result: SQLBlockStatementOutcome
            switch step {
            case .statement(let statement):
                result = try await withinStatement(index: index, settings: settings, options: options,
                    timeline: timeline, transaction: transaction, workMeter: workMeter, ddlRequest: ddlRequest) { effective, temporal in
                    try await execute(statement, parameters: parameters, options: effective,
                        binder: binder, transaction: transaction, temporal: temporal, workMeter: workMeter,
                        ddlRequest: ddlRequest, stepIndex: index, occurrences: occurrences, occurrence: stepOccurrence)
                }
            case .transaction(let control):
                switch control {
                case .begin: result = .command(.begin, nil)
                case .commit: result = .command(.commit, nil)
                case .rollback: result = .command(.rollback, nil)
                }
            case .setting(let command):
                switch command {
                case .set(let name, let expression, _):
                    let evaluated = try await withinStatement(index: index, settings: settings, options: options,
                        timeline: timeline, transaction: transaction, workMeter: workMeter, ddlRequest: ddlRequest) { effective, temporal in
                        try await evaluateExpressions([expression], parameters: parameters, options: effective,
                            transaction: transaction, temporal: temporal, workMeter: workMeter,
                            ddlRequest: ddlRequest, stepIndex: index)
                    }
                    guard case .rows(let rows) = evaluated, rows.visibleRange.count == 1 else {
                        throw SQLBlockExecutionError.invalidSettingValue(name)
                    }
                    let value = rows.rows.withElement(at: rows.visibleRange.lowerBound) { $0.values[0] }
                    let inherited = context.makeSQLTemporalEnvironment()
                    let environment = SQLTemporalEnvironment(transactionInstant: inherited.transactionInstant,
                        statementInstant: timeline.instant(at: index, clock: inherited.wallClock),
                        wallClock: inherited.wallClock, configuration: settings.temporal)
                    try settings.set(name, value: value, environment: environment, workMeter: workMeter)
                    result = .command(.set, nil)
                case .reset(let name, _):
                    settings.reset(name, defaults: defaults)
                    result = .command(.reset, nil)
                case .show(let name):
                    let names = name.map { [$0] } ?? SQLBlockSettings.names
                    let state = settings
                    let query = SelectQuery(projection: name == nil
                        ? [.column("column1", as: "name"), .column("column2", as: "setting")]
                        : [.column("column1", as: SQLBlockSettings.label(names[0]))],
                        source: .values(names.map { selected in
                            name == nil ? [.string(SQLBlockSettings.label(selected)), .string(state.text(selected))]
                                : [.string(state.text(selected))]
                        }))
                    result = try await withinStatement(index: index, settings: settings, options: options,
                        timeline: timeline, transaction: transaction, workMeter: workMeter, ddlRequest: ddlRequest) { effective, temporal in
                        try await execute(.select(query), parameters: [], options: effective,
                            binder: QueryParameterBinder(parameters: [], structuralLimits: effective.structuralLimits),
                            transaction: transaction, temporal: temporal, workMeter: workMeter,
                            ddlRequest: ddlRequest, stepIndex: index, occurrences: nil, occurrence: nil)
                    }
                }
            case .prepare(let definition):
                result = try await withinStatement(index: index, settings: settings, options: options,
                    timeline: timeline, transaction: transaction, workMeter: workMeter, ddlRequest: ddlRequest) { effective, temporal in
                    try await prepared.prepare(definition, context: context, transaction: transaction,
                        options: effective, temporal: temporal, workMeter: workMeter,
                        occurrences: occurrences, occurrence: stepOccurrence)
                    return .command(.prepare, nil)
                }
            case .execute(let invocation):
                result = try await withinStatement(index: index, settings: settings, options: options,
                    timeline: timeline, transaction: transaction, workMeter: workMeter, ddlRequest: ddlRequest) { effective, temporal in
                    let values: [QueryValue]
                    let argumentOutcome: SQLBlockStatementOutcome?
                    if invocation.arguments.isEmpty { values = []; argumentOutcome = nil }
                    else {
                        let arguments = try await evaluateExpressions(invocation.arguments, parameters: parameters,
                            options: effective, transaction: transaction, temporal: temporal, workMeter: workMeter,
                            ddlRequest: ddlRequest, stepIndex: index)
                        argumentOutcome = arguments
                        guard case .rows(let rows) = arguments, rows.visibleRange.count == 1 else {
                            throw SQLBlockExecutionError.unsupportedStatement
                        }
                        values = rows.rows.withElement(at: rows.visibleRange.lowerBound) { $0.values }
                    }
                    defer { withExtendedLifetime(argumentOutcome) {} }
                    let lease = await transaction.currentSQLSchemaLease()
                    let call = try await prepared.execution(invocation, arguments: values,
                        schemaGeneration: lease.generation, searchPath: effective.searchPath, schemaIdentity: lease.sqlCatalog.digest,
                        temporal: temporal, workMeter: workMeter)
                    defer { withExtendedLifetime(call) {} }
                    let callBinder = try QueryParameterBinder(parameters: call.parameters, structuralLimits: effective.structuralLimits)
                    return try await execute(call.statement, parameters: call.parameters, options: effective,
                        binder: callBinder, transaction: transaction, temporal: temporal, workMeter: workMeter,
                        ddlRequest: ddlRequest, stepIndex: index, occurrences: nil, occurrence: nil)
                }
            case .deallocate(let name):
                try prepared.deallocate(name)
                result = .command(.deallocate, nil)
            case .explain(let explanation):
                result = try await withinStatement(index: index, settings: settings, options: options,
                    timeline: timeline, transaction: transaction, workMeter: workMeter, ddlRequest: ddlRequest) { effective, temporal in
                    try await SQLExplainExecutor(context: context, transaction: transaction, options: effective,
                        temporal: temporal, workMeter: workMeter).execute(explanation, parameters: parameters, occurrences: occurrences, occurrence: stepOccurrence) { statement, supplied in
                        try await execute(statement, parameters: supplied, options: effective,
                            binder: QueryParameterBinder(parameters: supplied, structuralLimits: effective.structuralLimits),
                            transaction: transaction, temporal: temporal, workMeter: workMeter,
                            ddlRequest: ddlRequest, stepIndex: index, occurrences: occurrences, occurrence: stepOccurrence)
                    }
                }
            case .copy(let command):
                result = try await withinStatement(index: index, settings: settings, options: options,
                    timeline: timeline, transaction: transaction, workMeter: workMeter, ddlRequest: ddlRequest) { effective, temporal in
                    try await SQLCopyExecutor(context: context, transaction: transaction, options: effective,
                        temporal: temporal, workMeter: workMeter).execute(command, parameters: parameters) { statement, supplied in
                        try await execute(statement, parameters: supplied, options: effective,
                            binder: QueryParameterBinder(parameters: supplied, structuralLimits: effective.structuralLimits),
                            transaction: transaction, temporal: temporal, workMeter: workMeter,
                            ddlRequest: ddlRequest, stepIndex: index, occurrences: occurrences, occurrence: stepOccurrence)
                    }
                }
            case .setConstraints(let control):
                let names: [String]?
                switch control.selection {
                case .all: names = nil
                case .named(let selected): names = selected
                }
                result = try await withinStatement(index: index, settings: settings, options: options,
                    timeline: timeline, transaction: transaction, workMeter: workMeter, ddlRequest: ddlRequest) { _, _ in
                    try await transaction.setSQLConstraintsDeferred(control.deferred, names: names)
                    return .command(.setConstraints, nil)
                }
            case .sequence(let command):
                guard let request = SQLSequenceRuntime.request else { throw SQLSequenceExecutionError.requestRequired }
                result = try await withinStatement(index: index, settings: settings, options: options,
                    timeline: timeline, transaction: transaction, workMeter: workMeter, ddlRequest: ddlRequest) { _, _ in
                    let kind = try await request.administer(command, step: index, transaction: transaction)
                    return .command(kind, nil)
                }
            case .savepoint(let name):
                let checkpoint = try await withinCheckpointControl(index: index, settings: settings,
                    timeline: timeline) { try await transaction.createCheckpoint() }
                checkpoints.append(NamedCheckpoint(name: name, token: checkpoint, settings: settings))
                result = .command(.savepoint, nil)
            case .rollbackTo(let name):
                let selected = checkpoints
                let position = try await withinCheckpointControl(index: index, settings: settings, timeline: timeline) {
                    let position = try checkpointIndex(name, in: selected, workMeter: workMeter)
                    try await transaction.rollback(to: selected[position].token)
                    return position
                }
                settings = checkpoints[position].settings
                checkpoints.removeSubrange((position + 1)..<checkpoints.count)
                result = .command(.rollbackTo, nil)
            case .release(let name):
                let selected = checkpoints
                let position = try await withinCheckpointControl(index: index, settings: settings, timeline: timeline) {
                    let position = try checkpointIndex(name, in: selected, workMeter: workMeter)
                    try await transaction.release(selected[position].token)
                    return position
                }
                checkpoints.removeSubrange(position..<checkpoints.count)
                result = .command(.release, nil)
            }
            results.append(result, using: consume admission)
            } catch {
                failurePosition?.record(index)
                throw error
            }
        }
        try await SQLDDLExecutor(context: context, transaction: transaction, options: options, workMeter: workMeter,
            parameters: parameters, request: ddlRequest).cleanupTemporaryObjects(step: block.steps.count)
        let output = SQLBlockOutputView(statements: results.finish())
        let encoded: SQLEncodedPayload?
        if let encoder { encoded = try SQLEncodedPayload.encode(output, encoder: encoder, workMeter: workMeter) }
        else { encoded = nil }
        return Collecting(output: consume output, outputClaim: outputClaim, encoded: encoded)
    }

    private func execute(
        _ statement: QueryStatement, parameters: [QueryParameter], options: SQLBlockExecutionOptions,
        binder: QueryParameterBinder, transaction: DatabaseTransaction,
        temporal: SQLTemporalEnvironment, workMeter: DatabaseWorkMeter, ddlRequest: SQLDDLRequest, stepIndex: Int,
        occurrences: QueryOccurrenceArena?, occurrence: QueryOccurrenceID?, recordsOutputRows: Bool = true
    ) async throws -> SQLBlockStatementOutcome {
        switch statement {
        case .ddl:
            guard case .ddl(let command) = try binder.bind(statement) else { throw SQLBlockExecutionError.unsupportedStatement }
            let kind = try await SQLDDLExecutor(context: context, transaction: transaction, options: options,
                workMeter: workMeter, parameters: parameters, request: ddlRequest).execute(command, step: stepIndex)
            return .command(kind, nil)
        case .select(let query):
            guard !query.source.usesSPARQLSemantics else { throw SQLBlockExecutionError.unsupportedStatement }
            let boxed = try await DatabaseReadSession.withSession(context: context, workMeter: workMeter) { session in
                let resolution = try session.makeSQLReadResolution(searchPath: options.searchPath, workMeter: workMeter)
                let execution = ReadExecutionContext(options: ReadExecutionOptions(budget: options.budget),
                    monotonicClock: context.container.monotonicClock, workMeter: workMeter,
                    queryStructuralLimits: options.structuralLimits, temporalEnvironment: temporal)
                    .withoutExternalPageWindow()
                    .withSQLResolution(resolution)
                let rows = try await context.collectSQLBlockRows(query, execution: execution,
                    session: session, parameters: parameters, occurrences: occurrences, occurrence: occurrence)
                guard let count = UInt32(exactly: rows.visibleRange.count) else {
                    throw DatabaseWorkLimitError.maximumRows(stage: .resultMaterialization,
                        consumed: workMeter.consumedRows, requested: .max, maximum: options.budget.maximumRows)
                }
                if recordsOutputRows { try workMeter.recordOutputRows(count) }
                return rows
            }
            return .rows(boxed)
        case .insert, .update, .delete, .merge, .truncate, .with:
            let bound = try binder.bind(statement)
            let operation: @Sendable (DatabaseReadSession) async throws -> sending SQLBlockStatementOutcome = { session in
                let resolution = try session.makeSQLReadResolution(searchPath: options.searchPath, workMeter: workMeter)
                let execution = ReadExecutionContext(options: ReadExecutionOptions(budget: options.budget),
                    monotonicClock: context.container.monotonicClock, workMeter: workMeter,
                    queryStructuralLimits: options.structuralLimits, temporalEnvironment: temporal)
                    .withoutExternalPageWindow().withSQLResolution(resolution)
                let scope = try SQLStatementReadScope(context: context, session: session, execution: execution)
                return try await ActiveSQLStatementReads.$scope.withValue(scope) {
                    do {
                        let prepared = try await prepare(bound, scope: scope, transaction: transaction,
                            options: options, workMeter: workMeter)
                        try scope.seal()
                        let result = try await run(prepared, scope: scope, transaction: transaction,
                            options: options, temporal: temporal, workMeter: workMeter)
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
            var main = bound
            while case .with(let composed) = main { main = composed.body }
            if case .select = main {
                return try await DatabaseReadSession.withSession(context: context,
                    workMeter: workMeter, operation)
            }
            return try await DatabaseReadSession.withSQLMutationSession(context: context,
                transaction: transaction, workMeter: workMeter, operation)
        case .createGraph, .dropGraph, .sparqlUpdate, .construct, .ask, .describe:
            throw SQLBlockExecutionError.unsupportedStatement
        }
    }

    private indirect enum PreparedStatement: Sendable {
        case select(SelectQuery)
        case mutation(QueryStatement, SQLMutationPreparation)
        case truncate(TruncateQuery)
        case composed([PreparedNamedStatement], PreparedStatement)
    }

    private struct PreparedNamedStatement: Sendable {
        let declaration: SQLStatementRelation
        let statement: PreparedStatement
    }

    private func mutationExecutor(options: SQLBlockExecutionOptions,
        workMeter: DatabaseWorkMeter) throws -> DatabaseEntityStatementMutationExecutor {
        guard options.budget.maximumRows > 0 else {
            throw DatabaseWorkLimitError.maximumRows(stage: .mutationPlanning,
                consumed: workMeter.consumedRows, requested: 1, maximum: 0)
        }
        guard let count = Int(exactly: options.budget.maximumRows) else {
            throw SQLBlockExecutionError.mutationLimitOutOfRange(options.budget.maximumRows)
        }
        return DatabaseEntityStatementMutationExecutor(container: context.container,
            limits: try DatabaseEntityMutationLimits(maximumChanges: count, maximumPreconditions: 1))
    }

    private func prepare(_ statement: QueryStatement, scope: SQLStatementReadScope,
        transaction: DatabaseTransaction, options: SQLBlockExecutionOptions,
        workMeter: DatabaseWorkMeter) async throws -> PreparedStatement {
        try workMeter.consume(at: .mutationPlanning)
        switch statement {
        case .select(let query):
            guard !query.source.usesSPARQLSemantics else { throw SQLBlockExecutionError.unsupportedStatement }
            try await scope.capture(query)
            return .select(query)
        case .insert, .update, .delete, .merge:
            let executor = try mutationExecutor(options: options, workMeter: workMeter)
            return .mutation(statement, try await executor.prepareSQLReads(statement, scope: scope,
                transaction: transaction, workMeter: workMeter))
        case .truncate(let query): return .truncate(query)
        case .with(let with):
            // SELECT-only WITH keeps the existing complete recursive/demand semantics.
            if case .select(let body) = with.body,
               with.statements.allSatisfy({ if case .select = $0.statement { return true }; return false }) {
                let definitions = with.statements.map { definition -> NamedSubquery in
                    guard case .select(let query) = definition.statement else { preconditionFailure() }
                    return NamedSubquery(name: definition.name, columns: definition.columns, query: query,
                        materialized: definition.materialized, search: definition.search, cycle: definition.cycle)
                }
                let query = body.replacing(subqueries: definitions + (body.subqueries ?? []))
                    .replacing(withRecursive: with.recursive || body.withRecursive)
                try await scope.capture(query)
                return .select(query)
            }
            var definitions = try SQLArrayBuffer<PreparedNamedStatement>(meter: workMeter)
            for definition in try orderedDefinitions(with, scope: scope, workMeter: workMeter) {
                switch definition.statement {
                case .select(let body):
                    let query: SelectQuery
                    if with.recursive {
                        let selected = NamedSubquery(name: definition.name, columns: definition.columns,
                            query: body, materialized: definition.materialized,
                            search: definition.search, cycle: definition.cycle)
                        query = SelectQuery(projection: [.wildcard()], source: .table(TableRef(definition.name)),
                            subqueries: [selected], withRecursive: true)
                    } else { query = body }
                    _ = try await context.prepareStatementSelect(query, name: definition.name,
                        aliases: with.recursive ? nil : definition.columns, scope: scope)
                case .insert, .update, .delete, .merge:
                    guard definition.search == nil, definition.cycle == nil else {
                        throw SQLStatementReadError.invalidDeclaration(definition.name)
                    }
                    let prepared = try await prepare(definition.statement, scope: scope,
                        transaction: transaction, options: options, workMeter: workMeter)
                    guard case .mutation(_, let mutation) = prepared else { throw SQLBlockExecutionError.unsupportedStatement }
                    let relation = try scope.register(name: definition.name, columns: mutation.returningColumns,
                        aliases: definition.columns)
                    try definitions.append(PreparedNamedStatement(declaration: relation, statement: prepared))
                default: throw SQLBlockExecutionError.unsupportedStatement
                }
            }
            guard case .with = with.body else {
                let body = try await prepare(with.body, scope: scope, transaction: transaction,
                    options: options, workMeter: workMeter)
                let finished = definitions.finish()
                // The prepared graph retains its metered storage through its last execution.
                try scope.retain(finished.reservation)
                return .composed(finished.values, body)
            }
            throw SQLBlockExecutionError.unsupportedStatement
        case .ddl, .createGraph, .dropGraph, .sparqlUpdate, .construct, .ask, .describe:
            throw SQLBlockExecutionError.unsupportedStatement
        }
    }

    func orderedDefinitions(_ with: SQLWithStatement, scope: SQLStatementReadScope,
        workMeter: DatabaseWorkMeter) throws -> [SQLNamedStatement] {
        guard with.recursive else { return with.statements }
        let names = CanonicalSourceNames(with.statements.map { $0.name })
        let footprint = try DatabaseIntermediateCollectionMeter.arrayFootprint(count: with.statements.count,
            element: CanonicalSourceNames.self).adding(DatabaseIntermediateCollectionMeter.arrayFootprint(
                count: with.statements.count, element: Int.self))
        let claim = try workMeter.reserveIntermediate(bytes: footprint.bytes, at: .bindingCandidate)
        try scope.retain(claim)
        let dependencies = with.statements.map { context.statementReadDependencies($0.statement, among: names) }
        try claim.reserveAdditional(bytes: UInt64(dependencies.reduce(0) { $0 + $1.count }) * 128, at: .bindingCandidate)
        var pending = Array(with.statements.indices)
        var result = try SQLArrayBuffer<SQLNamedStatement>(meter: workMeter)
        while !pending.isEmpty {
            var ready: Int?
            for (position, index) in pending.enumerated() {
                try workMeter.consume(at: .bindingCandidate)
                let definition = with.statements[index]
                if dependencies[index].contains(definition.name) {
                    guard case .select = definition.statement else {
                        throw SQLStatementReadError.invalidDeclaration(definition.name)
                    }
                }
                if dependencies[index].allSatisfy({ name in
                    CanonicalSourceName.equal(name, definition.name)
                        || !pending.contains { CanonicalSourceName.equal(with.statements[$0].name, name) }
                }) { ready = position; break }
            }
            guard let ready else { throw SQLBlockExecutionError.unsupportedStatement }
            try result.append(with.statements[pending.remove(at: ready)])
        }
        let finished = result.finish(); try scope.retain(finished.reservation)
        return finished.values
    }

    private func run(_ prepared: PreparedStatement, scope: SQLStatementReadScope,
        transaction: DatabaseTransaction, options: SQLBlockExecutionOptions,
        temporal: SQLTemporalEnvironment, workMeter: DatabaseWorkMeter) async throws -> SQLBlockStatementOutcome {
        switch prepared {
        case .select(let query):
            let result = try await scope.select(query)
            guard let count = UInt32(exactly: result.visibleRange.count) else {
                throw DatabaseWorkLimitError.maximumRows(stage: .resultMaterialization,
                    consumed: workMeter.consumedRows, requested: .max, maximum: options.budget.maximumRows)
            }
            try workMeter.recordOutputRows(count)
            return .rows(result)
        case .mutation(let statement, let preparation):
            let executor = try mutationExecutor(options: options, workMeter: workMeter)
            let result = try await executor.executeSQL(statement, searchPath: options.searchPath,
                preparation: preparation, readScope: scope, transaction: transaction,
                workMeter: workMeter, temporalEnvironment: temporal)
            if let returning = result.returning { return .rows(returning) }
            let kind: SQLCommandKind
            switch statement {
            case .insert: kind = .insert
            case .update: kind = .update
            case .delete: kind = .delete
            case .merge: kind = .merge
            default: throw SQLBlockExecutionError.unsupportedStatement
            }
            return .command(kind, result.affectedRows)
        case .truncate(let query):
            guard let session = ActiveSQLSession.binding else { throw SQLBlockExecutionError.unsupportedStatement }
            let relations = try query.tables.map { table in
                try session.resolution.rejectReadOnlyMutation(table)
                return try session.resolution.resolve(table).declaration
            }
            let outcome = try await transaction.withEntityMutationOperation(workMeter: workMeter) { operation in
                try await transaction.truncateSQLRelations(relations, cascade: query.references == .cascade,
                    within: operation, workMeter: workMeter)
            }
            if query.identity == .restart {
                guard let request = SQLSequenceRuntime.request else { throw SQLSequenceExecutionError.requestRequired }
                try await request.restartOwnedIdentities(relationIDs: outcome.relationIDs,
                    step: SQLSequenceRuntime.statementIndex, transaction: transaction)
            }
            return .command(.truncate, outcome.affectedRows)
        case .composed(let definitions, let body):
            for definition in definitions {
                let result = try await run(definition.statement, scope: scope, transaction: transaction,
                    options: options, temporal: temporal, workMeter: workMeter)
                if case .rows(let rows) = result { try definition.declaration.install(rows) }
            }
            return try await run(body, scope: scope, transaction: transaction,
                options: options, temporal: temporal, workMeter: workMeter)
        }
    }

    private func requiredAccess(for block: SQLBlock, workMeter: DatabaseWorkMeter) throws -> Security.Access {
        var access: Security.Access = []
        for (stepIndex, original) in block.steps.enumerated() {
            let step: SQLBlockStep
            switch original {
            case .execute(let invocation):
                var definition: SQLPrepare?
                lookup: for prior in block.steps[..<stepIndex].reversed() {
                    try workMeter.consume(at: .bindingCandidate)
                    switch prior {
                    case .prepare(let value) where value.name.utf8.elementsEqual(invocation.name.utf8):
                        definition = value
                        break lookup
                    case .deallocate(nil): break lookup
                    case .deallocate(.some(let name)) where name.utf8.elementsEqual(invocation.name.utf8):
                        break lookup
                    default: break
                    }
                }
                if let definition { step = .statement(definition.statement) }
                else { step = original }
            case .explain(let explanation) where explanation.options.analyze:
                step = .statement(explanation.statement)
            case .copy(let command):
                if command.direction == .from { access.insert(.write) }
                if case .query(let query) = command.target { step = .statement(.select(query)) }
                else { step = original }
            default: step = original
            }
            switch step {
            case .sequence: access.insert(.administer)
            case .statement(let statement):
                switch statement {
                case .ddl: access.formUnion([.administer, .read, .write])
                case .insert, .update, .delete, .merge, .truncate: access.insert(.write)
                case .with(let with):
                    var pending = try SQLArrayBuffer<QueryStatement>(meter: workMeter)
                    try pending.append(with.body)
                    for declaration in with.statements { try pending.append(declaration.statement) }
                    var index = 0
                    while index < pending.values.count {
                        let child = pending.values[index]; index += 1
                        switch child {
                        case .insert, .update, .delete, .merge, .truncate: access.insert(.write)
                        case .with(let nested):
                            try pending.append(nested.body)
                            for declaration in nested.statements { try pending.append(declaration.statement) }
                        default:
                            if try SQLSequenceEffectAnalysis.containsEffect(child, workMeter: workMeter) { access.insert(.write) }
                        }
                    }
                default:
                    if try SQLSequenceEffectAnalysis.containsEffect(statement, workMeter: workMeter) {
                        access.insert(.write)
                    }
                }
            default: break
            }
        }
        return access.isEmpty ? .read : access
    }

    private func validateTransactionControls(_ block: SQLBlock) throws {
        var began = false
        var terminal = false
        for (index, step) in block.steps.enumerated() {
            guard case .transaction(let control) = step else { continue }
            switch control {
            case .begin:
                guard index == 0, !began else { throw SQLBlockExecutionError.invalidTransactionControl }
                began = true
            case .commit, .rollback:
                guard index == block.steps.count - 1, !terminal else { throw SQLBlockExecutionError.invalidTransactionControl }
                terminal = true
            }
        }
        guard !began || terminal else { throw SQLBlockExecutionError.invalidTransactionControl }
    }

    private func withinCheckpointControl<Value: Sendable>(index: Int, settings: SQLBlockSettings,
        timeline: SQLBlockStatementTimeline, operation: @escaping @Sendable () async throws -> Value
    ) async throws -> Value {
        let clock = context.container.monotonicClock
        return try await SQLBlockStatementDeadline.run(milliseconds: settings.statementTimeoutMilliseconds,
            clock: clock, deadline: timeline.deadline(at: index,
                milliseconds: settings.statementTimeoutMilliseconds, clock: clock), operation: operation)
    }

    private func withinStatement(index: Int, settings: SQLBlockSettings,
        options: SQLBlockExecutionOptions, timeline: SQLBlockStatementTimeline,
        transaction: DatabaseTransaction, workMeter: DatabaseWorkMeter, ddlRequest: SQLDDLRequest,
        operation: @escaping @Sendable (SQLBlockExecutionOptions, SQLTemporalEnvironment) async throws -> SQLBlockStatementOutcome
    ) async throws -> SQLBlockStatementOutcome {
        let instant = timeline.instant(at: index, clock: context.container.wallClock)
        let statementDeadline = timeline.deadline(at: index, milliseconds: settings.statementTimeoutMilliseconds,
            clock: context.container.monotonicClock)
        let catalog = await transaction.currentSQLSchemaLease().sqlCatalog.catalog
        let path = try ddlRequest.searchPath(settings.searchPath, catalog: catalog)
        let effective = SQLBlockExecutionOptions(checkpointLimits: options.checkpointLimits, budget: options.budget,
            structuralLimits: options.structuralLimits, searchPath: path, configuration: options.configuration)
        let session = try await transaction.makeSQLSessionBinding(searchPath: path, workMeter: workMeter)
        return try await SQLBlockStatementDeadline.run(milliseconds: settings.statementTimeoutMilliseconds,
            clock: context.container.monotonicClock,
            deadline: statementDeadline) {
            try await ActiveSQLSession.$binding.withValue(session) {
                try await transaction.withStatementScope {
                    let inherited = context.makeSQLTemporalEnvironment()
                    let temporal = SQLTemporalEnvironment(transactionInstant: inherited.transactionInstant,
                        statementInstant: instant, wallClock: inherited.wallClock, configuration: settings.temporal)
                    return try await SQLMutationRuntime.$statementIndex.withValue(index) {
                        try await SQLSequenceRuntime.$statementIndex.withValue(index) {
                            try await context.withSQLTemporalStatement(environment: temporal) {
                                try await operation(effective, temporal)
                            }
                        }
                    }
                }
            }
        }
    }

    private func evaluateExpressions(_ expressions: [Expression], parameters: [QueryParameter],
        options: SQLBlockExecutionOptions, transaction: DatabaseTransaction, temporal: SQLTemporalEnvironment,
        workMeter: DatabaseWorkMeter, ddlRequest: SQLDDLRequest, stepIndex: Int) async throws -> SQLBlockStatementOutcome {
        try await execute(.select(SelectQuery(projection: expressions.map { .expression($0) }, source: .values([[]]))),
            parameters: parameters, options: options,
            binder: QueryParameterBinder(parameters: parameters, structuralLimits: options.structuralLimits),
            transaction: transaction, temporal: temporal, workMeter: workMeter, ddlRequest: ddlRequest,
            stepIndex: stepIndex, occurrences: nil, occurrence: nil, recordsOutputRows: false)
    }

    private struct RollbackDemand: Error, Sendable {
        let result: DatabaseReadResultBox<Collecting>
    }

    private func checkpointIndex(_ name: String, in checkpoints: [NamedCheckpoint],
        workMeter: DatabaseWorkMeter) throws -> Int {
        for index in checkpoints.indices.reversed() {
            try DatabaseByteProcessingMeter.consume(byteCount: UInt64(checkpoints[index].name.utf8.count),
                workMeter: workMeter, stage: .mutationPlanning)
            if checkpoints[index].name.utf8.elementsEqual(name.utf8) { return index }
        }
        throw SQLBlockExecutionError.unknownCheckpoint(name)
    }

    private struct NamedCheckpoint: Sendable {
        let name: String
        let token: DatabaseTransactionCheckpoint
        let settings: SQLBlockSettings
    }


    private struct Collecting: ~Copyable, Sendable {
        let output: SQLBlockOutputView
        let outputClaim: DatabaseIntermediateReservation?
        let encoded: SQLEncodedPayload?
    }

    private struct Ready: ~Copyable {
        private let collecting: Collecting
        init(_ collecting: consuming Collecting) { self.collecting = consume collecting }

        consuming func promoteToEncodedOutput() throws -> SQLEncodedResponse {
            guard let encoded = collecting.encoded else { throw SQLBlockProjectionError.unexpectedOutputMode }
            return SQLEncodedResponse(encoded)
        }

        consuming func promoteToOutput() throws -> SQLBlockResult {
            guard let claim = collecting.outputClaim else { throw SQLBlockProjectionError.unexpectedOutputMode }
            defer { withExtendedLifetime(claim) {} }
            var results: [SQLStatementResult] = []
            results.reserveCapacity(collecting.output.statements.count)
            for index in 0..<collecting.output.statements.count {
                try collecting.output.statements.withElement(at: index) { value in
                    switch value {
                    case .command(let kind, let count): results.append(.command(kind, affectedRows: count))
                    case .copy(let value, let reservation):
                        withExtendedLifetime(reservation) { results.append(.copy(value)) }
                    case .rows(let rows):
                        // Public ownership requires one final positional Array; the collecting shared backing remains charged throughout this transfer.
                        let output = rows.rows.materializePage(rows.visibleRange)
                        results.append(.rows(try QueryResponse(columns: rows.rows.columns, rows: output, metadata: rows.metadata, affectedRows: rows.affectedRows)))
                    }
                }
            }
            return SQLBlockResult(results: consume results)
        }
    }
}
