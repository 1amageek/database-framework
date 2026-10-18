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

    package func execute(
        _ block: SQLBlock, parameters: [QueryParameter] = [], options: SQLBlockExecutionOptions,
        workMeter meter: DatabaseWorkMeter, executionDeadline deadline: TransactionExecutionDeadline,
        occurrences: QueryOccurrenceArena? = nil, occurrence: QueryOccurrenceID? = nil
    ) async throws -> SQLBlockResult {
        guard (occurrences == nil) == (occurrence == nil),
              occurrences == nil || block.steps.count == 1 else { throw SQLBlockExecutionError.unsupportedStatement }
        let temporal = context.inheritedSQLTemporalEnvironment ?? context.makeSQLTemporalEnvironment()
        let boxed = try await context.withDataOperation {
            try await context.withSQLTemporalStatement(environment: temporal) {
                try await executeAdmitted(block, parameters: parameters, options: options,
                    workMeter: meter, deadline: deadline, occurrences: occurrences, occurrence: occurrence)
            }
        }
        try meter.checkpoint(at: .resultMaterialization)
        let ready = Ready(boxed.take())
        return try ready.promoteToOutput()
    }

    private func executeAdmitted(
        _ block: SQLBlock, parameters: [QueryParameter], options: SQLBlockExecutionOptions,
        workMeter meter: DatabaseWorkMeter, deadline: TransactionExecutionDeadline,
        occurrences: QueryOccurrenceArena?, occurrence: QueryOccurrenceID?
    ) async throws -> DatabaseReadResultBox<Collecting> {
        let clock = context.container.monotonicClock
        try QueryStructuralValidator.validate(block, parameters: parameters, limits: options.structuralLimits)
        let binder = try QueryParameterBinder(parameters: parameters, structuralLimits: options.structuralLimits)
        for step in block.steps {
            try meter.consume(at: .bindingCandidate)
            if case .statement(let statement) = step { try binder.validate(statement) }
        }
        let timeline = try SQLBlockStatementTimeline(count: block.steps.count, workMeter: meter)
        let requiredAccess: Security.Access = try requiredAccess(for: block, workMeter: meter)
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
                                await request.beginAttempt()
                                await mutations.beginAttempt()
                                let collecting = try await collect(block, parameters: parameters, options: options,
                                    binder: binder, transaction: transaction, timeline: timeline, workMeter: meter,
                                    occurrences: occurrences, occurrence: occurrence)
                                return DatabaseReadResultBox(consume collecting)
                            }
                        } catch let demand as SQLSequenceAllocationDemand {
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
        timeline: SQLBlockStatementTimeline, workMeter: DatabaseWorkMeter,
        occurrences: QueryOccurrenceArena?, occurrence: QueryOccurrenceID?
    ) async throws -> Collecting {
        var results = try DatabaseRetainedArrayBuilder<CollectedStatement>(workMeter: workMeter,
            stage: .resultMaterialization,
            layout: try DatabaseRetainedArrayLayout.forElement(CollectedStatement.self),
            expectedCount: block.steps.count)
        let outputFootprint = try DatabaseIntermediateCollectionMeter.arrayFootprint(
            count: block.steps.count, element: SQLStatementResult.self)
        let outputClaim = try workMeter.reserveIntermediate(bytes: outputFootprint.bytes, at: .resultMaterialization)
        let checkpointFootprint = try DatabaseIntermediateCollectionMeter.arrayFootprint(
            count: block.steps.count, element: NamedCheckpoint.self)
        let checkpointClaim = try workMeter.reserveIntermediate(bytes: checkpointFootprint.bytes, at: .mutationPlanning)
        var checkpoints: [NamedCheckpoint] = []
        checkpoints.reserveCapacity(block.steps.count)
        defer { withExtendedLifetime(checkpointClaim) {} }

        for (index, step) in block.steps.enumerated() {
            try workMeter.consume(at: .mutationPlanning)
            let admission = try results.prepareAppend(footprint: DatabaseIntermediateFootprint(), at: .resultMaterialization)
            let result: CollectedStatement
            switch step {
            case .statement(let statement):
                let instant = timeline.instant(at: index, clock: context.container.wallClock)
                let session = try await transaction.makeSQLSessionBinding(searchPath: options.searchPath, workMeter: workMeter)
                result = try await ActiveSQLSession.$binding.withValue(session) {
                    try await transaction.withStatementScope {
                        let inherited = context.makeSQLTemporalEnvironment()
                        let temporal = SQLTemporalEnvironment(transactionInstant: inherited.transactionInstant,
                            statementInstant: instant, wallClock: inherited.wallClock, configuration: inherited.configuration)
                        return try await SQLMutationRuntime.$statementIndex.withValue(index) {
                          try await SQLSequenceRuntime.$statementIndex.withValue(index) {
                            try await context.withSQLTemporalStatement(environment: temporal) {
                                try await execute(statement, parameters: parameters, options: options,
                                    binder: binder, transaction: transaction, temporal: temporal, workMeter: workMeter,
                                    occurrences: occurrences, occurrence: occurrence)
                            }
                          }
                        }
                    }
                }
            case .setConstraints(let control):
                let names: [String]?
                switch control.selection {
                case .all: names = nil
                case .named(let selected): names = selected
                }
                try await transaction.setSQLConstraintsDeferred(control.deferred, names: names)
                result = .command(.setConstraints, nil)
            case .sequence(let command):
                guard let request = SQLSequenceRuntime.request else { throw SQLSequenceExecutionError.requestRequired }
                result = try await transaction.withStatementScope {
                    let kind = try await request.administer(command, step: index, transaction: transaction)
                    return .command(kind, nil)
                }
            case .savepoint(let name):
                let checkpoint = try await transaction.createCheckpoint()
                checkpoints.append(NamedCheckpoint(name: name, token: checkpoint))
                result = .command(.savepoint, nil)
            case .rollbackTo(let name):
                let position = try checkpointIndex(name, in: checkpoints, workMeter: workMeter)
                try await transaction.rollback(to: checkpoints[position].token)
                checkpoints.removeSubrange((position + 1)..<checkpoints.count)
                result = .command(.rollbackTo, nil)
            case .release(let name):
                let position = try checkpointIndex(name, in: checkpoints, workMeter: workMeter)
                try await transaction.release(checkpoints[position].token)
                checkpoints.removeSubrange(position..<checkpoints.count)
                result = .command(.release, nil)
            }
            results.append(result, using: consume admission)
        }
        return Collecting(statements: results.finish(), outputClaim: outputClaim)
    }

    private func execute(
        _ statement: QueryStatement, parameters: [QueryParameter], options: SQLBlockExecutionOptions,
        binder: QueryParameterBinder, transaction: DatabaseTransaction,
        temporal: SQLTemporalEnvironment, workMeter: DatabaseWorkMeter,
        occurrences: QueryOccurrenceArena?, occurrence: QueryOccurrenceID?
    ) async throws -> CollectedStatement {
        switch statement {
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
                try workMeter.recordOutputRows(count)
                return rows
            }
            return .rows(boxed)
        case .insert, .update, .delete, .merge, .truncate, .with:
            let bound = try binder.bind(statement)
            let operation: @Sendable (DatabaseReadSession) async throws -> sending CollectedStatement = { session in
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
        case .createGraph, .dropGraph, .sparqlUpdate, .construct, .ask, .describe:
            throw SQLBlockExecutionError.unsupportedStatement
        }
    }

    private func orderedDefinitions(_ with: SQLWithStatement, scope: SQLStatementReadScope,
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
        temporal: SQLTemporalEnvironment, workMeter: DatabaseWorkMeter) async throws -> CollectedStatement {
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
            let relations = try query.tables.map { try session.resolution.resolve($0).declaration }
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
        for step in block.steps {
            switch step {
            case .sequence: access.insert(.administer)
            case .statement(let statement):
                switch statement {
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
    }

    private enum CollectedStatement: Sendable {
        case rows(SQLBlockCollectedRows)
        case command(SQLCommandKind, UInt64?)
    }

    private struct Collecting: ~Copyable, Sendable {
        let statements: DatabaseRetainedBuffer<CollectedStatement>
        let outputClaim: DatabaseIntermediateReservation
    }

    private struct Ready: ~Copyable {
        private let collecting: Collecting
        init(_ collecting: consuming Collecting) { self.collecting = consume collecting }

        consuming func promoteToOutput() throws -> SQLBlockResult {
            let claim = collecting.outputClaim
            defer { withExtendedLifetime(claim) {} }
            var results: [SQLStatementResult] = []
            results.reserveCapacity(collecting.statements.count)
            for index in 0..<collecting.statements.count {
                try collecting.statements.withElement(at: index) { value in
                    switch value {
                    case .command(let kind, let count): results.append(.command(kind, affectedRows: count))
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
