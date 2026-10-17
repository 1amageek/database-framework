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
        let request = SQLSequenceRequest(context: context, searchPath: options.searchPath,
            meter: meter, configuration: options.configuration, deadline: deadline)
        try await context.container.sqlSequenceCoordinator.enter(clock: clock, deadline: deadline)
        let boxed: DatabaseReadResultBox<Collecting>
        do {
            boxed = try await SQLSequenceRuntime.$request.withValue(request) {
                try await SQLSequenceRuntime.$workMeter.withValue(meter) {
                    while true {
                        do {
                            return try await context.withCheckpointTransaction(requiredAccess: requiredAccess,
                                configuration: options.configuration, checkpointLimits: options.checkpointLimits,
                                workMeter: meter, executionDeadline: deadline) { transaction in
                                await request.beginAttempt()
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
                        return try await SQLSequenceRuntime.$statementIndex.withValue(index) {
                            try await context.withSQLTemporalStatement(environment: temporal) {
                                try await execute(statement, parameters: parameters, options: options,
                                    binder: binder, transaction: transaction, temporal: temporal, workMeter: workMeter,
                                    occurrences: occurrences, occurrence: occurrence)
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
        case .insert, .update, .delete:
            let bound = try binder.bind(statement)
            guard options.budget.maximumRows > 0 else {
                throw DatabaseWorkLimitError.maximumRows(stage: .mutationPlanning,
                    consumed: workMeter.consumedRows, requested: 1, maximum: 0)
            }
            guard let maximumChanges = Int(exactly: options.budget.maximumRows) else {
                throw SQLBlockExecutionError.mutationLimitOutOfRange(options.budget.maximumRows)
            }
            let limits = try DatabaseEntityMutationLimits(maximumChanges: maximumChanges, maximumPreconditions: 1)
            let effects = try await DatabaseEntityStatementMutationExecutor(container: context.container, limits: limits)
                .executeSQL(bound, searchPath: options.searchPath, transaction: transaction,
                    workMeter: workMeter, temporalEnvironment: temporal)
            let kind: SQLCommandKind
            switch statement { case .insert: kind = .insert; case .update: kind = .update; default: kind = .delete }
            return .command(kind, UInt64(effects.count))
        case .createGraph, .dropGraph, .sparqlUpdate, .construct, .ask, .describe:
            throw SQLBlockExecutionError.unsupportedStatement
        }
    }

    private func requiredAccess(for block: SQLBlock, workMeter: DatabaseWorkMeter) throws -> Security.Access {
        var access: Security.Access = []
        for step in block.steps {
            switch step {
            case .sequence: access.insert(.administer)
            case .statement(let statement):
                switch statement {
                case .insert, .update, .delete: access.insert(.write)
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
                        results.append(.rows(try QueryResponse(columns: rows.rows.columns, rows: output, metadata: rows.metadata)))
                    }
                }
            }
            return SQLBlockResult(results: consume results)
        }
    }
}
