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
        try QueryStructuralValidator.validate(block, parameters: parameters, limits: options.structuralLimits)
        let binder = try QueryParameterBinder(parameters: parameters, structuralLimits: options.structuralLimits)
        for step in block.steps {
            try meter.consume(at: .bindingCandidate)
            if case .statement(let statement) = step { try binder.validate(statement) }
        }
        let timeline = try SQLBlockStatementTimeline(count: block.steps.count, workMeter: meter)
        let requiredAccess: Security.Access = block.steps.contains { step in
            guard case .statement(let statement) = step else { return false }
            switch statement { case .insert, .update, .delete: return true; default: return false }
        } ? .write : .read
        let boxed = try await context.withCheckpointTransaction(requiredAccess: requiredAccess,
            configuration: options.configuration, checkpointLimits: options.checkpointLimits,
            workMeter: meter, executionDeadline: deadline) { transaction in
            let collecting = try await collect(block, parameters: parameters, options: options,
                binder: binder, transaction: transaction, timeline: timeline, workMeter: meter)
            return DatabaseReadResultBox(consume collecting)
        }
        try meter.checkpoint(at: .resultMaterialization)
        let ready = Ready(boxed.take())
        return try ready.promoteToOutput()
    }

    private func collect(
        _ block: SQLBlock, parameters: [QueryParameter], options: SQLBlockExecutionOptions,
        binder: QueryParameterBinder, transaction: DatabaseTransaction,
        timeline: SQLBlockStatementTimeline, workMeter: DatabaseWorkMeter
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
                result = try await transaction.withStatementScope {
                    let inherited = context.makeSQLTemporalEnvironment()
                    let temporal = SQLTemporalEnvironment(transactionInstant: inherited.transactionInstant,
                        statementInstant: instant, wallClock: inherited.wallClock, configuration: inherited.configuration)
                    return try await context.withSQLTemporalStatement(environment: temporal) {
                        try await execute(statement, parameters: parameters, options: options,
                            binder: binder, transaction: transaction, temporal: temporal, workMeter: workMeter)
                    }
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
        temporal: SQLTemporalEnvironment, workMeter: DatabaseWorkMeter
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
                    session: session, parameters: parameters)
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
            let physical = try physicalMutation(bound, searchPath: options.searchPath, workMeter: workMeter)
            guard options.budget.maximumRows > 0 else {
                throw DatabaseWorkLimitError.maximumRows(stage: .mutationPlanning,
                    consumed: workMeter.consumedRows, requested: 1, maximum: 0)
            }
            guard let maximumChanges = Int(exactly: options.budget.maximumRows) else {
                throw SQLBlockExecutionError.mutationLimitOutOfRange(options.budget.maximumRows)
            }
            let limits = try DatabaseEntityMutationLimits(maximumChanges: maximumChanges, maximumPreconditions: 1)
            let effects = try await DatabaseEntityStatementMutationExecutor(container: context.container, limits: limits)
                .execute(physical, transaction: transaction, workMeter: workMeter, temporalEnvironment: temporal)
            let kind: SQLCommandKind
            switch statement { case .insert: kind = .insert; case .update: kind = .update; default: kind = .delete }
            return .command(kind, UInt64(effects.count))
        case .createGraph, .dropGraph, .sparqlUpdate, .construct, .ask, .describe:
            throw SQLBlockExecutionError.unsupportedStatement
        }
    }

    private func physicalMutation(
        _ statement: QueryStatement, searchPath: SQLSearchPath, workMeter: DatabaseWorkMeter
    ) throws -> QueryStatement {
        let table: TableRef
        switch statement {
        case .insert(let query):
            // FIXME(INCOMPLETE_IMPLEMENTATION): The active SQL block path admits only existing INSERT VALUES/default mutations. S14 must implement retained RETURNING and transaction-scoped INSERT SELECT before these forms can succeed.
            guard query.returning == nil else { throw SQLBlockExecutionError.unsupportedStatement }
            if case .select = query.source { throw SQLBlockExecutionError.unsupportedStatement }
            table = query.target
        case .update(let query):
            // FIXME(INCOMPLETE_IMPLEMENTATION): The active SQL block path does not execute UPDATE FROM or RETURNING. S14 must provide their bound transaction-scoped evaluation and retained result before admission succeeds.
            guard query.from == nil, query.returning == nil else { throw SQLBlockExecutionError.unsupportedStatement }
            table = query.target
        case .delete(let query):
            // FIXME(INCOMPLETE_IMPLEMENTATION): The active SQL block path does not execute DELETE USING or RETURNING. S14 must provide their bound transaction-scoped evaluation and retained result before admission succeeds.
            guard query.using == nil, query.returning == nil else { throw SQLBlockExecutionError.unsupportedStatement }
            table = query.target
        default: throw SQLBlockExecutionError.unsupportedStatement
        }
        let resolution = try context.makeSQLReadResolution(searchPath: searchPath, workMeter: workMeter)
        let relation = try resolution.resolve(table)
        guard relation.declaration.name.utf8.elementsEqual(relation.entity.name.utf8),
              relation.columns.count == relation.entity.fields.count,
              context.container.runtimeConfiguration.entityRuntimes.registration(named: relation.entity.name)?.compiledModelContract != nil,
              relation.columns.allSatisfy({ $0.declaration.name.utf8.elementsEqual($0.field.name.utf8) }) else {
            // FIXME(INCOMPLETE_IMPLEMENTATION): SQL block mutation currently requires exact static-model catalog mappings. S14 must bind renamed/hidden columns and schema-driven targets without a physical-name fallback before they can succeed.
            throw SQLBlockExecutionError.unsupportedMutationMapping(table.table)
        }
        let target = TableRef(table: relation.entity.name, alias: table.alias, partitions: table.partitions)
        func validateName(_ name: String) throws {
            for column in relation.columns {
                try workMeter.consume(at: .bindingCandidate)
                if column.declaration.name.utf8.elementsEqual(name.utf8) { return }
            }
            throw SQLBlockExecutionError.unsupportedMutationMapping(name)
        }
        switch statement {
        case .insert(let query):
            if let columns = query.columns { for column in columns { try validateName(column) } }
            if case .some(.doUpdate(let assignments, _)) = query.onConflict {
                for assignment in assignments { try validateName(assignment.column) }
            }
            return .insert(InsertQuery(target: target, columns: query.columns, source: query.source, onConflict: query.onConflict))
        case .update(let query):
            for assignment in query.assignments { try validateName(assignment.column) }
            return .update(UpdateQuery(target: target, assignments: query.assignments, filter: query.filter))
        case .delete(let query):
            return .delete(DeleteQuery(target: target, filter: query.filter))
        default: throw SQLBlockExecutionError.unsupportedStatement
        }
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
