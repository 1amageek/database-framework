import DatabaseKit
import DatabaseTypes

/// Composes COPY with ordinary statement execution inside the admitted block.
struct SQLCopyExecutor: Sendable {
    let context: DatabaseContext
    let transaction: DatabaseTransaction
    let options: SQLBlockExecutionOptions
    let temporal: SQLTemporalEnvironment
    let workMeter: DatabaseWorkMeter

    func execute(_ command: SQLCopyCommand, parameters: [QueryParameter],
        execute: @Sendable (QueryStatement, [QueryParameter]) async throws -> SQLBlockStatementOutcome
    ) async throws -> SQLBlockStatementOutcome {
        try workMeter.checkpoint(at: .resultMaterialization)
        switch command.direction {
        case .from:
            guard case .table(let table, let namedColumns) = command.target, let input = command.data else {
                throw SQLCopyExecutionError.invalidTarget
            }
            let resolution = try context.makeSQLReadResolution(searchPath: options.searchPath, workMeter: workMeter)
            try resolution.rejectReadOnlyMutation(table)
            let relation = try resolution.resolve(table)
            var columns = try SQLArrayBuffer<String>(meter: workMeter)
            if namedColumns.isEmpty {
                for column in relation.columns {
                    if case .generated = column.declaration.policy { continue }
                    try columns.append(column.declaration.name)
                }
            } else {
                for name in namedColumns {
                    try workMeter.consume(at: .bindingCandidate)
                    guard relation.columns.contains(where: { CanonicalSourceName.equal($0.declaration.name, name) }),
                          !columns.values.contains(where: { CanonicalSourceName.equal($0, name) }) else {
                        throw SQLCopyExecutionError.invalidTarget
                    }
                    try columns.append(name)
                }
            }
            let selectedColumns = columns.finish()
            defer { withExtendedLifetime(selectedColumns.reservation) {} }
            var decoder = try SQLCopyDecoder(bytes: input, options: command.options, workMeter: workMeter)
            if command.options.header {
                _ = try decoder.next(expectedColumnCount: selectedColumns.values.count)
            }
            var count: UInt64 = 0
            while let row = try decoder.next(expectedColumnCount: selectedColumns.values.count) {
                defer { withExtendedLifetime(row.reservation) {} }
                var expressions = try SQLArrayBuffer<Expression>(count: row.fields.count, meter: workMeter)
                for field in row.fields {
                    try expressions.append(.literal(field.map(Literal.sqlUnknownString) ?? .null))
                }
                let supplied = expressions.finish()
                defer { withExtendedLifetime(supplied.reservation) {} }
                let statement = QueryStatement.insert(InsertQuery(target: table, columns: selectedColumns.values,
                    source: .values([supplied.values]), overriding: .systemValue))
                guard case .command(_, let affected) = try await execute(statement, parameters), affected == 1 else {
                    throw SQLCopyExecutionError.invalidStatementOutcome
                }
                count += 1
            }
            return .command(.copy, count)
        case .to:
            guard command.data == nil else { throw SQLCopyExecutionError.invalidTarget }
            let query: SelectQuery
            let projectionClaim: DatabaseIntermediateReservation?
            switch command.target {
            case .query(let selected): query = selected; projectionClaim = nil
            case .table(let table, let names):
                var projection = try SQLArrayBuffer<ProjectionItem>(meter: workMeter)
                if names.isEmpty {
                    let resolution = try context.makeSQLReadResolution(searchPath: options.searchPath, workMeter: workMeter)
                    let relation = try resolution.resolve(table)
                    for column in relation.columns {
                        if case .generated = column.declaration.policy { continue }
                        try projection.append(.column(column.declaration.name))
                    }
                }
                else { for name in names { try projection.append(.column(name)) } }
                let selected = projection.finish()
                projectionClaim = selected.reservation
                query = SelectQuery(projection: selected.values, source: .table(table))
            }
            defer { withExtendedLifetime(projectionClaim) {} }
            guard case .rows(let rows) = try await execute(.select(query), parameters) else {
                throw SQLCopyExecutionError.invalidStatementOutcome
            }
            var encoder = try SQLCopyEncoder(options: command.options, workMeter: workMeter)
            if command.options.header {
                for (index, column) in rows.rows.columns.enumerated() {
                    if index > 0 { try encoder.separator() }
                    try encoder.field(column.name[...])
                }
                try encoder.endRecord()
            }
            for index in rows.visibleRange {
                try workMeter.checkpoint(at: .resultMaterialization)
                try rows.rows.withElement(at: index) { row in
                    guard row.values.count == rows.rows.columns.count else { throw SQLCopyExecutionError.invalidStatementOutcome }
                    for position in row.values.indices {
                        if position > 0 { try encoder.separator() }
                        let value = row.values[position]
                        if value.isNull { try encoder.null() }
                        else {
                            try CanonicalSQLScalarText.withText(of: value, type: rows.rows.columns[position].type,
                                preservingCharacterPadding: true, temporalEnvironment: temporal, workMeter: workMeter) { text in
                                try encoder.field(text)
                            }
                        }
                    }
                    try encoder.endRecord()
                }
            }
            let (result, claim) = encoder.finish(affectedRows: UInt64(rows.visibleRange.count))
            return .copy(result, claim)
        }
    }
}
