import DatabaseKit

/// A provisional aggregate; borrowing it does not establish transaction closure.
@_spi(DatabaseExecution)
public struct SQLBlockOutputView: ~Copyable, Sendable {
    let statements: DatabaseRetainedBuffer<SQLBlockStatementOutcome>

    init(statements: consuming DatabaseRetainedBuffer<SQLBlockStatementOutcome>) {
        self.statements = consume statements
    }

    public var statementCount: Int { statements.count }

    public borrowing func withStatement(at index: Int,
        command: (SQLCommandKind, UInt64?) throws -> Void,
        rows: (borrowing SQLBlockRowsView) throws -> Void,
        copy: (borrowing SQLBlockCopyView) throws -> Void
    ) throws {
        guard index >= 0, index < statementCount else { throw SQLBlockProjectionError.invalidIndex }
        try statements.workMeter.consume(at: .resultMaterialization)
        try statements.withElement(at: index) { outcome in
            switch outcome {
            case .command(let kind, let affected): try command(kind, affected)
            case .copy(let value, _):
                let view = SQLBlockCopyView(value: value)
                try copy(view)
            case .rows(let value):
                let meter = statements.workMeter
                let footprint = try DatabaseIntermediateCollectionMeter.arrayFootprint(
                    count: value.rows.columns.count, element: QueryColumn.self)
                let claim = try meter.reserveIntermediate(bytes: footprint.bytes, at: .resultMaterialization)
                defer { withExtendedLifetime(claim) {} }
                let view = SQLBlockRowsView(rows: value)
                try rows(view)
            }
        }
    }
}
