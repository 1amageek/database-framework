import DatabaseKit
import DatabaseTypes

/// One row whose payload and conversion scratch remain owned by its caller.
@_spi(DatabaseExecution)
public struct SQLBlockRowView: ~Copyable, Sendable {
    private let row: ExecutionQueryRow
    private let workMeter: DatabaseWorkMeter
    init(row: ExecutionQueryRow, workMeter: DatabaseWorkMeter) { self.row = row; self.workMeter = workMeter }
    public var valueCount: Int { row.values.count }

    public borrowing func withValue(at index: Int,
        _ body: (borrowing QueryValue) throws -> Void) throws {
        guard index >= 0, index < valueCount else { throw SQLBlockProjectionError.invalidIndex }
        try workMeter.consume(at: .resultMaterialization)
        try body(row.values[index])
    }
    public borrowing func withAnnotations(
        _ body: (borrowing [String: FieldValue]) throws -> Void) throws {
        try body(row.annotations)
    }
    public borrowing func withVersion(
        _ body: (borrowing PersistableVersionToken?) throws -> Void) throws {
        try body(row.version)
    }
}
