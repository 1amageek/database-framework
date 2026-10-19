import DatabaseKit
import DatabaseTypes

/// Bounded statement schema and positional row borrows during projection.
@_spi(DatabaseExecution)
public struct SQLBlockRowsView: ~Copyable, Sendable {
    private let rows: SQLBlockCollectedRows
    init(rows: SQLBlockCollectedRows) { self.rows = rows }
    public var columnCount: Int { rows.rows.columns.count }
    public var rowCount: Int { rows.visibleRange.count }
    public var affectedRows: Int? { rows.affectedRows }

    public borrowing func withColumn(at index: Int,
        _ body: (borrowing QueryColumn) throws -> Void) throws {
        guard index >= 0, index < columnCount else { throw SQLBlockProjectionError.invalidIndex }
        try rows.rows.workMeter.consume(at: .resultMaterialization)
        try body(rows.rows.columns[index])
    }

    public borrowing func withRow(at index: Int,
        _ body: (borrowing SQLBlockRowView) throws -> Void) throws {
        guard index >= 0, index < rowCount else { throw SQLBlockProjectionError.invalidIndex }
        let meter = rows.rows.workMeter
        try meter.consume(at: .resultMaterialization)
        try rows.rows.withElement(at: rows.visibleRange.lowerBound + index) { row in
            var footprint = DatabaseIntermediateFootprint()
            if !row.annotations.isEmpty {
                let annotations = try DatabaseIntermediateCollectionMeter.arrayFootprint(
                    count: row.annotations.count, element: (key: String, value: FieldValue).self)
                footprint = try annotations.adding(annotations)
            }
            if let version = row.version {
                footprint = try footprint.adding(DatabaseIntermediateFootprint(bytes: UInt64(version.value.utf8.count / 2)))
            }
            let claim = footprint.bytes == 0 ? nil
                : try meter.reserveIntermediate(bytes: footprint.bytes, at: .resultMaterialization)
            defer { withExtendedLifetime(claim) {} }
            let view = SQLBlockRowView(row: copy row, workMeter: meter)
            try body(view)
        }
    }
}
