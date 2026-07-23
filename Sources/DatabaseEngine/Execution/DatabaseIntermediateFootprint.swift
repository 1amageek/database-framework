package struct DatabaseIntermediateFootprint: Sendable, Equatable {
    package let rows: UInt64
    package let bytes: UInt64

    package init(rows: UInt64 = 0, bytes: UInt64 = 0) {
        self.rows = rows
        self.bytes = bytes
    }

    package func adding(
        _ other: DatabaseIntermediateFootprint
    ) throws -> DatabaseIntermediateFootprint {
        let (combinedRows, rowsOverflow) = rows.addingReportingOverflow(
            other.rows
        )
        guard !rowsOverflow else {
            throw DatabaseIntermediateFootprintError.rowAdditionOverflow(
                left: rows,
                right: other.rows
            )
        }
        let (combinedBytes, bytesOverflow) = bytes.addingReportingOverflow(
            other.bytes
        )
        guard !bytesOverflow else {
            throw DatabaseIntermediateFootprintError.byteAdditionOverflow(
                left: bytes,
                right: other.bytes
            )
        }
        return DatabaseIntermediateFootprint(
            rows: combinedRows,
            bytes: combinedBytes
        )
    }

    package func multiplied(
        by multiplier: UInt64
    ) throws -> DatabaseIntermediateFootprint {
        let (multipliedRows, rowsOverflow) = rows.multipliedReportingOverflow(
            by: multiplier
        )
        guard !rowsOverflow else {
            throw DatabaseIntermediateFootprintError
                .rowMultiplicationOverflow(
                    value: rows,
                    multiplier: multiplier
                )
        }
        let (multipliedBytes, bytesOverflow) = bytes
            .multipliedReportingOverflow(by: multiplier)
        guard !bytesOverflow else {
            throw DatabaseIntermediateFootprintError
                .byteMultiplicationOverflow(
                    value: bytes,
                    multiplier: multiplier
                )
        }
        return DatabaseIntermediateFootprint(
            rows: multipliedRows,
            bytes: multipliedBytes
        )
    }
}
