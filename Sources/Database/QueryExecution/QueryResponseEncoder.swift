import DatabaseKit
import DatabaseTypes
@_spi(DatabaseExecution) import DatabaseEngine
@_spi(DatabaseExecution) import DatabaseWire

/// Projects borrowed SQL outcomes directly into the Wire-owned scoped writer.
@_spi(DatabaseExecution)
public struct QueryResponseEncoder: SQLBlockResponseEncoder {
    public let requestID: UInt64
    public let limits: DatabaseWireLimits

    public init(requestID: UInt64, limits: DatabaseWireLimits = .default) {
        self.requestID = requestID; self.limits = limits
    }

    public func measure(_ output: borrowing SQLBlockOutputView) throws -> UInt64 {
        do {
            let count = try DatabaseWireEncoder(limits: limits).measureQueryResponse(
                requestID: requestID, resultCount: output.statementCount
            ) { writer in
                try write(output, into: &writer)
            }
            guard let bytes = UInt64(exactly: count) else { throw DatabaseWireError.byteCountOverflow }
            return bytes
        } catch let error as DatabaseWireError {
            try throwResponseFailure(error)
        }
    }

    public func encode(
        _ output: borrowing SQLBlockOutputView,
        into destination: inout MutableSpan<UInt8>
    ) throws {
        // The span owns the complete initialized destination for this synchronous
        // borrow. Wire neither retains the pointer nor changes its byte capacity.
        do {
            try destination.withUnsafeMutableBytes { buffer in
                try DatabaseWireEncoder(limits: limits).encodeQueryResponse(
                    requestID: requestID, resultCount: output.statementCount, into: buffer
                ) { writer in
                    try write(output, into: &writer)
                }
            }
        } catch let error as DatabaseWireError {
            try throwResponseFailure(error)
        }
    }

    private func throwResponseFailure(_ error: DatabaseWireError) throws -> Never {
        guard QueryResponseEncodingError.isResourceLimit(error) else { throw error }
        throw SQLExecutionFailure(underlyingError: QueryResponseEncodingError.resourceLimit(error),
            diagnostic: try SQLDiagnostic(sqlState: "54000", category: .resource,
                message: "SQL response exceeds the configured Wire resource limits"))
    }

    private func write(
        _ output: borrowing SQLBlockOutputView,
        into writer: inout QueryResponseWriter
    ) throws {
        for index in 0..<output.statementCount {
            try output.withStatement(at: index, command: { kind, affectedRows in
                try writer.writeCommand(kind, affectedRows: affectedRows)
            }, rows: { rows in
                try writeRows(rows, into: &writer)
            }, copy: { copied in
                try copied.withBytes { try writer.writeCopy(bytes: $0, affectedRows: copied.affectedRows) }
            })
        }
    }

    package func writeRows(_ rows: borrowing SQLBlockRowsView,
        into writer: inout QueryResponseWriter) throws {
        let affectedRows: UInt64?
        if let count = rows.affectedRows {
            guard let value = UInt64(exactly: count) else { throw DatabaseWireError.invalidSQLResponse }
            affectedRows = value
        } else { affectedRows = nil }
        try rows.withMetadata { value in
            let metadata = try QueryRowProjection.fields(value)
            try writer.writeRows(columnCount: rows.columnCount, rowCount: rows.rowCount,
                affectedRows: affectedRows, metadata: metadata) { rowWriter in
                for column in 0..<rows.columnCount {
                    try rows.withColumn(at: column) { try rowWriter.writeColumn($0) }
                }
                for row in 0..<rows.rowCount {
                    try rows.withRow(at: row) { value in
                        try rowWriter.beginRow()
                        for cell in 0..<value.valueCount {
                            try value.withValue(at: cell) { try rowWriter.writeValue($0) }
                        }
                        try value.withAnnotations { annotations in
                            let fields = try QueryRowProjection.fields(annotations)
                            try value.withVersion { token in
                                let version: ByteString?
                                if let token { version = try PersistableVersionTokenCodec.digest(from: token) }
                                else { version = nil }
                                try rowWriter.endRow(annotations: fields, version: version)
                            }
                        }
                    }
                }
            }
        }
    }

}
