import DatabaseTypes

/// Scoped COPY bytes; this view cannot publish or retain a raw byte result.
@_spi(DatabaseExecution)
public struct SQLBlockCopyView: ~Copyable, Sendable {
    private let value: SQLCopyResult
    init(value: SQLCopyResult) { self.value = value }
    public var affectedRows: UInt64 { value.affectedRows }
    public borrowing func withBytes(_ body: (borrowing ByteString) throws -> Void) throws {
        try body(value.bytes)
    }
}
