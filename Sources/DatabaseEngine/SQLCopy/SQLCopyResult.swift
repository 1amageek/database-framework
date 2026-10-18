import DatabaseTypes

/// Request-contained COPY output after the enclosing block has closed.
public struct SQLCopyResult: Sendable {
    public let bytes: ByteString
    public let affectedRows: UInt64

    public init(bytes: ByteString, affectedRows: UInt64) {
        self.bytes = bytes
        self.affectedRows = affectedRows
    }
}
