/// Synchronous complete-response encoding over one provisional SQL aggregate.
@_spi(DatabaseExecution)
public protocol SQLBlockResponseEncoder: Sendable {
    func measure(_ output: borrowing SQLBlockOutputView) throws -> UInt64
    func encode(_ output: borrowing SQLBlockOutputView,
        into destination: inout MutableSpan<UInt8>) throws
}
