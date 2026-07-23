public import DatabaseValue
import DatabaseWire

public struct DatabaseCommandResult<Output: DatabaseWireValue>:
    DatabaseWireValue {
    public let output: Output
    public let continuation: DatabaseBytes?

    public init(
        output: Output,
        continuation: DatabaseBytes? = nil
    ) {
        self.output = output
        self.continuation = continuation
    }

    public func encode(
        into writer: inout DatabaseWireWriter
    ) throws(DatabaseWireError) {
        try output.encode(into: &writer)
        try writer.writeOptionalBytes(continuation)
    }

    public init(
        from reader: inout DatabaseWireReader
    ) throws(DatabaseWireError) {
        self.init(
            output: try Output(from: &reader),
            continuation: try reader.readOptionalBytes()
        )
    }
}
