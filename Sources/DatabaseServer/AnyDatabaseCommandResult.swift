import DatabaseValue
import DatabaseWire

struct AnyDatabaseCommandResult: Sendable {
    let continuation: DatabaseBytes?

    private let writeOutput: @Sendable (
        inout DatabaseWireWriter
    ) throws(DatabaseWireError) -> Void

    init<Output: DatabaseWireValue>(
        _ result: DatabaseCommandResult<Output>
    ) {
        self.continuation = result.continuation
        self.writeOutput = {
            (writer: inout DatabaseWireWriter) throws(DatabaseWireError) in
            try result.output.encode(into: &writer)
        }
    }

    func encodeLengthPrefixedOutput(
        into writer: inout DatabaseWireWriter
    ) throws(DatabaseWireError) {
        try writer.writeLengthPrefixed(writeOutput)
    }

}
