import DatabaseWire

public struct DatabaseOperationResponseEncoder: Sendable {
    private let writeResponse: @Sendable (
        inout DatabaseWireWriter
    ) throws(DatabaseWireError) -> Void

    public init<Response: DatabaseWireValue>(_ response: Response) {
        self.writeResponse = {
            (writer: inout DatabaseWireWriter) throws(DatabaseWireError) in
            try response.encode(into: &writer)
        }
    }

    public init(
        encode: @escaping @Sendable (
            inout DatabaseWireWriter
        ) throws(DatabaseWireError) -> Void
    ) {
        self.writeResponse = encode
    }

    public func encode(
        into writer: inout DatabaseWireWriter
    ) throws(DatabaseWireError) {
        try writeResponse(&writer)
    }

}
