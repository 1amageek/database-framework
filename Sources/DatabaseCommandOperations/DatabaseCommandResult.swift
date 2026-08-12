import DatabaseOperationCore
public import DatabaseTypes

public struct DatabaseCommandResult: Sendable, Hashable {
    public let output: FieldValue
    public let continuation: ByteString?

    public init(
        output: FieldValue,
        continuation: ByteString? = nil
    ) {
        self.output = output
        self.continuation = continuation
    }
}
