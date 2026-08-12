import DatabaseTypes
@_spi(DatabaseOperations) import DatabaseWire

/// A typed authorization failure returned before operation dispatch.
public struct DatabaseOperationAdmissionDenial: Sendable, Hashable {
    public let code: String
    public let message: String
    public let retryability: OperationRetryability
    public let details: FieldObject

    public init(
        code: String,
        message: String,
        retryability: OperationRetryability,
        details: FieldObject = FieldObject()
    ) {
        self.code = code
        self.message = message
        self.retryability = retryability
        self.details = details
    }
}
