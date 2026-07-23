import DatabaseValue
import DatabaseWire

/// A typed authorization failure returned before operation dispatch.
public struct DatabaseOperationAdmissionDenial: Sendable, Hashable {
    public let code: String
    public let message: String
    public let retryability: DatabaseRetryability
    public let details: [DatabaseObjectField]

    public init(
        code: String,
        message: String,
        retryability: DatabaseRetryability,
        details: [DatabaseObjectField] = []
    ) {
        self.code = code
        self.message = message
        self.retryability = retryability
        self.details = details
    }
}
