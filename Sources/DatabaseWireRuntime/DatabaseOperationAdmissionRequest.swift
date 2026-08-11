import DatabaseKit
@_spi(DatabaseWireRuntime) import DatabaseWire

/// State-independent request metadata evaluated before extensible dispatch.
public struct DatabaseOperationAdmissionRequest: Sendable, Hashable {
    public let requestID: UInt64
    public let operation: DatabaseOperationIdentifier
    public let target: DatabaseOperationTarget
    public let metadata: OperationRequestMetadata
    public let authorization: AuthorizationContext

    public init(
        requestID: UInt64,
        operation: DatabaseOperationIdentifier,
        target: DatabaseOperationTarget,
        metadata: OperationRequestMetadata,
        authorization: AuthorizationContext
    ) {
        self.requestID = requestID
        self.operation = operation
        self.target = target
        self.metadata = metadata
        self.authorization = authorization
    }
}
