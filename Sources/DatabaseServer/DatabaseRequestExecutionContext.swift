import DatabaseKit

/// Authenticated request state supplied by a hosting adapter.
///
/// DatabaseServer consumes the validated authorization value but does not
/// authenticate transport credentials.
public struct DatabaseRequestExecutionContext: Sendable, Hashable {
    public let authorization: AuthorizationContext

    public init(authorization: AuthorizationContext) {
        self.authorization = authorization
    }
}
