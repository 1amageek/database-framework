import DatabaseKit

/// Authenticated request state supplied by a hosting adapter.
///
/// DatabaseOperations consumes the validated authorization value but does not
/// authenticate transport credentials.
public struct DatabaseRequestExecutionContext: Sendable, Hashable {
    public let authorization: AuthorizationContext
    public let jobAuthorizationReference: DatabaseJobAuthorizationReference?

    public init(
        authorization: AuthorizationContext,
        jobAuthorizationReference: DatabaseJobAuthorizationReference? = nil
    ) {
        self.authorization = authorization
        self.jobAuthorizationReference = jobAuthorizationReference
    }
}
