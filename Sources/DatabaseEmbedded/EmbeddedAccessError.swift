/// Request metadata with server-owned meaning cannot be silently ignored locally.
public enum EmbeddedAccessError: Error, Sendable, Equatable {
    case unsupportedRequestMetadata
}
