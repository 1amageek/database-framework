public enum AuthorizationPolicyHandlerError: Error, Sendable, Equatable {
    case modelTypeMismatch(expected: String, actual: String)
}
