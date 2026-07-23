public enum IndexVerificationError: Error, Sendable, Equatable {
    case expectedKeysUnsupported(maintainerType: String)
}
