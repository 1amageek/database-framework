/// Errors raised while resolving a registered index maintenance provider.
public enum IndexMaintainerProviderError: Error, Sendable, Equatable {
    case kindMismatch(registered: String, actual: String)
    case invalidMetadata(kindIdentifier: String, key: String)
    case uniquenessNotSupported(kindIdentifier: String)
}
