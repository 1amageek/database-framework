/// Errors raised when an index is executed without its registered provider.
public enum IndexMaintainerProviderRegistryError: Error, Sendable, Equatable {
    case providerNotRegistered(kindIdentifier: String, indexName: String)
}
