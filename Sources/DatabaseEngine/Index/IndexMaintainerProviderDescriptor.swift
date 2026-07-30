/// Immutable capabilities used to validate one index maintenance provider.
///
/// Executable provider behavior remains bound to an
/// `EntityRuntimeDefinition<Model>`. Runtime-wide validation stores only this
/// concrete description, so model-generic provider methods are never erased
/// into an existential value.
public struct IndexMaintainerProviderDescriptor: Sendable {
    public let kindIdentifier: String
    public let runtimeRequirements: IndexRuntimeRequirements
    public let physicalEntryCapabilities: IndexPhysicalEntryCapabilities?
    public let supportsUniquenessConstraints: Bool

    public init<Provider: IndexMaintainerProvider>(
        describing provider: Provider
    ) {
        self.kindIdentifier = provider.kindIdentifier
        self.runtimeRequirements = provider.runtimeRequirements
        self.physicalEntryCapabilities = provider.physicalEntryCapabilities
        self.supportsUniquenessConstraints = provider.supportsUniquenessConstraints
    }
}
