import DatabaseKit

/// Immutable capabilities used to validate one index maintenance provider.
///
/// Executable provider behavior remains bound to an
/// `EntityRuntimeDefinition<Model>`. Runtime-wide validation stores only this
/// concrete description, so model-generic provider methods are never erased
/// into an existential value.
public struct IndexMaintainerProviderDescriptor: Sendable {
    public let indexType: IndexType
    public let runtimeRequirements: IndexRuntimeRequirements
    public let physicalEntryCapabilities: IndexPhysicalEntryCapabilities?
    public let supportsUniquenessConstraints: Bool
    private let resolvePhysicalLayout:
        @Sendable (
            ResolvedIndex,
            [any IndexRuntimeConfiguration]
        ) throws -> IndexPhysicalLayout

    public init<Provider: IndexMaintainerProvider>(
        describing provider: Provider
    ) {
        self.indexType = provider.indexType
        self.runtimeRequirements = provider.runtimeRequirements
        self.physicalEntryCapabilities = provider.physicalEntryCapabilities
        self.supportsUniquenessConstraints = provider.supportsUniquenessConstraints
        self.resolvePhysicalLayout = { index, configurations in
            try provider.physicalLayout(
                for: index,
                configurations: configurations
            )
        }
    }

    package func physicalLayout(
        for index: ResolvedIndex,
        configurations: [any IndexRuntimeConfiguration]
    ) throws -> IndexPhysicalLayout {
        try resolvePhysicalLayout(index, configurations)
    }
}
