import DatabaseKit
import StorageKit

/// Immutable index maintenance provider registry for one runtime generation.
public struct IndexMaintainerProviderRegistry: Sendable {
    private let descriptors: [IndexType: IndexMaintainerProviderDescriptor]

    public init(
        descriptors: [IndexMaintainerProviderDescriptor]
    ) throws(DatabaseRuntimeConfigurationError) {
        var mapped: [IndexType: IndexMaintainerProviderDescriptor] = [:]
        mapped.reserveCapacity(descriptors.count)
        for descriptor in descriptors {
            guard mapped.updateValue(
                descriptor,
                forKey: descriptor.indexType
                ) == nil else {
                throw .duplicateIndexMaintainerProvider(
                    descriptor.indexType
                )
            }
        }
        self.descriptors = mapped
    }

    public func contains(indexType: IndexType) -> Bool {
        descriptors[indexType] != nil
    }

    public func runtimeRequirements(
        for indexType: IndexType
    ) -> IndexRuntimeRequirements? {
        descriptors[indexType]?.runtimeRequirements
    }

    public func physicalEntryCapabilities(
        for indexType: IndexType
    ) -> IndexPhysicalEntryCapabilities? {
        descriptors[indexType]?.physicalEntryCapabilities
    }

    public func supportsUniquenessConstraints(
        for indexType: IndexType
    ) -> Bool? {
        descriptors[indexType]?.supportsUniquenessConstraints
    }

    package func physicalLayout(
        for index: ResolvedIndex,
        configurations: [any IndexRuntimeConfiguration]
    ) throws -> IndexPhysicalLayout {
        guard let descriptor = descriptors[index.type] else {
            throw IndexMaintainerProviderRegistryError.providerNotRegistered(
                indexType: index.type,
                indexName: index.name
            )
        }
        return try descriptor.physicalLayout(
            for: index,
            configurations: configurations
        )
    }

}
