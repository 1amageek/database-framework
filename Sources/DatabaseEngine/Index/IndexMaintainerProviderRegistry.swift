import DatabaseKit
import StorageKit

/// Immutable, container-scoped index maintenance provider registry.
public struct IndexMaintainerProviderRegistry: Sendable {
    private let descriptors: [String: IndexMaintainerProviderDescriptor]

    public init(
        descriptors: [IndexMaintainerProviderDescriptor]
    ) throws(DatabaseRuntimeConfigurationError) {
        var mapped: [String: IndexMaintainerProviderDescriptor] = [:]
        mapped.reserveCapacity(descriptors.count)
        for descriptor in descriptors {
            guard mapped.updateValue(
                descriptor,
                forKey: descriptor.kindIdentifier
            ) == nil else {
                throw .duplicateIndexMaintainerProvider(
                    descriptor.kindIdentifier
                )
            }
        }
        self.descriptors = mapped
    }

    public func contains(kindIdentifier: String) -> Bool {
        descriptors[kindIdentifier] != nil
    }

    public func runtimeRequirements(
        for kindIdentifier: String
    ) -> IndexRuntimeRequirements? {
        descriptors[kindIdentifier]?.runtimeRequirements
    }

    public func physicalEntryCapabilities(
        for kindIdentifier: String
    ) -> IndexPhysicalEntryCapabilities? {
        descriptors[kindIdentifier]?.physicalEntryCapabilities
    }

    public func supportsUniquenessConstraints(
        for kindIdentifier: String
    ) -> Bool? {
        descriptors[kindIdentifier]?.supportsUniquenessConstraints
    }

}
