import DatabaseKit
import StorageKit

/// Immutable, container-scoped index maintenance provider registry.
public struct IndexMaintainerProviderRegistry: Sendable {
    private let providers: [String: any IndexMaintainerProvider]

    public init(
        providers: [any IndexMaintainerProvider]
    ) throws(DatabaseRuntimeConfigurationError) {
        var mapped: [String: any IndexMaintainerProvider] = [:]
        mapped.reserveCapacity(providers.count)
        for provider in providers {
            guard mapped.updateValue(
                provider,
                forKey: provider.kindIdentifier
            ) == nil else {
                throw .duplicateIndexMaintainerProvider(
                    provider.kindIdentifier
                )
            }
        }
        self.providers = mapped
    }

    public func contains(kindIdentifier: String) -> Bool {
        providers[kindIdentifier] != nil
    }

    public func runtimeRequirements(
        for kindIdentifier: String
    ) -> IndexRuntimeRequirements? {
        providers[kindIdentifier]?.runtimeRequirements
    }

    public func physicalEntryCapabilities(
        for kindIdentifier: String
    ) -> IndexPhysicalEntryCapabilities? {
        providers[kindIdentifier]?.physicalEntryCapabilities
    }

    public func supportsUniquenessConstraints(
        for kindIdentifier: String
    ) -> Bool? {
        providers[kindIdentifier]?.supportsUniquenessConstraints
    }

    public func makeIndexMaintainer<Item: Persistable>(
        index: Index,
        subspace: Subspace,
        idExpression: KeyExpression,
        configurations: [any IndexRuntimeConfiguration]
    ) throws -> any IndexMaintainer<Item> {
        let kindIdentifier = index.kind.identifier
        guard let provider = providers[kindIdentifier] else {
            throw IndexMaintainerProviderRegistryError.providerNotRegistered(
                kindIdentifier: kindIdentifier,
                indexName: index.name
            )
        }
        return try provider.makeIndexMaintainer(
            index: index,
            subspace: subspace,
            idExpression: idExpression,
            configurations: configurations
        )
    }

    public func makeIndexUniquenessMaintainer<Item: Persistable>(
        index: Index,
        subspace: Subspace,
        idExpression: KeyExpression,
        configurations: [any IndexRuntimeConfiguration]
    ) throws -> any IndexUniquenessMaintainer<Item> {
        let kindIdentifier = index.kind.identifier
        guard let provider = providers[kindIdentifier] else {
            throw IndexMaintainerProviderRegistryError.providerNotRegistered(
                kindIdentifier: kindIdentifier,
                indexName: index.name
            )
        }
        return try provider.makeIndexUniquenessMaintainer(
            index: index,
            subspace: subspace,
            idExpression: idExpression,
            configurations: configurations
        )
    }
}
