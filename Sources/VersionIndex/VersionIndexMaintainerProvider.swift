import DatabaseKit
import DatabaseEngine
import StorageKit

/// Canonical runtime provider for version-history indexes.
public struct VersionIndexMaintainerProvider: IndexMaintainerProvider {
    public let kindIdentifier = "version"
    public let runtimeRequirements: IndexRuntimeRequirements = .entityAndPolymorphicReads

    public init() {}

    public func makeIndexMaintainer<Item: Persistable>(
        index: Index,
        subspace: Subspace,
        idExpression: KeyExpression,
        configurations: [any IndexRuntimeConfiguration]
    ) throws -> any IndexMaintainer<Item> {
        let definition = try IndexDefinition(metadata: index.kind)
        guard case .version(let strategy) = definition else {
            throw IndexMaintainerProviderError.invalidMetadata(
                kindIdentifier: kindIdentifier,
                key: "identifier"
            )
        }

        return VersionIndexMaintainer<Item>(
            index: index,
            strategy: strategy,
            subspace: subspace,
            idExpression: idExpression
        )
    }
}
