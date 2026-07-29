import DatabaseKit
import DatabaseEngine
import StorageKit

/// Canonical runtime provider for scalar indexes.
public struct ScalarIndexMaintainerProvider: IndexMaintainerProvider {
    public let kindIdentifier = "scalar"
    public let supportsUniquenessConstraints = true

    public var physicalEntryCapabilities: IndexPhysicalEntryCapabilities? {
        IndexPhysicalEntryCapabilities(
            decoder: ScalarIndexPhysicalEntryDecoder(),
            supportsItemReferenceValidation: true,
            supportsIndependentEntryRepair: true
        )
    }

    public init() {}

    public func makeIndexMaintainer<Item: Persistable>(
        index: Index,
        subspace: Subspace,
        idExpression: KeyExpression,
        configurations: [any IndexRuntimeConfiguration]
    ) throws -> any IndexMaintainer<Item> {
        _ = configurations
        try validateIndexDefinition(index)

        return ScalarIndexMaintainer<Item>(
            index: index,
            subspace: subspace,
            idExpression: idExpression
        )
    }

    public func makeIndexUniquenessMaintainer<Item: Persistable>(
        index: Index,
        subspace: Subspace,
        idExpression: KeyExpression,
        configurations: [any IndexRuntimeConfiguration]
    ) throws -> any IndexUniquenessMaintainer<Item> {
        _ = configurations
        try validateIndexDefinition(index)
        return ScalarIndexMaintainer<Item>(
            index: index,
            subspace: subspace,
            idExpression: idExpression
        )
    }

    private func validateIndexDefinition(_ index: Index) throws {
        guard index.kind.identifier == kindIdentifier else {
            throw IndexMaintainerProviderError.kindMismatch(
                registered: kindIdentifier,
                actual: index.kind.identifier
            )
        }
        guard !index.kind.fieldNames.isEmpty else {
            throw IndexMaintainerProviderError.invalidMetadata(
                kindIdentifier: kindIdentifier,
                key: "fieldNames"
            )
        }
        guard index.kind.metadata.isEmpty else {
            throw IndexMaintainerProviderError.invalidMetadata(
                kindIdentifier: kindIdentifier,
                key: "metadata"
            )
        }
    }
}
