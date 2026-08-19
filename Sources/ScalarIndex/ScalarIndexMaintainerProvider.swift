import DatabaseEngine
import DatabaseKit
import StorageKit

/// Canonical runtime provider for scalar indexes.
public struct ScalarIndexMaintainerProvider: IndexMaintainerProvider {
    public let indexType: IndexType = .ordered
    public let supportsUniquenessConstraints = true

    public var physicalEntryCapabilities: IndexPhysicalEntryCapabilities? {
        IndexPhysicalEntryCapabilities(
            decoder: ScalarIndexPhysicalEntryDecoder(),
            supportsItemReferenceValidation: true,
            supportsIndependentEntryRepair: true
        )
    }

    public init() {}

    public func makeIndexMaintainer<Item: PersistedEntityValue>(
        index: ResolvedIndex,
        subspace: Subspace,
        idExpression: KeyExpression,
        configurations: [any IndexRuntimeConfiguration],
        wallClock: any WallClock
    ) throws -> any IndexMaintainer<Item> {
        _ = configurations
        try validateIndexDefinition(index)

        return ScalarIndexMaintainer<Item>(
            index: index,
            subspace: subspace,
            idExpression: idExpression
        )
    }

    public func makeIndexUniquenessMaintainer<Item: PersistedEntityValue>(
        index: ResolvedIndex,
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

    private func validateIndexDefinition(_ index: ResolvedIndex) throws {
        guard case .ordered = index.definition else {
            throw IndexMaintainerProviderError.typeMismatch(
                registered: indexType,
                actual: index.type
            )
        }
    }
}
