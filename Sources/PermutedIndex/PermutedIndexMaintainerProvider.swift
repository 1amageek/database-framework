import DatabaseKit
import DatabaseEngine
import StorageKit

/// Canonical runtime provider for permuted indexes.
public struct PermutedIndexMaintainerProvider: IndexMaintainerProvider {
    public let kindIdentifier = "permuted"
    public let runtimeRequirements: IndexRuntimeRequirements = .entityAndPolymorphicReads

    public init() {}

    public func makeIndexMaintainer<Item: PersistedEntityValue>(
        index: Index,
        subspace: Subspace,
        idExpression: KeyExpression,
        configurations: [any IndexRuntimeConfiguration],
        wallClock: any WallClock
    ) throws -> any IndexMaintainer<Item> {
        guard index.kind.identifier == kindIdentifier else {
            throw IndexMaintainerProviderError.kindMismatch(
                registered: kindIdentifier,
                actual: index.kind.identifier
            )
        }
        let definition = try IndexDefinition(metadata: index.kind)
        guard case .permuted(let pattern) = definition else {
            throw IndexMaintainerProviderError.invalidMetadata(
                kindIdentifier: kindIdentifier,
                key: "permutation"
            )
        }
        let permutation = try pattern.resolve()
        return PermutedIndexMaintainer<Item>(
            index: index,
            permutation: permutation,
            subspace: subspace,
            idExpression: idExpression
        )
    }
}
