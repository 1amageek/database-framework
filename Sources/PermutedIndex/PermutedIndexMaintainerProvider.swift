import Core
import DatabaseEngine
import Permuted
import StorageKit

/// Canonical runtime provider for permuted indexes.
public struct PermutedIndexMaintainerProvider: IndexMaintainerProvider {
    public let kindIdentifier = "permuted"
    public let runtimeRequirements: IndexRuntimeRequirements = .entityAndPolymorphicReads

    public init() {}

    public func makeIndexMaintainer<Item: Persistable>(
        index: Index,
        subspace: Subspace,
        idExpression: KeyExpression,
        configurations: [any IndexConfiguration]
    ) throws -> any IndexMaintainer<Item> {
        let kind = try PermutedIndexKind<Item>(canonical: index.kind)
        return PermutedIndexMaintainer<Item>(
            index: index,
            permutation: kind.permutation,
            subspace: subspace,
            idExpression: idExpression
        )
    }
}
