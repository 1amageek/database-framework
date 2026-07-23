import Core
import DatabaseEngine
import FullText
import StorageKit

/// Canonical runtime provider for full-text indexes.
public struct FullTextIndexMaintainerProvider: IndexMaintainerProvider {
    public let kindIdentifier = "fulltext"
    public let runtimeRequirements: IndexRuntimeRequirements = .entityAndPolymorphicReads

    public init() {}

    public func makeIndexMaintainer<Item: Persistable>(
        index: Index,
        subspace: Subspace,
        idExpression: KeyExpression,
        configurations: [any IndexConfiguration]
    ) throws -> any IndexMaintainer<Item> {
        let kind = try FullTextIndexKind<Item>(canonical: index.kind)
        return FullTextIndexMaintainer<Item>(
            index: index,
            tokenizer: kind.tokenizer,
            storePositions: kind.storePositions,
            ngramSize: kind.ngramSize,
            minTermLength: kind.minTermLength,
            subspace: subspace,
            idExpression: idExpression
        )
    }
}
