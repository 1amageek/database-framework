import DatabaseKit
import DatabaseEngine
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
        configurations: [any IndexRuntimeConfiguration]
    ) throws -> any IndexMaintainer<Item> {
        let configuration = try FullTextIndexConfiguration(
            metadata: index.kind
        )
        return FullTextIndexMaintainer<Item>(
            index: index,
            tokenizer: configuration.tokenizer,
            storePositions: configuration.storePositions,
            ngramSize: configuration.ngramSize,
            minTermLength: configuration.minTermLength,
            subspace: subspace,
            idExpression: idExpression
        )
    }
}
