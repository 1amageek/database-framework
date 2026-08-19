import DatabaseEngine
import DatabaseKit
import StorageKit

/// Canonical runtime provider for full-text indexes.
public struct FullTextIndexMaintainerProvider: IndexMaintainerProvider {
    public let indexType: IndexType = .text(.fullText)
    public let runtimeRequirements: IndexRuntimeRequirements = .entityAndPolymorphicReads

    public init() {}

    public func makeIndexMaintainer<Item: PersistedEntityValue>(
        index: ResolvedIndex,
        subspace: Subspace,
        idExpression: KeyExpression,
        configurations: [any IndexRuntimeConfiguration],
        wallClock: any WallClock
    ) throws -> any IndexMaintainer<Item> {
        let configuration = try FullTextIndexConfiguration(
            definition: index.definition
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
