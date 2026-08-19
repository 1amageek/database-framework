import DatabaseEngine
import DatabaseKit
import StorageKit

public struct AutocompleteIndexMaintainerProvider: IndexMaintainerProvider {
    public let indexType: IndexType = .text(.autocomplete)

    public init() {}

    public func makeIndexMaintainer<Item: PersistedEntityValue>(
        index: ResolvedIndex,
        subspace: Subspace,
        idExpression: KeyExpression,
        configurations: [any IndexRuntimeConfiguration],
        wallClock: any WallClock
    ) throws -> any IndexMaintainer<Item> {
        let configuration = try AutocompleteIndexConfiguration(
            definition: index.definition
        )
        return AutocompleteMaintainer<Item>(
            index: index,
            subspace: subspace,
            idExpression: idExpression,
            fields: index.descriptor.fieldIdentities,
            minPrefixLength: configuration.minPrefixLength,
            maxPrefixLength: configuration.maxPrefixLength
        )
    }
}
