import DatabaseEngine
import DatabaseKit
import StorageKit

public struct AutocompleteIndexMaintainerProvider: IndexMaintainerProvider {
    public let kindIdentifier = "autocomplete"

    public init() {}

    public func makeIndexMaintainer<Item: Persistable>(
        index: Index,
        subspace: Subspace,
        idExpression: KeyExpression,
        configurations: [any IndexRuntimeConfiguration],
        wallClock: any WallClock
    ) throws -> any IndexMaintainer<Item> {
        let configuration = try AutocompleteIndexConfiguration(
            metadata: index.kind
        )
        return AutocompleteMaintainer<Item>(
            index: index,
            subspace: subspace,
            idExpression: idExpression,
            fields: index.kind.fields.map {
                FieldIdentity(name: $0.name, number: $0.number)
            },
            minPrefixLength: configuration.minPrefixLength,
            maxPrefixLength: configuration.maxPrefixLength
        )
    }
}
