import DatabaseEngine
import DatabaseKit
import StorageKit

/// Canonical runtime provider for count-not-null indexes.
public struct CountNotNullIndexMaintainerProvider: IndexMaintainerProvider {
    public let indexType: IndexType = .aggregate(.nonNullCount)

    public init() {}

    public func makeIndexMaintainer<Item: PersistedEntityValue>(
        index: ResolvedIndex,
        subspace: Subspace,
        idExpression: KeyExpression,
        configurations: [any IndexRuntimeConfiguration],
        wallClock: any WallClock
    ) throws -> any IndexMaintainer<Item> {
        let definition = try index.aggregateDefinition(.nonNullCount)
        guard let valueFieldName = definition.value?.name else {
            throw IndexMaintainerProviderError.invalidDefinition(
                indexType: indexType,
                reason: "Non-null count requires a value field"
            )
        }
        return CountNotNullIndexMaintainer<Item>(
            index: index,
            subspace: subspace,
            idExpression: idExpression,
            groupByFieldNames: definition.groupBy.map { $0.field.name },
            valueFieldName: valueFieldName
        )
    }
}
