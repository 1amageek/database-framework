import DatabaseEngine
import DatabaseKit
import StorageKit

/// Canonical runtime provider for property graph indexes.
public struct GraphIndexMaintainerProvider: IndexMaintainerProvider {
    public let indexType: IndexType = .graph(.property)
    public let runtimeRequirements: IndexRuntimeRequirements = .graphQueries

    public init() {}

    public func makeIndexMaintainer<Item: PersistedEntityValue>(
        index: ResolvedIndex,
        subspace: Subspace,
        idExpression: KeyExpression,
        configurations: [any IndexRuntimeConfiguration],
        wallClock: any WallClock
    ) throws -> any IndexMaintainer<Item> {
        guard case .graph(let definition, _) = index.definition,
            case .property = definition
        else {
            throw IndexMaintainerProviderError.typeMismatch(
                registered: indexType,
                actual: index.type
            )
        }
        return try GraphIndexMaintainer<Item>(
            index: index,
            subspace: subspace,
            idExpression: idExpression,
            definition: definition
        )
    }
}
