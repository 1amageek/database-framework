import Core
import DatabaseEngine
import Graph
import StorageKit

/// Canonical runtime provider for property graph indexes.
public struct GraphIndexMaintainerProvider: IndexMaintainerProvider {
    public let kindIdentifier = "graph"
    public let runtimeRequirements: IndexRuntimeRequirements = .graphQueries

    public init() {}

    public func makeIndexMaintainer<Item: Persistable>(
        index: Index,
        subspace: Subspace,
        idExpression: KeyExpression,
        configurations: [any IndexConfiguration]
    ) throws -> any IndexMaintainer<Item> {
        let kind = try GraphIndexKind<Item>(canonical: index.kind)
        return GraphIndexMaintainer<Item>(
            index: index,
            subspace: subspace,
            idExpression: idExpression,
            fromField: kind.fromField,
            edgeField: kind.edgeField,
            toField: kind.toField,
            graphField: kind.graphField,
            strategy: kind.strategy
        )
    }
}
