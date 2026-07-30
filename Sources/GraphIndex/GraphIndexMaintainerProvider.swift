import DatabaseKit
import DatabaseEngine
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
        configurations: [any IndexRuntimeConfiguration],
        wallClock: any WallClock
    ) throws -> any IndexMaintainer<Item> {
        let metadata = try PropertyGraphIndexMetadata(canonical: index.kind)
        return try GraphIndexMaintainer<Item>(
            index: index,
            subspace: subspace,
            idExpression: idExpression,
            metadata: metadata
        )
    }
}
