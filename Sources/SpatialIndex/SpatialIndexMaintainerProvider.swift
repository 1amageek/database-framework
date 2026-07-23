import Core
import DatabaseEngine
import Geospatial
import StorageKit

/// Canonical runtime provider for spatial indexes.
public struct SpatialIndexMaintainerProvider: IndexMaintainerProvider {
    public let kindIdentifier = "spatial"

    public init() {}

    public func makeIndexMaintainer<Item: Persistable>(
        index: Index,
        subspace: Subspace,
        idExpression: KeyExpression,
        configurations: [any IndexConfiguration]
    ) throws -> any IndexMaintainer<Item> {
        let kind = try SpatialIndexKind<Item>(canonical: index.kind)
        return SpatialIndexMaintainer<Item>(
            index: index,
            encoding: kind.encoding,
            level: kind.level,
            subspace: subspace,
            idExpression: idExpression
        )
    }
}
