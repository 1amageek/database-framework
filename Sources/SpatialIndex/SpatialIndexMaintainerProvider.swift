import DatabaseKit
import DatabaseEngine
import StorageKit

/// Canonical runtime provider for spatial indexes.
public struct SpatialIndexMaintainerProvider: IndexMaintainerProvider {
    public let kindIdentifier = "spatial"

    public init() {}

    public func makeIndexMaintainer<Item: PersistedEntityValue>(
        index: Index,
        subspace: Subspace,
        idExpression: KeyExpression,
        configurations: [any IndexRuntimeConfiguration],
        wallClock: any WallClock
    ) throws -> any IndexMaintainer<Item> {
        guard index.kind.identifier == kindIdentifier else {
            throw IndexMaintainerProviderError.kindMismatch(
                registered: kindIdentifier,
                actual: index.kind.identifier
            )
        }
        let definition = try IndexDefinition(metadata: index.kind)
        guard case .spatial(let encoding, let level) = definition else {
            throw IndexMaintainerProviderError.invalidMetadata(
                kindIdentifier: kindIdentifier,
                key: "encoding"
            )
        }
        return SpatialIndexMaintainer<Item>(
            index: index,
            encoding: encoding,
            level: level,
            subspace: subspace,
            idExpression: idExpression
        )
    }
}
