import Core
import DatabaseEngine
import StorageKit

/// Canonical runtime provider for version-history indexes.
public struct VersionIndexMaintainerProvider: IndexMaintainerProvider {
    public let kindIdentifier = "version"
    public let runtimeRequirements: IndexRuntimeRequirements = .entityAndPolymorphicReads

    public init() {}

    public func makeIndexMaintainer<Item: Persistable>(
        index: Index,
        subspace: Subspace,
        idExpression: KeyExpression,
        configurations: [any IndexConfiguration]
    ) throws -> any IndexMaintainer<Item> {
        try index.kind.validateIdentity(
            identifier: kindIdentifier,
            subspaceStructure: .hierarchical
        )
        try index.kind.validateFieldCount(1)

        let strategyName = try index.kind.requireString("strategy")
        let strategy: VersionHistoryStrategy
        switch strategyName {
        case "keepAll":
            try index.kind.validateMetadataKeys(required: ["strategy"])
            strategy = .keepAll
        case "keepLast":
            try index.kind.validateMetadataKeys(
                required: ["strategy", "strategyCount"]
            )
            let count = try index.kind.requireInt("strategyCount")
            guard count > 0 else {
                throw IndexMaintainerProviderError.invalidMetadata(
                    kindIdentifier: kindIdentifier,
                    key: "strategyCount"
                )
            }
            strategy = .keepLast(count)
        case "keepForDuration":
            try index.kind.validateMetadataKeys(
                required: ["strategy", "strategyDurationSeconds"]
            )
            let duration = try index.kind.requireDouble(
                "strategyDurationSeconds"
            )
            guard duration.isFinite, duration > 0 else {
                throw IndexMaintainerProviderError.invalidMetadata(
                    kindIdentifier: kindIdentifier,
                    key: "strategyDurationSeconds"
                )
            }
            strategy = .keepForDuration(duration)
        default:
            throw IndexMaintainerProviderError.invalidMetadata(
                kindIdentifier: kindIdentifier,
                key: "strategy"
            )
        }

        return VersionIndexMaintainer<Item>(
            index: index,
            strategy: strategy,
            subspace: subspace,
            idExpression: idExpression
        )
    }
}
