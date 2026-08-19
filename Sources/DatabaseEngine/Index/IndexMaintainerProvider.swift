import DatabaseKit
import StorageKit

/// Runtime-generation provider for one index type's maintenance behavior.
public protocol IndexMaintainerProvider: Sendable {
    var indexType: IndexType { get }
    var runtimeRequirements: IndexRuntimeRequirements { get }
    var physicalEntryCapabilities: IndexPhysicalEntryCapabilities? { get }
    var supportsUniquenessConstraints: Bool { get }

    func physicalLayout(
        for index: ResolvedIndex,
        configurations: [any IndexRuntimeConfiguration]
    ) throws -> IndexPhysicalLayout

    func makeIndexMaintainer<Item: PersistedEntityValue>(
        index: ResolvedIndex,
        subspace: Subspace,
        idExpression: KeyExpression,
        configurations: [any IndexRuntimeConfiguration],
        wallClock: any WallClock
    ) throws -> any IndexMaintainer<Item>

    func makeIndexUniquenessMaintainer<Item: PersistedEntityValue>(
        index: ResolvedIndex,
        subspace: Subspace,
        idExpression: KeyExpression,
        configurations: [any IndexRuntimeConfiguration]
    ) throws -> any IndexUniquenessMaintainer<Item>
}

/// Maintenance provider configured from one canonical entity declaration.
///
/// Entity-specific metadata is captured when the provider is created. Both
/// compiled and schema-driven runtimes therefore maintain the same
/// `PersistedModel` representation without reopening a concrete Swift type.
public protocol CanonicalEntityIndexMaintainerProvider: Sendable {
    var indexType: IndexType { get }
    var runtimeRequirements: IndexRuntimeRequirements { get }
    var physicalEntryCapabilities: IndexPhysicalEntryCapabilities? { get }
    var supportsUniquenessConstraints: Bool { get }

    func physicalLayout(
        for index: ResolvedIndex,
        configurations: [any IndexRuntimeConfiguration]
    ) throws -> IndexPhysicalLayout

    func makeIndexMaintainer(
        index: ResolvedIndex,
        subspace: Subspace,
        idExpression: KeyExpression,
        configurations: [any IndexRuntimeConfiguration],
        wallClock: any WallClock
    ) throws -> any IndexMaintainer<PersistedModel>

    func makeIndexUniquenessMaintainer(
        index: ResolvedIndex,
        subspace: Subspace,
        idExpression: KeyExpression,
        configurations: [any IndexRuntimeConfiguration]
    ) throws -> any IndexUniquenessMaintainer<PersistedModel>
}

extension IndexMaintainerProvider {
    public var runtimeRequirements: IndexRuntimeRequirements { .none }
    public var physicalEntryCapabilities: IndexPhysicalEntryCapabilities? { nil }
    public var supportsUniquenessConstraints: Bool { false }

    public func physicalLayout(
        for index: ResolvedIndex,
        configurations: [any IndexRuntimeConfiguration]
    ) throws -> IndexPhysicalLayout {
        guard configurations.isEmpty else {
            throw IndexMaintainerProviderError.unhandledRuntimeConfiguration(
                indexType: indexType,
                indexName: index.name
            )
        }
        return try IndexPhysicalLayout(name: "standard", revision: 1)
    }

    public func makeIndexUniquenessMaintainer<Item: PersistedEntityValue>(
        index: ResolvedIndex,
        subspace: Subspace,
        idExpression: KeyExpression,
        configurations: [any IndexRuntimeConfiguration]
    ) throws -> any IndexUniquenessMaintainer<Item> {
        _ = index
        _ = subspace
        _ = idExpression
        _ = configurations
        throw IndexMaintainerProviderError.uniquenessNotSupported(
            indexType: indexType
        )
    }
}

extension CanonicalEntityIndexMaintainerProvider {
    public var runtimeRequirements: IndexRuntimeRequirements { .none }
    public var physicalEntryCapabilities: IndexPhysicalEntryCapabilities? { nil }
    public var supportsUniquenessConstraints: Bool { false }

    public func physicalLayout(
        for index: ResolvedIndex,
        configurations: [any IndexRuntimeConfiguration]
    ) throws -> IndexPhysicalLayout {
        guard configurations.isEmpty else {
            throw IndexMaintainerProviderError.unhandledRuntimeConfiguration(
                indexType: indexType,
                indexName: index.name
            )
        }
        return try IndexPhysicalLayout(name: "standard", revision: 1)
    }

    public func makeIndexUniquenessMaintainer(
        index: ResolvedIndex,
        subspace: Subspace,
        idExpression: KeyExpression,
        configurations: [any IndexRuntimeConfiguration]
    ) throws -> any IndexUniquenessMaintainer<PersistedModel> {
        _ = index
        _ = subspace
        _ = idExpression
        _ = configurations
        throw IndexMaintainerProviderError.uniquenessNotSupported(
            indexType: indexType
        )
    }
}
