import DatabaseKit
import StorageKit

/// Container-scoped provider for one index kind's maintenance runtime.
public protocol IndexMaintainerProvider: Sendable {
    var kindIdentifier: String { get }
    var runtimeRequirements: IndexRuntimeRequirements { get }
    var physicalEntryCapabilities: IndexPhysicalEntryCapabilities? { get }
    var supportsUniquenessConstraints: Bool { get }

    func makeIndexMaintainer<Item: PersistedEntityValue>(
        index: Index,
        subspace: Subspace,
        idExpression: KeyExpression,
        configurations: [any IndexRuntimeConfiguration],
        wallClock: any WallClock
    ) throws -> any IndexMaintainer<Item>

    func makeIndexUniquenessMaintainer<Item: PersistedEntityValue>(
        index: Index,
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
    var kindIdentifier: String { get }
    var runtimeRequirements: IndexRuntimeRequirements { get }
    var physicalEntryCapabilities: IndexPhysicalEntryCapabilities? { get }
    var supportsUniquenessConstraints: Bool { get }

    func makeIndexMaintainer(
        index: Index,
        subspace: Subspace,
        idExpression: KeyExpression,
        configurations: [any IndexRuntimeConfiguration],
        wallClock: any WallClock
    ) throws -> any IndexMaintainer<PersistedModel>

    func makeIndexUniquenessMaintainer(
        index: Index,
        subspace: Subspace,
        idExpression: KeyExpression,
        configurations: [any IndexRuntimeConfiguration]
    ) throws -> any IndexUniquenessMaintainer<PersistedModel>
}

extension IndexMaintainerProvider {
    public var runtimeRequirements: IndexRuntimeRequirements { .none }
    public var physicalEntryCapabilities: IndexPhysicalEntryCapabilities? { nil }
    public var supportsUniquenessConstraints: Bool { false }

    public func makeIndexUniquenessMaintainer<Item: PersistedEntityValue>(
        index: Index,
        subspace: Subspace,
        idExpression: KeyExpression,
        configurations: [any IndexRuntimeConfiguration]
    ) throws -> any IndexUniquenessMaintainer<Item> {
        _ = index
        _ = subspace
        _ = idExpression
        _ = configurations
        throw IndexMaintainerProviderError.uniquenessNotSupported(
            kindIdentifier: kindIdentifier
        )
    }
}

extension CanonicalEntityIndexMaintainerProvider {
    public var runtimeRequirements: IndexRuntimeRequirements { .none }
    public var physicalEntryCapabilities: IndexPhysicalEntryCapabilities? { nil }
    public var supportsUniquenessConstraints: Bool { false }

    public func makeIndexUniquenessMaintainer(
        index: Index,
        subspace: Subspace,
        idExpression: KeyExpression,
        configurations: [any IndexRuntimeConfiguration]
    ) throws -> any IndexUniquenessMaintainer<PersistedModel> {
        _ = index
        _ = subspace
        _ = idExpression
        _ = configurations
        throw IndexMaintainerProviderError.uniquenessNotSupported(
            kindIdentifier: kindIdentifier
        )
    }
}
