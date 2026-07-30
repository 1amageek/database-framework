import DatabaseKit
import StorageKit

/// Container-scoped provider for one index kind's maintenance runtime.
public protocol IndexMaintainerProvider: Sendable {
    var kindIdentifier: String { get }
    var runtimeRequirements: IndexRuntimeRequirements { get }
    var physicalEntryCapabilities: IndexPhysicalEntryCapabilities? { get }
    var supportsUniquenessConstraints: Bool { get }

    func makeIndexMaintainer<Item: Persistable>(
        index: Index,
        subspace: Subspace,
        idExpression: KeyExpression,
        configurations: [any IndexRuntimeConfiguration],
        wallClock: any WallClock
    ) throws -> any IndexMaintainer<Item>

    func makeIndexUniquenessMaintainer<Item: Persistable>(
        index: Index,
        subspace: Subspace,
        idExpression: KeyExpression,
        configurations: [any IndexRuntimeConfiguration]
    ) throws -> any IndexUniquenessMaintainer<Item>
}

/// Maintenance provider whose model type is fixed by one entity registration.
///
/// Use this contract when the index semantics require capabilities expressed by
/// the concrete model type. The entity definition erases the provider only
/// after `Model` equality has been established by the compiler.
public protocol EntityIndexMaintainerProvider: Sendable {
    associatedtype Model: Persistable

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
    ) throws -> any IndexMaintainer<Model>

    func makeIndexUniquenessMaintainer(
        index: Index,
        subspace: Subspace,
        idExpression: KeyExpression,
        configurations: [any IndexRuntimeConfiguration]
    ) throws -> any IndexUniquenessMaintainer<Model>
}

extension IndexMaintainerProvider {
    public var runtimeRequirements: IndexRuntimeRequirements { .none }
    public var physicalEntryCapabilities: IndexPhysicalEntryCapabilities? { nil }
    public var supportsUniquenessConstraints: Bool { false }

    public func makeIndexUniquenessMaintainer<Item: Persistable>(
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

extension EntityIndexMaintainerProvider {
    public var runtimeRequirements: IndexRuntimeRequirements { .none }
    public var physicalEntryCapabilities: IndexPhysicalEntryCapabilities? { nil }
    public var supportsUniquenessConstraints: Bool { false }

    public func makeIndexUniquenessMaintainer(
        index: Index,
        subspace: Subspace,
        idExpression: KeyExpression,
        configurations: [any IndexRuntimeConfiguration]
    ) throws -> any IndexUniquenessMaintainer<Model> {
        _ = index
        _ = subspace
        _ = idExpression
        _ = configurations
        throw IndexMaintainerProviderError.uniquenessNotSupported(
            kindIdentifier: kindIdentifier
        )
    }
}
