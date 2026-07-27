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
        configurations: [any IndexRuntimeConfiguration]
    ) throws -> any IndexMaintainer<Item>
}

extension IndexMaintainerProvider {
    public var runtimeRequirements: IndexRuntimeRequirements { .none }
    public var physicalEntryCapabilities: IndexPhysicalEntryCapabilities? { nil }
    public var supportsUniquenessConstraints: Bool { false }
}
