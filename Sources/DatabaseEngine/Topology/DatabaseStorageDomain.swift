import DatabaseKit
import StorageKit

/// One independently transacted storage domain owned by a database container.
public struct DatabaseStorageDomain: Sendable {
    /// Stable logical identity used by placement and consistency records.
    public struct ID: Sendable, Hashable, Comparable, CustomStringConvertible {
        public let value: String

        public init(_ value: String) throws(BaseIdentifierError) {
            _ = try Base.ID(value)
            self.value = value
        }

        public static func < (lhs: Self, rhs: Self) -> Bool {
            lhs.value.utf8.lexicographicallyPrecedes(rhs.value.utf8)
        }

        public var description: String { value }
    }

    public let id: ID
    public let namespacePath: [String]
    public let storageEngine: any StorageEngine

    public init(
        id: ID,
        namespacePath: [String],
        storageEngine: any StorageEngine
    ) throws(DatabaseStorageTopologyError) {
        guard !namespacePath.isEmpty else {
            throw .emptyDomainNamespace(domainID: id)
        }
        for component in namespacePath {
            guard !component.isEmpty else {
                throw .emptyNamespaceComponent(domainID: id)
            }
        }
        self.id = id
        self.namespacePath = namespacePath
        self.storageEngine = storageEngine
    }
}
