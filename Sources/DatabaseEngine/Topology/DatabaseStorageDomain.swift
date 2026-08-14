#if DATABASE_MULTIPLE_BASES
import StorageKit

/// One independently transacted storage domain owned by a database container.
public struct DatabaseStorageDomain: Sendable {
    /// Stable logical identity used by placement and consistency records.
    public struct ID: Sendable, Hashable, Comparable, CustomStringConvertible {
        public let value: String

        public init(
            _ value: String
        ) throws(DatabaseStorageTopologyError) {
            let bytes = value.utf8
            guard !bytes.isEmpty, bytes.count <= 128 else {
                throw .invalidDomainID(value)
            }
            var segmentStart = true
            var previousWasHyphen = false
            for byte in bytes {
                switch byte {
                case 0x61...0x7a, 0x30...0x39:
                    segmentStart = false
                    previousWasHyphen = false
                case 0x2d:
                    guard !segmentStart else {
                        throw .invalidDomainID(value)
                    }
                    previousWasHyphen = true
                case 0x2e:
                    guard !segmentStart, !previousWasHyphen else {
                        throw .invalidDomainID(value)
                    }
                    segmentStart = true
                    previousWasHyphen = false
                default:
                    throw .invalidDomainID(value)
                }
            }
            guard !segmentStart, !previousWasHyphen else {
                throw .invalidDomainID(value)
            }
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
#endif
