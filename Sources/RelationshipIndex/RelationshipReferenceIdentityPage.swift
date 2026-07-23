import DatabaseValue
import StorageKit

public struct RelationshipReferenceIdentityPage: Sendable {
    public let identities: [PersistableIdentity]
    public let continuation: Bytes?

    public init(
        identities: [PersistableIdentity],
        continuation: Bytes?
    ) {
        self.identities = identities
        self.continuation = continuation
    }
}
