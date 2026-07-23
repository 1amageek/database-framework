import DatabaseValue
import StorageKit

public struct RelationshipReferenceIdentityPage: Sendable {
    public let identities: [RecordIdentity]
    public let continuation: Bytes?

    public init(
        identities: [RecordIdentity],
        continuation: Bytes?
    ) {
        self.identities = identities
        self.continuation = continuation
    }
}
