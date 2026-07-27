import DatabaseTypes
import StorageKit

public struct RelationshipReferenceIdentityPage: Sendable {
    public let identities: [EntityReference]
    public let continuation: Bytes?

    public init(
        identities: [EntityReference],
        continuation: Bytes?
    ) {
        self.identities = identities
        self.continuation = continuation
    }
}
